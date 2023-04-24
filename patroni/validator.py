#!/usr/bin/env python3
"""Patroni configuration validation helpers.

This module contains facilities for validating configuration of Patroni processes.

:var schema: configuration schema of the daemon launched by `patroni` command.
"""
import os
import re
import shutil
import socket
import subprocess

from typing import Any, Dict, Union, Iterator, List, Optional as OptionalType

from .utils import split_host_port, data_directory_is_empty
from .dcs import dcs_modules
from .exceptions import ConfigParseError


def data_directory_empty(data_dir: str) -> bool:
    """Check if PostgreSQL data directory is empty.

    :param data_dir: path to the PostgreSQL data directory to be checked.
    :returns: ``True`` if the data directory is empty.
    """
    if os.path.isfile(os.path.join(data_dir, "global", "pg_control")):
        return False
    return data_directory_is_empty(data_dir)


def validate_connect_address(address: str) -> bool:
    """Check if options related to connection address were properly configured.

    :param address: address to be validated in the format
        ``host:ip``.
    :returns: ``True`` if the address is valid.
    :raises :class:`patroni.exceptions.ConfigParseError`:
        * If the address is not in the expected format; or
        * If the host is set to not allowed values (``127.0.0.1``, ``0.0.0.0``, ``*``, ``::1``, or ``localhost``).
    """
    try:
        host, _ = split_host_port(address, 1)
    except (AttributeError, TypeError, ValueError):
        raise ConfigParseError("contains a wrong value")
    if host in ["127.0.0.1", "0.0.0.0", "*", "::1", "localhost"]:
        raise ConfigParseError('must not contain "127.0.0.1", "0.0.0.0", "*", "::1", "localhost"')
    return True


def validate_host_port(host_port: str, listen: bool = False, multiple_hosts: bool = False) -> bool:
    """Check if host(s) and port are valid and available for usage.

    :param host_port: the host(s) and port to be validated. It can be in either of these formats
        * ``host:ip``, if *multiple_hosts* is ``False``; or
        * ``host_1,host_2,...,host_n:port``, if *multiple_hosts* is ``True``.

    :param listen: if the address is expected to be available for binding. ``False`` means it expects to connect to that
        address, and ``True`` that it expects to bind to that address.
    :param multiple_hosts: if *host_port* can contain multiple hosts.
    :returns: ``True`` if the host(s) and port are valid.
    :raises: :class:`patroni.exceptions.ConfigParserError`:
        * If the *host_port* is not in the expected format; or
        * If ``*`` was specified along with more hosts in *host_port*; or
        * If we are expecting to bind to an address that is already in use; or
        * If we are not able to connect to an address that we are expecting to do so; or
        * If :class:`socket.gaierror` is thrown by socket module when attempting to connect to the given address(es).
    """
    try:
        hosts, port = split_host_port(host_port, 1)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    else:
        if multiple_hosts:
            hosts = hosts.split(",")
        else:
            hosts = [hosts]
        if "*" in hosts:
            if len(hosts) != 1:
                raise ConfigParseError("expecting '*' alone")
            # If host is set to "*" get all hostnames and/or IP addresses that the host would be able to listen to
            hosts = [p[-1][0] for p in socket.getaddrinfo(None, port, 0, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)]
        for host in hosts:
            # Check if "socket.IF_INET" or "socket.IF_INET6" is being used and instantiate a socket with the identified
            # protocol
            proto = socket.getaddrinfo(host, "", 0, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
            s = socket.socket(proto[0][0], socket.SOCK_STREAM)
            try:
                if s.connect_ex((host, port)) == 0:
                    if listen:
                        raise ConfigParseError("Port {} is already in use.".format(port))
                elif not listen:
                    raise ConfigParseError("{} is not reachable".format(host_port))
            except socket.gaierror as e:
                raise ConfigParseError(e)
            finally:
                s.close()
    return True


def validate_host_port_list(value: List[str]) -> bool:
    """Validate a list of host(s) and port items.

    Call :func:`validate_host_port` with each item in *value*.

    :param value: list of host(s) and port items to be validated.
    :returns: ``True`` if all items are valid.
    """
    assert all([validate_host_port(v) for v in value]), "didn't pass the validation"
    return True


def comma_separated_host_port(string: str) -> bool:
    """Validate a list of host and port items.

    Call :func:`validate_host_port_list` with a list represented by the CSV *string*.

    :param string: comma-separated list of host and port items.
    :returns: ``True`` if all items in the CSV string are valid.
    """
    return validate_host_port_list([s.strip() for s in string.split(",")])


def validate_host_port_listen(host_port: str) -> bool:
    """Check if host and port are valid and available for binding.

    Call :func:`validate_host_port` with *listen* set to ``True``.

    :param host_port: the host and port to be validated. Must be in the format
        `host:ip`.

    :returns: ``True`` if the host and port are valid and available for binding.
    """
    return validate_host_port(host_port, listen=True)


def validate_host_port_listen_multiple_hosts(host_port: str) -> bool:
    """Check if host(s) and port are valid and available for binding.

    Call :func:`validate_host_port` with both *listen* and *multiple_hosts* set to ``True``.

    :param host_port: the host(s) and port to be validated. It can be in either of these formats
        * `host:ip`; or
        * `host_1,host_2,...,host_n:port`

    :returns: ``True`` if the host(s) and port are valid and available for binding.
    """
    return validate_host_port(host_port, listen=True, multiple_hosts=True)


def is_ipv4_address(ip: str) -> bool:
    """Check if *ip* is a valid IPv4 address.

    :param ip: the IP to be checked.
    :returns: ``True`` if the IP is an IPv4 address.
    :raises :class:`patroni.exceptions.ConfigParserError`: if *ip* is not a valid IPv4 address.
    """
    try:
        socket.inet_aton(ip)
    except Exception:
        raise ConfigParseError("Is not a valid ipv4 address")
    return True


def is_ipv6_address(ip: str) -> bool:
    """Check if *ip* is a valid IPv6 address.

    :param ip: the IP to be checked.
    :returns: ``True`` if the IP is an IPv6 address.
    :raises :class:`patroni.exceptions.ConfigParserError`: if *ip* is not a valid IPv6 address.
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except Exception:
        raise ConfigParseError("Is not a valid ipv6 address")
    return True


def get_major_version(bin_dir: OptionalType[str] = None) -> str:
    """Get the major version of PostgreSQL.

    It is based on the output of ``postgres --version``.

    :param bin_dir: path to PostgreSQL binaries directory. If ``None`` it will use the first ``postgres`` binary that
        is found by subprocess in the ``PATH``.
    :returns: the PostgreSQL major version.

    :Example:

        * Returns `9.6` for PostgreSQL 9.6.24
        * Returns `15` for PostgreSQL 15.2
    """
    if not bin_dir:
        binary = 'postgres'
    else:
        binary = os.path.join(bin_dir, 'postgres')
    version = subprocess.check_output([binary, '--version']).decode()
    version = re.match(r'^[^\s]+ [^\s]+ (\d+)(\.(\d+))?', version)
    assert version is not None
    return '.'.join([version.group(1), version.group(3)]) if int(version.group(1)) < 10 else version.group(1)


def validate_data_dir(data_dir: str) -> bool:
    """Validate the value of ``postgresql.data_dir`` configuration option.

    It requires that ``postgresql.data_dir`` is set and match one of following conditions:

        * Point to a path that does not exist yet; or
        * Point to an empty directory; or
        * Point to a non-empty directory that seems to contain a valid PostgreSQL data directory.

    :param data_dir: the value of ``postgresql.data_dir`` configuration option.
    :returns: ``True`` if the PostgreSQL data directory is valid.
    :raises :class:`patroni.exceptions.ConfigParserError`:
        * If no *data_dir* was given; or
        * If *data_dir* is a file and not a directory; or
        * If *data_dir* is a non-empty directory and:
            * ``PG_VERSION`` file is not available in the directory
            * ``pg_wal``/``pg_xlog`` is not available in the directory
            * ``PG_VERSION`` content does not match the major version reported by ``postgres --version``
    """
    if not data_dir:
        raise ConfigParseError("is an empty string")
    elif os.path.exists(data_dir) and not os.path.isdir(data_dir):
        raise ConfigParseError("is not a directory")
    elif not data_directory_empty(data_dir):
        if not os.path.exists(os.path.join(data_dir, "PG_VERSION")):
            raise ConfigParseError("doesn't look like a valid data directory")
        else:
            with open(os.path.join(data_dir, "PG_VERSION"), "r") as version:
                pgversion = version.read().strip()
            waldir = ("pg_wal" if float(pgversion) >= 10 else "pg_xlog")
            if not os.path.isdir(os.path.join(data_dir, waldir)):
                raise ConfigParseError("data dir for the cluster is not empty, but doesn't contain"
                                       " \"{}\" directory".format(waldir))
            bin_dir = schema.data.get("postgresql", {}).get("bin_dir", None)
            major_version = get_major_version(bin_dir)
            if pgversion != major_version:
                raise ConfigParseError("data_dir directory postgresql version ({}) doesn't match with "
                                       "'postgres --version' output ({})".format(pgversion, major_version))
    return True


class Result(object):
    """Represent the result of a given validation that was performed.

    :ivar status: If the validation succeeded.
    :ivar path: YAML tree path of the configuration option.
    :ivar data: value of the configuration option.
    :ivar level: error level, in case of error.
    :ivar error: error message if the validation failed, otherwise ``None``.
    """

    def __init__(self, status: bool, error: OptionalType[str] = "didn't pass validation", level: int = 0,
                 path: str = "", data: Any = "") -> None:
        """Create a :class:`Result` object based on the given arguments.

        .. note::

            ``error`` attribute is only set if ``status`` is failed.

        :param status: if the validation succeeded.
        :param error: error message related to the validation that was performed, if the validation failed.
        :param level: error level, in case of error.
        :param path: YAML tree path of the configuration option.
        :param data: value of the configuration option.
        """
        self.status = status
        self.path = path
        self.data = data
        self.level = level
        self._error = error
        if not self.status:
            self.error = error
        else:
            self.error = None

    def __repr__(self) -> str:
        """Show configuration path and value. If the validation failed, also show the error message."""
        return str(self.path) + (" " + str(self.data) + " " + str(self._error) if self.error else "")


class Case(object):
    """Map how a list of available configuration options should be validated.

    .. note::

        It should be used together with an :class:`Or` object. The :class:`Or` object will define the list of possible
        configuration options in a given context, and the :class:`Case` object will dictate how to validate each of
        them, if they are set.
    """

    def __init__(self, schema: Dict[str, Any]) -> None:
        """Create a :class:`Case` object.

        :param schema: the schema for validating a set of attributes that may be available in the configuration.
            Each key is the configuration that is available in a given scope and that should be validated, and the
            related value is the validation function or expected type.

        :Example:

            Case({
                "host": validate_host_port,
                "url": str,
            })

        That will check that ``host`` configuration, if given, is valid based on ``validate_host_port`` function, and
        will also check that ``url`` configuration, if given, is a ``str`` instance.
        """
        self._schema = schema


class Or(object):
    """Represent the list of options that are available.

    It can represent either a list of configuration options that are available in a given scope, or a list of
    validation functions and/or expected types for a given configuration option.
    """

    def __init__(self, *args: Any) -> None:
        """Create an :class:`Or` object.

        :param `*args`: any arguments that the caller wants to be stored in this :class:`Or` object.

        :Example:

            Or("host", "hosts"): Case({
                "host": validate_host_port,
                "hosts": Or(comma_separated_host_port, [validate_host_port]),
            })

        The outer :class:`Or` is used to define that ``host`` and ``hosts`` are possible options in this scope.
        The inner :class`Or` in the ``hosts`` key value is used to define that ``hosts`` option is valid if either of
            the functions ``comma_separated_host_port`` or ``validate_host_port`` succeed to validate it.
        """
        self.args = args


class Optional(object):
    """Mark a configuration option as optional.

    :ivar name: name of the configuration option.
    """

    def __init__(self, name: str) -> None:
        """Create an :class:`Optional` object.

        :param name: name of the configuration option.
        """
        self.name = name


class Directory(object):
    """Check if a directory contains the expected files.

    The attributes of objects of this class are used by their :func:`validate` method.

    :param contains: list of paths that should exist relative to a given directory.
    :param contains_executable: list of executable files that should exist directly under a given directory.
    """

    def __init__(self, contains: OptionalType[List[str]] = None,
                 contains_executable: OptionalType[List[str]] = None) -> None:
        """Create a :class:`Directory` object.

        :param contains: list of paths that should exist relative to a given directory.
        :param contains_executable: list of executable files that should exist directly under a given directory.
        """
        self.contains = contains
        self.contains_executable = contains_executable

    def validate(self, name: str) -> Iterator[Result]:
        """Check if the expected paths and executables can be found under *name* directory.

        :param name: path to the base directory against which paths and executables will be validated.
        :rtype: Iterator[:class:`Result`] objects with the error message related to the failure, if any check fails.
        """
        if not name:
            yield Result(False, "is an empty string")
        elif not os.path.exists(name):
            yield Result(False, "Directory '{}' does not exist.".format(name))
        elif not os.path.isdir(name):
            yield Result(False, "'{}' is not a directory.".format(name))
        else:
            if self.contains:
                for path in self.contains:
                    if not os.path.exists(os.path.join(name, path)):
                        yield Result(False, "'{}' does not contain '{}'".format(name, path))
            if self.contains_executable:
                for program in self.contains_executable:
                    if not shutil.which(program, path=name):
                        yield Result(False, "'{}' does not contain '{}'".format(name, program))


class Schema(object):
    """Define a configuration schema.

    It contains all the configuration options that are available in each scope, including the validation(s) that should
    be performed against each one of them. The validations will be performed whenever the :class:`Schema` object is
    called, or its :func:`validate` method is called.

    :ivar validator: validator of the configuration schema. Can be any of these
        * :class:`str`: defines that a string value is required; or
        * :class:`type`: any subclass of `type`, defines that a value of the given type is required; or
        * `callable`: any callable object, defines that validation will follow the code defined in the callable
            object. If the callable object contains an ``expected_type`` attribute, then it will check if the
            configuration value is of the expected type before calling the code of the callable object; or
        * :class:`list`: list representing one or more values in the configuration; or
        * :class:`dict`: dictionary representing the YAML configuration tree.
    """

    def __init__(self, validator: Any) -> None:
        """Create a :class:`Schema` object.

        .. note::

            This class is expected to be initially instantiated with a :class:`dict` based *validator* argument. The
            idea is that dict represents the full YAML tree of configuration options. The :func:`validate` method will
            then walk recursively through the configuration tree, creating new instances of :class:`Schema` with the
            new "base path", to validate the structure and the leaf values of the tree. The recursion stops on leaf
            nodes, when it performs checks of the actual setting values.

        :param validator: validator of the configuration schema. Can be any of these:
            * :class:`str`: defines that a string value is required; or
            * :class:`type`: any subclass of :class:`type`, defines that a value of the given type is required; or
            * `callable`: Any callable object, defines that validation will follow the code defined in the callable
                object. If the callable object contains an ``expected_type`` attribute, then it will check if the
                configuration value is of the expected type before calling the code of the callable object; or
            * :class:`list`: list representing it expects to contain one or more values in the configuration; or
            * :class:`dict`: dictionary representing the YAML configuration tree.

            The first 3 items in the above list are here referenced as "base validators", which cause the recursion
            to stop.

            If *validator* is a :class:`dict`, then you should follow these rules:
            * For the keys it can be either:
                * A :class:`str` instance. It will be the name of the configuration option; or
                * An :class:`Optional` instance. The ``name`` attribute of that object will be the name of the
                    configuration option, and that class makes this configuration option as optional to the
                    user, allowing it to not be specified in the YAML; or
                * An :class:`Or` instance. The ``args`` attribute of that object will contain a tuple of
                    configuration option names. At least one of them should be specified by the user in the YAML;
            * For the values it can be either:
                * A new :class:`dict` instance. It will represent a new level in the YAML configuration tree; or
                * A :class:`Case` instance. This is required if the key of this value is an :class:`Or` instance,
                    and the :class:`Case` instance is used to map each of the ``args`` in :class:`Or` to their
                    corresponding base validator in :class:`Case`; or
                * An :class:`Or` instance with one or more base validators; or
                * A :class:`list` instance with a single item which is the base validator; or
                * A base validator.

        :Example:

            Schema({
                "application_name": str,
                "bind": {
                    "host": validate_host,
                    "port": int,
                },
                "aliases": [str],
                Optional("data_directory"): "/var/lib/myapp",
                Or("log_to_file", "log_to_db"): Case({
                    "log_to_file": bool,
                    "log_to_db": bool,
                }),
                "version": Or(int, float),
            })

        This sample schema defines that your YAML configuration follows these rules:
        * It must contain an ``application_name`` entry which value should be a :class:`str` instance;
        * It must contain a ``bind.host`` entry which value should be valid as per function ``validate_host``;
        * It must contain a ``bind.port`` entry which value should be an :class:`int` instance;
        * It must contain a ``aliases`` entry which value should be a :class:`list` of :class:`str` instances;
        * It may optionally contain a ``data_directory`` entry. If not given it will assume the value
            ``/var/lib/myapp``;
        * It must contain at least one of ``log_to_file`` or ``log_to_db``, with a value which should be a
            :class:`bool` instance;
        * It must contain a ``version`` entry which value should be either an :class:`int` or a :class:`float`
            instance.
        """
        self.validator = validator

    def __call__(self, data: Any) -> List[str]:
        """Perform validation of data using the rules defined in this schema.

        :param data: configuration to be validated against ``validator``.
        :returns: list of errors identified while validating the *data*, if any.
        """
        errors: List[str] = []
        for i in self.validate(data):
            if not i.status:
                errors.append(str(i))
        return errors

    def validate(self, data: Any) -> Iterator[Result]:
        """Perform all validations from the schema against the given configuration.

        It first checks that *data* argument type is compliant with the type of ``validator`` attribute.

        Additionally:
        * If ``validator`` attribute is a callable object, calls it to validate *data* argument. Before doing so, if
            `validator` contains an ``expected_type`` attribute, check if *data* argument is compliant with that
            expected type.
        * If ``validator`` attribute is an iterable object (:class:`dict`, :class:`list`, :class:`Directory` or
            :class:`Or`), then it iterates over it to validate each of the corresponding entries in *data* argument.

        :param data: configuration to be validated against ``validator``.
        :rtype: Iterator[:class:`Result`] objects with the error message related to the failure, if any check fails.
        """
        self.data = data

        # New `Schema` objects can be created while validating a given `Schema`, depending on its structure. The first
        # 3 IF statements deal with the situation where we already reached a leaf node in the `Schema` structure, then
        # we are dealing with an actual value validation. The remaining logic in this method is used to iterate through
        # iterable objects in the structure, until we eventually reach a leaf node to validate its value.
        if isinstance(self.validator, str):
            yield Result(isinstance(self.data, str), "is not a string", level=1, data=self.data)
        elif issubclass(type(self.validator), type):
            validator = self.validator
            if self.validator == str:
                validator = str
            yield Result(isinstance(self.data, validator),
                         "is not {}".format(_get_type_name(self.validator)), level=1, data=self.data)
        elif callable(self.validator):
            if hasattr(self.validator, "expected_type"):
                if not isinstance(data, self.validator.expected_type):
                    yield Result(False, "is not {}"
                                 .format(_get_type_name(self.validator.expected_type)), level=1, data=self.data)
                    return
            try:
                self.validator(data)
                yield Result(True, data=self.data)
            except Exception as e:
                yield Result(False, "didn't pass validation: {}".format(e), data=self.data)
        elif isinstance(self.validator, dict):
            if not isinstance(self.data, dict):
                yield Result(isinstance(self.data, dict), "is not a dictionary", level=1, data=self.data)
        elif isinstance(self.validator, list):
            if not isinstance(self.data, list):
                yield Result(isinstance(self.data, list), "is not a list", level=1, data=self.data)
                return
        yield from self.iter()

    def iter(self) -> Iterator[Result]:
        """Iterate over ``validator``, if it is an iterable object, to validate the corresponding entries in ``data``.

        Only :class:`dict`, :class:`list`, :class:`Directory` and :class:`Or` objects are considered iterable objects.

        :rtype: Iterator[:class:`Result`] objects with the error message related to the failure, if any check fails.
        """
        if isinstance(self.validator, dict):
            if not isinstance(self.data, dict):
                yield Result(False, "is not a dictionary.", level=1)
            else:
                yield from self.iter_dict()
        elif isinstance(self.validator, list):
            if len(self.data) == 0:
                yield Result(False, "is an empty list", data=self.data)
            if self.validator:
                for key, value in enumerate(self.data):
                    # Although the value in the configuration (`data`) is expected to contain 1 or more entries, only
                    # the first validator defined in `validator` property list will be used. It is only defined as a
                    # `list` in `validator` so this logic can understand that the value in `data` attribute should be a
                    # `list`. For example: "pg_hba": [str] in `validator` attribute defines that "pg_hba" in `data`
                    # attribute should contain a list with one or more `str` entries.
                    for v in Schema(self.validator[0]).validate(value):
                        yield Result(v.status, v.error,
                                     path=(str(key) + ("." + v.path if v.path else "")), level=v.level, data=value)
        elif isinstance(self.validator, Directory):
            yield from self.validator.validate(self.data)
        elif isinstance(self.validator, Or):
            yield from self.iter_or()

    def iter_dict(self) -> Iterator[Result]:
        """Iterate over a :class:`dict` based ``validator`` to validate the corresponding entries in ``data``.

        :rtype: Iterator[:class:`Result`] objects with the error message related to the failure, if any check fails.
        """
        # One key in `validator` attribute (`key` variable) can be mapped to one or more keys in `data` attribute (`d`
        # variable), depending on the `key` type.
        for key in self.validator.keys():
            for d in self._data_key(key):
                if d not in self.data and not isinstance(key, Optional):
                    yield Result(False, "is not defined.", path=d)
                elif d not in self.data and isinstance(key, Optional):
                    continue
                else:
                    validator = self.validator[key]
                    if isinstance(key, Or) and isinstance(self.validator[key], Case):
                        validator = self.validator[key]._schema[d]
                    # In this loop we may be calling a new `Schema` either over an intermediate node in the tree, or
                    # over a leaf node. In the latter case the recursive calls in the given path will finish.
                    for v in Schema(validator).validate(self.data[d]):
                        yield Result(v.status, v.error,
                                     path=(d + ("." + v.path if v.path else "")), level=v.level, data=v.data)

    def iter_or(self) -> Iterator[Result]:
        """Perform all validations defined in an `Or` object for a given configuration option.

        This method can be only called against leaf nodes in the configuration tree. :class:`Or` objects defined in the
        ``validator`` keys will be handled by :func:`iter_dict` method.

        :rtype: Iterator[:class:`Result`] objects with the error message related to the failure, if any check fails.
        """
        results: List[Result] = []
        for a in self.validator.args:
            r: List[Result] = []
            # Each of the `Or` validators can throw 0 to many `Result` instances.
            for v in Schema(a).validate(self.data):
                r.append(v)
            if any([x.status for x in r]) and not all([x.status for x in r]):
                results += [x for x in r if not x.status]
            else:
                results += r
        # None of the `Or` validators succeeded to validate `data`, so we report the issues back.
        if not any([x.status for x in results]):
            max_level = 3
            for v in sorted(results, key=lambda x: x.level):
                if v.level > max_level:
                    break
                max_level = v.level
                yield Result(v.status, v.error, path=v.path, level=v.level, data=v.data)

    def _data_key(self, key: Union[str, Optional, Or]) -> Iterator[str]:
        """Map a key from the ``validator`` dictionary to the corresponding key(s) in the ``data`` dictionary.

        :param key: key from the ``validator`` attribute.

        :rtype: Iterator[str], keys that should be used to access corresponding value in the ``data`` attribute.
        """
        # If the key was defined as a `str` object in `validator` attribute, then it is already the final key to access
        # the `data` dictionary.
        if isinstance(self.data, dict) and isinstance(key, str):
            yield key
        # If the key was defined as an `Optional` object in `validator` attribute, then its name is the key to access
        # the `data` dictionary.
        elif isinstance(key, Optional):
            yield key.name
        # If the key was defined as an `Or` object in `validator` attribute, then each of its values are the keys to
        # access the `data` dictionary.
        elif isinstance(key, Or):
            # At least one of the `Or` entries should be available in the `data` dictionary. If we find at least one of
            # them in `data`, then we return all found entries so the caller method can validate them all.
            if any([i in self.data for i in key.args]):
                for i in key.args:
                    if i in self.data:
                        yield i
            # If none of the `Or` entries is available in the `data` dictionary, then we return all entries so the
            # caller method will issue errors that they are all absent.
            else:
                for i in key.args:
                    yield i


def _get_type_name(python_type: Any) -> str:
    """Get a user friendly name for a given Python type.

    :param python_type: Python type which user friendly name should be taken.

    Returns:
        User friendly name of the given Python type.
    """
    types: Dict[Any, str] = {str: 'a string', int: 'an integer', float: 'a number',
                             bool: 'a boolean', list: 'an array', dict: 'a dictionary'}
    return types.get(python_type, getattr(python_type, __name__, "unknown type"))


def assert_(condition: bool, message: str = "Wrong value") -> None:
    """Assert that a given condition is ``True``.

    If the assertion fails, then throw a message.

    :param condition: result of a condition to be asserted.
    :param message: message to be thrown if the condition is ``False``.
    """
    assert condition, message


class IntValidator(object):
    expected_type = int

    def __init__(self, min: OptionalType[int] = None, max: OptionalType[int] = None):
        self.min = min
        self.max = max

    def __call__(self, value: int) -> None:
        assert_((self.min is None or value >= self.min) and (self.max is None or value <= self.max))


def validate_watchdog_mode(value: Any):
    assert_(isinstance(value, (str, bool)), "expected type is not a string")
    assert_(value in (False, "off", "automatic", "required"))


userattributes = {"username": "", Optional("password"): ""}
available_dcs = [m.split(".")[-1] for m in dcs_modules()]
setattr(validate_host_port_list, 'expected_type', list)
setattr(comma_separated_host_port, 'expected_type', str)
setattr(validate_connect_address, 'expected_type', str)
setattr(validate_host_port_listen, 'expected_type', str)
setattr(validate_host_port_listen_multiple_hosts, 'expected_type', str)
setattr(validate_data_dir, 'expected_type', str)
validate_etcd = {
    Or("host", "hosts", "srv", "srv_suffix", "url", "proxy"): Case({
        "host": validate_host_port,
        "hosts": Or(comma_separated_host_port, [validate_host_port]),
        "srv": str,
        "srv_suffix": str,
        "url": str,
        "proxy": str})
}

schema = Schema({
    "name": str,
    "scope": str,
    "restapi": {
        "listen": validate_host_port_listen,
        "connect_address": validate_connect_address,
        Optional("request_queue_size"): IntValidator(0, 4096)
    },
    Optional("bootstrap"): {
        "dcs": {
            Optional("ttl"): int,
            Optional("loop_wait"): int,
            Optional("retry_timeout"): int,
            Optional("maximum_lag_on_failover"): int
        },
        "pg_hba": [str],
        "initdb": [Or(str, dict)]
    },
    Or(*available_dcs): Case({
        "consul": {
            Or("host", "url"): Case({
                "host": validate_host_port,
                "url": str})
        },
        "etcd": validate_etcd,
        "etcd3": validate_etcd,
        "exhibitor": {
            "hosts": [str],
            "port": IntValidator(None, 65535),
            Optional("pool_interval"): int
        },
        "raft": {
            "self_addr": validate_connect_address,
            Optional("bind_addr"): validate_host_port_listen,
            "partner_addrs": validate_host_port_list,
            Optional("data_dir"): str,
            Optional("password"): str
        },
        "zookeeper": {
            "hosts": Or(comma_separated_host_port, [validate_host_port]),
        },
        "kubernetes": {
            "labels": {},
            Optional("namespace"): str,
            Optional("scope_label"): str,
            Optional("role_label"): str,
            Optional("use_endpoints"): bool,
            Optional("pod_ip"): Or(is_ipv4_address, is_ipv6_address),
            Optional("ports"): [{"name": str, "port": int}],
            Optional("retriable_http_codes"): Or(int, [int]),
        },
    }),
    Optional("citus"): {
        "database": str,
        "group": int
    },
    "postgresql": {
        "listen": validate_host_port_listen_multiple_hosts,
        "connect_address": validate_connect_address,
        Optional("proxy_address"): validate_connect_address,
        "authentication": {
            "replication": userattributes,
            "superuser": userattributes,
            "rewind": userattributes
        },
        "data_dir": validate_data_dir,
        Optional("bin_dir"): Directory(contains_executable=["pg_ctl", "initdb", "pg_controldata", "pg_basebackup",
                                                            "postgres", "pg_isready"]),
        Optional("parameters"): {
            Optional("unix_socket_directories"): str
        },
        Optional("pg_hba"): [str],
        Optional("pg_ident"): [str],
        Optional("pg_ctl_timeout"): int,
        Optional("use_pg_rewind"): bool
    },
    Optional("watchdog"): {
        Optional("mode"): validate_watchdog_mode,
        Optional("device"): str
    },
    Optional("tags"): {
        Optional("nofailover"): bool,
        Optional("clonefrom"): bool,
        Optional("noloadbalance"): bool,
        Optional("replicatefrom"): str,
        Optional("nosync"): bool
    }
})
