#!/usr/bin/env python3
"""Patroni configuration validation helpers.

This module contains facilities for validating configuration of Patroni processes.

:var schema: configuration schema of the daemon launched by ``patroni`` command.
"""
import os
import shutil
import socket

from typing import Any, cast, Dict, Iterator, List, Optional as OptionalType, Tuple, TYPE_CHECKING, Union

from .collections import CaseInsensitiveSet, EMPTY_DICT
from .dcs import dcs_modules
from .exceptions import ConfigParseError, PatroniAssertionError
from .log import type_logformat
from .utils import data_directory_is_empty, get_major_version, parse_int, split_host_port

# Additional parameters to fine-tune validation process
_validation_params: Dict[str, Any] = {}


def populate_validate_params(ignore_listen_port: bool = False) -> None:
    """Populate parameters used to fine-tune the validation of the Patroni config.

    :param ignore_listen_port: ignore the bind failures for the ports marked as `listen`.
    """
    _validation_params['ignore_listen_port'] = ignore_listen_port


def validate_log_field(field: Any) -> bool:
    """Checks if log field is valid.

    :param field: A log field to be validated.

    :returns: ``True`` if the field is either a string or a dictionary with exactly one key
              that has string value, ``False`` otherwise.
    """
    if isinstance(field, str):
        return True
    elif isinstance(field, dict):
        field = cast(Dict[str, Any], field)
        return len(field) == 1 and isinstance(next(iter(field.values())), str)
    return False


def validate_log_format(logformat: type_logformat) -> bool:
    """Checks if log format is valid.

    :param logformat: A log format to be validated.

    :returns: ``True`` if the log format is either a string or a list of valid log fields.

    :raises:
        :exc:`~patroni.exceptions.ConfigParseError`:
            * If the logformat is not a string or a list; or
            * If the logformat is an empty list; or
            * If the log format is a list and it with values that don't pass validation using
              :func:`validate_log_field`.
    """
    if isinstance(logformat, str):
        return True
    elif isinstance(logformat, list):
        logformat = cast(List[Any], logformat)
        if len(logformat) == 0:
            raise ConfigParseError('should contain at least one item')
        if not all(map(validate_log_field, logformat)):
            raise ConfigParseError('each item should be a string or a dictionary with string values')

        return True
    else:
        raise ConfigParseError('Should be a string or a list')


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

    :param address: address to be validated in the format ``host:ip``.

    :returns: ``True`` if the address is valid.

    :raises:
        :class:`~patroni.exceptions.ConfigParseError`:
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

    :param host_port: the host(s) and port to be validated. It can be in either of these formats:

        * ``host:ip``, if *multiple_hosts* is ``False``; or
        * ``host_1,host_2,...,host_n:port``, if *multiple_hosts* is ``True``.

    :param listen: if the address is expected to be available for binding. ``False`` means it expects to connect to that
        address, and ``True`` that it expects to bind to that address.
    :param multiple_hosts: if *host_port* can contain multiple hosts.

    :returns: ``True`` if the host(s) and port are valid.

    :raises:
        :class:`~patroni.exceptions.ConfigParseError`:
            * If the *host_port* is not in the expected format; or
            * If ``*`` was specified along with more hosts in *host_port*; or
            * If we are expecting to bind to an address that is already in use; or
            * If we are not able to connect to an address that we are expecting to do so; or
            * If :class:`~socket.gaierror` is thrown by socket module when attempting to connect to the given
              address(es).
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

        # If host is set to "*" get all hostnames and/or IP addresses that the host would be able to listen to
        if "*" in hosts:
            if len(hosts) != 1:
                raise ConfigParseError("expecting '*' alone")
            # Filter out unexpected results when python is compiled with --disable-ipv6 and running on IPv6 system.
            hosts = [a[4][0] for a in socket.getaddrinfo(None, port, 0, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
                     if isinstance(a[4][0], str)]
        for host in hosts:
            # Check if "socket.IF_INET" or "socket.IF_INET6" is being used and instantiate a socket with the identified
            # protocol
            proto = socket.getaddrinfo(host, None, 0, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
            s = socket.socket(proto[0][0], socket.SOCK_STREAM)
            try:
                if s.connect_ex((host, port)) == 0:
                    # Do not raise an exception if ignore_listen_port is set to True.
                    if listen and not _validation_params.get('ignore_listen_port', False):
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

    .. note::
        :func:`validate_host_port` will raise an exception if validation failed.
    """

    return all(validate_host_port(v) for v in value)


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
        ``host:ip``.

    :returns: ``True`` if the host and port are valid and available for binding.
    """
    return validate_host_port(host_port, listen=True)


def validate_host_port_listen_multiple_hosts(host_port: str) -> bool:
    """Check if host(s) and port are valid and available for binding.

    Call :func:`validate_host_port` with both *listen* and *multiple_hosts* set to ``True``.

    :param host_port: the host(s) and port to be validated. It can be in either of these formats

        * ``host:ip``; or
        * ``host_1,host_2,...,host_n:port``

    :returns: ``True`` if the host(s) and port are valid and available for binding.
    """
    return validate_host_port(host_port, listen=True, multiple_hosts=True)


def is_ipv4_address(ip: str) -> bool:
    """Check if *ip* is a valid IPv4 address.

    :param ip: the IP to be checked.

    :returns: ``True`` if the IP is an IPv4 address.

    :raises:
        :class:`~patroni.exceptions.ConfigParseError`: if *ip* is not a valid IPv4 address.
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

    :raises:
        :class:`~patroni.exceptions.ConfigParseError`: if *ip* is not a valid IPv6 address.
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except Exception:
        raise ConfigParseError("Is not a valid ipv6 address")
    return True


def get_bin_name(bin_name: str) -> str:
    """Get the value of ``postgresql.bin_name[*bin_name*]`` configuration option.

    :param bin_name: a key to be retrieved from ``postgresql.bin_name`` configuration.

    :returns: value of ``postgresql.bin_name[*bin_name*]``, if present, otherwise *bin_name*.
    """
    data = cast(Dict[Any, Any], schema.data)
    return (data.get('postgresql', {}).get('bin_name', {}) or EMPTY_DICT).get(bin_name, bin_name)


def validate_data_dir(data_dir: str) -> bool:
    """Validate the value of ``postgresql.data_dir`` configuration option.

    It requires that ``postgresql.data_dir`` is set and match one of following conditions:

        * Point to a path that does not exist yet; or
        * Point to an empty directory; or
        * Point to a non-empty directory that seems to contain a valid PostgreSQL data directory.

    :param data_dir: the value of ``postgresql.data_dir`` configuration option.

    :returns: ``True`` if the PostgreSQL data directory is valid.

    :raises:
        :class:`~patroni.exceptions.ConfigParseError`:
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
            data = cast(Dict[Any, Any], schema.data)
            bin_dir = data.get("postgresql", {}).get("bin_dir", None)
            major_version = get_major_version(bin_dir, get_bin_name('postgres'))
            if pgversion != major_version:
                raise ConfigParseError("data_dir directory postgresql version ({}) doesn't match with "
                                       "'postgres --version' output ({})".format(pgversion, major_version))
    return True


def validate_binary_name(bin_name: str) -> bool:
    """Validate the value of ``postgresql.binary_name[*bin_name*]`` configuration option.

    If ``postgresql.bin_dir`` is set and the value of the *bin_name* meets these conditions:

        * The path join of ``postgresql.bin_dir`` plus the *bin_name* value exists; and
        * The path join as above is executable

    If ``postgresql.bin_dir`` is not set, then validate that the value of *bin_name* meets this
    condition:

        * Is found in the system PATH using ``which``

    :param bin_name: the value of the ``postgresql.bin_name[*bin_name*]``

    :returns: ``True`` if the conditions are true

    :raises:
        :class:`~patroni.exceptions.ConfigParseError` if:
            * *bin_name* is not set; or
            * the path join of the ``postgresql.bin_dir`` plus *bin_name* does not exist; or
            * the path join as above is not executable; or
            * the *bin_name* cannot be found in the system PATH

    """
    if not bin_name:
        raise ConfigParseError("is an empty string")
    data = cast(Dict[Any, Any], schema.data)
    bin_dir = data.get('postgresql', {}).get('bin_dir', None)
    if not shutil.which(bin_name, path=bin_dir):
        raise ConfigParseError(f"does not contain '{bin_name}' in '{bin_dir or '$PATH'}'")
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

            ``error`` attribute is only set if *status* is failed.

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
                       Each key is the configuration that is available in a given scope and that should be validated,
                       and the related value is the validation function or expected type.

        :Example:

            .. code-block:: python

                Case({
                    "host": validate_host_port,
                    "url": str,
                })

        That will check that ``host`` configuration, if given, is valid based on :func:`validate_host_port`, and will
        also check that ``url`` configuration, if given, is a ``str`` instance.
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

            .. code-block:: python

                Or("host", "hosts"): Case({
                    "host": validate_host_port,
                    "hosts": Or(comma_separated_host_port, [validate_host_port]),
                })

            The outer :class:`Or` is used to define that ``host`` and ``hosts`` are possible options in this scope.
            The inner :class`Or` in the ``hosts`` key value is used to define that ``hosts`` option is valid if either
            of :func:`comma_separated_host_port` or :func:`validate_host_port` succeed to validate it.
        """
        self.args = args


class AtMostOne(object):
    """Mark that at most one option from a :class:`Case` can be supplied.

    Represents a list of possible configuration options in a given scope, where at most one can actually
    be provided.

    .. note::

        It should be used together with a :class:`Case` object.
    """

    def __init__(self, *args: str) -> None:
        """Create a :class`AtMostOne` object.

        :param `*args`: any arguments that the caller wants to be stored in this :class:`Or` object.

        :Example:

            .. code-block:: python

                AtMostOne("nofailover", "failover_priority"): Case({
                    "nofailover": bool,
                    "failover_priority": IntValidator(min=0, raise_assert=True),
                })

        The :class`AtMostOne` object is used to define that at most one of ``nofailover`` and
        ``failover_priority`` can be provided.
        """
        self.args = args


class Optional(object):
    """Mark a configuration option as optional.

    :ivar name: name of the configuration option.
    :ivar default: value to set if the configuration option is not explicitly provided
    """

    def __init__(self, name: str, default: OptionalType[Any] = None) -> None:
        """Create an :class:`Optional` object.

        :param name: name of the configuration option.
        :param default: value to set if the configuration option is not explicitly provided
        """
        self.name = name
        self.default = default


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

    def _check_executables(self, path: OptionalType[str] = None) -> Iterator[Result]:
        """Check that all executables from contains_executable list exist within the given directory or within ``PATH``.

        :param path: optional path to the base directory against which executables will be validated.
                     If not provided, check within ``PATH``.

        :yields: objects with the error message containing the name of the executable, if any check fails.
        """
        for program in self.contains_executable or []:
            if not shutil.which(program, path=path):
                yield Result(False, f"does not contain '{program}' in '{(path or '$PATH')}'")

    def validate(self, name: str) -> Iterator[Result]:
        """Check if the expected paths and executables can be found under *name* directory.

        :param name: path to the base directory against which paths and executables will be validated.
                     Check against ``PATH`` if name is not provided.

        :yields: objects with the error message related to the failure, if any check fails.
        """
        if not name:
            yield from self._check_executables()
        elif not os.path.exists(name):
            yield Result(False, "Directory '{}' does not exist.".format(name))
        elif not os.path.isdir(name):
            yield Result(False, "'{}' is not a directory.".format(name))
        else:
            if self.contains:
                for path in self.contains:
                    if not os.path.exists(os.path.join(name, path)):
                        yield Result(False, "'{}' does not contain '{}'".format(name, path))
            yield from self._check_executables(path=name)


class BinDirectory(Directory):
    """Check if a Postgres binary directory contains the expected files.

    It is a subclass of :class:`Directory` with an extended capability: translating ``BINARIES`` according to configured
    ``postgresql.bin_name``, if any.

    :cvar BINARIES: list of executable files that should exist directly under a given Postgres binary directory.
    """

    # ``pg_rewind`` is not in the list because its usage by Patroni is optional. Also, it is not available by default on
    # Postgres 9.3 and 9.4, versions which Patroni supports.
    BINARIES = ["pg_ctl", "initdb", "pg_controldata", "pg_basebackup", "postgres", "pg_isready"]

    def validate(self, name: str) -> Iterator[Result]:
        """Check if the expected executables can be found under *name* binary directory.

        :param name: path to the base directory against which executables will be validated. Check against PATH if
            *name* is not provided.

        :yields: objects with the error message related to the failure, if any check fails.
        """
        self.contains_executable: List[str] = [get_bin_name(binary) for binary in self.BINARIES]
        yield from super().validate(name)


class Schema(object):
    """Define a configuration schema.

    It contains all the configuration options that are available in each scope, including the validation(s) that should
    be performed against each one of them. The validations will be performed whenever the :class:`Schema` object is
    called, or its :func:`validate` method is called.

    :ivar validator: validator of the configuration schema. Can be any of these:

        * :class:`str`: defines that a string value is required; or
        * :class:`type`: any subclass of :class:`type`, defines that a value of the given type is required; or
        * ``callable``: any callable object, defines that validation will follow the code defined in the callable
          object. If the callable object contains an ``expected_type`` attribute, then it will check if the
          configuration value is of the expected type before calling the code of the callable object; or
        * :class:`list`: list representing one or more values in the configuration; or
        * :class:`dict`: dictionary representing the YAML configuration tree.
    """

    def __init__(self, validator: Union[Dict[Any, Any], List[Any], Any]) -> None:
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
            * ``callable``: Any callable object, defines that validation will follow the code defined in the callable
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

            .. code-block:: python

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
                * It may optionally contain a ``data_directory`` entry, with a value which should be a string;
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

        :yields: objects with the error message related to the failure, if any check fails.
        """
        self.data = data

        # New `Schema` objects can be created while validating a given `Schema`, depending on its structure. The first
        # 3 IF statements deal with the situation where we already reached a leaf node in the `Schema` structure, then
        # we are dealing with an actual value validation. The remaining logic in this method is used to iterate through
        # iterable objects in the structure, until we eventually reach a leaf node to validate its value.
        if isinstance(self.validator, str):
            yield Result(isinstance(self.data, str), "is not a string", level=1, data=self.data)
        elif isinstance(self.validator, type):
            yield Result(isinstance(self.data, self.validator),
                         "is not {}".format(_get_type_name(self.validator)), level=1, data=self.data)
        elif callable(self.validator):
            if hasattr(self.validator, "expected_type"):
                expected_type = getattr(self.validator, 'expected_type')
                if not isinstance(data, expected_type):
                    yield Result(False, "is not {}".format(_get_type_name(expected_type)), level=1, data=self.data)
                    return
            try:
                self.validator(data)
                yield Result(True, data=self.data)
            except Exception as e:
                yield Result(False, "didn't pass validation: {}".format(e), data=self.data)
        elif isinstance(self.validator, dict):
            if not isinstance(self.data, dict):
                yield Result(isinstance(self.data, dict), "is not a dictionary", level=1, data=self.data)
            else:
                yield from self.iter_dict()
        elif isinstance(self.validator, list):
            if not isinstance(self.data, list):
                yield Result(isinstance(self.data, list), "is not a list", level=1, data=self.data)
            else:
                yield from self.iter_list()
        elif isinstance(self.validator, Or):
            yield from self.iter_or()
        elif isinstance(self.validator, Directory) and isinstance(self.data, str):
            yield from self.validator.validate(self.data)

    def iter_list(self) -> Iterator[Result]:
        """Iterate over a ``data`` object and perform validations using the first element of the ``validator``.

        :yields: objects with the error message related to the failure, if any check fails.
        """
        data = cast(List[Any], self.data)
        if len(data) == 0:
            yield Result(False, "is an empty list", data=data)

        validators = cast(List[Any], self.validator)
        if len(validators):
            for key, value in enumerate(data):
                # Although the value in the configuration (`data`) is expected to contain 1 or more entries, only
                # the first validator defined in `validator` property list will be used. It is only defined as a
                # `list` in `validator` so this logic can understand that the value in `data` attribute should be a
                # `list`. For example: "pg_hba": [str] in `validator` attribute defines that "pg_hba" in `data`
                # attribute should contain a list with one or more `str` entries.
                for v in Schema(validators[0]).validate(value):
                    yield Result(v.status, v.error,
                                 path=(str(key) + ("." + v.path if v.path else "")), level=v.level, data=value)

    def iter_dict(self) -> Iterator[Result]:
        """Iterate over a :class:`dict` based ``validator`` to validate the corresponding entries in ``data``.

        :yields: objects with the error message related to the failure, if any check fails.
        """
        # One key in `validator` attribute (`key` variable) can be mapped to one or more keys in `data` attribute (`d`
        # variable), depending on the `key` type.
        data = cast(Dict[Any, Any], self.data)
        validators = cast(Dict[Any, Any], self.validator)
        for key in validators.keys():
            if isinstance(key, AtMostOne) and len(list(self._data_key(key))) > 1:
                yield Result(False, f"Multiple of {key.args} provided")
                continue
            for d in self._data_key(key):
                if d not in data and not isinstance(key, Optional):
                    yield Result(False, "is not defined.", path=d)
                elif d not in data and isinstance(key, Optional) and key.default is None:
                    continue
                else:
                    if d not in data and isinstance(key, Optional):
                        data[d] = key.default
                    validator = validators[key]
                    if isinstance(key, (Or, AtMostOne)) and isinstance(validators[key], Case):
                        validator = validators[key]._schema[d]
                    # In this loop we may be calling a new `Schema` either over an intermediate node in the tree, or
                    # over a leaf node. In the latter case the recursive calls in the given path will finish.
                    for v in Schema(validator).validate(data[d]):
                        yield Result(v.status, v.error,
                                     path=(d + ("." + v.path if v.path else "")), level=v.level, data=v.data)

    def iter_or(self) -> Iterator[Result]:
        """Perform all validations defined in an :class:`Or` object for a given configuration option.

        This method can be only called against leaf nodes in the configuration tree. :class:`Or` objects defined in the
        ``validator`` keys will be handled by :func:`iter_dict` method.

        :yields: objects with the error message related to the failure, if any check fails.
        """
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(self.validator, Or)
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

    def _data_key(self, key: Union[str, Optional, Or, AtMostOne]) -> Iterator[str]:
        """Map a key from the ``validator`` dictionary to the corresponding key(s) in the ``data`` dictionary.

        :param key: key from the ``validator`` attribute.

        :yields: keys that should be used to access corresponding value in the ``data`` attribute.
        """
        data = cast(Dict[Any, Any], self.data)

        # If the key was defined as an `Optional` object in `validator` attribute, then its name is the key to access
        # the `data` dictionary.
        if isinstance(key, Optional):
            yield key.name
        # If the key was defined as a `str` object in `validator` attribute, then it is already the final key
        # to access the `data` dictionary.
        elif isinstance(key, str):
            yield key
        # If the key was defined as an `Or` object in `validator` attribute, then each of its values are
        # the keys to access the `data` dictionary.
        elif isinstance(key, Or):
            # At least one of the `Or` entries should be available in the `data` dictionary. If we find at least
            # one of them in `data`, then we return all found entries so the caller method can validate them all.
            if any([item in data for item in key.args]):
                for item in key.args:
                    if item in data:
                        yield item
            # If none of the `Or` entries is available in the `data` dictionary, then we return all entries so the
            # caller method will issue errors that they are all absent.
            else:
                for item in key.args:
                    yield item
        # If the key was defined as a `AtMostOne` object in `validator` attribute, then each of its values
        # are the keys to access the `data` dictionary.
        elif isinstance(key, AtMostOne):  # pyright: ignore [reportUnnecessaryIsInstance]
            # Yield back all of the entries from the `data` dictionary, each will be validated and then counted
            # to inform us if we've provided too many
            for item in key.args:
                if item in data:
                    yield item


def _get_type_name(python_type: Any) -> str:
    """Get a user-friendly name for a given Python type.

    :param python_type: Python type which user friendly name should be taken.

    :returns: User friendly name of the given Python type.
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
    if not condition:
        raise PatroniAssertionError(message)


class IntValidator(object):
    """Validate an integer setting.

    :ivar min: minimum allowed value for the setting, if any.
    :ivar max: maximum allowed value for the setting, if any.
    :ivar base_unit: the base unit to convert the value to before checking if it's within *min* and *max* range.
    :ivar expected_type: the expected Python type.
    :ivar raise_assert: if an ``assert`` test should be performed regarding expected type and valid range.
    """

    def __init__(self, min: OptionalType[int] = None, max: OptionalType[int] = None,
                 base_unit: OptionalType[str] = None, expected_type: Any = None, raise_assert: bool = False) -> None:
        """Create an :class:`IntValidator` object with the given rules.

        :param min: minimum allowed value for the setting, if any.
        :param max: maximum allowed value for the setting, if any.
        :param base_unit: the base unit to convert the value to before checking if it's within *min* and *max* range.
        :param expected_type: the expected Python type.
        :param raise_assert: if an ``assert`` test should be performed regarding expected type and valid range.
        """
        self.min = min
        self.max = max
        self.base_unit = base_unit
        if expected_type:
            self.expected_type = expected_type
        self.raise_assert = raise_assert

    def __call__(self, value: Any) -> bool:
        """Check if *value* is a valid integer and within the expected range.

        .. note::
            If ``raise_assert`` is ``True`` and *value* is not valid, then an :class:`AssertionError` will be triggered.

        :param value: value to be checked against the rules defined for this :class:`IntValidator` instance.

        :returns: ``True`` if *value* is valid and within the expected range.
        """
        value = parse_int(value, self.base_unit)
        ret = isinstance(value, int)\
            and (self.min is None or value >= self.min)\
            and (self.max is None or value <= self.max)

        if self.raise_assert:
            assert_(ret)
        return ret


class EnumValidator(object):
    """Validate enum setting

    :ivar allowed_values: a ``set`` or ``CaseInsensitiveSet`` object with allowed enum values.
    :ivar raise_assert: if an ``assert`` call should be performed regarding expected type and valid range.
    """

    def __init__(self, allowed_values: Tuple[str, ...],
                 case_sensitive: bool = False, raise_assert: bool = False) -> None:
        """Create an :class:`EnumValidator` object with given allowed values.

        :param allowed_values: a tuple with allowed enum values
        :param case_sensitive: set to ``True`` to do case sensitive comparisons
        :param raise_assert: if an ``assert`` call should be performed regarding expected values.
        """
        self.allowed_values = set(allowed_values) if case_sensitive else CaseInsensitiveSet(allowed_values)
        self.raise_assert = raise_assert

    def __call__(self, value: Any) -> bool:
        """Check if provided *value* could be found within *allowed_values*.

        .. note::
            If ``raise_assert`` is ``True`` and *value* is not valid, then an ``AssertionError`` will be triggered.
        :param value: value to be checked.
        :returns: ``True`` if *value* could be found within *allowed_values*.
        """
        ret = isinstance(value, str) and value in self.allowed_values

        if self.raise_assert:
            assert_(ret)
        return ret


def validate_watchdog_mode(value: Any) -> None:
    """Validate ``watchdog.mode`` configuration option.

    :param value: value of ``watchdog.mode`` to be validated.
    """
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
setattr(validate_binary_name, 'expected_type', str)
validate_etcd = {
    Or("host", "hosts", "srv", "srv_suffix", "url", "proxy"): Case({
        "host": validate_host_port,
        "hosts": Or(comma_separated_host_port, [validate_host_port]),
        "srv": str,
        "srv_suffix": str,
        "url": str,
        "proxy": str
    }),
    Optional("protocol"): str,
    Optional("username"): str,
    Optional("password"): str,
    Optional("cacert"): str,
    Optional("cert"): str,
    Optional("key"): str
}

schema = Schema({
    "name": str,
    "scope": str,
    Optional("log"): {
        Optional("type"): EnumValidator(('plain', 'json'), case_sensitive=True, raise_assert=True),
        Optional("level"): EnumValidator(('DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'FATAL', 'CRITICAL'),
                                         case_sensitive=True, raise_assert=True),
        Optional("traceback_level"): EnumValidator(('DEBUG', 'ERROR'), raise_assert=True),
        Optional("format"): validate_log_format,
        Optional("dateformat"): str,
        Optional("static_fields"): dict,
        Optional("max_queue_size"): int,
        Optional("dir"): str,
        Optional("file_num"): int,
        Optional("file_size"): int,
        Optional("mode"): IntValidator(min=0, max=511, expected_type=int, raise_assert=True),
        Optional("loggers"): dict,
        Optional("deduplicate_heartbeat_logs"): bool
    },
    Optional("ctl"): {
        Optional("insecure"): bool,
        Optional("cacert"): str,
        Optional("certfile"): str,
        Optional("keyfile"): str,
        Optional("keyfile_password"): str
    },
    "restapi": {
        "listen": validate_host_port_listen,
        "connect_address": validate_connect_address,
        Optional("authentication"): {
            "username": str,
            "password": str
        },
        Optional("certfile"): str,
        Optional("keyfile"): str,
        Optional("keyfile_password"): str,
        Optional("cafile"): str,
        Optional("ciphers"): str,
        Optional("verify_client"): EnumValidator(("none", "optional", "required"),
                                                 case_sensitive=True, raise_assert=True),
        Optional("allowlist"): [str],
        Optional("allowlist_include_members"): bool,
        Optional("http_extra_headers"): dict,
        Optional("https_extra_headers"): dict,
        Optional("request_queue_size"): IntValidator(min=0, max=4096, expected_type=int, raise_assert=True),
        Optional("server_tokens"): EnumValidator(('minimal', 'productonly', 'original'),
                                                 case_sensitive=False, raise_assert=True)
    },
    Optional("bootstrap"): {
        "dcs": {
            Optional("ttl"): IntValidator(min=20, raise_assert=True),
            Optional("loop_wait"): IntValidator(min=1, raise_assert=True),
            Optional("retry_timeout"): IntValidator(min=3, raise_assert=True),
            Optional("maximum_lag_on_failover"): IntValidator(min=0, raise_assert=True),
            Optional("maximum_lag_on_syncnode"): IntValidator(min=-1, raise_assert=True),
            Optional('member_slots_ttl'): IntValidator(min=0, base_unit='s', raise_assert=True),
            Optional("postgresql"): {
                Optional("parameters"): {
                    Optional("max_connections"): IntValidator(1, 262143, raise_assert=True),
                    Optional("max_locks_per_transaction"): IntValidator(10, 2147483647, raise_assert=True),
                    Optional("max_prepared_transactions"): IntValidator(0, 262143, raise_assert=True),
                    Optional("max_replication_slots"): IntValidator(0, 262143, raise_assert=True),
                    Optional("max_wal_senders"): IntValidator(0, 262143, raise_assert=True),
                    Optional("max_worker_processes"): IntValidator(0, 262143, raise_assert=True),
                },
                Optional("use_pg_rewind"): bool,
                Optional("pg_hba"): [str],
                Optional("pg_ident"): [str],
                Optional("pg_ctl_timeout"): IntValidator(min=0, raise_assert=True),
                Optional("use_slots"): bool,
            },
            Optional("primary_start_timeout"): IntValidator(min=0, raise_assert=True),
            Optional("primary_stop_timeout"): IntValidator(min=0, raise_assert=True),
            Optional("standby_cluster"): {
                Or("host", "port", "restore_command"): Case({
                    "host": str,
                    "port": IntValidator(max=65535, expected_type=int, raise_assert=True),
                    "restore_command": str
                }),
                Optional("primary_slot_name"): str,
                Optional("create_replica_methods"): [str],
                Optional("archive_cleanup_command"): str,
                Optional("recovery_min_apply_delay"): str
            },
            Optional("synchronous_mode"): bool,
            Optional("synchronous_mode_strict"): bool,
            Optional("synchronous_node_count"): IntValidator(min=1, raise_assert=True),
        },
        Optional("initdb"): [Or(str, dict)],
        Optional("method"): str
    },
    Or(*available_dcs): Case({
        "consul": {
            Or("host", "url"): Case({
                "host": validate_host_port,
                "url": str
            }),
            Optional("port"): IntValidator(max=65535, expected_type=int, raise_assert=True),
            Optional("scheme"): str,
            Optional("token"): str,
            Optional("verify"): bool,
            Optional("cacert"): str,
            Optional("cert"): str,
            Optional("key"): str,
            Optional("dc"): str,
            Optional("checks"): [str],
            Optional("register_service"): bool,
            Optional("service_tags"): [str],
            Optional("service_check_interval"): str,
            Optional("service_check_tls_server_name"): str,
            Optional("consistency"): EnumValidator(('default', 'consistent', 'stale'),
                                                   case_sensitive=True, raise_assert=True)
        },
        "etcd": validate_etcd,
        "etcd3": validate_etcd,
        "exhibitor": {
            "hosts": [str],
            "port": IntValidator(max=65535, expected_type=int, raise_assert=True),
            Optional("poll_interval"): IntValidator(min=1, expected_type=int, raise_assert=True),
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
            Optional("use_ssl"): bool,
            Optional("cacert"): str,
            Optional("cert"): str,
            Optional("key"): str,
            Optional("key_password"): str,
            Optional("verify"): bool,
            Optional("set_acls"): dict,
            Optional("auth_data"): dict,
        },
        "kubernetes": {
            "labels": {},
            Optional("bypass_api_service"): bool,
            Optional("namespace"): str,
            Optional("scope_label"): str,
            Optional("role_label"): str,
            Optional("leader_label_value"): str,
            Optional("follower_label_value"): str,
            Optional("standby_leader_label_value"): str,
            Optional("tmp_role_label"): str,
            Optional("use_endpoints"): bool,
            Optional("pod_ip"): Or(is_ipv4_address, is_ipv6_address),
            Optional("ports"): [{"name": str, "port": IntValidator(max=65535, expected_type=int, raise_assert=True)}],
            Optional("cacert"): str,
            Optional("retriable_http_codes"): Or(int, [int]),
            Optional("bootstrap_labels"): dict,
        },
    }),
    Optional("citus"): {
        "database": str,
        "group": IntValidator(min=0, expected_type=int, raise_assert=True),
    },
    "postgresql": {
        "listen": validate_host_port_listen_multiple_hosts,
        "connect_address": validate_connect_address,
        Optional("proxy_address"): validate_connect_address,
        "authentication": {
            "replication": userattributes,
            "superuser": userattributes,
            Optional("rewind"): userattributes
        },
        "data_dir": validate_data_dir,
        Optional("bin_name"): {
            Optional("pg_ctl"): validate_binary_name,
            Optional("initdb"): validate_binary_name,
            Optional("pg_controldata"): validate_binary_name,
            Optional("pg_basebackup"): validate_binary_name,
            Optional("postgres"): validate_binary_name,
            Optional("pg_isready"): validate_binary_name,
            Optional("pg_rewind"): validate_binary_name,
        },
        Optional("bin_dir", ""): BinDirectory(),
        Optional("parameters"): {
            Optional("unix_socket_directories"): str
        },
        Optional("pg_hba"): [str],
        Optional("pg_ident"): [str],
        Optional("pg_ctl_timeout"): IntValidator(min=0, raise_assert=True),
        Optional("use_pg_rewind"): bool
    },
    Optional("watchdog"): {
        Optional("mode"): validate_watchdog_mode,
        Optional("device"): str,
        Optional("safety_margin"): IntValidator(min=-1, expected_type=int, raise_assert=True),
    },
    Optional("tags"): {
        AtMostOne("nofailover", "failover_priority"): Case({
            "nofailover": bool,
            "failover_priority": IntValidator(min=0, expected_type=int, raise_assert=True),
        }),
        Optional("clonefrom"): bool,
        Optional("noloadbalance"): bool,
        Optional("replicatefrom"): str,
        AtMostOne("nosync", "sync_priority"): Case({
            "nosync": bool,
            "sync_priority": IntValidator(min=0, expected_type=int, raise_assert=True),
        }),
        Optional("nostream"): bool
    }
})
