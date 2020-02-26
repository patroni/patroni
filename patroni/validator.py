#!/usr/bin/env python3
import os
import socket
import re
import subprocess

from patroni.utils import split_host_port, data_directory_is_empty
from patroni.ctl import find_executable
from patroni.dcs import dcs_modules
from patroni.exceptions import ConfigParseError
from six import string_types


def data_directory_empty(data_dir):
    if os.path.isfile(os.path.join(data_dir, "global", "pg_control")):
        return False
    return data_directory_is_empty(data_dir)


def validate_connect_address(address):
    try:
        host, _ = split_host_port(address, None)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    if host in ["127.0.0.1", "0.0.0.0", "*", "::1"]:
        raise ConfigParseError('must not contain "127.0.0.1", "0.0.0.0", "*", "::1"')
    return True


def validate_host_port(host_port, listen=False, multiple_hosts=False):
    try:
        hosts, port = split_host_port(host_port, None)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    else:
        if multiple_hosts:
            hosts = hosts.split(",")
        else:
            hosts = [hosts]
        for host in hosts:
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


def comma_separated_host_port(string):
    assert all([validate_host_port(s.strip()) for s in string.split(",")]), "didn't pass the validation"
    return True


def validate_host_port_listen(host_port):
    return validate_host_port(host_port, listen=True)


def validate_host_port_listen_multiple_hosts(host_port):
    return validate_host_port(host_port, listen=True, multiple_hosts=True)


def is_ipv4_address(ip):
    try:
        socket.inet_aton(ip)
    except Exception:
        raise ConfigParseError("Is not a valid ipv4 address")
    return True


def is_ipv6_address(ip):
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except Exception:
        raise ConfigParseError("Is not a valid ipv6 address")
    return True


def get_major_version(bin_dir=None):
    if not bin_dir:
        binary = 'postgres'
    else:
        binary = os.path.join(bin_dir, 'postgres')
    version = subprocess.check_output([binary, '--version']).decode()
    version = re.match(r'^[^\s]+ [^\s]+ (\d+)(\.(\d+))?', version)
    return '.'.join([version.group(1), version.group(3)]) if int(version.group(1)) < 10 else version.group(1)


def validate_data_dir(data_dir):
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
                raise ConfigParseError("data_dir directory postgresql version ({}) doesn't match"
                                       "with 'postgres --version' output ({})".format(pgversion, major_version))
    return True


class Result(object):
    def __init__(self, status, error="didn't pass validation", level=0, path="", data=""):
        self.status = status
        self.path = path
        self.data = data
        self.level = level
        self._error = error
        if not self.status:
            self.error = error
        else:
            self.error = None

    def __repr__(self):
        return self.path + (" " + str(self.data) + " " + self._error if self.error else "")


class Case(object):
    def __init__(self, schema):
        self._schema = schema


class Or(object):
    def __init__(self, *args):
        self.args = args


class Optional(object):
    def __init__(self, name):
        self.name = name


class Directory(object):
    def __init__(self, contains=None, contains_executable=None):
        self.contains = contains
        self.contains_executable = contains_executable

    def validate(self, name):
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
                    if not find_executable(program, name):
                        yield Result(False, "'{}' does not contain '{}'".format(name, program))


class Schema(object):
    def __init__(self, validator):
        self.validator = validator

    def __call__(self, data):
        for i in self.validate(data):
            if not i.status:
                print(i)

    def validate(self, data):
        self.data = data
        if isinstance(self.validator, string_types):
            yield Result(isinstance(self.data, string_types), "is not a string", level=1, data=self.data)
        elif issubclass(type(self.validator), type):
            validator = self.validator
            if self.validator == str:
                validator = string_types
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
            if not len(self.validator):
                yield Result(isinstance(self.data, dict), "is not a dictionary", level=1, data=self.data)
        elif isinstance(self.validator, list):
            if not isinstance(self.data, list):
                yield Result(isinstance(self.data, list), "is not a list", level=1, data=self.data)
                return
        for i in self.iter():
            yield i

    def iter(self):
        if isinstance(self.validator, dict):
            if not isinstance(self.data, dict):
                yield Result(False, "is not a dictionary.", level=1)
            else:
                for i in self.iter_dict():
                    yield i
        elif isinstance(self.validator, list):
            if len(self.data) == 0:
                yield Result(False, "is an empty list", data=self.data)
            if len(self.validator) > 0:
                for key, value in enumerate(self.data):
                    for v in Schema(self.validator[0]).validate(value):
                        yield Result(v.status, v.error,
                                     path=(str(key) + ("." + v.path if v.path else "")), level=v.level, data=value)
        elif isinstance(self.validator, Directory):
            for v in self.validator.validate(self.data):
                yield v
        elif isinstance(self.validator, Or):
            for i in self.iter_or():
                yield i

    def iter_dict(self):
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
                    for v in Schema(validator).validate(self.data[d]):
                        yield Result(v.status, v.error,
                                     path=(d + ("." + v.path if v.path else "")), level=v.level, data=v.data)

    def iter_or(self):
        results = []
        for a in self.validator.args:
            r = []
            for v in Schema(a).validate(self.data):
                r.append(v)
            if any([x.status for x in r]) and not all([x.status for x in r]):
                results += filter(lambda x: not x.status, r)
            else:
                results += r
        if not any([x.status for x in results]):
            max_level = 3
            for v in sorted(results, key=lambda x: x.level):
                if v.level > max_level:
                    break
                max_level = v.level
                yield Result(v.status, v.error, path=v.path, level=v.level, data=v.data)

    def _data_key(self, key):
        if isinstance(self.data, dict) and isinstance(key, str):
            yield key
        elif isinstance(key, Optional):
            yield key.name
        elif isinstance(key, Or):
            if any([i in self.data for i in key.args]):
                for i in key.args:
                    if i in self.data:
                        yield i
            else:
                for i in key.args:
                    yield i


def _get_type_name(python_type):
    return {str: 'a string', int: 'and integer', float: 'a number', bool: 'a boolean',
            list: 'an array', dict: 'a dictionary', string_types: "a string"}.get(
                    python_type, getattr(python_type, __name__, "unknown type"))


def assert_(condition, message="Wrong value"):
    assert condition, message


userattributes = {"username": "", Optional("password"): ""}
available_dcs = [m.split(".")[-1] for m in dcs_modules()]
comma_separated_host_port.expected_type = string_types
validate_connect_address.expected_type = string_types
validate_host_port_listen.expected_type = string_types
validate_host_port_listen_multiple_hosts.expected_type = string_types
validate_data_dir.expected_type = string_types

schema = Schema({
  "name": str,
  "scope": str,
  "restapi": {
    "listen": validate_host_port_listen,
    "connect_address": validate_connect_address
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
      "etcd": {
          Or("host", "hosts", "srv", "url", "proxy"): Case({
              "host": validate_host_port,
              "hosts": Or(comma_separated_host_port, [validate_host_port]),
              "srv": str,
              "url": str,
              "proxy": str})
         },
      "exhibitor": {
          "hosts": [str],
          "port": lambda i: assert_(int(i) <= 65535),
          Optional("pool_interval"): int
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
          },
      }),
  "postgresql": {
    "listen": validate_host_port_listen_multiple_hosts,
    "connect_address": validate_connect_address,
    "authentication": {
      "replication": userattributes,
      "superuser": userattributes,
      "rewind":  userattributes
    },
    "data_dir": validate_data_dir,
    Optional("bin_dir"): Directory(contains_executable=["pg_ctl", "initdb", "pg_controldata", "pg_basebackup",
                                                        "postgres", "pg_isready"]),
    Optional("parameters"): {
      Optional("unix_socket_directories"): lambda s: assert_(all([isinstance(s, string_types), len(s)]))
    },
    Optional("pg_hba"): [str],
    Optional("pg_ident"): [str],
    Optional("pg_ctl_timeout"): int,
    Optional("use_pg_rewind"): bool
  },
  Optional("watchdog"): {
    Optional("mode"): lambda m: assert_(m in ["off", "automatic", "required"]),
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
