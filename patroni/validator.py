#!/usr/bin/env python3
import logging
import os
import socket

from patroni.utils import split_host_port, data_directory_is_empty
from patroni.ctl import find_executable
from patroni.dcs import dcs_modules
from patroni.exceptions import ConfigParseError

logger = logging.getLogger(__name__)


def data_directory_empty(data_dir):
    if os.path.isfile(os.path.join(data_dir, "global", "pg_control")):
        return False
    return data_directory_is_empty(data_dir)


def validate_connect_address(address):
    if not isinstance(address, str):
        raise ConfigParseError("is not a string")
    else:
        for s in ["127.0.0.1", "0.0.0.0", "*", "::1"]:
            if s in address:
                raise ConfigParseError("must not contain {}".format(s))
    return True


def validate_host_port(host_port, listen=False, connect=False):
    try:
        host, port = split_host_port(host_port, None)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                if s.connect_ex((host, port)) == 0 and listen:
                    ConfigParseError("Port {} is already in use.".format(port))
                elif connect:
                    ConfigParseError("{} is not reachable".format(host_port))
            except socket.gaierror as e:
                raise ConfigParseError(e)
    return True


def comma_separated_host_port(string):
    if not isinstance(string, str):
        raise ConfigParseError("is not a string")
    if not all([validate_host_port(s.strip()) for s in string.split(",")]):
        raise ConfigParseError("didn't pass the validation")
    return True


def validate_host_port_listen(host_port):
    return validate_host_port(host_port, listen=True)


def is_ipv4_address(ip):
    try:
        a = ip.split(".")
        assert len(a) == 4
        assert all([int(i) <= 255 for i in a])
    except Exception:
        raise ConfigParseError("{} doesn't look like a valid ipv4 address".format(ip))
    return True


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
                pgversion = int(version.read())
            waldir = ("pg_wal" if pgversion >= 10 else "pg_xlog")
            if not os.path.isdir(os.path.join(data_dir, waldir)):
                raise ConfigParseError("data dir for the cluster is not empty, but doesn't contain"
                                       " \"{}\" directory".format(waldir))
    return True


def validate_bin_dir(bin_dir):
    if not bin_dir:
        raise ConfigParseError("is an empty string")
    elif not os.path.exists(bin_dir):
        raise ConfigParseError("Directory '{}' does not exist.".format(bin_dir))
    elif not os.path.isdir(bin_dir):
        raise ConfigParseError("'{}' is not a directory.".format(bin_dir))
    for program in ["pg_ctl", "initdb", "pg_controldata", "pg_basebackup", "postgres"]:
        if not find_executable(program, bin_dir):
            logger.warning("Program '%s' not found.", program)
    return True


class Result(object):
    def __init__(self, status, error="didn't pass validation", path="", data=""):
        self.status = status
        self.path = path
        self.data = data
        self._error = error
        if not self.status:
            self.error = error
        else:
            self.error = None

    def __repr__(self):
        return self.path + (" " + str(self.data) + " " + self._error if self.error else "")

    def __bool__(self):
        return self.status


class Case(object):
    def __init__(self, schema):
        self._schema = schema


class Or(object):
    def __init__(self, *args):
        self.args = args


class Optional(object):
    def __init__(self, name):
        self.name = name


class Schema(object):
    def __init__(self, validator):
        self.validator = validator

    def __call__(self, data):
        for i in self.validate(data):
            if not i:
                print(i)

    def validate(self, data):
        self.data = data
        if isinstance(self.validator, str):
            yield Result(isinstance(self.data, str), "is not a string", data=self.data)
        elif issubclass(type(self.validator), type):
            yield Result(isinstance(self.data, self.validator),
                         "is not {}".format(_get_type_name(self.validator)), data=self.data)
        elif callable(self.validator):
            try:
                self.validator(data)
                yield Result(True, data=self.data)
            except Exception as e:
                yield Result(False, "didn't pass validation: {}".format(e), data=self.data)
        elif isinstance(self.validator, dict):
            if not len(self.validator):
                yield Result(isinstance(self.data, dict), "is not a dictionary", data=self.data)
        elif isinstance(self.validator, list):
            if not isinstance(self.data, list):
                yield Result(isinstance(self.data, list), "is not a list", data=self.data)
                return
        for i in self.iter():
            yield i

    def iter(self):
        if isinstance(self.validator, dict):
            if not isinstance(self.data, dict):
                yield Result(False, "is not a dictionary.")
            else:
                for i in self.iter_dict():
                    yield i
        elif isinstance(self.validator, list):
            if len(self.validator) > 0:
                for key, value in enumerate(self.data):
                    for v in Schema(self.validator[0]).validate(value):
                        yield Result(v.status, v.error,
                                     path=(str(key) + ("." + v.path if v.path else "")), data=value)
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
                                     path=(d + ("." + v.path if v.path else "")), data=v.data)

    def iter_or(self):
        results = []
        for a in self.validator.args:
            r = []
            for v in Schema(a).validate(self.data):
                r.append(v)
            if any(r) and not all(r):
                results += filter(lambda x: not x, r)
            else:
                results += r
        if not any(results):
            for v in results:
                yield Result(v.status, v.error, path=v.path, data=v.data)

    def _data_key(self, key):
        if isinstance(self.data, dict) and isinstance(key, str):
            yield key
        elif isinstance(key, Optional):
            yield key.name
        elif isinstance(key, Or):
            for i in key.args:
                if i in self.data:
                    yield i


def _get_type_name(python_type):
    if python_type == str:
        return "a string"
    elif python_type == int:
        return "an integer"
    elif python_type == float:
        return "a number"
    elif python_type == bool:
        return "a boolean"
    elif python_type == list:
        return "an array"
    elif python_type == dict:
        return "a dictionary"
    return python_type.__name__


def assert_(condition, message="Wrong value"):
    assert condition, message


userattributes = {"username": "", Optional("password"): ""}
available_dcs = [m.split(".")[-1] for m in dcs_modules()]

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
              "host": str,
              "url": str})
          },
      "etcd": {
          Or("host", "hosts", "srv", "url", "proxy"): Case({
              "host": str,
              "hosts": Or(comma_separated_host_port, [validate_host_port]),
              "srv": str,
              "url": str,
              "proxy": str})
         },
      "exhibitor": {
          "hosts": str,
          "port": lambda i: assert_(int(i) <= 65535),
          Optional("pool_interval"): int
          },
      "zookeeper": {
          "hosts": Or(comma_separated_host_port, [validate_host_port]),
          },
      "kubernetes": {
          Optional("namespace"): str,
          Optional("labels"): {},
          Optional("scope_label"): str,
          Optional("role_label"): str,
          Optional("use_endpoints"): bool,
          Optional("pod_ip"): is_ipv4_address,
          Optional("ports"): [{"name": str, "port": int}],
          },
      }),
  "postgresql": {
    "listen": validate_host_port_listen,
    "connect_address": validate_connect_address,
    "authentication": {
      "replication": userattributes,
      "superuser": userattributes,
      "rewind":  userattributes
    },
    "data_dir": validate_data_dir,
    "bin_dir": validate_bin_dir,
    "parameters": {
      Optional("unix_socket_directories"): lambda s: assert_(all([isinstance(s, str), len(s)]))
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
    Optional("nosync"): bool
  }
})
