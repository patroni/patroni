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


def validate_host_port(host_port, listen=False):
    try:
        host, port = split_host_port(host_port, None)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                if s.connect_ex((host, port)) == 0 and listen:
                    ConfigParseError("Port {} is already in use.".format(port))
                else:
                    ConfigParseError("{} is not reachable".format(host_port))
            except socket.gaierror as e:
                raise ConfigParseError(e)


def validate_host_port_listen(host_port):
    validate_host_port(host_port, listen=True)


def validate_data_dir(data_dir):
    if not data_dir:
        raise ConfigParseError("is an empty string")
    elif os.path.exists(data_dir) and not os.path.isdir(data_dir):
        raise ConfigParseError("is not a directory")
    elif not data_directory_empty(data_dir):
        if not os.path.exists(os.path.join(data_dir, "PG_VERSION")):
            raise ConfigParseError("doesn't look like a valid data directory")
        elif not os.path.isdir(os.path.join(data_dir, "pg_wal")) and not os.path.isdir(
                 os.path.join(data_dir, "pg_xlog")):
            raise ConfigParseError("data dir for the cluster is not empty, but doesn't contain"
                                   "\"pg_wal\" nor \"pg_xlog\" directory")


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

    def __repr__(self):
        return "Case(" + ", ".join([str(s) for s in self._schema]) + ")"


class Or(object):
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        return "Or(" + ", ".join([str(s) for s in self.args]) + ")"


class Optional(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name + "(optional)"


class Schema(object):
    def __init__(self, validator):
        self.validator = validator

    def __call__(self, data):
        for i in self.validate(data):
            print(i) if not i else None

    def validate(self, data):
        self.data = data
        if isinstance(self.validator, str):
            yield Result(isinstance(self.data, str), "is not a string", data=self.data)
        elif isinstance(self.validator, dict):
            if not len(self.validator):
                yield Result(isinstance(self.data, dict), "is not a dictionary", data=self.data)
            else:
                for i in self.iter():
                    yield i
        elif isinstance(self.validator, list):
            for i in self.iter():
                yield i
        elif issubclass(type(self.validator), type):
            yield Result(isinstance(self.data, self.validator),
                         "is not a(n) {}".format(_get_type_name(self.validator)), data=self.data)
        elif callable(self.validator):
            try:
                self.validator(data)
                yield Result(True)
            except Exception as e:
                yield Result(False, "didn't pass validation: {}".format(e), data=self.data)
        elif isinstance(self.validator, Or):
            for i in self.iter():
                yield i
        elif isinstance(self.validator, Case):
            for i in self.iter():
                yield i
        else:
            raise NotImplementedError()

    def iter(self):
        if isinstance(self.validator, dict):
            for key in self.validator.keys():
                if isinstance(self.data, dict):
                    orcase = False
                    if isinstance(key, Or) and isinstance(self.validator[key], Case):
                        orcase = True
                    for d in self._data_key(key):
                        if d not in self.data and not isinstance(key, Optional):
                            yield Result(False, "is not defined.", path=d)
                        elif d not in self.data and isinstance(key, Optional):
                            continue
                        else:
                            validator = self.validator[key]
                            if orcase:
                                validator = self.validator[key]._schema[d]
                            for v in Schema(validator).validate(self.data[d]):
                                yield Result(v.status, v.error,
                                             path=(d + ("." + v.path if v.path else "")), data=v.data)
                else:
                    yield Result(False, "is not a dictionary.")
                    return
        elif isinstance(self.validator, list):
            if len(self.validator) == 0:
                yield Result(isinstance(self.data, list), "is not a list", data=self.data)
            elif len(self.validator) == 1:
                if isinstance(self.data, list):
                    for key, value in enumerate(self.data):
                        for v in Schema(self.validator[0]).validate(value):
                            yield Result(v.status, v.error,
                                         path=(str(key) + ("." + v.path if v.path else "")), data=value)
        elif isinstance(self.validator, Or):
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
        elif isinstance(self.validator, Case):
            for key in self.validator._schema.keys():
                for v in Schema(self.validator._schema[key]).validate(self.data):
                    yield v
        else:
            raise NotImplementedError()

    def _data_key(self, key):
        if isinstance(self.data, dict) and isinstance(key, str):
            yield key
        elif isinstance(key, Optional):
            yield key.name
        elif isinstance(key, Or):
            for i in key.args:
                if i in self.data:
                    yield i
        else:
            raise NotImplementedError()


def _get_type_name(python_type):
    if python_type == str:
        return "string"
    elif python_type == int:
        return "integer"
    elif python_type == float:
        return "number"
    elif python_type == bool:
        return "boolean"
    elif python_type == list:
        return "array"
    elif python_type == dict:
        return "dictionary"
    return python_type.__name__


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
              "hosts": Or(str, [validate_host_port]),
              "srv": str,
              "url": str,
              "proxy": str})
         },
      "exhibitor": {
          "hosts": str,
          "port": lambda i: int(i),
          Optional("pool_interval"): int
          },
      "zookeeper": {
          "hosts": [validate_host_port],
          },
      "kubernetes": {
          Optional("namespace"): str,
          Optional("labels"): {},
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
      Optional("unix_socket_directories"): lambda s: all([isinstance(s, str), len(s)])
    }
  }
})
