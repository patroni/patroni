#!/usr/bin/env python3
import logging
import os
import socket

from patroni.utils import split_host_port
from patroni.ctl import find_executable
from patroni.dcs import dcs_modules
from patroni.exceptions import ConfigParseError

logger = logging.getLogger(__name__)


def data_directory_empty(data_dir):
    if os.path.isfile(os.path.join(data_dir, "global", "pg_control")):
        return False
    if not os.path.exists(data_dir):
        return True
    return all(
        os.name != "nt" and (n.startswith(".") or n == "lost+found")
        for n in os.listdir(data_dir)
    )


def validate_host_port(listen):
    try:
        host, port = split_host_port(listen, None)
    except (ValueError, TypeError):
        raise ConfigParseError("contains a wrong value")
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                if s.connect_ex((host, port)) == 0:
                    logger.warning("Port %s is already in use.", port)
            except socket.gaierror as e:
                raise ConfigParseError(e)


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
    def __init__(self, schema, path=""):
        self._schema = schema
        self._path = str(path)

    def __call__(self, data):
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s')
        for key in self._schema:
            optional = False
            if isinstance(self._schema, dict):
                subschema = self._schema[key]
            elif isinstance(self._schema, list):
                subschema = self._schema[0]
                for key, value in enumerate(data):
                    self.validator(key, value, subschema)
                continue
            if not isinstance(key, str):
                if isinstance(key, Optional):
                    key = key.name
                    optional = True
                if isinstance(key, Or):
                    self.validate_orcase(key, subschema, data)
                    continue
            if key not in data and not optional:
                logger.warning("%s is not defined", self.get_fullpath(key))
            else:
                self.validator(key, data[key], subschema)

    def validator(self, key, data, schema, quiet=False):
        if not isinstance(data, type(schema)):
            if issubclass(type(schema), type):
                if isinstance(data, schema):
                    return True
                else:
                    if not quiet:
                        logger.warning("%s ('%s') didn't pass validation", self.get_fullpath(key), data)
            elif callable(schema):
                try:
                    schema(data)
                    return True
                except Exception as e:
                    if not quiet:
                        logger.warning("%s ('%s') didn't pass validation: %s", self.get_fullpath(key), data, e)
            elif isinstance(schema, Or):
                counter = 0
                for v in schema.args:
                    counter += (1 if self.validator(key, data, v, quiet=True) else 0)
                if counter < 1:
                    logger.warning("%s ('%s') didn't pass validation", self.get_fullpath(key), data)
                else:
                    return True

            else:
                if not quiet:
                    logger.warning("%s type is not %s", self.get_fullpath(key), _get_type_name(type(schema)))
        elif isinstance(data, dict):
            Schema(schema, path=self.get_fullpath(key))(data)
        elif isinstance(data, list):
            Schema(schema, path=self.get_fullpath(key))(data)

    def validate_orcase(self, key, schema, data):
        counter = 0
        for a in key.args:
            if a in data:
                counter += 1
                subschema = schema._schema[a]
                self.validator(a, data[a], subschema)
        if counter < 1:
            logger.warning("neither of %s is defined", ", ".join([self.get_fullpath(a) for a in key.args]))

    def get_fullpath(self, path):
        return self._path + ("." if self._path else "") + str(path)


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
    return "string"


userattributes = {"username": "", Optional("password"): ""}
available_dcs = [m.split(".")[-1] for m in dcs_modules()]

schema = Schema({
  "name": str,
  "scope": str,
  "restapi": {
    "listen": validate_host_port
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
      "etcd": {
          Or("host", "hosts", "srv", "url", "proxy"): Case({
              "host": str,
              "hosts": Or(str, list),
              "srv": str,
              "url": str,
              "proxy": str})
         },
      "exhibitor": {
          "hosts": str,
          },
      "zookeeper": {
          "hosts": [validate_host_port],
          },
      "kubernetes": {
          Optional("namespace"): str,
          },
      }),
  "postgresql": {
    "listen": validate_host_port,
    "authentication": {
      "replication": userattributes,
      "superuser": userattributes,
      "rewind":  userattributes
    },
    "data_dir": validate_data_dir,
    "bin_dir": validate_bin_dir,
    "parameters": {
      "unix_socket_directories": lambda s: all([str(s), len(s)])
    }
  }
})
