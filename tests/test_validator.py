import unittest
import os
import socket
import copy
from mock import Mock, patch, mock_open
from patroni.validator import schema
import sys
if not sys.version_info.major == 3:
    from StringIO import StringIO
else:
    from io import StringIO

config = {
    "name": "string",
    "scope": "string",
    "restapi": {
        "listen": "127.0.0.2:8000",
        "connect_address": "127.0.0.2:8000"
    },
    "bootstrap": {
        "dcs": {
            "ttl": 1000,
            "loop_wait": 1000,
            "retry_timeout": 1000,
            "maximum_lag_on_failover": 1000
            },
        "pg_hba": ["string"],
        "initdb": ["string", {"key":"value"}]
    },
    "consul": {
        "host": "string"
    },
    "etcd": {
        "hosts": "127.0.0.1:2379,127.0.0.1:2380"
    },
    "exhibitor": {
        "hosts": "string",
        "port": 1000,
        "pool_interval": 1000
    },
    "zookeeper": {
        "hosts":  "127.0.0.1:3379,127.0.0.1:3380"
    },
    "kubernetes": {
        "namespace": "string",
        "labels": {},
        "scope_label": "string",
        "role_label": "string",
        "use_endpoints": False,
        "pod_ip": "127.0.0.1",
        "ports": [{"name": "string", "port": 1000}],
    },
    "postgresql": {
        "listen": "127.0.0.2:5432",
        "connect_address": "127.0.0.2:5432",
        "authentication": {
            "replication": {"username": "user"},
            "superuser": {"username": "user"},
            "rewind": {"username": "user"},
        },
        "data_dir": "/tmp/data_dir",
        "bin_dir": "/tmp/bin_dir",
        "parameters": {
            "unix_socket_directories": "."
        },
        "pg_hba": ["string"],
        "pg_ident": ["string"],
        "pg_ctl_timeout": 1000,
        "use_pg_rewind": False
    },
    "watchdog": {
        "mode": "off",
        "device": "string"
    },
    "tags": {
      "nofailover": False,
      "clonefrom": False,
      "noloadbalance": False,
      "nosync": False
    }
}

directories = []
files = []

def isfile_side_effect(arg):
    if arg in files:
        return True

def isdir_side_effect(arg):
    if arg in directories:
        return True

def exists_side_effect(arg):
    if arg in directories or arg in files:
        return True


class TestValidator(unittest.TestCase):

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_empty_config(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        schema({})
        output = mock_out.getvalue()
        self.assertIn("name  is not defined.", output)
        self.assertIn("scope  is not defined.", output)
        self.assertIn("restapi  is not defined.", output)
        self.assertIn("postgresql  is not defined.", output)

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    @patch('os.path.exists', Mock(side_effect=exists_side_effect))
    @patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
    @patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_complete_config(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        schema(config)
        output = mock_out.getvalue()
        self.assertIn(("postgresql.bin_dir " + config["postgresql"]["bin_dir"] + " didn't pass validation:"), output)

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    @patch('os.path.exists', Mock(side_effect=exists_side_effect))
    @patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
    @patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_bin_dir_is_file(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        files.append(config["postgresql"]["data_dir"])
        files.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = False
        c["etcd"]["hosts"] = ["127.0.0.1:2379","1244.0.0.1:2379","127.0.0.1:invalidport"]
        c["kubernetes"]["pod_ip"] = "127.0.0.1111"
        schema(c)
        output = mock_out.getvalue()
        self.assertIn(("restapi.connect_address False didn't pass validation:"), output.strip())

    @patch('socket.socket.connect_ex', Mock(side_effect=socket.gaierror))
    @patch('os.path.exists', Mock(side_effect=exists_side_effect))
    @patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
    @patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_bin_dir_is_empty(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = "127.0.0.1"
        with patch('patroni.validator.open', mock_open(read_data='9')):
            schema(c)
        output = mock_out.getvalue()
        self.assertIn(("restapi.listen " + config["restapi"]["listen"] + " didn't pass validation:"), output.strip())

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    @patch('os.path.exists', Mock(side_effect=exists_side_effect))
    @patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
    @patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_data_dir_contains_pg_version(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        c = copy.deepcopy(config)
        with patch('patroni.validator.open', mock_open(read_data='9')):
            schema(c)
        output = mock_out.getvalue()
        self.assertIn(("postgresql.data_dir " + config["postgresql"]["data_dir"] + " didn't pass validation:"), output.strip())


    @patch('socket.socket.connect_ex', Mock(return_value=0))
    @patch('os.path.exists', Mock(side_effect=exists_side_effect))
    @patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
    @patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_data_dir_is_empty_string(self, mock_out, mock_err):
        while len(files):
            del files[0]
        while len(directories):
            del directories[0]
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["kubernetes"] = False
        c["postgresql"]["pg_hba"] = ""
        c["postgresql"]["data_dir"] = ""
        c["postgresql"]["bin_dir"] = ""
        schema(c)
        output = mock_out.getvalue()
        self.assertIn("kubernetes  is not a dictionary." , output.strip())
