import unittest
import os
import socket
import copy
from mock import Mock, patch, mock_open
from patroni.validator import schema
from six import StringIO

config = {
    "name": "string",
    "scope": "string",
    "restapi": {
        "listen": "127.0.0.2:800",
        "connect_address": "127.0.0.2:800"
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
        "host": "127.0.0.1:5000"
    },
    "etcd": {
        "hosts": "127.0.0.1:2379,127.0.0.1:2380"
    },
    "exhibitor": {
        "hosts": ["string"],
        "port": 4000,
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
        "listen": "127.0.0.2,::1:543",
        "connect_address": "127.0.0.2:543",
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
        "pg_hba": [u"string"],
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
    return arg in files


def isdir_side_effect(arg):
    return arg in directories


def exists_side_effect(arg):
    return isfile_side_effect(arg) or isdir_side_effect(arg)


def connect_side_effect(host_port):
    _, port = host_port
    if port < 1000:
        return 1
    elif port < 10000:
        return 0
    else:
        raise socket.gaierror()


def parse_output(output):
    result = []
    for s in output.split("\n"):
        x = s.split(" ")[0]
        if x and x not in result:
            result.append(x)
    result.sort()
    return result


@patch('socket.socket.connect_ex', Mock(side_effect=connect_side_effect))
@patch('os.path.exists', Mock(side_effect=exists_side_effect))
@patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
@patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
@patch('sys.stderr', new_callable=StringIO)
@patch('sys.stdout', new_callable=StringIO)
class TestValidator(unittest.TestCase):

    def setUp(self):
        del files[:]
        del directories[:]

    def test_empty_config(self, mock_out, mock_err):
        schema({})
        output = mock_out.getvalue()
        self.assertEqual(['consul', 'etcd', 'exhibitor', 'kubernetes', 'name', 'postgresql', 'restapi', 'scope', 'zookeeper'], parse_output(output))

    def test_complete_config(self, mock_out, mock_err):
        schema(config)
        output = mock_out.getvalue()
        self.assertEqual(['postgresql.bin_dir'], parse_output(output))

    def test_bin_dir_is_file(self, mock_out, mock_err):
        files.append(config["postgresql"]["data_dir"])
        files.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = False
        c["etcd"]["hosts"] = ["127.0.0.1:2379","1244.0.0.1:2379","127.0.0.1:invalidport"]
        c["kubernetes"]["pod_ip"] = "127.0.0.1111"
        schema(c)
        output = mock_out.getvalue()
        self.assertEqual(['etcd.hosts.1', 'etcd.hosts.2', 'kubernetes.pod_ip', 'postgresql.bin_dir',
                          'postgresql.data_dir', 'restapi.connect_address'] , parse_output(output))

    def test_bin_dir_is_empty(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = "127.0.0.1"
        c["kubernetes"]["pod_ip"] = "::1"
        c["consul"]["host"] = "127.0.0.1:50000"
        c["etcd"]["host"] = "127.0.0.1:237"
        c["postgresql"]["listen"] = "127.0.0.1:5432"
        with patch('patroni.validator.open', mock_open(read_data='9')):
            schema(c)
        output = mock_out.getvalue()
        self.assertEqual(['consul.host', 'etcd.host', 'postgresql.bin_dir', 'postgresql.data_dir',
                          'postgresql.listen', 'restapi.connect_address'], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_data_dir_contains_pg_version(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        directories.append(os.path.join(config["postgresql"]["data_dir"], "pg_wal"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "pg_ctl"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "initdb"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "pg_controldata"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "pg_basebackup"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "postgres"))
        files.append(os.path.join(config["postgresql"]["bin_dir"], "pg_isready"))
        with patch('patroni.validator.open', mock_open(read_data='12')):
            schema(config)
        output = mock_out.getvalue()
        self.assertEqual([], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_pg_version_missmatch(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        directories.append(os.path.join(config["postgresql"]["data_dir"], "pg_wal"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        c = copy.deepcopy(config)
        c["etcd"]["hosts"] = []
        del c["postgresql"]["bin_dir"]
        with patch('patroni.validator.open', mock_open(read_data='11')):
            schema(c)
        output = mock_out.getvalue()
        self.assertEqual(['etcd.hosts', 'postgresql.data_dir'], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_pg_wal_doesnt_exist(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        c = copy.deepcopy(config)
        del c["postgresql"]["bin_dir"]
        with patch('patroni.validator.open', mock_open(read_data='11')):
            schema(c)
        output = mock_out.getvalue()
        self.assertEqual(['postgresql.data_dir'], parse_output(output))


    def test_data_dir_is_empty_string(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["kubernetes"] = False
        c["postgresql"]["pg_hba"] = ""
        c["postgresql"]["data_dir"] = ""
        c["postgresql"]["bin_dir"] = ""
        schema(c)
        output = mock_out.getvalue()
        self.assertEqual(['kubernetes', 'postgresql.bin_dir', 'postgresql.data_dir', 'postgresql.pg_hba'], parse_output(output))
