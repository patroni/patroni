import copy
import os
import socket
import tempfile
import unittest

from io import StringIO
from unittest.mock import Mock, mock_open, patch

from patroni.dcs import dcs_modules
from patroni.validator import Directory, populate_validate_params, schema, Schema

available_dcs = [m.split(".")[-1] for m in dcs_modules()]
config = {
    "name": "string",
    "scope": "string",
    "log": {
        "type": "plain",
        "level": "DEBUG",
        "traceback_level": "DEBUG",
        "format": "%(asctime)s %(levelname)s: %(message)s",
        "dateformat": "%Y-%m-%d %H:%M:%S",
        "max_queue_size": 100,
        "dir": "/tmp",
        "file_num": 10,
        "file_size": 1000000,
        "loggers": {
            "patroni.postmaster": "WARNING",
            "urllib3": "DEBUG"
        }
    },
    "restapi": {
        "listen": "127.0.0.2:800",
        "connect_address": "127.0.0.2:800",
        "verify_client": 'none'
    },
    "bootstrap": {
        "dcs": {
            "ttl": 1000,
            "loop_wait": 1000,
            "retry_timeout": 1000,
            "maximum_lag_on_failover": 1000
        },
        "initdb": ["string", {"key": "value"}]
    },
    "consul": {
        "host": "127.0.0.1:5000"
    },
    "etcd": {
        "hosts": "127.0.0.1:2379,127.0.0.1:2380"
    },
    "etcd3": {
        "url": "https://127.0.0.1:2379"
    },
    "exhibitor": {
        "hosts": ["string"],
        "port": 4000,
        "pool_interval": 1000
    },
    "raft": {
        "self_addr": "127.0.0.1:2222",
        "bind_addr": "0.0.0.0:2222",
        "partner_addrs": ["127.0.0.1:2223", "127.0.0.1:2224"],
        "data_dir": "/",
        "password": "12345"
    },
    "zookeeper": {
        "hosts": "127.0.0.1:3379,127.0.0.1:3380"
    },
    "kubernetes": {
        "namespace": "string",
        "labels": {},
        "scope_label": "string",
        "role_label": "string",
        "use_endpoints": False,
        "pod_ip": "127.0.0.1",
        "ports": [{"name": "string", "port": 1000}],
        "retriable_http_codes": [401],
    },
    "postgresql": {
        "listen": "127.0.0.2,::1:543",
        "connect_address": "127.0.0.2:543",
        "proxy_address": "127.0.0.2:5433",
        "authentication": {
            "replication": {"username": "user"},
            "superuser": {"username": "user"},
            "rewind": {"username": "user"},
        },
        "data_dir": os.path.join(tempfile.gettempdir(), "data_dir"),
        "bin_dir": os.path.join(tempfile.gettempdir(), "bin_dir"),
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
        "nosync": False,
        "nostream": False
    }
}

config_2 = {
    "some_dir": "very_interesting_dir"
}

schema2 = Schema({
    "some_dir": Directory(contains=["very_interesting_subdir", "another_interesting_subdir"])
})

required_binaries = ["pg_ctl", "initdb", "pg_controldata", "pg_basebackup", "postgres", "pg_isready"]

directories = []
files = []
binaries = []


def isfile_side_effect(arg):
    return arg in files


def which_side_effect(arg, path=None):
    binary = arg if path is None else os.path.join(path, arg)
    return arg if binary in binaries else None


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


def mock_getaddrinfo(host, port, *args):
    if port is None or port == "":
        port = 0
    port = int(port)
    if port not in range(0, 65536):
        raise socket.gaierror()

    if host == "127.0.0.1" or host == "" or host is None:
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('127.0.0.1', port))]
    elif host == "127.0.0.2":
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('127.0.0.2', port))]
    elif host == "::1":
        return [(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('::1', port, 0, 0))]
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
@patch('socket.getaddrinfo', Mock(side_effect=mock_getaddrinfo))
@patch('os.path.exists', Mock(side_effect=exists_side_effect))
@patch('os.path.isdir', Mock(side_effect=isdir_side_effect))
@patch('os.path.isfile', Mock(side_effect=isfile_side_effect))
@patch('shutil.which', Mock(side_effect=which_side_effect))
@patch('sys.stderr', new_callable=StringIO)
@patch('sys.stdout', new_callable=StringIO)
class TestValidator(unittest.TestCase):

    def setUp(self):
        del files[:]
        del directories[:]
        del binaries[:]

    def test_empty_config(self, mock_out, mock_err):
        errors = schema({})
        output = "\n".join(errors)
        expected = list(sorted(['name', 'postgresql', 'restapi', 'scope'] + available_dcs))
        self.assertEqual(expected, parse_output(output))

    def test_complete_config(self, mock_out, mock_err):
        errors = schema(config)
        output = "\n".join(errors)
        self.assertEqual(['postgresql.bin_dir', 'raft.bind_addr', 'raft.self_addr'], parse_output(output))

    def test_bin_dir_is_file(self, mock_out, mock_err):
        files.append(config["postgresql"]["data_dir"])
        files.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = 'False:blabla'
        c["postgresql"]["listen"] = '*:543'
        c["etcd"]["hosts"] = ["127.0.0.1:2379", "1244.0.0.1:2379", "127.0.0.1:invalidport"]
        c["kubernetes"]["pod_ip"] = "127.0.0.1111"
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['etcd.hosts.1', 'etcd.hosts.2', 'kubernetes.pod_ip', 'postgresql.bin_dir',
                          'postgresql.data_dir', 'raft.bind_addr', 'raft.self_addr',
                          'restapi.connect_address'], parse_output(output))

    @patch('socket.inet_pton', Mock(), create=True)
    def test_bin_dir_is_empty(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        c = copy.deepcopy(config)
        c["restapi"]["connect_address"] = "127.0.0.1:8008"
        c["kubernetes"]["pod_ip"] = "::1"
        c["consul"]["host"] = "127.0.0.1:50000"
        c["etcd"]["host"] = "127.0.0.1:237"
        c["postgresql"]["listen"] = "127.0.0.1:5432"
        with patch('patroni.validator.open', mock_open(read_data='9')):
            errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['consul.host', 'etcd.host', 'postgresql.bin_dir', 'postgresql.data_dir', 'postgresql.listen',
                          'raft.bind_addr', 'raft.self_addr', 'restapi.connect_address'], parse_output(output))

    def test_bin_dir_is_empty_string_excutables_in_path(self, mock_out, mock_err):
        binaries.extend(required_binaries)
        c = copy.deepcopy(config)
        c["postgresql"]["bin_dir"] = ""
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['raft.bind_addr', 'raft.self_addr'], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_data_dir_contains_pg_version(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        directories.append(os.path.join(config["postgresql"]["data_dir"], "pg_wal"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        binaries.extend(required_binaries)
        c = copy.deepcopy(config)
        c["postgresql"]["bin_dir"] = ""  # to cover postgres --version call from PATH
        with patch('patroni.validator.open', mock_open(read_data='12')):
            errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['raft.bind_addr', 'raft.self_addr'], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_pg_version_missmatch(self, mock_out, mock_err):
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        directories.append(os.path.join(config["postgresql"]["data_dir"], "pg_wal"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        binaries.extend([os.path.join(config["postgresql"]["bin_dir"], i) for i in required_binaries])
        c = copy.deepcopy(config)
        c["etcd"]["hosts"] = []
        c["postgresql"]["listen"] = '127.0.0.2,*:543'
        with patch('patroni.validator.open', mock_open(read_data='11')):
            errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['etcd.hosts', 'postgresql.data_dir', 'postgresql.listen',
                          'raft.bind_addr', 'raft.self_addr'], parse_output(output))

    @patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 12.1"))
    def test_pg_wal_doesnt_exist(self, mock_out, mock_err):
        binaries.extend([os.path.join(config["postgresql"]["bin_dir"], i) for i in required_binaries])
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        files.append(os.path.join(config["postgresql"]["data_dir"], "global", "pg_control"))
        files.append(os.path.join(config["postgresql"]["data_dir"], "PG_VERSION"))
        c = copy.deepcopy(config)
        with patch('patroni.validator.open', mock_open(read_data='11')):
            errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['postgresql.data_dir', 'raft.bind_addr', 'raft.self_addr'], parse_output(output))

    def test_data_dir_is_empty_string(self, mock_out, mock_err):
        binaries.extend(required_binaries)
        directories.append(config["postgresql"]["data_dir"])
        directories.append(config["postgresql"]["bin_dir"])
        c = copy.deepcopy(config)
        c["kubernetes"] = False
        c["postgresql"]["pg_hba"] = ""
        c["postgresql"]["data_dir"] = ""
        c["postgresql"]["bin_dir"] = ""
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['kubernetes', 'postgresql.data_dir',
                          'postgresql.pg_hba', 'raft.bind_addr', 'raft.self_addr'], parse_output(output))

    def test_directory_contains(self, mock_out, mock_err):
        directories.extend([config_2["some_dir"], os.path.join(config_2["some_dir"], "very_interesting_subdir")])
        errors = schema2(config_2)
        output = "\n".join(errors)
        self.assertEqual(['some_dir'], parse_output(output))

    def test_validate_binary_name(self, mock_out, mock_err):
        r = copy.copy(required_binaries)
        r.remove('postgres')
        r.append('fake-postgres')
        binaries.extend(r)
        c = copy.deepcopy(config)
        c["postgresql"]["bin_name"] = {"postgres": "fake-postgres"}
        del c["postgresql"]["bin_dir"]
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['raft.bind_addr', 'raft.self_addr'], parse_output(output))

    def test_validate_binary_name_missing(self, mock_out, mock_err):
        r = copy.copy(required_binaries)
        r.remove('postgres')
        binaries.extend(r)
        c = copy.deepcopy(config)
        c["postgresql"]["bin_name"] = {"postgres": "fake-postgres"}
        del c["postgresql"]["bin_dir"]
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['postgresql.bin_dir', 'postgresql.bin_name.postgres', 'raft.bind_addr', 'raft.self_addr'],
                         parse_output(output))

    def test_validate_binary_name_empty_string(self, mock_out, mock_err):
        r = copy.copy(required_binaries)
        binaries.extend(r)
        c = copy.deepcopy(config)
        c["postgresql"]["bin_name"] = {"postgres": ""}
        del c["postgresql"]["bin_dir"]
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['postgresql.bin_dir', 'postgresql.bin_name.postgres', 'raft.bind_addr', 'raft.self_addr'],
                         parse_output(output))

    def test_one_of(self, _, __):
        c = copy.deepcopy(config)
        # Providing neither is fine
        del c["tags"]["nofailover"]
        errors = schema(c)
        self.assertNotIn("tags  Multiple of ('nofailover', 'failover_priority') provided", errors)
        # Just nofailover is fine
        c["tags"]["nofailover"] = False
        errors = schema(c)
        self.assertNotIn("tags  Multiple of ('nofailover', 'failover_priority') provided", errors)
        # Just failover_priority is fine
        del c["tags"]["nofailover"]
        c["tags"]["failover_priority"] = 1
        errors = schema(c)
        self.assertNotIn("tags  Multiple of ('nofailover', 'failover_priority') provided", errors)
        # Providing both is not fine
        c["tags"]["nofailover"] = False
        errors = schema(c)
        self.assertIn("tags  Multiple of ('nofailover', 'failover_priority') provided", errors)

    def test_failover_priority_int(self, *args):
        c = copy.deepcopy(config)
        del c["tags"]["nofailover"]
        c["tags"]["failover_priority"] = 'a string'
        errors = schema(c)
        self.assertIn('tags.failover_priority a string is not an integer', errors)
        c = copy.deepcopy(config)
        del c["tags"]["nofailover"]
        c["tags"]["failover_priority"] = -6
        errors = schema(c)
        self.assertIn('tags.failover_priority -6 didn\'t pass validation: Wrong value', errors)

    def test_json_log_format(self, *args):
        c = copy.deepcopy(config)
        c["log"]["type"] = "json"
        c["log"]["format"] = {"levelname": "level"}
        errors = schema(c)
        self.assertIn("log.format {'levelname': 'level'} didn't pass validation: Should be a string or a list", errors)

        c["log"]["format"] = []
        errors = schema(c)
        self.assertIn("log.format [] didn't pass validation: should contain at least one item", errors)

        c["log"]["format"] = [{"levelname": []}]
        errors = schema(c)
        self.assertIn("log.format [{'levelname': []}] didn't pass validation: "
                      "each item should be a string or a dictionary with string values", errors)

        c["log"]["format"] = [[]]
        errors = schema(c)
        self.assertIn("log.format [[]] didn't pass validation: "
                      "each item should be a string or a dictionary with string values", errors)

        c["log"]["format"] = ['foo']
        errors = schema(c)
        output = "\n".join(errors)
        self.assertEqual(['postgresql.bin_dir', 'raft.bind_addr', 'raft.self_addr'], parse_output(output))

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    def test_bound_port_checks_without_ignore(self, mock_out, mock_err):
        # When ignore_listen_port is False (default case), an error should be raised if the ports are already bound.
        c = copy.deepcopy(config)
        c['restapi']['listen'] = "127.0.0.1:8000"
        c['postgresql']['listen'] = "127.0.0.1:9000"
        c['raft']['self_addr'] = "127.0.0.2:9200"

        populate_validate_params(ignore_listen_port=False)

        errors = schema(c)
        output = "\n".join(errors)

        self.assertEqual(['postgresql.bin_dir', 'postgresql.listen',
                          'raft.bind_addr', 'restapi.listen'],
                         parse_output(output))

    @patch('socket.socket.connect_ex', Mock(return_value=0))
    def test_bound_port_checks_with_ignore(self, mock_out, mock_err):
        c = copy.deepcopy(config)
        c['restapi']['listen'] = "127.0.0.1:8000"
        c['postgresql']['listen'] = "127.0.0.1:9000"
        c['raft']['self_addr'] = "127.0.0.2:9200"
        c['raft']['bind_addr'] = "127.0.0.1:9300"

        # Case: When ignore_listen_port is True, error should NOT be raised
        # even if the ports are already bound.
        populate_validate_params(ignore_listen_port=True)

        errors = schema(c)
        output = "\n".join(errors)

        self.assertEqual(['postgresql.bin_dir'],
                         parse_output(output))
