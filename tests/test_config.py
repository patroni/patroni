import os
import sys
import unittest
import io

from copy import deepcopy
from mock import MagicMock, Mock, patch
from patroni.config import Config, ConfigParseError, GlobalConfig


class TestConfig(unittest.TestCase):

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('json.load', Mock(side_effect=Exception))
    @patch('builtins.open', MagicMock())
    def setUp(self):
        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = 'restapi: {}\npostgresql: {data_dir: foo}'
        self.config = Config(None)

    def test_set_dynamic_configuration(self):
        with patch.object(Config, '_build_effective_configuration', Mock(side_effect=Exception)):
            self.assertFalse(self.config.set_dynamic_configuration({'foo': 'bar'}))
        self.assertTrue(self.config.set_dynamic_configuration({'standby_cluster': {}, 'postgresql': {
            'parameters': {'cluster_name': 1, 'hot_standby': 1, 'wal_keep_size': 1,
                           'track_commit_timestamp': 1, 'wal_level': 1, 'max_connections': '100'}}}))

    def test_reload_local_configuration(self):
        os.environ.update({
            'PATRONI_NAME': 'postgres0',
            'PATRONI_NAMESPACE': '/patroni/',
            'PATRONI_SCOPE': 'batman2',
            'PATRONI_LOGLEVEL': 'ERROR',
            'PATRONI_LOG_LOGGERS': 'patroni.postmaster: WARNING, urllib3: DEBUG',
            'PATRONI_LOG_FILE_NUM': '5',
            'PATRONI_CITUS_DATABASE': 'citus',
            'PATRONI_CITUS_GROUP': '0',
            'PATRONI_CITUS_HOST': '0',
            'PATRONI_RESTAPI_USERNAME': 'username',
            'PATRONI_RESTAPI_PASSWORD': 'password',
            'PATRONI_RESTAPI_LISTEN': '0.0.0.0:8008',
            'PATRONI_RESTAPI_CONNECT_ADDRESS': '127.0.0.1:8008',
            'PATRONI_RESTAPI_CERTFILE': '/certfile',
            'PATRONI_RESTAPI_KEYFILE': '/keyfile',
            'PATRONI_RESTAPI_ALLOWLIST_INCLUDE_MEMBERS': 'on',
            'PATRONI_POSTGRESQL_LISTEN': '0.0.0.0:5432',
            'PATRONI_POSTGRESQL_CONNECT_ADDRESS': '127.0.0.1:5432',
            'PATRONI_POSTGRESQL_PROXY_ADDRESS': '127.0.0.1:5433',
            'PATRONI_POSTGRESQL_DATA_DIR': 'data/postgres0',
            'PATRONI_POSTGRESQL_CONFIG_DIR': 'data/postgres0',
            'PATRONI_POSTGRESQL_PGPASS': '/tmp/pgpass0',
            'PATRONI_ETCD_HOST': '127.0.0.1:2379',
            'PATRONI_ETCD_URL': 'https://127.0.0.1:2379',
            'PATRONI_ETCD_PROXY': 'http://127.0.0.1:2379',
            'PATRONI_ETCD_SRV': 'test',
            'PATRONI_ETCD_CACERT': '/cacert',
            'PATRONI_ETCD_CERT': '/cert',
            'PATRONI_ETCD_KEY': '/key',
            'PATRONI_CONSUL_HOST': '127.0.0.1:8500',
            'PATRONI_CONSUL_REGISTER_SERVICE': 'on',
            'PATRONI_KUBERNETES_LABELS': 'a: b: c',
            'PATRONI_KUBERNETES_SCOPE_LABEL': 'a',
            'PATRONI_KUBERNETES_PORTS': '[{"name": "postgresql"}]',
            'PATRONI_KUBERNETES_RETRIABLE_HTTP_CODES': '401',
            'PATRONI_ZOOKEEPER_HOSTS': "'host1:2181','host2:2181'",
            'PATRONI_EXHIBITOR_HOSTS': 'host1,host2',
            'PATRONI_EXHIBITOR_PORT': '8181',
            'PATRONI_RAFT_PARTNER_ADDRS': "'host1:1234','host2:1234'",
            'PATRONI_foo_HOSTS': '[host1,host2',  # Exception in parse_list
            'PATRONI_SUPERUSER_USERNAME': 'postgres',
            'PATRONI_SUPERUSER_PASSWORD': 'zalando',
            'PATRONI_REPLICATION_USERNAME': 'replicator',
            'PATRONI_REPLICATION_PASSWORD': 'rep-pass',
            'PATRONI_admin_PASSWORD': 'admin',
            'PATRONI_admin_OPTIONS': 'createrole,createdb',
            'PATRONI_POSTGRESQL_BIN_POSTGRES': 'sergtsop'
        })
        config = Config('postgres0.yml')
        with patch.object(Config, '_load_config_file', Mock(return_value={'restapi': {}})):
            with patch.object(Config, '_build_effective_configuration', Mock(side_effect=Exception)):
                config.reload_local_configuration()
            self.assertTrue(config.reload_local_configuration())
            self.assertIsNone(config.reload_local_configuration())

    @patch('tempfile.mkstemp', Mock(return_value=[3000, 'blabla']))
    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.remove', Mock(side_effect=IOError))
    @patch('os.close', Mock(side_effect=IOError))
    @patch('os.chmod', Mock())
    @patch('shutil.move', Mock(return_value=None))
    @patch('json.dump', Mock())
    def test_save_cache(self):
        self.config.set_dynamic_configuration({'ttl': 30, 'postgresql': {'foo': 'bar'}})
        with patch('os.fdopen', Mock(side_effect=IOError)):
            self.config.save_cache()
        with patch('os.fdopen', MagicMock()):
            self.config.save_cache()

    def test_standby_cluster_parameters(self):
        dynamic_configuration = {
            'standby_cluster': {
                'create_replica_methods': ['wal_e', 'basebackup'],
                'host': 'localhost',
                'port': 5432
            }
        }
        self.config.set_dynamic_configuration(dynamic_configuration)
        for name, value in dynamic_configuration['standby_cluster'].items():
            self.assertEqual(self.config['standby_cluster'][name], value)

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isfile', Mock(side_effect=lambda fname: fname != 'postgres0'))
    @patch('os.path.isdir', Mock(return_value=True))
    @patch('os.listdir', Mock(return_value=['01-specific.yml', '00-base.yml']))
    def test_configuration_directory(self):
        def open_mock(fname, *args, **kwargs):
            if fname.endswith('00-base.yml'):
                return io.StringIO(
                    u'''
                    test: True
                    test2:
                      child-1: somestring
                      child-2: 5
                      child-3: False
                    test3: True
                    test4:
                     - abc: 3
                     - abc: 4
                    ''')
            elif fname.endswith('01-specific.yml'):
                return io.StringIO(
                    u'''
                    test: False
                    test2:
                      child-2: 10
                      child-3: !!null
                    test4:
                     - ab: 5
                    new-attr: True
                    ''')

        with patch('builtins.open', MagicMock(side_effect=open_mock)):
            config = Config('postgres0')
            self.assertEqual(config._local_configuration,
                             {'test': False, 'test2': {'child-1': 'somestring', 'child-2': 10},
                              'test3': True, 'test4': [{'ab': 5}], 'new-attr': True})

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isfile', Mock(return_value=False))
    @patch('os.path.isdir', Mock(return_value=False))
    def test_invalid_path(self):
        self.assertRaises(ConfigParseError, Config, 'postgres0')

    @patch.object(Config, 'get')
    @patch('patroni.config.logger')
    def test__validate_failover_tags(self, mock_logger, mock_get):
        """Ensures that only one of `nofailover` or `failover_priority` can be provided"""
        mock_logger.warning.reset_mock()
        config = Config("postgres0.yml")
        # Providing one of `nofailover` or `failover_priority` is fine
        just_nofailover = {"nofailover": True}
        mock_get.side_effect = [just_nofailover] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_not_called()
        just_failover_priority = {"failover_priority": 1}
        mock_get.side_effect = [just_failover_priority] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_not_called()
        # Providing both `nofailover` and `failover_priority` is fine if consistent
        consistent_false = {"nofailover": False, "failover_priority": 1}
        mock_get.side_effect = [consistent_false] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_not_called()
        consistent_true = {"nofailover": True, "failover_priority": 0}
        mock_get.side_effect = [consistent_true] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_not_called()
        # Providing both inconsistently should log a warning
        inconsistent_false = {"nofailover": False, "failover_priority": 0}
        mock_get.side_effect = [inconsistent_false] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_called_once_with(
            'Conflicting configuration between nofailover: %s and failover_priority: %s.'
            + ' Defaulting to nofailover: %s',
            False,
            0,
            False
        )
        mock_logger.warning.reset_mock()
        inconsistent_true = {"nofailover": True, "failover_priority": 1}
        mock_get.side_effect = [inconsistent_true] * 2
        self.assertIsNone(config._validate_failover_tags())
        mock_logger.warning.assert_called_once_with(
            'Conflicting configuration between nofailover: %s and failover_priority: %s.'
            + ' Defaulting to nofailover: %s',
            True,
            1,
            True
        )

    def test__process_postgresql_parameters(self):
        expected_params = {
            'f.oo': 'bar',  # not in ConfigHandler.CMDLINE_OPTIONS
            'max_connections': 100,  # IntValidator
            'wal_keep_size': '128MB',  # IntValidator
            'wal_level': 'hot_standby',  # EnumValidator
        }
        input_params = deepcopy(expected_params)

        input_params['max_connections'] = '100'
        self.assertEqual(self.config._process_postgresql_parameters(input_params), expected_params)

        expected_params['f.oo'] = input_params['f.oo'] = '100'
        self.assertEqual(self.config._process_postgresql_parameters(input_params), expected_params)

        input_params['wal_level'] = 'cold_standby'
        expected_params.pop('wal_level')
        self.assertEqual(self.config._process_postgresql_parameters(input_params), expected_params)

        input_params['max_connections'] = 10
        expected_params.pop('max_connections')
        self.assertEqual(self.config._process_postgresql_parameters(input_params), expected_params)

    def test__validate_and_adjust_timeouts(self):
        with patch('patroni.config.logger.warning') as mock_logger:
            self.config._validate_and_adjust_timeouts({'ttl': 15})
            self.assertEqual(mock_logger.call_args_list[0][0],
                             ("%s=%d can't be smaller than %d, adjusting...", 'ttl', 15, 20))
        with patch('patroni.config.logger.warning') as mock_logger:
            self.config._validate_and_adjust_timeouts({'loop_wait': 0})
            self.assertEqual(mock_logger.call_args_list[0][0],
                             ("%s=%d can't be smaller than %d, adjusting...", 'loop_wait', 0, 1))
        with patch('patroni.config.logger.warning') as mock_logger:
            self.config._validate_and_adjust_timeouts({'retry_timeout': 1})
            self.assertEqual(mock_logger.call_args_list[0][0],
                             ("%s=%d can't be smaller than %d, adjusting...", 'retry_timeout', 1, 3))
        with patch('patroni.config.logger.warning') as mock_logger:
            self.config._validate_and_adjust_timeouts({'ttl': 20, 'loop_wait': 11, 'retry_timeout': 5})
            self.assertEqual(mock_logger.call_args_list[0][0],
                             ('Violated the rule "loop_wait + 2*retry_timeout <= ttl", where ttl=%d '
                              'and retry_timeout=%d. Adjusting loop_wait from %d to %d', 20, 5, 11, 10))
        with patch('patroni.config.logger.warning') as mock_logger:
            self.config._validate_and_adjust_timeouts({'ttl': 20, 'loop_wait': 10, 'retry_timeout': 10})
            self.assertEqual(mock_logger.call_args_list[0][0],
                             ('Violated the rule "loop_wait + 2*retry_timeout <= ttl", where ttl=%d. Adjusting'
                              ' loop_wait from %d to %d and retry_timeout from %d to %d', 20, 10, 1, 10, 9))

    def test_global_config_is_synchronous_mode(self):
        # we should ignore synchronous_mode setting in a standby cluster
        config = {'standby_cluster': {'host': 'some_host'}, 'synchronous_mode': True}
        self.assertFalse(GlobalConfig(config).is_synchronous_mode)
