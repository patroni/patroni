import os
import sys
import unittest

from mock import MagicMock, Mock, patch
from patroni.config import Config
from six.moves import builtins


class TestConfig(unittest.TestCase):

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('json.load', Mock(side_effect=Exception))
    @patch.object(builtins, 'open', MagicMock())
    def setUp(self):
        sys.argv = ['patroni.py']
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = 'restapi: {}\npostgresql: {data_dir: foo}'
        self.config = Config(None)

    def test_set_dynamic_configuration(self):
        with patch.object(Config, '_build_effective_configuration', Mock(side_effect=Exception)):
            self.assertIsNone(self.config.set_dynamic_configuration({'foo': 'bar'}))
        self.assertTrue(self.config.set_dynamic_configuration({'synchronous_mode': True, 'standby_cluster': {}}))

    def test_reload_local_configuration(self):
        os.environ.update({
            'PATRONI_NAME': 'postgres0',
            'PATRONI_NAMESPACE': '/patroni/',
            'PATRONI_SCOPE': 'batman2',
            'PATRONI_LOGLEVEL': 'ERROR',
            'PATRONI_LOG_LOGGERS': 'patroni.postmaster: WARNING, urllib3: DEBUG',
            'PATRONI_RESTAPI_USERNAME': 'username',
            'PATRONI_RESTAPI_PASSWORD': 'password',
            'PATRONI_RESTAPI_LISTEN': '0.0.0.0:8008',
            'PATRONI_RESTAPI_CONNECT_ADDRESS': '127.0.0.1:8008',
            'PATRONI_RESTAPI_CERTFILE': '/certfile',
            'PATRONI_RESTAPI_KEYFILE': '/keyfile',
            'PATRONI_POSTGRESQL_LISTEN': '0.0.0.0:5432',
            'PATRONI_POSTGRESQL_CONNECT_ADDRESS': '127.0.0.1:5432',
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
            'PATRONI_ZOOKEEPER_HOSTS': "'host1:2181','host2:2181'",
            'PATRONI_EXHIBITOR_HOSTS': 'host1,host2',
            'PATRONI_EXHIBITOR_PORT': '8181',
            'PATRONI_foo_HOSTS': '[host1,host2',  # Exception in parse_list
            'PATRONI_SUPERUSER_USERNAME': 'postgres',
            'PATRONI_SUPERUSER_PASSWORD': 'zalando',
            'PATRONI_REPLICATION_USERNAME': 'replicator',
            'PATRONI_REPLICATION_PASSWORD': 'rep-pass',
            'PATRONI_admin_PASSWORD': 'admin',
            'PATRONI_admin_OPTIONS': 'createrole,createdb'
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
