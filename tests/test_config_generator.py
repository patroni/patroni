import os
import psutil
import unittest
import yaml

from . import MockConnect, MockCursor
from copy import deepcopy
from unittest.mock import MagicMock, Mock, PropertyMock, mock_open as _mock_open, patch

from patroni.__main__ import main as _main
from patroni.config import Config
from patroni.config_generator import AbstractConfigGenerator, get_address, NO_VALUE_MSG
from patroni.log import PatroniLogger
from patroni.utils import patch_config

from . import psycopg_connect

HOSTNAME = 'test_hostname'
IP = '1.9.8.4'


def mock_open(*args, **kwargs):
    ret = _mock_open(*args, **kwargs)
    ret.return_value.__iter__ = lambda o: iter(o.readline, '')
    if not kwargs.get('read_data'):
        ret.return_value.readline = Mock(return_value=None)
    return ret


@patch('patroni.psycopg.connect', psycopg_connect)
@patch('builtins.open', MagicMock())
@patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 17.0"))
@patch('psutil.Process.exe', Mock(return_value='/bin/dir/from/running/postgres'))
@patch('psutil.Process.__init__', Mock(return_value=None))
@patch('patroni.config_generator.get_address', Mock(return_value=(HOSTNAME, IP)))
class TestGenerateConfig(unittest.TestCase):

    def setUp(self):
        os.environ['PATRONI_SCOPE'] = 'scope_from_env'
        os.environ['PATRONI_POSTGRESQL_BIN_DIR'] = '/bin/from/env'
        os.environ['PATRONI_SUPERUSER_USERNAME'] = 'su_user_from_env'
        os.environ['PATRONI_SUPERUSER_PASSWORD'] = 'su_pwd_from_env'
        os.environ['PATRONI_REPLICATION_USERNAME'] = 'repl_user_from_env'
        os.environ['PATRONI_REPLICATION_PASSWORD'] = 'repl_pwd_from_env'
        os.environ['PATRONI_REWIND_USERNAME'] = 'rewind_user_from_env'
        os.environ['PGUSER'] = 'pguser_from_env'
        os.environ['PGPASSWORD'] = 'pguser_pwd_from_env'
        os.environ['PATRONI_RESTAPI_CONNECT_ADDRESS'] = 'localhost:8080'
        os.environ['PATRONI_RESTAPI_LISTEN'] = 'localhost:8080'
        os.environ['PATRONI_POSTGRESQL_BIN_POSTGRES'] = 'custom_postgres_bin_from_env'

        self.environ = deepcopy(os.environ)

        dynamic_config = Config.get_default_config()
        dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
        del dynamic_config['standby_cluster']
        dynamic_config['postgresql']['parameters']['wal_keep_segments'] = 8
        dynamic_config['postgresql']['use_pg_rewind'] = True

        self.config = {
            'scope': self.environ['PATRONI_SCOPE'],
            'name': HOSTNAME,
            'log': {
                'type': PatroniLogger.DEFAULT_TYPE,
                'format': PatroniLogger.DEFAULT_FORMAT,
                'level': PatroniLogger.DEFAULT_LEVEL,
                'traceback_level': PatroniLogger.DEFAULT_TRACEBACK_LEVEL,
                'max_queue_size': PatroniLogger.DEFAULT_MAX_QUEUE_SIZE
            },
            'restapi': {
                'connect_address': self.environ['PATRONI_RESTAPI_CONNECT_ADDRESS'],
                'listen': self.environ['PATRONI_RESTAPI_LISTEN']
            },
            'bootstrap': {
                'dcs': dynamic_config
            },
            'postgresql': {
                'connect_address': IP + ':5432',
                'data_dir': NO_VALUE_MSG,
                'listen': IP + ':5432',
                'pg_hba': ['host all all all md5',
                           f'host replication {self.environ["PATRONI_REPLICATION_USERNAME"]} all md5'],
                'authentication': {'superuser': {'username': self.environ['PATRONI_SUPERUSER_USERNAME'],
                                                 'password': self.environ['PATRONI_SUPERUSER_PASSWORD']},
                                   'replication': {'username': self.environ['PATRONI_REPLICATION_USERNAME'],
                                                   'password': self.environ['PATRONI_REPLICATION_PASSWORD']},
                                   'rewind': {'username': self.environ['PATRONI_REWIND_USERNAME']}},
                'bin_dir': self.environ['PATRONI_POSTGRESQL_BIN_DIR'],
                'bin_name': {'postgres': self.environ['PATRONI_POSTGRESQL_BIN_POSTGRES']},
                'parameters': {'password_encryption': 'md5'}
            }
        }

    def _set_running_instance_config_vals(self):
        # values are taken from tests/__init__.py
        conf = {
            'scope': 'my_cluster',
            'bootstrap': {
                'dcs': {
                    'postgresql': {
                        'parameters': {
                            'max_connections': 42,
                            'max_locks_per_transaction': 73,
                            'max_replication_slots': 21,
                            'max_wal_senders': 37,
                            'wal_level': 'replica',
                            'wal_keep_segments': None
                        },
                        'use_pg_rewind': None
                    }
                }
            },
            'postgresql': {
                'connect_address': f'{IP}:bar',
                'listen': '6.6.6.6:1984',
                'data_dir': 'data',
                'bin_dir': '/bin/dir/from/running',
                'parameters': {
                    'archive_command': 'my archive command',
                    'hba_file': os.path.join('data', 'pg_hba.conf'),
                    'ident_file': os.path.join('data', 'pg_ident.conf'),
                    'password_encryption': None
                },
                'authentication': {
                    'superuser': {
                        'username': 'foobar',
                        'password': 'qwerty',
                        'channel_binding': 'prefer',
                        'gssencmode': 'prefer',
                        'sslmode': 'prefer',
                        'sslnegotiation': 'postgres'
                    },
                    'replication': {
                        'username': NO_VALUE_MSG,
                        'password': NO_VALUE_MSG
                    },
                    'rewind': None
                },
            },
            'tags': {
                'failover_priority': 1,
                'noloadbalance': False,
                'clonefrom': True,
                'nosync': False,
                'nostream': False
            }
        }
        patch_config(self.config, conf)

    def _get_running_instance_open_res(self):
        hba_content = '\n'.join(self.config['postgresql']['pg_hba'] + ['#host all all all md5',
                                                                       '  host all all all md5',
                                                                       '',
                                                                       'hostall all all md5'])
        ident_content = '\n'.join(['# something very interesting', '  '])

        self.config['postgresql']['pg_hba'] += ['host all all all md5']
        return [
            mock_open(read_data=hba_content)(),
            mock_open(read_data=ident_content)(),
            mock_open(read_data='1984')(),
            mock_open()()
        ]

    @patch('os.makedirs')
    def test_generate_sample_config_pre_13_dir_creation(self, mock_makedir):
        with patch('sys.argv', ['patroni.py', '--generate-sample-config', '/foo/bar.yml']), \
             patch('subprocess.check_output', Mock(return_value=b"postgres (PostgreSQL) 9.4.3")) as pg_bin_mock, \
             patch('builtins.open', _mock_open()) as mocked_file, \
             self.assertRaises(SystemExit) as e:
            _main()
            self.assertEqual(self.config, yaml.safe_load(mocked_file().write.call_args_list[0][0][0]))
        self.assertEqual(e.exception.code, 0)
        mock_makedir.assert_called_once()
        pg_bin_mock.assert_called_once_with([os.path.join(self.environ['PATRONI_POSTGRESQL_BIN_DIR'],
                                                          self.environ['PATRONI_POSTGRESQL_BIN_POSTGRES']),
                                             '--version'])

    @patch('os.makedirs', Mock())
    def test_generate_sample_config_17(self):
        conf = {
            'bootstrap': {
                'dcs': {
                    'postgresql': {
                        'parameters': {
                            'wal_keep_size': '128MB',
                            'wal_keep_segments': None
                        },
                    }
                }
            },
            'postgresql': {
                'parameters': {
                    'password_encryption': 'scram-sha-256'
                },
                'pg_hba': ['host all all all scram-sha-256',
                           f'host replication {self.environ["PATRONI_REPLICATION_USERNAME"]} all scram-sha-256'],
                'authentication': {
                    'rewind': {
                        'username': self.environ['PATRONI_REWIND_USERNAME'],
                        'password': NO_VALUE_MSG}
                },
            }
        }
        patch_config(self.config, conf)

        with patch('sys.argv', ['patroni.py', '--generate-sample-config', '/foo/bar.yml']), \
             patch('builtins.open', _mock_open()) as mocked_file, \
             self.assertRaises(SystemExit) as e:
            _main()
            self.assertEqual(self.config, yaml.safe_load(mocked_file().write.call_args_list[0][0][0]))
        self.assertEqual(e.exception.code, 0)

    @patch('os.makedirs', Mock())
    @patch('sys.stdout')
    def test_generate_config_running_instance_17(self, mock_sys_stdout):
        self._set_running_instance_config_vals()

        with patch('builtins.open', Mock(side_effect=self._get_running_instance_open_res())), \
             patch('sys.argv', ['patroni.py', '--generate-config',
                                '--dsn', 'host=foo port=bar user=foobar password=qwerty']), \
                self.assertRaises(SystemExit) as e:
            _main()
        self.assertEqual(e.exception.code, 0)
        self.assertEqual(self.config, yaml.safe_load(mock_sys_stdout.write.call_args_list[0][0][0]))

    @patch('os.makedirs', Mock())
    @patch('sys.stdout')
    def test_generate_config_running_instance_17_connect_from_env(self, mock_sys_stdout):
        self._set_running_instance_config_vals()
        # su auth params and connect host from env
        os.environ['PGCHANNELBINDING'] = \
            self.config['postgresql']['authentication']['superuser']['channel_binding'] = 'disable'
        os.environ['PGSSLNEGOTIATION'] = \
            self.config['postgresql']['authentication']['superuser']['sslnegotiation'] = 'direct'

        conf = {
            'scope': 'my_cluster',
            'bootstrap': {
                'dcs': {
                    'postgresql': {
                        'parameters': {
                            'max_connections': 42,
                            'max_locks_per_transaction': 73,
                            'max_replication_slots': 21,
                            'max_wal_senders': 37,
                            'wal_level': 'replica',
                            'wal_keep_segments': None
                        },
                        'use_pg_rewind': None
                    }
                }
            },
            'postgresql': {
                'connect_address': f'{IP}:1984',
                'authentication': {
                    'superuser': {
                        'username': self.environ['PGUSER'],
                        'password': self.environ['PGPASSWORD'],
                        'gssencmode': None,
                        'sslmode': None
                    },
                },
            }
        }
        patch_config(self.config, conf)

        with patch('builtins.open', Mock(side_effect=self._get_running_instance_open_res())), \
             patch('sys.argv', ['patroni.py', '--generate-config']), \
             patch.object(MockConnect, 'server_version', PropertyMock(return_value=170000)), \
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertEqual(e.exception.code, 0)
        self.assertEqual(self.config, yaml.safe_load(mock_sys_stdout.write.call_args_list[0][0][0]))

    def test_generate_config_running_instance_errors(self):
        # 1. Wrong DSN format
        with patch('sys.argv', ['patroni.py', '--generate-config', '--dsn', 'host:foo port:bar user:foobar']), \
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertIn('Failed to parse DSN string', e.exception.code)

        # 2. User is not a superuser
        with patch('sys.argv', ['patroni.py',
                                '--generate-config', '--dsn', 'host=foo port=bar user=foobar password=pwd_from_dsn']), \
             patch.object(MockCursor, 'rowcount', PropertyMock(return_value=0), create=True), \
             patch.object(MockConnect, 'get_parameter_status', Mock(return_value='off')), \
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertIn('The provided user does not have superuser privilege', e.exception.code)

        # 3. Error while calling postgres --version
        with patch('subprocess.check_output', Mock(side_effect=OSError)), \
             patch('sys.argv', ['patroni.py', '--generate-sample-config']), \
             self.assertRaises(SystemExit) as e:
            _main()
        self.assertIn('Failed to get postgres version:', e.exception.code)

        with patch('sys.argv', ['patroni.py', '--generate-config']):

            # 4. empty postmaster.pid
            with patch('builtins.open', Mock(side_effect=[mock_open(read_data='hba_content')(),
                                                          mock_open(read_data='ident_content')(),
                                                          mock_open(read_data='')()])), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Failed to obtain postmaster pid from postmaster.pid file', e.exception.code)

            # 5. Failed to open postmaster.pid
            with patch('builtins.open', Mock(side_effect=[mock_open(read_data='hba_content')(),
                                                          mock_open(read_data='ident_content')(),
                                                          OSError])), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Error while reading postmaster.pid file', e.exception.code)

            # 6. Invalid postmaster pid
            with patch('builtins.open', Mock(side_effect=[mock_open(read_data='hba_content')(),
                                                          mock_open(read_data='ident_content')(),
                                                          mock_open(read_data='1984')()])), \
                 patch('psutil.Process.__init__', Mock(return_value=None)), \
                 patch('psutil.Process.exe', Mock(side_effect=psutil.NoSuchProcess(1984))), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn("Obtained postmaster pid doesn't exist", e.exception.code)

            # 7. Failed to open pg_hba
            with patch('builtins.open', Mock(side_effect=OSError)), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Failed to read pg_hba.conf', e.exception.code)

            # 8. Failed to open pg_ident
            with patch('builtins.open', Mock(side_effect=[mock_open(read_data='hba_content')(), OSError])), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Failed to read pg_ident.conf', e.exception.code)

            # 9. Failed PG connection
            from . import psycopg
            with patch('patroni.psycopg.connect', side_effect=psycopg.Error), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Failed to establish PostgreSQL connection', e.exception.code)

            # 10. An unexpected error
            with patch.object(AbstractConfigGenerator, '__init__', side_effect=psycopg.Error), \
                 self.assertRaises(SystemExit) as e:
                _main()
            self.assertIn('Unexpected exception', e.exception.code)

    def test_get_address(self):
        with patch('socket.getaddrinfo', Mock(side_effect=[[(2, 1, 6, '', ('127.0.0.1', 0))],
                                                           Exception])), \
             patch('socket.gethostname', Mock(return_value='foo')), \
             patch('logging.warning') as mock_warning:
            self.assertEqual(get_address(), ('foo', '127.0.0.1'))
            self.assertEqual(get_address(), (NO_VALUE_MSG, NO_VALUE_MSG))
            self.assertIn('Failed to obtain address: %r', mock_warning.call_args_list[0][0])
