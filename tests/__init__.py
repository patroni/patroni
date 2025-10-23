import datetime
import os
import shutil
import unittest

from unittest.mock import Mock, patch, PropertyMock

import urllib3

import patroni.psycopg as psycopg

from patroni.dcs import Leader, Member
from patroni.postgresql import Postgresql
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.misc import PostgresqlState
from patroni.postgresql.mpp import get_mpp
from patroni.utils import RetryFailedError, tzutc


class SleepException(Exception):
    pass


mock_available_gucs = PropertyMock(return_value={
    'cluster_name', 'constraint_exclusion', 'force_parallel_mode', 'hot_standby', 'listen_addresses', 'max_connections',
    'max_locks_per_transaction', 'max_prepared_transactions', 'max_replication_slots', 'max_stack_depth',
    'max_wal_senders', 'max_worker_processes', 'port', 'search_path', 'shared_preload_libraries',
    'stats_temp_directory', 'synchronous_standby_names', 'track_commit_timestamp', 'unix_socket_directories',
    'vacuum_cost_delay', 'vacuum_cost_limit', 'wal_keep_size', 'wal_level', 'wal_log_hints', 'zero_damaged_pages',
    'autovacuum', 'wal_segment_size', 'wal_block_size', 'shared_buffers', 'wal_buffers', 'fork_specific_param',
})

GET_PG_SETTINGS_RESULT = [
    ('wal_segment_size', '2048', '8kB', 'integer', 'internal'),
    ('wal_block_size', '8192', None, 'integer', 'internal'),
    ('shared_buffers', '16384', '8kB', 'integer', 'postmaster'),
    ('wal_buffers', '-1', '8kB', 'integer', 'postmaster'),
    ('max_connections', '100', None, 'integer', 'postmaster'),
    ('max_prepared_transactions', '200', None, 'integer', 'postmaster'),
    ('max_worker_processes', '8', None, 'integer', 'postmaster'),
    ('max_locks_per_transaction', '64', None, 'integer', 'postmaster'),
    ('max_wal_senders', '5', None, 'integer', 'postmaster'),
    ('search_path', 'public', None, 'string', 'user'),
    ('port', '5432', None, 'integer', 'postmaster'),
    ('listen_addresses', '127.0.0.2, 127.0.0.3', None, 'string', 'postmaster'),
    ('autovacuum', 'on', None, 'bool', 'sighup'),
    ('unix_socket_directories', '/tmp', None, 'string', 'postmaster'),
    ('shared_preload_libraries', 'citus', None, 'string', 'postmaster'),
    ('wal_keep_size', '128', 'MB', 'integer', 'sighup'),
    ('cluster_name', 'batman', None, 'string', 'postmaster'),
    ('vacuum_cost_delay', '200', 'ms', 'real', 'user'),
    ('vacuum_cost_limit', '-1', None, 'integer', 'user'),
    ('max_stack_depth', '2048', 'kB', 'integer', 'superuser'),
    ('constraint_exclusion', '', None, 'enum', 'user'),
    ('force_parallel_mode', '1', None, 'enum', 'user'),
    ('zero_damaged_pages', 'off', None, 'bool', 'superuser'),
    ('stats_temp_directory', '/tmp', None, 'string', 'sighup'),
    ('track_commit_timestamp', 'off', None, 'bool', 'postmaster'),
    ('wal_log_hints', 'on', None, 'bool', 'postmaster'),
    ('hot_standby', 'on', None, 'bool', 'postmaster'),
    ('max_replication_slots', '5', None, 'integer', 'postmaster'),
    ('wal_level', 'logical', None, 'enum', 'postmaster'),
]


class MockResponse(object):

    def __init__(self, status_code=200):
        self.status_code = status_code
        self.headers = {'content-type': 'json', 'lsn': 100}
        self.content = '{}'
        self.reason = 'Not Found'

    @property
    def data(self):
        return self.content.encode('utf-8')

    @property
    def status(self):
        return self.status_code

    @staticmethod
    def getheader(*args):
        return ''


def requests_get(url, method='GET', endpoint=None, data='', **kwargs):
    members = '[{"id":14855829450254237642,"peerURLs":["http://localhost:2380","http://localhost:7001"],' +\
              '"name":"default","clientURLs":["http://localhost:2379","http://localhost:4001"]}]'
    response = MockResponse()
    if endpoint == 'failsafe':
        response.content = 'Accepted'
    elif url.startswith('http://local'):
        raise urllib3.exceptions.HTTPError()
    elif ':8011/patroni' in url:
        response.content = '{"role": "replica", "wal": {"received_location": 0}, "tags": {}}'
    elif url.endswith('/members'):
        response.content = '[{}]' if url.startswith('http://error') else members
    elif url.startswith('http://exhibitor'):
        response.content = '{"servers":["127.0.0.1","127.0.0.2","127.0.0.3"],"port":2181}'
    elif url.endswith(':8011/reinitialize'):
        if ' false}' in data:
            response.status_code = 503
            response.content = 'restarting after failure already in progress'
    else:
        response.status_code = 404
    return response


class MockPostmaster(object):
    def __init__(self, pid=1):
        self.is_running = Mock(return_value=self)
        self.wait_for_user_backends_to_close = Mock()
        self.signal_stop = Mock(return_value=None)
        self.wait = Mock()
        self.signal_kill = Mock(return_value=False)


class MockCursor(object):

    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.rowcount = 0
        self.results = []
        self.description = [Mock()]

    def execute(self, sql, *params):
        if isinstance(sql, bytes):
            sql = sql.decode('utf-8')
        if sql.startswith('blabla'):
            raise psycopg.ProgrammingError()
        elif sql == 'CHECKPOINT' or sql.startswith('SELECT pg_catalog.pg_create_'):
            raise psycopg.OperationalError()
        elif sql.startswith('RetryFailedError'):
            raise RetryFailedError('retry')
        elif sql.startswith('SELECT slot_name, catalog_xmin'):
            self.results = [('postgresql0', 100), ('ls', 100)]
        elif sql.startswith('SELECT slot_name, slot_type, datname, plugin, catalog_xmin'):
            self.results = [('ls', 'logical', 'a', 'b', 100, 500, b'123456')]
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla', 'physical', 1, 12345),
                            ('foobar', 'physical', 1, 12345),
                            ('ls', 'logical', 1, 499, 'b', 'a', 5, 100, 500)]
        elif sql.startswith('WITH slots AS (SELECT slot_name, active'):
            self.results = [(False, True)] if self.rowcount == 1 else []
        elif sql.startswith('SELECT CASE WHEN pg_catalog.pg_is_in_recovery()'):
            self.results = [(1, 2, 1, 0, False, 1, 1, 1, None, None, 'streaming', '',
                             [{"slot_name": "ls", "confirmed_flush_lsn": 12345, "restart_lsn": 12344}],
                             'on', 'n1', None)]
        elif sql.startswith('SELECT pg_catalog.pg_is_in_recovery()'):
            self.results = [(False, 2)]
        elif sql.startswith('SELECT pg_catalog.pg_postmaster_start_time'):
            self.results = [(datetime.datetime.now(tzutc),)]
        elif sql.endswith('AND pending_restart'):
            self.results = []
        elif sql.startswith('SELECT name, pg_catalog.current_setting(name) FROM pg_catalog.pg_settings'):
            self.results = [('data_directory', 'data'),
                            ('hba_file', os.path.join('data', 'pg_hba.conf')),
                            ('ident_file', os.path.join('data', 'pg_ident.conf')),
                            ('max_connections', 42),
                            ('max_locks_per_transaction', 73),
                            ('max_prepared_transactions', 0),
                            ('max_replication_slots', 21),
                            ('max_wal_senders', 37),
                            ('track_commit_timestamp', 'off'),
                            ('wal_level', 'replica'),
                            ('listen_addresses', '6.6.6.6'),
                            ('port', 1984),
                            ('archive_command', 'my archive command'),
                            ('cluster_name', 'my_cluster')]
        elif sql.startswith('SELECT name, setting'):
            self.results = GET_PG_SETTINGS_RESULT
        elif sql.startswith('IDENTIFY_SYSTEM'):
            self.results = [('1', 3, '0/402EEC0', '')]
        elif sql.startswith('TIMELINE_HISTORY '):
            self.results = [('', b'x\t0/40159C0\tno recovery target specified\n\n'
                                 b'1\t0/40159C0\tno recovery target specified\n\n'
                                 b'2\t0/402DD98\tno recovery target specified\n\n'
                                 b'3\t0/403DD98\tno recovery target specified\n')]
        elif sql.startswith('SELECT pg_catalog.citus_add_node'):
            self.results = [(2,)]
        elif sql.startswith('SELECT groupid, nodename'):
            self.results = [(0, 'host1', 5432, 'primary', 1),
                            (0, '127.0.0.1', 5436, 'secondary', 2),
                            (1, 'host4', 5432, 'primary', 3),
                            (1, '127.0.0.1', 5437, 'secondary', 4),
                            (1, '127.0.0.1', 5438, 'secondary', 5)]
        else:
            self.results = [(None, None, None, None, None, None, None, None, None, None)]
        self.rowcount = len(self.results)

    def fetchone(self):
        return self.results[0]

    def fetchall(self):
        return self.results

    def __iter__(self):
        for i in self.results:
            yield i

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnect(object):

    pgconn = None
    connection = None
    server_version = 99999
    autocommit = False
    closed = 0

    def get_parameter_status(self, param_name):
        if param_name == 'is_superuser':
            return 'on'
        return '0'

    def cursor(self):
        return MockCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @staticmethod
    def close():
        pass


def psycopg_connect(*args, **kwargs):
    return MockConnect()


class PostgresInit(unittest.TestCase):
    _PARAMETERS = {'wal_level': 'hot_standby', 'max_replication_slots': 5, 'f.oo': 'bar',
                   'search_path': 'public', 'hot_standby': 'on', 'max_wal_senders': 5,
                   'wal_keep_segments': 8, 'wal_log_hints': 'on', 'max_locks_per_transaction': 64,
                   'max_worker_processes': 8, 'max_connections': 100, 'max_prepared_transactions': 200,
                   'track_commit_timestamp': 'off', 'unix_socket_directories': '/tmp',
                   'trigger_file': 'bla', 'stats_temp_directory': '/tmp', 'zero_damaged_pages': 'off',
                   'force_parallel_mode': '1', 'constraint_exclusion': '',
                   'max_stack_depth': 2048, 'vacuum_cost_limit': -1, 'vacuum_cost_delay': 200}

    @patch('patroni.psycopg._connect', psycopg_connect)
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(ConfigHandler, 'write_postgresql_conf', Mock())
    @patch.object(ConfigHandler, 'replace_pg_hba', Mock())
    @patch.object(ConfigHandler, 'replace_pg_ident', Mock())
    @patch.object(Postgresql, 'get_postgres_role_from_data_directory', Mock(return_value='primary'))
    def setUp(self):
        self._tmp_dir = 'data'
        data_dir = os.path.join(self._tmp_dir, 'test0')
        config = {'name': 'postgresql0', 'scope': 'batman', 'data_dir': data_dir,
                  'config_dir': data_dir, 'retry_timeout': 10,
                  'krbsrvname': 'postgres', 'pgpass': os.path.join(data_dir, 'pgpass0'),
                  'listen': '127.0.0.2, 127.0.0.3:5432',
                  'connect_address': '127.0.0.2:5432', 'proxy_address': '127.0.0.2:5433',
                  'authentication': {'superuser': {'username': 'foo', 'password': 'test'},
                                     'replication': {'username': '', 'password': 'rep-pass'},
                                     'rewind': {'username': 'rewind', 'password': 'test'}},
                  'remove_data_directory_on_rewind_failure': True,
                  'use_pg_rewind': True, 'pg_ctl_timeout': 'bla', 'use_unix_socket': True,
                  'parameters': self._PARAMETERS,
                  'recovery_conf': {'foo': 'bar'},
                  'pg_hba': ['host all all 0.0.0.0/0 md5'],
                  'pg_ident': ['krb realm postgres'],
                  'callbacks': {'on_start': 'true', 'on_stop': 'true', 'on_reload': 'true',
                                'on_restart': 'true', 'on_role_change': 'true'},
                  'citus': {'group': 0, 'database': 'citus'}}
        self.p = Postgresql(config, get_mpp(config))


class BaseTestPostgresql(PostgresInit):

    @patch('time.sleep', Mock())
    def setUp(self):
        super(BaseTestPostgresql, self).setUp()

        if not os.path.exists(self.p.data_dir):
            os.makedirs(self.p.data_dir)

        self.leadermem = Member(0, 'leader', 28, {'xlog_location': 100, 'state': PostgresqlState.RUNNING,
                                                  'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres'})
        self.leader = Leader(-1, 28, self.leadermem)
        self.other = Member(0, 'test-1', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres',
                                              'state': PostgresqlState.RUNNING, 'tags': {'replicatefrom': 'leader'}})
        self.me = Member(0, 'test0', 28, {
            'state': PostgresqlState.RUNNING, 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres'})

    def tearDown(self):
        if os.path.exists(self.p.data_dir):
            shutil.rmtree(self.p.data_dir)

        if not os.listdir(self._tmp_dir):
            shutil.rmtree(self._tmp_dir)
