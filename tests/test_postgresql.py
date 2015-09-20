import os
import psycopg2
import shutil
import unittest

from mock import Mock, patch
from patroni.dcs import Cluster, Leader, Member
from patroni.exceptions import PostgresConnectionException
from patroni.postgresql import Postgresql
from patroni.utils import RetryFailedError
from test_ha import false


class MockCursor:

    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.results = []

    def execute(self, sql, *params):
        if sql.startswith('blabla') or sql == 'CHECKPOINT':
            raise psycopg2.OperationalError()
        elif sql.startswith('RetryFailedError'):
            raise RetryFailedError('retry')
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla',), ('foobar',)]
        elif sql.startswith('SELECT pg_current_xlog_location()'):
            self.results = [(0,)]
        elif sql.startswith('SELECT pg_is_in_recovery(), %s'):
            if params[0][0] == 1:
                raise psycopg2.OperationalError()
            elif params[0][0] == 2:
                self.results = [(True, -1)]
            else:
                self.results = [(False, 0)]
        elif sql.startswith('SELECT pg_xlog_location_diff'):
            self.results = [(0,)]
        elif sql.startswith('SELECT pg_is_in_recovery()'):
            self.results = [(False, )]
        elif sql.startswith('SELECT to_char(pg_postmaster_start_time'):
            self.results = [('', True, '', '', '', False)]
        else:
            self.results = [(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )]

    def fetchone(self):
        return self.results[0]

    def close(self):
        pass

    def __iter__(self):
        for i in self.results:
            yield i

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnect(Mock):

    autocommit = False
    closed = 0

    def cursor(self):
        return MockCursor(self)


def psycopg2_connect(*args, **kwargs):
    return MockConnect()


@patch('subprocess.call', Mock(return_value=0))
@patch('shutil.copy', Mock())
@patch('psycopg2.connect', psycopg2_connect)
class TestPostgresql(unittest.TestCase):

    def setUp(self):
        self.p = Postgresql({'name': 'test0', 'scope': 'batman', 'data_dir': 'data/test0',
                             'listen': '127.0.0.1, *:5432', 'connect_address': '127.0.0.2:5432',
                             'pg_hba': ['hostssl all all 0.0.0.0/0 md5', 'host all all 0.0.0.0/0 md5'],
                             'superuser': {'password': ''},
                             'admin': {'username': 'admin', 'password': 'admin'},
                             'replication': {'username': 'replicator',
                                             'password': 'rep-pass',
                                             'network': '127.0.0.1/32'},
                             'parameters': {'foo': 'bar'}, 'recovery_conf': {'foo': 'bar'},
                             'callbacks': {'on_start': 'true', 'on_stop': 'true',
                                           'on_restart': 'true', 'on_role_change': 'true',
                                           'on_reload': 'true'
                                           },
                             'restore': 'true'})
        if not os.path.exists(self.p.data_dir):
            os.makedirs(self.p.data_dir)
        self.leadermem = Member(0, 'leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres', None, None, 28)
        self.leader = Leader(-1, None, 28, self.leadermem)
        self.other = Member(0, 'test1', 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres', None, None, 28)
        self.me = Member(0, 'test0', 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres', None, None, 28)

    def tearDown(self):
        shutil.rmtree('data')

    def test_data_directory_empty(self):
        self.assertTrue(self.p.data_directory_empty())

    def test_initialize(self):
        self.assertTrue(self.p.initialize())
        self.assertTrue(os.path.exists(os.path.join(self.p.data_dir, 'pg_hba.conf')))

    def test_start_stop(self):
        self.assertFalse(self.p.start())
        self.p.is_running = false
        with open(os.path.join(self.p.data_dir, 'postmaster.pid'), 'w'):
            pass
        self.assertTrue(self.p.start())
        self.assertTrue(self.p.stop())

    def test_sync_from_leader(self):
        self.assertTrue(self.p.sync_from_leader(self.leader))

    def test_follow_the_leader(self):
        self.p.demote(self.leader)
        self.p.follow_the_leader(None)
        self.p.demote(self.leader)
        self.p.follow_the_leader(self.leader)
        self.p.follow_the_leader(Leader(-1, None, 28, self.other))

    def test_create_replica(self):
        self.p.delete_trigger_file = Mock(side_effect=OSError())
        self.assertEquals(self.p.create_replica({'host': '', 'port': '', 'user': ''}, ''), 1)

    def test_create_connection_users(self):
        cfg = self.p.config
        cfg['superuser']['username'] = 'test'
        p = Postgresql(cfg)
        p.create_connection_users()

    def test_sync_replication_slots(self):
        self.p.start()
        cluster = Cluster(True, self.leader, 0, [self.me, self.other, self.leadermem])
        self.p.sync_replication_slots(cluster)

    @patch.object(MockConnect, 'closed', 2)
    def test__query(self):
        self.assertRaises(PostgresConnectionException, self.p._query, 'blabla')

    def test_query(self):
        self.p.query('select 1')
        self.assertRaises(PostgresConnectionException, self.p.query, 'RetryFailedError')
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')

    def test_is_healthiest_node(self):
        cluster = Cluster(True, self.leader, 0, [self.me, self.other, self.leadermem])
        self.assertTrue(self.p.is_healthiest_node(cluster))
        self.p.is_leader = false
        self.assertFalse(self.p.is_healthiest_node(cluster))
        self.p.xlog_position = lambda: 1
        self.assertTrue(self.p.is_healthiest_node(cluster))
        self.p.xlog_position = lambda: 2
        self.assertFalse(self.p.is_healthiest_node(cluster))
        self.p.config['maximum_lag_on_failover'] = -3
        self.assertFalse(self.p.is_healthiest_node(cluster))

    def test_reload(self):
        self.assertTrue(self.p.reload())

    def test_is_healthy(self):
        self.assertTrue(self.p.is_healthy())
        self.p.is_running = false
        self.assertFalse(self.p.is_healthy())

    def test_promote(self):
        self.assertTrue(self.p.promote())
        self.assertTrue(self.p.promote())

    def test_last_operation(self):
        self.assertEquals(self.p.last_operation(), '0')

    @patch('subprocess.Popen', Mock(side_effect=OSError()))
    def test_call_nowait(self):
        self.assertFalse(self.p.call_nowait('on_start'))

    def test_non_existing_callback(self):
        self.assertFalse(self.p.call_nowait('foobar'))

    def test_is_leader_exception(self):
        self.p.start()
        self.p.query = Mock(side_effect=psycopg2.OperationalError("not supported"))
        self.assertTrue(self.p.stop())

    @patch('os.rename', Mock())
    @patch('os.path.isdir', Mock(return_value=True))
    def test_move_data_directory(self):
        self.p.is_running = false
        self.p.move_data_directory()
        with patch('os.rename', Mock(side_effect=OSError())):
            self.p.move_data_directory()
