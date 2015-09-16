import os
import psycopg2
import shutil
import subprocess
import unittest

from patroni.dcs import Cluster, Leader, Member
from patroni.postgresql import Postgresql
from test_ha import true, false


def nop(*args, **kwargs):
    pass


def subprocess_call(cmd, shell=False, env=None):
    return 0


def false(*args, **kwargs):
    return False


class MockCursor:

    def __init__(self):
        self.closed = False
        self.results = []

    def execute(self, sql, *params):
        if sql.startswith('blabla'):
            raise psycopg2.OperationalError()
        elif sql.startswith('InterfaceError'):
            raise psycopg2.InterfaceError()
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla',), ('foobar',)]
        elif sql.startswith('SELECT pg_xlog_location_diff'):
            self.results = [(0,)]
        elif sql == 'SELECT pg_is_in_recovery()':
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


class MockConnect:

    def __init__(self):
        self.autocommit = False
        self.closed = 0

    def cursor(self):
        return MockCursor()

    def close(self):
        pass


def psycopg2_connect(*args, **kwargs):
    return MockConnect()


def is_running():
    return False


class TestPostgresql(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestPostgresql, self).__init__(method_name)

    def set_up(self):
        subprocess.call = subprocess_call
        shutil.copy = nop
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
        psycopg2.connect = psycopg2_connect
        if not os.path.exists(self.p.data_dir):
            os.makedirs(self.p.data_dir)
        self.leadermem = Member(0, 'leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres', None, None, 28)
        self.leader = Leader(-1, None, 28, self.leadermem)
        self.other = Member(0, 'test1', 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres', None, None, 28)
        self.me = Member(0, 'test0', 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres', None, None, 28)

    def tear_down(self):
        shutil.rmtree('data')

    def mock_query(self, p):
        raise psycopg2.OperationalError("not supported")

    def test_data_directory_empty(self):
        self.assertTrue(self.p.data_directory_empty())

    def test_initialize(self):
        self.assertTrue(self.p.initialize())
        self.assertTrue(os.path.exists(os.path.join(self.p.data_dir, 'pg_hba.conf')))

    def test_start_stop(self):
        self.assertFalse(self.p.start())
        self.p.is_running = is_running
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

    def test_create_connection_users(self):
        cfg = self.p.config
        cfg['superuser']['username'] = 'test'
        p = Postgresql(cfg)
        p.create_connection_users()

    def test_create_replication_slots(self):
        self.p.start()
        cluster = Cluster(True, self.leader, 0, [self.me, self.other, self.leadermem])
        self.p.create_replication_slots(cluster)

    def test_query(self):
        self.p.query('select 1')
        self.assertRaises(psycopg2.InterfaceError, self.p.query, 'InterfaceError')
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')
        self.p._connection.closed = 2
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')
        self.p._connection.closed = 2
        self.p.disconnect = false
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')

    def test_is_leader(self):
        self.p.is_promoted = True
        self.assertTrue(self.p.is_leader())
        self.assertFalse(self.p.is_promoted)

    def test_reload(self):
        self.assertTrue(self.p.reload())

    def test_is_healthy(self):
        self.assertTrue(self.p.is_healthy())
        self.p.is_running = is_running
        self.assertFalse(self.p.is_healthy())

    def test_promote(self):
        self.assertTrue(self.p.promote())

    def test_last_operation(self):
        self.assertEquals(self.p.last_operation(), '0')

    def test_non_existing_callback(self):
        self.assertFalse(self.p.call_nowait('foobar'))

    def test_is_leader_exception(self):
        self.p.start()
        self.p.query = self.mock_query
        self.assertTrue(self.p.stop())

    def test_check_replication_lag(self):
        self.assertTrue(self.p.check_replication_lag(0))

    def test_move_data_directory(self):
        self.p.is_running = is_running
        os.rename = nop
        os.path.isdir = true
        self.p.move_data_directory()
