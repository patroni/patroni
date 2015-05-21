import os
import psycopg2
import unittest
import shutil

from helpers.etcd import Cluster, Member
from helpers.postgresql import Postgresql


def nop(*args, **kwargs):
    pass


def os_system(cmd):
    return 0


def false(*args, **kwargs):
    return False


class MockCursor:

    def __init__(self):
        self.current = 0
        self.results = []

    def execute(self, sql, *params):
        if sql.startswith('blabla'):
            raise psycopg2.OperationalError()
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla'), ('foobar')]
        elif sql.startswith('SELECT pg_current_xlog_location()'):
            self.results = [(0,)]
        elif sql.startswith('SELECT %s - (pg_last_xlog_replay_location()'):
            self.results = [(0,)]
        elif sql.startswith('SELECT pg_last_xlog_replay_location()'):
            self.results = [(0,)]
        elif sql.startswith('SELECT pg_is_in_recovery()'):
            self.results = [(False, None, None, None, None, None, None, None, None, None)]
        else:
            self.results = [(None, None, None, None, None, None, None, None, None, None)]

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

    def cursor(self):
        return MockCursor()

    def close(self):
        if not self.autocommit:
            raise psycopg2.OperationalError()


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
        os.system = os_system
        shutil.copy = nop
        self.p = Postgresql({'name': 'test0', 'data_dir': 'data/test0', 'listen': '127.0.0.1, 127.0.0.2:5432',
                             'connect_address': '127.0.0.2:5432', 'superuser': {'password': ''},
                             'admin': {'username': 'admin', 'password': 'admin'}, 'replication': {
                             'username': 'replicator', 'password': 'rep-pass', 'network': '127.0.0.1/32'},
                             'parameters': {'foo': 'bar'}, 'recovery_conf': {'foo': 'bar'}})
        psycopg2.connect = psycopg2_connect
        if not os.path.exists(self.p.data_dir):
            os.makedirs(self.p.data_dir)
        self.leader = Member('leader', 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres', 28)

    def tear_down(self):
        shutil.rmtree('data')

    def test_data_directory_empty(self):
        self.assertTrue(self.p.data_directory_empty())

    def test_initialize(self):
        self.assertTrue(self.p.initialize())
        self.assertTrue(os.path.exists(os.path.join(self.p.data_dir, 'pg_hba.conf')))

    def test_start(self):
        self.assertFalse(self.p.start())
        self.p.is_running = is_running
        with open(os.path.join(self.p.data_dir, 'postmaster.pid'), 'w'):
            pass
        self.assertTrue(self.p.start())

    def test_sync_from_leader(self):
        self.assertTrue(self.p.sync_from_leader(self.leader))

    def test_follow_the_leader(self):
        self.p.demote(self.leader)
        self.p.follow_the_leader(None)
        self.p.demote(self.leader)
        self.p.follow_the_leader(self.leader)
        self.p.follow_the_leader(Member('leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres', 28))

    def test_create_replication_slots(self):
        self.p.start()
        self.p.create_replication_slots('qaz')

    def test_query(self):
        self.p.query('select 1')
        self.p.conn.autocommit = False
        self.assertRaises(psycopg2.OperationalError, self.p.query, 'blabla')
        self.p.query('select %s', 1)

    def test_is_healthiest_node(self):
        leader = Member('leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres', 28)
        me = Member('test0', 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres', 28)
        other = Member('test1', 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres', 28)
        cluster = Cluster(leader, 0, [leader, me, other])
        self.assertTrue(self.p.is_healthiest_node(cluster))
        self.p.config['maximum_lag_on_failover'] = -1
        self.assertFalse(self.p.is_healthiest_node(cluster))

    def test_is_leader(self):
        self.assertTrue(self.p.is_leader())

    def test_reload(self):
        self.assertTrue(self.p.reload())

    def test_is_healthy(self):
        self.assertTrue(self.p.is_healthy())
        self.p.is_running = is_running
        self.assertFalse(self.p.is_healthy())

    def test_promote(self):
        self.assertTrue(self.p.promote())

    def test_last_operation(self):
        self.assertEquals(self.p.last_operation(), 0)
