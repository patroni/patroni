import unittest

from mock import Mock, patch
from patroni.dcs import Cluster, DCSError, Leader, Member
from patroni.etcd import Client, Etcd
from patroni.exceptions import PostgresException
from patroni.ha import Ha
from test_etcd import socket_getaddrinfo, etcd_read, etcd_write


def true(*args, **kwargs):
    return True


def false(*args, **kwargs):
    return False


def get_cluster(initialize, leader):
    return Cluster(initialize, leader, None, None)


def get_cluster_not_initialized_without_leader():
    return get_cluster(None, None)


def get_cluster_initialized_without_leader():
    return get_cluster(True, None)


def get_cluster_initialized_with_leader():
    return get_cluster(True, Leader(0, 0, 0,
                       Member(0, 'leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres',
                              None, None, 28)))


class MockPostgresql(Mock):

    name = 'postgresql0'
    role = 'replica'

    def is_healthy(self):
        return True

    def start(self):
        return True

    def is_healthiest_node(self, members):
        return True

    def is_leader(self):
        return True

    def last_operation(self):
        return 0

    def data_directory_empty(self):
        return False

    def bootstrap(self, *args, **kwargs):
        return True


class TestHa(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch.object(Client, 'machines')
    def setUp(self, mock_machines):
        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        self.p = MockPostgresql()
        self.e = Etcd('foo', {'ttl': 30, 'host': 'ok:2379', 'scope': 'test'})
        self.e.client.read = etcd_read
        self.e.client.write = etcd_write
        self.ha = Ha(self.p, self.e)
        self.ha.load_cluster_from_dcs()
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.ha.load_cluster_from_dcs = Mock()

    def test_load_cluster_from_dcs(self):
        ha = Ha(self.p, self.e)
        ha.load_cluster_from_dcs()
        self.e.get_cluster = get_cluster_not_initialized_without_leader
        ha.load_cluster_from_dcs()

    def test_start_as_slave(self):
        self.p.is_healthy = false
        self.assertEquals(self.ha.run_cycle(), 'started as a secondary')

    def test_start_as_readonly(self):
        self.ha.cluster.is_unlocked = false
        self.p.is_leader = self.p.is_healthy = false
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_acquire_lock_as_master(self):
        self.assertEquals(self.ha.run_cycle(), 'acquired session lock as a leader')

    def test_promoted_by_acquiring_lock(self):
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_demote_after_failing_to_obtain_lock(self):
        self.ha.acquire_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoted self due after trying and failing to obtain lock')

    def test_follow_new_leader_after_failing_to_obtain_lock(self):
        self.ha.acquire_lock = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following new leader after trying and failing to obtain lock')

    def test_demote_because_not_healthiest(self):
        self.p.is_healthiest_node = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')

    def test_follow_new_leader_because_not_healthiest(self):
        self.p.is_healthiest_node = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

    def test_promote_because_have_lock(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_leader_with_lock(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')

    def test_demote_because_not_having_lock(self):
        self.ha.cluster.is_unlocked = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i do not have the lock and i was a leader')

    def test_demote_because_update_lock_failed(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.ha.update_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i do not have the lock and i was a leader')

    def test_follow_the_leader(self):
        self.ha.cluster.is_unlocked = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')

    def test_no_etcd_connection_master_demote(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.assertEquals(self.ha.run_cycle(), 'demoted self because DCS is not accessible and i was a leader')

    def test_bootstrap_from_leader(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEquals(self.ha.bootstrap(), 'bootstrapped from leader')

    def test_bootstrap_from_leader_failed(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.p.bootstrap = false
        self.assertEquals(self.ha.bootstrap(), 'failed to bootstrap from leader')

    def test_bootstrap_waiting_for_leader(self):
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.assertEquals(self.ha.bootstrap(), 'waiting for leader to bootstrap')

    def test_bootstrap_initialize_lock_failed(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.assertEquals(self.ha.bootstrap(), 'failed to acquire initialize lock')

    def test_bootstrap_initialized_new_cluster(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.assertEquals(self.ha.bootstrap(), 'initialized a new cluster')

    def test_bootstrap_release_initialize_key_on_failure(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.p.bootstrap = Mock(side_effect=PostgresException("Could not bootstrap master PostgreSQL"))
        self.assertRaises(PostgresException, self.ha.bootstrap)
