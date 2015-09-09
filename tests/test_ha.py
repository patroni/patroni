import unittest

from mock import Mock, patch
from patroni.dcs import Cluster, Leader, Member, DCSError
from patroni.etcd import Client, Etcd
from patroni.ha import Ha
from test_etcd import etcd_read, etcd_write, requests_get


def true(*args, **kwargs):
    return True


def false(*args, **kwargs):
    return False


class MockPostgresql:

    def __init__(self):
        self.name = 'postgresql0'
        self.is_promoted = False

    def is_healthy(self):
        return True

    def write_recovery_conf(self, _):
        return True

    def start(self):
        return True

    def check_replication_lag(self, last_leader_operation):
        return True

    def is_leader(self):
        return True

    def promote(self):
        return True

    def demote(self, _):
        return True

    def follow_the_leader(self, _):
        return True

    def create_replication_slots(self, _):
        return True

    def xlog_position(self):
        return 0

    def last_operation(self):
        return 0


def nop(*args, **kwargs):
    pass


def dead_etcd():
    raise DCSError('Etcd is not responding properly')


def get_unlocked_cluster():
    return Cluster(False, None, None, [])


class TestHa(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestHa, self).__init__(method_name)

    def set_up(self):
        self.p = MockPostgresql()
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            self.e = Etcd('foo', {'ttl': 30, 'host': 'remotehost:2379', 'scope': 'test'})
            self.e.client.read = etcd_read
            self.e.client.write = etcd_write
            self.ha = Ha(self.p, self.e)
            self.ha.load_cluster_from_dcs()
            self.ha.cluster = get_unlocked_cluster()
            self.ha.load_cluster_from_dcs = nop

    def test_load_cluster_from_dcs(self):
        ha = Ha(self.p, self.e)
        ha.load_cluster_from_dcs()
        self.e.get_cluster = get_unlocked_cluster
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
        self.ha.is_healthiest_node = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_demote_after_failing_to_obtain_lock(self):
        self.ha.acquire_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoted self due after trying and failing to obtain lock')

    def test_follow_new_leader_after_failing_to_obtain_lock(self):
        self.ha.is_healthiest_node = true
        self.ha.acquire_lock = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following new leader after trying and failing to obtain lock')

    def test_demote_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')

    def test_follow_new_leader_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
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

    def test_follow_the_leader(self):
        self.ha.cluster.is_unlocked = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')

    def test_no_etcd_connection_master_demote(self):
        self.ha.load_cluster_from_dcs = dead_etcd
        self.assertEquals(self.ha.run_cycle(), 'demoted self because DCS is not accessible and i was a leader')

    def test_is_healthiest_node(self):
        self.assertTrue(self.ha.is_healthiest_node())
        self.p.is_leader = false
        self.ha.fetch_node_status = lambda e: (e, True, True, 0)  # accessible, in_recovery
        self.assertTrue(self.ha.is_healthiest_node())
        self.ha.fetch_node_status = lambda e: (e, True, False, 0)  # accessible, not in_recovery
        self.assertFalse(self.ha.is_healthiest_node())
        self.ha.fetch_node_status = lambda e: (e, True, True, 1)  # accessible, in_recovery, xlog location ahead
        self.assertFalse(self.ha.is_healthiest_node())
        self.p.check_replication_lag = false
        self.assertFalse(self.ha.is_healthiest_node())

    def test_fetch_node_status(self):
        import requests
        requests.get = requests_get
        member = Member(0, 'test', '', 'http://127.0.0.1:8011/patroni', None, None)
        self.ha.fetch_node_status(member)
        member = Member(0, 'test', '', 'http://localhost:8011/patroni', None, None)
        self.ha.fetch_node_status(member)
