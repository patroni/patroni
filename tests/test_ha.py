import unittest
import requests

from helpers.etcd import Etcd
from helpers.ha import Ha
from test_etcd import requests_get, requests_put, requests_delete


def true(*args, **kwargs):
    return True


def false(*args, **kwargs):
    return False


class MockPostgresql:

    def __init__(self):
        self.name = 'postgresql0'

    def is_healthy(self):
        return True

    def write_recovery_conf(self, _):
        return True

    def start(self):
        return True

    def is_healthiest_node(self, members):
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

    def last_operation(self):
        return 0


class TestHa(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestHa, self).__init__(method_name)

    def set_up(self):
        requests.get = requests_get
        requests.put = requests_put
        requests.delete = requests_delete
        self.p = MockPostgresql()
        self.e = Etcd({'ttl': 30, 'host': 'remotehost', 'scope': 'test'})
        self.ha = Ha(self.p, self.e)

    def test_start_as_slave(self):
        self.p.is_healthy = false
        self.assertEquals(self.ha.run_cycle(), 'started as a secondary')

    def test_start_as_readonly(self):
        self.p.is_leader = self.p.is_healthy = false
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_acquire_lock_as_master(self):
        self.ha.is_unlocked = true
        self.assertEquals(self.ha.run_cycle(), 'acquired session lock as a leader')

    def test_promoted_by_acquiring_lock(self):
        self.ha.is_unlocked = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_demote_after_failing_to_obtain_lock(self):
        self.ha.is_unlocked = true
        self.ha.acquire_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoted self due after trying and failing to obtain lock')

    def test_follow_new_leader_after_failing_to_obtain_lock(self):
        self.ha.is_unlocked = true
        self.ha.acquire_lock = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following new leader after trying and failing to obtain lock')

    def test_demote_because_not_healthiest(self):
        self.ha.is_unlocked = true
        self.p.is_healthiest_node = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')

    def test_follow_new_leader_because_not_healthiest(self):
        self.ha.is_unlocked = true
        self.p.is_healthiest_node = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

    def test_promote_because_have_lock(self):
        self.ha.has_lock = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_leader_with_lock(self):
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')

    def test_demote_because_not_having_lock(self):
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i do not have the lock and i was a leader')

    def test_follow_the_leader(self):
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')
