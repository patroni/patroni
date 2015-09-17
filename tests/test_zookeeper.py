import patroni.zookeeper
import requests
import six
import unittest

from patroni.dcs import Leader
from patroni.zookeeper import ExhibitorEnsembleProvider, ZooKeeper, ZooKeeperError
from kazoo.client import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.protocol.states import ZnodeStat
from test_etcd import MockPostgresql, requests_get


class MockEvent:

    def clear(self):
        pass

    def set(self):
        pass

    def wait(self, timeout):
        pass

    def isSet(self):
        return True


class MockEventHandler:

    def event_object(self):
        return MockEvent()


class SleepException(Exception):
    pass


class MockKazooClient:

    def __init__(self, **kwargs):
        self.handler = MockEventHandler()
        self.leader = False
        self.exists = True

    def start(self, timeout):
        pass

    @property
    def client_id(self):
        return (-1, '')

    def add_listener(self, cb):
        pass

    def retry(self, func, *args, **kwargs):
        func(*args, **kwargs)

    def get(self, path, watch=None):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path == '/no_node':
            raise NoNodeError
        elif '/members/' in path:
            return (
                b'postgres://repuser:rep-pass@localhost:5434/postgres?application_name=http://127.0.0.1:8009/patroni',
                ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            )
        elif path.endswith('/optime/leader'):
            return (b'1', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        elif path.endswith('/leader'):
            if self.leader:
                return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
            return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        elif path.endswith('/initialize'):
            return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    def get_children(self, path, watch=None, include_data=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path == '/no_node':
            raise NoNodeError
        elif path in ['/service/bla/', '/service/test/']:
            return ['initialize', 'leader', 'members', 'optime']
        return ['foo', 'bar', 'buzz']

    def create(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, (six.binary_type,)):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if path.endswith('/initialize') or path == '/service/test/optime/leader':
            raise Exception
        elif value == b'retry' or (value == b'exists' and self.exists):
            raise NodeExistsError

    def set(self, path, value, version=-1):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, (six.binary_type,)):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if path == '/service/bla/optime/leader':
            raise Exception
        raise NoNodeError

    def delete(self, path, version=-1, recursive=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        self.exists = False
        if path == '/service/test/leader':
            if self.leader:
                return
            self.leader = True
            raise Exception
        elif path.endswith('/initialize'):
            raise NoNodeError

    def set_hosts(self, hosts, randomize_hosts=None):
        pass


def exhibitor_sleep(_):
    raise SleepException


class TestExhibitorEnsembleProvider(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestExhibitorEnsembleProvider, self).__init__(method_name)

    def set_up(self):
        requests.get = requests_get
        patroni.zookeeper.sleep = exhibitor_sleep

    def test_init(self):
        self.assertRaises(SleepException, ExhibitorEnsembleProvider, ['localhost'], 8181)


class TestZooKeeper(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestZooKeeper, self).__init__(method_name)

    def set_up(self):
        requests.get = requests_get
        patroni.zookeeper.KazooClient = MockKazooClient
        self.zk = ZooKeeper('foo', {'exhibitor': {'hosts': ['localhost', 'exhibitor'], 'port': 8181}, 'scope': 'test'})

    def test_session_listener(self):
        self.zk.session_listener(KazooState.SUSPENDED)

    def test_get_node(self):
        self.assertIsNone(self.zk.get_node('/no_node'))

    def test_get_children(self):
        self.assertListEqual(self.zk.get_children('/no_node'), [])

    def test__inner_load_cluster(self):
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk._inner_load_cluster()

    def test_get_cluster(self):
        self.assertRaises(ZooKeeperError, self.zk.get_cluster)
        self.zk.exhibitor.poll = lambda: True
        cluster = self.zk.get_cluster()
        self.assertIsInstance(cluster.leader, Leader)
        self.zk.touch_member('foo')
        self.zk.delete_leader()

    def test_initialize(self):
        self.assertFalse(self.zk.initialize())

    def test_cancel_initialization(self):
        self.zk.cancel_initialization()

    def test_touch_member(self):
        self.zk.touch_member('new')
        self.zk.touch_member('exists')
        self.zk.touch_member('retry')

    def test_take_leader(self):
        self.zk.take_leader()

    def test_update_leader(self):
        self.zk.last_leader_operation = -1
        self.assertTrue(self.zk.update_leader(MockPostgresql()))
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk.last_leader_operation = -1
        self.assertTrue(self.zk.update_leader(MockPostgresql()))

    def test_watch(self):
        self.zk.watch(0)
        self.zk.cluster_event.isSet = lambda: False
        self.zk.watch(0)
