import select
import six
import unittest

from kazoo.client import KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.protocol.states import ZnodeStat
from mock import Mock, patch
from patroni.dcs.zookeeper import Leader, PatroniSequentialThreadingHandler, ZooKeeper, ZooKeeperError


class MockKazooClient(Mock):

    leader = False
    exists = True

    def __init__(self, *args, **kwargs):
        super(MockKazooClient, self).__init__()
        self._session_timeout = 30000

    @property
    def client_id(self):
        return (-1, '')

    @staticmethod
    def retry(func, *args, **kwargs):
        return func(*args, **kwargs)

    def get(self, path, watch=None):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path == '/no_node':
            raise NoNodeError
        elif '/members/' in path:
            return (
                b'postgres://repuser:rep-pass@localhost:5434/postgres?application_name=http://127.0.0.1:8009/patroni',
                ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0 if self.exists else -1, 0, 0, 0)
            )
        elif path.endswith('/optime/leader'):
            return (b'500', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
        elif path.endswith('/leader'):
            if self.leader:
                return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
            return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        elif path.endswith('/initialize'):
            return (b'foo', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
        return (b'', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    @staticmethod
    def get_children(path, watch=None, include_data=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path.startswith('/no_node'):
            raise NoNodeError
        elif path in ['/service/bla/', '/service/test/']:
            return ['initialize', 'leader', 'members', 'optime', 'failover', 'sync']
        return ['foo', 'bar', 'buzz']

    def create(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, (six.binary_type,)):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if b'Exception' in value:
            raise Exception
        if path.endswith('/initialize') or path == '/service/test/optime/leader':
            raise Exception
        elif b'retry' in value or (b'exists' in value and self.exists):
            raise NodeExistsError

    def create_async(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        return self.create(path, value, acl, ephemeral, sequence, makepath) or Mock()

    @staticmethod
    def set(path, value, version=-1):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, (six.binary_type,)):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if path == '/service/bla/optime/leader':
            raise Exception
        if path == '/service/test/members/bar' and b'retry' in value:
            return
        if path in ('/service/test/failover', '/service/test/config', '/service/test/sync'):
            if b'Exception' in value:
                raise Exception
            elif value == b'ok':
                return
        raise NoNodeError

    def set_async(self, path, value, version=-1):
        return self.set(path, value, version) or Mock()

    def delete(self, path, version=-1, recursive=False):
        if not isinstance(path, six.string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        self.exists = False
        if path == '/service/test/leader':
            self.leader = True
            raise Exception
        elif path == '/service/test/members/buzz':
            raise Exception
        elif path.endswith('/') or path.endswith('/initialize') or path == '/service/test/members/bar':
            raise NoNodeError

    def delete_async(self, path, version=-1, recursive=False):
        return self.delete(path, version, recursive) or Mock()


class TestPatroniSequentialThreadingHandler(unittest.TestCase):

    def setUp(self):
        self.handler = PatroniSequentialThreadingHandler(10)

    @patch.object(SequentialThreadingHandler, 'create_connection', Mock())
    def test_create_connection(self):
        self.assertIsNotNone(self.handler.create_connection(()))
        self.assertIsNotNone(self.handler.create_connection((), 40))
        self.assertIsNotNone(self.handler.create_connection(timeout=40))

    @patch.object(SequentialThreadingHandler, 'select', Mock(side_effect=ValueError))
    def test_select(self):
        self.assertRaises(select.error, self.handler.select)


class TestZooKeeper(unittest.TestCase):

    @patch('patroni.dcs.zookeeper.KazooClient', MockKazooClient)
    def setUp(self):
        self.zk = ZooKeeper({'hosts': ['localhost:2181'], 'scope': 'test',
                             'name': 'foo', 'ttl': 30, 'retry_timeout': 10, 'loop_wait': 10})

    def test_session_listener(self):
        self.zk.session_listener(KazooState.SUSPENDED)

    def test_reload_config(self):
        self.zk.reload_config({'ttl': 20, 'retry_timeout': 10, 'loop_wait': 10})
        self.zk.reload_config({'ttl': 20, 'retry_timeout': 10, 'loop_wait': 5})

    def test_get_node(self):
        self.assertIsNone(self.zk.get_node('/no_node'))

    def test_get_children(self):
        self.assertListEqual(self.zk.get_children('/no_node'), [])

    def test__inner_load_cluster(self):
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk._inner_load_cluster()
        self.zk._base_path = self.zk._base_path = '/no_node'
        self.zk._inner_load_cluster()

    def test_get_cluster(self):
        self.assertRaises(ZooKeeperError, self.zk.get_cluster)
        cluster = self.zk.get_cluster(True)
        self.assertIsInstance(cluster.leader, Leader)
        self.zk.touch_member({'foo': 'foo'})
        self.zk._name = 'bar'
        self.zk.optime_watcher(None)
        with patch.object(ZooKeeper, 'get_node', Mock(side_effect=Exception)):
            self.zk.get_cluster()
        cluster = self.zk.get_cluster()
        self.assertEqual(cluster.last_leader_operation, 500)

    def test_delete_leader(self):
        self.assertTrue(self.zk.delete_leader())

    def test_set_failover_value(self):
        self.zk.set_failover_value('')
        self.zk.set_failover_value('ok')
        self.zk.set_failover_value('Exception')

    def test_set_config_value(self):
        self.zk.set_config_value('', 1)
        self.zk.set_config_value('ok')
        self.zk.set_config_value('Exception')

    def test_initialize(self):
        self.assertFalse(self.zk.initialize())

    def test_cancel_initialization(self):
        self.zk.cancel_initialization()

    def test_touch_member(self):
        self.zk._name = 'buzz'
        self.zk.get_cluster()
        self.zk.touch_member({'new': 'new'})
        self.zk._name = 'bar'
        self.zk.touch_member({'new': 'new'})
        self.zk._name = 'na'
        self.zk._client.exists = 1
        self.zk.touch_member({'Exception': 'Exception'})
        self.zk._name = 'bar'
        self.zk.touch_member({'retry': 'retry'})
        self.zk._fetch_cluster = True
        self.zk.get_cluster()
        self.zk.touch_member({'conn_url': 'postgres://repuser:rep-pass@localhost:5434/postgres',
                              'api_url': 'http://127.0.0.1:8009/patroni'})

    def test_take_leader(self):
        self.zk.take_leader()
        with patch.object(MockKazooClient, 'create', Mock(side_effect=Exception)):
            self.zk.take_leader()

    def test_update_leader(self):
        self.assertTrue(self.zk.update_leader(None))

    def test_write_leader_optime(self):
        self.zk.last_leader_operation = '0'
        self.zk.write_leader_optime('1')
        with patch.object(MockKazooClient, 'create_async', Mock()):
            self.zk.write_leader_optime('1')
        with patch.object(MockKazooClient, 'set_async', Mock()):
            self.zk.write_leader_optime('2')
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk.write_leader_optime('3')

    def test_delete_cluster(self):
        self.assertTrue(self.zk.delete_cluster())

    def test_watch(self):
        self.zk.watch(None, 0)
        self.zk.event.isSet = Mock(return_value=True)
        self.zk._fetch_optime = False
        self.zk.watch(None, 0)

    def test__kazoo_connect(self):
        self.zk._client._retry.deadline = 1
        self.zk._orig_kazoo_connect = Mock(return_value=(0, 0))
        self.zk._kazoo_connect(None, None)

    def test_sync_state(self):
        self.zk.set_sync_state_value('')
        self.zk.set_sync_state_value('ok')
        self.zk.set_sync_state_value('Exception')
        self.zk.delete_sync_state()

    def test_set_history_value(self):
        self.zk.set_history_value('{}')
