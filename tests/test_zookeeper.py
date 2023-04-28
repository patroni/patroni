import select
import unittest

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.protocol.states import KeeperState, ZnodeStat
from kazoo.retry import RetryFailedError
from mock import Mock, PropertyMock, patch
from patroni.dcs.zookeeper import Cluster, Leader, PatroniKazooClient,\
        PatroniSequentialThreadingHandler, ZooKeeper, ZooKeeperError


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
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path == '/broken/status':
            return (b'{', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
        elif path in ('/no_node', '/legacy/status'):
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
        elif path.endswith('/status'):
            return (b'{"optime":500,"slots":{"ls":1234567}}', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
        elif path.endswith('/failsafe'):
            return (b'{a}', ZnodeStat(0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0))
        return (b'', ZnodeStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    @staticmethod
    def get_children(path, watch=None, include_data=False):
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if path.startswith('/no_node'):
            raise NoNodeError
        elif path in ['/service/bla/', '/service/test/']:
            return ['initialize', 'leader', 'members', 'optime', 'failover', 'sync', 'failsafe', '0', '1']
        return ['foo', 'bar', 'buzz']

    def create(self, path, value=b"", acl=None, ephemeral=False, sequence=False, makepath=False):
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, bytes):
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
        if not isinstance(path, str):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, bytes):
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
        if not isinstance(path, str):
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

    def test_select(self):
        with patch.object(SequentialThreadingHandler, 'select', Mock(side_effect=ValueError)):
            self.assertRaises(select.error, self.handler.select)
        with patch.object(SequentialThreadingHandler, 'select', Mock(side_effect=IOError)):
            self.assertRaises(Exception, self.handler.select)


class TestPatroniKazooClient(unittest.TestCase):

    def test__call(self):
        c = PatroniKazooClient()
        with patch.object(KazooClient, '_call', Mock()):
            self.assertIsNotNone(c._call(None, Mock()))
        c._state = KeeperState.CONNECTING
        self.assertFalse(c._call(None, Mock()))


class TestZooKeeper(unittest.TestCase):

    @patch('patroni.dcs.zookeeper.PatroniKazooClient', MockKazooClient)
    def setUp(self):
        self.zk = ZooKeeper({'hosts': ['localhost:2181'], 'scope': 'test',
                             'name': 'foo', 'ttl': 30, 'retry_timeout': 10, 'loop_wait': 10,
                             'set_acls': {'CN=principal2': ['ALL']}})

    def test_session_listener(self):
        self.zk.session_listener(KazooState.SUSPENDED)

    def test_members_watcher(self):
        self.zk._fetch_cluster = False
        self.zk.members_watcher(None)
        self.assertTrue(self.zk._fetch_cluster)

    def test_reload_config(self):
        self.zk.reload_config({'ttl': 20, 'retry_timeout': 10, 'loop_wait': 10})
        self.zk.reload_config({'ttl': 20, 'retry_timeout': 10, 'loop_wait': 5})

    def test_get_node(self):
        self.assertIsNone(self.zk.get_node('/no_node'))

    def test_get_children(self):
        self.assertListEqual(self.zk.get_children('/no_node'), [])

    def test__cluster_loader(self):
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk._cluster_loader(self.zk.client_path(''))
        self.zk._base_path = self.zk._base_path = '/broken'
        self.zk._cluster_loader(self.zk.client_path(''))
        self.zk._base_path = self.zk._base_path = '/legacy'
        self.zk._cluster_loader(self.zk.client_path(''))
        self.zk._base_path = self.zk._base_path = '/no_node'
        self.zk._cluster_loader(self.zk.client_path(''))

    def test_get_cluster(self):
        cluster = self.zk.get_cluster(True)
        self.assertIsInstance(cluster.leader, Leader)
        self.zk.status_watcher(None)
        self.zk.get_cluster()
        self.zk.touch_member({'foo': 'foo'})
        self.zk._name = 'bar'
        self.zk.status_watcher(None)
        with patch.object(ZooKeeper, 'get_node', Mock(side_effect=Exception)):
            self.zk.get_cluster()
        cluster = self.zk.get_cluster()
        self.assertEqual(cluster.last_lsn, 500)

    def test__get_citus_cluster(self):
        self.zk._citus_group = '0'
        for _ in range(0, 2):
            cluster = self.zk.get_cluster()
            self.assertIsInstance(cluster, Cluster)
            self.assertIsInstance(cluster.workers[1], Cluster)

    @patch('patroni.dcs.zookeeper.logger.error')
    @patch.object(ZooKeeper, '_cluster_loader', Mock(side_effect=Exception))
    def test_get_citus_coordinator(self, mock_logger):
        self.assertIsNone(self.zk.get_citus_coordinator())
        mock_logger.assert_called_once()

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
        self.zk.touch_member({'retry': 'retry'})
        self.zk.touch_member({'conn_url': 'postgres://repuser:rep-pass@localhost:5434/postgres',
                              'api_url': 'http://127.0.0.1:8009/patroni'})

    @patch.object(MockKazooClient, 'create', Mock(side_effect=[RetryFailedError, Exception]))
    def test_attempt_to_acquire_leader(self):
        self.assertRaises(ZooKeeperError, self.zk.attempt_to_acquire_leader)
        self.assertFalse(self.zk.attempt_to_acquire_leader())

    def test_take_leader(self):
        self.zk.take_leader()
        with patch.object(MockKazooClient, 'create', Mock(side_effect=Exception)):
            self.zk.take_leader()

    def test_update_leader(self):
        self.assertFalse(self.zk.update_leader(12345))
        with patch.object(MockKazooClient, 'delete', Mock(side_effect=RetryFailedError)):
            self.assertRaises(ZooKeeperError, self.zk.update_leader, 12345)
        with patch.object(MockKazooClient, 'delete', Mock(side_effect=NoNodeError)):
            self.assertTrue(self.zk.update_leader(12345, failsafe={'foo': 'bar'}))
            with patch.object(MockKazooClient, 'create', Mock(side_effect=[RetryFailedError, Exception])):
                self.assertRaises(ZooKeeperError, self.zk.update_leader, 12345)
                self.assertFalse(self.zk.update_leader(12345))

    @patch.object(Cluster, 'min_version', PropertyMock(return_value=(2, 0)))
    def test_write_leader_optime(self):
        self.zk.last_lsn = '0'
        self.zk.write_leader_optime('1')
        with patch.object(MockKazooClient, 'create_async', Mock()):
            self.zk.write_leader_optime('1')
        with patch.object(MockKazooClient, 'set_async', Mock()):
            self.zk.write_leader_optime('2')
        self.zk._base_path = self.zk._base_path.replace('test', 'bla')
        self.zk.get_cluster()
        self.zk.write_leader_optime('3')

    def test_delete_cluster(self):
        self.assertTrue(self.zk.delete_cluster())

    def test_watch(self):
        self.zk.watch(None, 0)
        self.zk.event.is_set = Mock(return_value=True)
        self.zk._fetch_status = False
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
