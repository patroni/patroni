import os
import unittest
import time

from mock import Mock, patch
from patroni.dcs.raft import DynMemberSyncObj, KVStoreTTL, Raft
from pysyncobj import SyncObjConf, FAIL_REASON


@patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
class TestDynMemberSyncObj(unittest.TestCase):

    @patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
    @patch('pysyncobj.node.Node.isConnected', Mock(return_value=False))
    def setUp(self):
        self.conf = SyncObjConf(appendEntriesUseBatch=False, dynamicMembershipChange=True, autoTick=False)
        self.so = DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    @patch.object(DynMemberSyncObj, '_DynMemberSyncObj__utility_message')
    def test_add_member(self, mock_utility_message):
        mock_utility_message.return_value = [{'addr': '127.0.0.1:1235'}, {'addr': '127.0.0.1:1236'}]
        mock_utility_message.replicated = False
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)
        self.conf.dynamicMembershipChange = False
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    @patch('pysyncobj.node.Node.isConnected', Mock(side_effect=[True, False]))
    def test___utility_message(self):
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    def test__onMessageReceived(self):
        self.so._onMessageReceived(Mock(), {})
        self.so._SyncObj__initialised = True
        self.so._onMessageReceived(Mock(), {'type': None})

    def test___onUtilityMessage(self):
        self.so._SyncObj__onMessageReceived(Mock(), ['members'])
        self.so._SyncObj__onMessageReceived(Mock(), ['status'])

    def test__SyncObj__doChangeCluster(self):
        self.so._SyncObj__doChangeCluster(['add', '127.0.0.1:1236'])


class TestKVStoreTTL(unittest.TestCase):

    def setUp(self):
        self.conf = SyncObjConf(appendEntriesUseBatch=False)
        callback = Mock()
        callback.replicated = False
        self.so = KVStoreTTL('127.0.0.1:1234', [], self.conf, on_set=callback, on_delete=callback)
        self.so.set_retry_timeout(10)

    def tearDown(self):
        self.so.destroy()
        self.so._SyncObj__thread.join()

    def test_set(self):
        self.assertTrue(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}, prevValue=''))
        self.assertTrue(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}))

    def test_delete(self):
        self.so.set('foo', 'bar')
        self.so.set('fooo', 'bar')
        self.assertFalse(self.so.delete('foo', prevValue='buz'))
        self.assertFalse(self.so.delete('foo', prevValue='bar', timeout=0.001))
        self.assertFalse(self.so.delete('foo', prevValue='bar'))
        self.assertTrue(self.so.delete('foo', recursive=True))
        self.assertFalse(self.so.retry(self.so._delete, 'foo', prevValue=''))

    def test_expire(self):
        self.so.set('foo', 'bar', ttl=0.001)
        time.sleep(1)
        self.assertIsNone(self.so.get('foo'))
        self.assertEquals(self.so.get('foo', recursive=True), {})

    @patch('time.sleep', Mock())
    def test_retry(self):
        return_values = [FAIL_REASON.QUEUE_FULL, FAIL_REASON.SUCCESS]

        def test(callback):
            callback(None, return_values.pop(0))
        self.so.retry(test)


class TestRaft(unittest.TestCase):

    def test_raft(self):
        raft = Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'self_addr': '127.0.0.1:1234', 'retry_timeout': 10})
        raft.set_retry_timeout(20)
        raft.set_ttl(60)
        self.assertTrue(raft.touch_member(''))
        self.assertTrue(raft.initialize())
        self.assertTrue(raft.cancel_initialization())
        self.assertTrue(raft.attempt_to_acquire_leader())
        self.assertTrue(raft.set_config_value('{}'))
        self.assertTrue(raft.write_sync_state('foo', 'bar'))
        raft.write_leader_optime('1')
        self.assertTrue(raft.update_leader())
        self.assertTrue(raft.manual_failover('foo', 'bar'))
        raft.get_cluster()
        self.assertTrue(raft.delete_sync_state())
        self.assertTrue(raft.delete_leader())
        self.assertTrue(raft.delete_cluster())
        raft.get_cluster()
        self.assertTrue(raft.take_leader())
        raft.watch(None, 0.001)
        raft._sync_obj.destroy()
        raft._sync_obj._SyncObj__thread.join()

    def tearDown(self):
        for f in ('journal', 'dump'):
            f = '127.0.0.1:1234.' + f
            if os.path.exists(f):
                os.unlink(f)

    def setUp(self):
        self.tearDown()

    @patch('patroni.dcs.raft.KVStoreTTL', Mock())
    @patch('threading.Event')
    def test_init(self, mock_event):
        mock_event.return_value.isSet.side_effect = [False, True]
        Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'self_addr': '127.0.0.1:1234', 'retry_timeout': 10})
