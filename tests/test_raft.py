import os
import unittest
import time

from mock import Mock, patch
from patroni.dcs.raft import DynMemberSyncObj, KVStoreTTL, Raft, SyncObjUtility
from pysyncobj import SyncObjConf, FAIL_REASON


def remove_files(prefix):
    for f in ('journal', 'dump'):
        f = prefix + f
        if os.path.isfile(f):
            for i in range(0, 15):
                try:
                    if os.path.isfile(f):
                        os.unlink(f)
                        break
                    else:
                        break
                except Exception:
                    time.sleep(1.0)


@patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
class TestDynMemberSyncObj(unittest.TestCase):

    @patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
    def setUp(self):
        self.conf = SyncObjConf(appendEntriesUseBatch=False, dynamicMembershipChange=True, autoTick=False)
        self.so = DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    @patch.object(SyncObjUtility, 'sendMessage')
    def test_add_member(self, mock_send_message):
        mock_send_message.return_value = [{'addr': '127.0.0.1:1235'}, {'addr': '127.0.0.1:1236'}]
        mock_send_message.ver = 0
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)
        self.conf.dynamicMembershipChange = False
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    def test___onUtilityMessage(self):
        self.so._SyncObj__encryptor = Mock()
        mock_conn = Mock()
        mock_conn.sendRandKey = None
        self.so._SyncObj__transport._onIncomingMessageReceived(mock_conn, 'randkey')
        self.so._SyncObj__transport._onIncomingMessageReceived(mock_conn, ['members'])
        self.so._SyncObj__transport._onIncomingMessageReceived(mock_conn, ['status'])

    def test__SyncObj__doChangeCluster(self):
        self.so._SyncObj__doChangeCluster(['add', '127.0.0.1:1236'])

    def test_utility(self):
        utility = SyncObjUtility(['127.0.0.1:1235'], self.conf)
        utility.setPartnerNode(list(utility._SyncObj__otherNodes)[0])
        utility.sendMessage(['members'])
        utility._onMessageReceived(0, '')


class TestKVStoreTTL(unittest.TestCase):

    def setUp(self):
        self.conf = SyncObjConf(appendEntriesUseBatch=False, appendEntriesPeriod=0.001,
                                raftMinTimeout=0.004, raftMaxTimeout=0.005, autoTickPeriod=0.001)
        callback = Mock()
        callback.replicated = False
        self.so = KVStoreTTL('127.0.0.1:1234', [], self.conf, on_set=callback, on_delete=callback)
        self.so.set_retry_timeout(10)

    @staticmethod
    def destroy(so):
        so.destroy()
        so._SyncObj__thread.join()

    def tearDown(self):
        if self.so:
            self.destroy(self.so)

    def test_set(self):
        self.assertTrue(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}, prevValue=''))
        self.assertTrue(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}))

    def test_delete(self):
        self.conf.autoTickPeriod = 0.1
        self.so.set('foo', 'bar')
        self.so.set('fooo', 'bar')
        self.assertFalse(self.so.delete('foo', prevValue='buz'))
        self.assertFalse(self.so.delete('foo', prevValue='bar', timeout=0.00001))
        self.assertFalse(self.so.delete('foo', prevValue='bar'))
        self.assertTrue(self.so.delete('foo', recursive=True))
        self.assertFalse(self.so.retry(self.so._delete, 'foo', prevValue=''))

    def test_expire(self):
        self.so.set('foo', 'bar', ttl=0.001)
        time.sleep(1)
        self.assertIsNone(self.so.get('foo'))
        self.assertEqual(self.so.get('foo', recursive=True), {})

    @patch('time.sleep', Mock())
    def test_retry(self):
        return_values = [FAIL_REASON.QUEUE_FULL, FAIL_REASON.SUCCESS, FAIL_REASON.REQUEST_DENIED]

        def test(callback):
            callback(True, return_values.pop(0))
        self.assertTrue(self.so.retry(test))
        self.assertFalse(self.so.retry(test))

    def test_on_ready_override(self):
        self.assertTrue(self.so.set('foo', 'bar'))
        self.destroy(self.so)
        self.so = None
        self.conf.onReady = Mock()
        self.conf.autoTick = False
        so = KVStoreTTL('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)
        so.doTick(0)
        so.destroy()


class TestRaft(unittest.TestCase):

    def test_raft(self):
        raft = Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'self_addr': '127.0.0.1:1234', 'retry_timeout': 10})
        raft.set_retry_timeout(20)
        raft.set_ttl(60)
        self.assertTrue(raft.touch_member(''))
        self.assertTrue(raft.initialize())
        self.assertTrue(raft.cancel_initialization())
        self.assertTrue(raft.set_config_value('{}'))
        self.assertTrue(raft.write_sync_state('foo', 'bar'))
        self.assertTrue(raft.update_leader('1'))
        self.assertTrue(raft.manual_failover('foo', 'bar'))
        raft.get_cluster()
        self.assertTrue(raft.delete_sync_state())
        self.assertTrue(raft.delete_leader())
        self.assertTrue(raft.set_history_value(''))
        self.assertTrue(raft.delete_cluster())
        raft.get_cluster()
        self.assertTrue(raft.take_leader())
        raft.watch(None, 0.001)
        raft._sync_obj.destroy()
        raft._sync_obj._SyncObj__thread.join()

    def tearDown(self):
        remove_files('127.0.0.1:1234.')

    def setUp(self):
        self.tearDown()

    @patch('patroni.dcs.raft.KVStoreTTL')
    @patch('threading.Event')
    def test_init(self, mock_event, mock_kvstore):
        mock_kvstore.return_value.applied_local_log = False
        mock_event.return_value.isSet.side_effect = [False, True]
        self.assertIsNotNone(Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'patronictl': True}))
