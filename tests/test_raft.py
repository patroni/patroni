import os
import unittest
import tempfile
import time

from mock import Mock, PropertyMock, patch
from patroni.dcs.raft import Cluster, DynMemberSyncObj, KVStoreTTL,\
        Raft, RaftError, SyncObjUtility, TCPTransport, _TCPTransport
from pysyncobj import SyncObjConf, FAIL_REASON


def remove_files(prefix):
    for f in ('journal', 'journal.meta', 'dump'):
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


class TestTCPTransport(unittest.TestCase):

    @patch.object(TCPTransport, '__init__', Mock())
    @patch.object(TCPTransport, 'setOnUtilityMessageCallback', Mock())
    @patch.object(TCPTransport, '_connectIfNecessarySingle', Mock(side_effect=Exception))
    def test__connectIfNecessarySingle(self):
        t = _TCPTransport(Mock(), None, [])
        self.assertFalse(t._connectIfNecessarySingle(None))


@patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
class TestDynMemberSyncObj(unittest.TestCase):

    @patch('pysyncobj.tcp_server.TcpServer.bind', Mock())
    def setUp(self):
        self.conf = SyncObjConf(appendEntriesUseBatch=False, dynamicMembershipChange=True, autoTick=False)
        self.so = DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    @patch.object(SyncObjUtility, 'executeCommand')
    def test_add_member(self, mock_execute_command):
        mock_execute_command.return_value = [{'addr': '127.0.0.1:1235'}, {'addr': '127.0.0.1:1236'}]
        mock_execute_command.ver = 0
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)
        self.conf.dynamicMembershipChange = False
        DynMemberSyncObj('127.0.0.1:1234', ['127.0.0.1:1235'], self.conf)

    def test_getMembers(self):
        mock_conn = Mock()
        self.so._SyncObj__transport._onIncomingMessageReceived(mock_conn, ['members'])

    def test__SyncObj__doChangeCluster(self):
        self.so._SyncObj__doChangeCluster(['add', '127.0.0.1:1236'])


@patch.object(SyncObjConf, 'fullDumpFile', PropertyMock(return_value=None), create=True)
@patch.object(SyncObjConf, 'journalFile', PropertyMock(return_value=None), create=True)
class TestKVStoreTTL(unittest.TestCase):

    @patch.object(SyncObjConf, 'fullDumpFile', PropertyMock(return_value=None), create=True)
    @patch.object(SyncObjConf, 'journalFile', PropertyMock(return_value=None), create=True)
    def setUp(self):
        callback = Mock()
        callback.replicated = False
        self.so = KVStoreTTL(None, callback, callback, self_addr='127.0.0.1:1234')
        self.so.startAutoTick()
        self.so.set_retry_timeout(10)

    def tearDown(self):
        if self.so:
            self.so.destroy()

    def test_set(self):
        self.assertTrue(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.set('foo', 'bar', prevExist=False, ttl=30))
        self.assertFalse(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}, prevValue=''))
        self.assertTrue(self.so.retry(self.so._set, 'foo', {'value': 'buz', 'created': 1, 'updated': 1}))
        with patch.object(KVStoreTTL, 'retry', Mock(side_effect=RaftError(''))):
            self.assertFalse(self.so.set('foo', 'bar'))
            self.assertRaises(RaftError, self.so.set, 'foo', 'bar', handle_raft_error=False)

    def test_delete(self):
        self.so.autoTickPeriod = 0.2
        self.so.set('foo', 'bar')
        self.so.set('fooo', 'bar')
        self.assertFalse(self.so.delete('foo', prevValue='buz'))
        self.assertTrue(self.so.delete('foo', recursive=True))
        self.assertFalse(self.so.retry(self.so._delete, 'foo', prevValue=''))
        with patch.object(KVStoreTTL, 'retry', Mock(side_effect=RaftError(''))):
            self.assertFalse(self.so.delete('foo'))

    def test_expire(self):
        self.so.set('foo', 'bar', ttl=0.001)
        time.sleep(1)
        self.assertIsNone(self.so.get('foo'))
        self.assertEqual(self.so.get('foo', recursive=True), {})

    @patch('time.sleep', Mock())
    def test_retry(self):
        return_values = [FAIL_REASON.QUEUE_FULL] * 2 + [FAIL_REASON.SUCCESS, FAIL_REASON.REQUEST_DENIED]

        def test(callback):
            callback(True, return_values.pop(0))

        with patch('time.time', Mock(side_effect=[1, 100])):
            self.assertRaises(RaftError, self.so.retry, test)

        self.assertTrue(self.so.retry(test))
        self.assertFalse(self.so.retry(test))

    def test_on_ready_override(self):
        self.assertTrue(self.so.set('foo', 'bar'))
        self.so.destroy()
        self.so = None
        so = KVStoreTTL(Mock(), None, None, self_addr='127.0.0.1:1234',
                        partner_addrs=['127.0.0.1:1235'], patronictl=True)
        so.doTick(0)
        so.destroy()


class TestRaft(unittest.TestCase):

    _TMP = tempfile.gettempdir()

    def test_raft(self):
        raft = Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'self_addr': '127.0.0.1:1234',
                     'retry_timeout': 10, 'data_dir': self._TMP,
                     'database': 'citus', 'group': 0})
        raft.reload_config({'retry_timeout': 20, 'ttl': 60, 'loop_wait': 10})
        self.assertTrue(raft._sync_obj.set(raft.members_path + 'legacy', '{"version":"2.0.0"}'))
        self.assertTrue(raft.touch_member(''))
        self.assertTrue(raft.initialize())
        self.assertTrue(raft.cancel_initialization())
        self.assertTrue(raft.set_config_value('{}'))
        self.assertTrue(raft.write_sync_state('foo', 'bar'))
        raft._citus_group = '1'
        self.assertTrue(raft.manual_failover('foo', 'bar'))
        raft._citus_group = '0'
        cluster = raft.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsInstance(cluster.workers[1], Cluster)
        self.assertTrue(raft._sync_obj.set(raft.status_path, '{"optime":1234567,"slots":{"ls":12345}}'))
        raft.get_cluster()
        self.assertTrue(raft.update_leader('1', failsafe={'foo': 'bat'}))
        self.assertTrue(raft._sync_obj.set(raft.failsafe_path, '{"foo"}'))
        self.assertTrue(raft._sync_obj.set(raft.status_path, '{'))
        raft.get_citus_coordinator()
        self.assertTrue(raft.delete_sync_state())
        self.assertTrue(raft.delete_leader())
        self.assertTrue(raft.set_history_value(''))
        self.assertTrue(raft.delete_cluster())
        raft._citus_group = '1'
        self.assertTrue(raft.delete_cluster())
        raft._citus_group = None
        raft.get_cluster()
        self.assertTrue(raft.take_leader())
        raft.get_cluster()
        raft.watch(None, 0.001)
        raft._sync_obj.destroy()

    def tearDown(self):
        remove_files(os.path.join(self._TMP, '127.0.0.1:1234.'))

    def setUp(self):
        self.tearDown()

    @patch('patroni.dcs.raft.KVStoreTTL')
    @patch('threading.Event')
    def test_init(self, mock_event, mock_kvstore):
        mock_kvstore.return_value.applied_local_log = False
        mock_event.return_value.is_set.side_effect = [False, True]
        self.assertIsNotNone(Raft({'ttl': 30, 'scope': 'test', 'name': 'pg', 'patronictl': True,
                                   'self_addr': '1', 'data_dir': self._TMP}))
