import datetime
import mock  # for the mock.call method, importing it without a namespace breaks python3
import os
import psycopg2
import shutil
import subprocess
import unittest

from mock import Mock, MagicMock, PropertyMock, patch, mock_open
from patroni.async_executor import CriticalTask
from patroni.dcs import Cluster, Leader, Member, SyncState
from patroni.exceptions import PostgresConnectionException, PostgresException
from patroni.postgresql import Postgresql, STATE_REJECT, STATE_NO_RESPONSE
from patroni.postmaster import PostmasterProcess
from patroni.utils import RetryFailedError
from six.moves import builtins
from threading import Thread


class MockCursor(object):

    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.rowcount = 0
        self.results = []

    def execute(self, sql, *params):
        if sql.startswith('blabla'):
            raise psycopg2.ProgrammingError()
        elif sql == 'CHECKPOINT':
            raise psycopg2.OperationalError()
        elif sql.startswith('RetryFailedError'):
            raise RetryFailedError('retry')
        elif sql.startswith('SELECT slot_name'):
            self.results = [('blabla',), ('foobar',)]
        elif sql.startswith('SELECT CASE WHEN pg_is_in_recovery()'):
            self.results = [(1, 2)]
        elif sql.startswith('SELECT pg_is_in_recovery()'):
            self.results = [(False, 2)]
        elif sql.startswith('WITH replication_info AS ('):
            replication_info = '[{"application_name":"walreceiver","client_addr":"1.2.3.4",' +\
                               '"state":"streaming","sync_state":"async","sync_priority":0}]'
            self.results = [('', 0, '', '', '', '', False, replication_info)]
        elif sql.startswith('SELECT name, setting'):
            self.results = [('wal_segment_size', '2048', '8kB', 'integer', 'internal'),
                            ('search_path', 'public', None, 'string', 'user'),
                            ('port', '5433', None, 'integer', 'postmaster'),
                            ('listen_addresses', '*', None, 'string', 'postmaster'),
                            ('autovacuum', 'on', None, 'bool', 'sighup'),
                            ('unix_socket_directories', '/tmp', None, 'string', 'postmaster')]
        elif sql.startswith('IDENTIFY_SYSTEM'):
            self.results = [('1', 2, '0/402EEC0', '')]
        elif sql.startswith('SELECT isdir, modification'):
            self.results = [(False, datetime.datetime.now())]
        elif sql.startswith('SELECT pg_read_file'):
            self.results = [('1\t0/40159C0\tno recovery target specified\n\n' +
                             '2\t1/40159C0\tno recovery target specified\n',)]
        elif sql.startswith('TIMELINE_HISTORY '):
            self.results = [('', b'x\t0/40159C0\tno recovery target specified\n\n' +
                                 b'1\t0/40159C0\tno recovery target specified\n\n' +
                                 b'2\t0/402DD98\tno recovery target specified\n\n' +
                                 b'3\t0/403DD98\tno recovery target specified\n')]
        else:
            self.results = [(None, None, None, None, None, None, None, None, None, None)]

    def fetchone(self):
        return self.results[0]

    def fetchall(self):
        return self.results

    def __iter__(self):
        for i in self.results:
            yield i

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockConnect(object):

    server_version = 99999
    autocommit = False
    closed = 0

    def cursor(self):
        return MockCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @staticmethod
    def close():
        pass


class MockPostmaster(object):
    def __init__(self, is_running=True, is_single_master=False):
        self.is_running = Mock(return_value=is_running)
        self.is_single_master = Mock(return_value=is_single_master)
        self.wait_for_user_backends_to_close = Mock()
        self.signal_stop = Mock(return_value=None)
        self.wait = Mock()


def pg_controldata_string(*args, **kwargs):
    return b"""
pg_control version number:            942
Catalog version number:               201509161
Database system identifier:           6200971513092291716
Database cluster state:               shut down in recovery
pg_control last modified:             Fri Oct  2 10:57:06 2015
Latest checkpoint location:           0/30000C8
Prior checkpoint location:            0/2000060
Latest checkpoint's REDO location:    0/3000090
Latest checkpoint's REDO WAL file:    000000020000000000000003
Latest checkpoint's TimeLineID:       2
Latest checkpoint's PrevTimeLineID:   2
Latest checkpoint's full_page_writes: on
Latest checkpoint's NextXID:          0/943
Latest checkpoint's NextOID:          24576
Latest checkpoint's NextMultiXactId:  1
Latest checkpoint's NextMultiOffset:  0
Latest checkpoint's oldestXID:        931
Latest checkpoint's oldestXID's DB:   1
Latest checkpoint's oldestActiveXID:  943
Latest checkpoint's oldestMultiXid:   1
Latest checkpoint's oldestMulti's DB: 1
Latest checkpoint's oldestCommitTs:   0
Latest checkpoint's newestCommitTs:   0
Time of latest checkpoint:            Fri Oct  2 10:56:54 2015
Fake LSN counter for unlogged rels:   0/1
Minimum recovery ending location:     0/30241F8
Min recovery ending loc's timeline:   2
Backup start location:                0/0
Backup end location:                  0/0
End-of-backup record required:        no
wal_level setting:                    hot_standby
Current wal_log_hints setting:                on
Current max_connections setting:              100
Current max_worker_processes setting:         8
Current max_prepared_xacts setting:           0
Current max_locks_per_xact setting:           64
Current track_commit_timestamp setting:       off
Maximum data alignment:               8
Database block size:                  8192
Blocks per segment of large relation: 131072
WAL block size:                       8192
Bytes per WAL segment:                16777216
Maximum length of identifiers:        64
Maximum columns in an index:          32
Maximum size of a TOAST chunk:        1996
Size of a large-object chunk:         2048
Date/time type storage:               64-bit integers
Float4 argument passing:              by value
Float8 argument passing:              by value
Data page checksum version:           0
"""


def psycopg2_connect(*args, **kwargs):
    return MockConnect()


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
class TestPostgresql(unittest.TestCase):
    _PARAMETERS = {'wal_level': 'hot_standby', 'max_replication_slots': 5, 'f.oo': 'bar',
                   'search_path': 'public', 'hot_standby': 'on', 'max_wal_senders': 5,
                   'wal_keep_segments': 8, 'wal_log_hints': 'on', 'max_locks_per_transaction': 64,
                   'max_worker_processes': 8, 'max_connections': 100, 'max_prepared_transactions': 0,
                   'track_commit_timestamp': 'off', 'unix_socket_directories': '/tmp'}

    @patch('subprocess.call', Mock(return_value=0))
    @patch('psycopg2.connect', psycopg2_connect)
    @patch('os.rename', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=90600))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def setUp(self):
        self.data_dir = 'data/test0'
        self.config_dir = self.data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        self.p = Postgresql({'name': 'test0', 'scope': 'batman', 'data_dir': self.data_dir,
                             'config_dir': self.config_dir, 'retry_timeout': 10, 'pgpass': '/tmp/pgpass0',
                             'listen': '127.0.0.2, 127.0.0.3:5432', 'connect_address': '127.0.0.2:5432',
                             'authentication': {'superuser': {'username': 'test', 'password': 'test'},
                                                'replication': {'username': 'replicator', 'password': 'rep-pass'}},
                             'remove_data_directory_on_rewind_failure': True,
                             'use_pg_rewind': True, 'pg_ctl_timeout': 'bla',
                             'parameters': self._PARAMETERS,
                             'recovery_conf': {'foo': 'bar'},
                             'pg_hba': ['host all all 0.0.0.0/0 md5'],
                             'callbacks': {'on_start': 'true', 'on_stop': 'true', 'on_reload': 'true',
                                           'on_restart': 'true', 'on_role_change': 'true'}})
        self.p._callback_executor = Mock()
        self.leadermem = Member(0, 'leader', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres'})
        self.leader = Leader(-1, 28, self.leadermem)
        self.other = Member(0, 'test-1', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres',
                            'tags': {'replicatefrom': 'leader'}})
        self.me = Member(0, 'test0', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5434/postgres'})

    def tearDown(self):
        shutil.rmtree('data')

    def test_get_initdb_options(self):
        self.assertEquals(self.p.get_initdb_options([{'encoding': 'UTF8'}, 'data-checksums']),
                          ['--encoding=UTF8', '--data-checksums'])
        self.assertRaises(Exception, self.p.get_initdb_options, [{'pgdata': 'bar'}])
        self.assertRaises(Exception, self.p.get_initdb_options, [{'foo': 'bar', 1: 2}])
        self.assertRaises(Exception, self.p.get_initdb_options, [1])

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.unlink', Mock())
    def test_delete_trigger_file(self):
        self.p.delete_trigger_file()

    @patch('subprocess.Popen')
    @patch.object(Postgresql, 'wait_for_startup')
    @patch.object(Postgresql, 'wait_for_port_open')
    @patch.object(Postgresql, 'is_running')
    def test_start(self, mock_is_running, mock_wait_for_port_open, mock_wait_for_startup, mock_popen):
        mock_is_running.return_value = MockPostmaster()
        mock_wait_for_port_open.return_value = True
        mock_wait_for_startup.return_value = False
        mock_popen.return_value.stdout.readline.return_value = '123'
        self.assertTrue(self.p.start())
        mock_is_running.return_value = None

        mock_postmaster = MockPostmaster()
        with patch.object(PostmasterProcess, 'start', return_value=mock_postmaster):
            pg_conf = os.path.join(self.data_dir, 'postgresql.conf')
            open(pg_conf, 'w').close()
            self.assertFalse(self.p.start(task=CriticalTask()))

            with open(pg_conf) as f:
                lines = f.readlines()
                self.assertTrue("f.oo = 'bar'\n" in lines)

            mock_wait_for_startup.return_value = None
            self.assertFalse(self.p.start(10))
            self.assertIsNone(self.p.start())

            mock_wait_for_port_open.return_value = False
            self.assertFalse(self.p.start())
            task = CriticalTask()
            task.cancel()
            self.assertFalse(self.p.start(task=task))

        self.p.cancel()
        self.assertFalse(self.p.start())

    @patch.object(Postgresql, 'pg_isready')
    @patch('patroni.postgresql.polling_loop', Mock(return_value=range(1)))
    def test_wait_for_port_open(self, mock_pg_isready):
        mock_pg_isready.return_value = STATE_NO_RESPONSE
        mock_postmaster = MockPostmaster(is_running=False)

        # No pid file and postmaster death
        self.assertFalse(self.p.wait_for_port_open(mock_postmaster, 1))

        mock_postmaster.is_running.return_value = True

        # timeout
        self.assertFalse(self.p.wait_for_port_open(mock_postmaster, 1))

        # pg_isready failure
        mock_pg_isready.return_value = 'garbage'
        self.assertTrue(self.p.wait_for_port_open(mock_postmaster, 1))

        # cancelled
        self.p.cancel()
        self.assertFalse(self.p.wait_for_port_open(mock_postmaster, 1))

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_running')
    @patch.object(Postgresql, '_wait_for_connection_close', Mock())
    def test_stop(self, mock_is_running):
        # Postmaster is not running
        mock_callback = Mock()
        mock_is_running.return_value = None
        self.assertTrue(self.p.stop(on_safepoint=mock_callback))
        mock_callback.assert_called()

        # Is running, stopped successfully
        mock_is_running.return_value = mock_postmaster = MockPostmaster()
        mock_callback.reset_mock()
        self.assertTrue(self.p.stop(on_safepoint=mock_callback))
        mock_callback.assert_called()
        mock_postmaster.signal_stop.assert_called()

        # Stop signal failed
        mock_postmaster.signal_stop.return_value = False
        self.assertFalse(self.p.stop())

        # Stop signal failed to find process
        mock_postmaster.signal_stop.return_value = True
        mock_callback.reset_mock()
        self.assertTrue(self.p.stop(on_safepoint=mock_callback))
        mock_callback.assert_called()

    def test_restart(self):
        self.p.start = Mock(return_value=False)
        self.assertFalse(self.p.restart())
        self.assertEquals(self.p.state, 'restart failed (restarting)')

    @patch.object(builtins, 'open', MagicMock())
    def test_write_pgpass(self):
        self.p.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo'})
        self.p.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo', 'password': 'bar'})

    def test_checkpoint(self):
        with patch.object(MockCursor, 'fetchone', Mock(return_value=(True, ))):
            self.assertEquals(self.p.checkpoint({'user': 'postgres'}), 'is_in_recovery=true')
        with patch.object(MockCursor, 'execute', Mock(return_value=None)):
            self.assertIsNone(self.p.checkpoint())
        self.assertEquals(self.p.checkpoint(), 'not accessible or not healty')

    @patch.object(Postgresql, 'cancellable_subprocess_call')
    @patch('patroni.postgresql.Postgresql.write_pgpass', MagicMock(return_value=dict()))
    def test_pg_rewind(self, mock_cancellable_subprocess_call):
        r = {'user': '', 'host': '', 'port': '', 'database': '', 'password': ''}
        mock_cancellable_subprocess_call.return_value = 0
        self.assertTrue(self.p.pg_rewind(r))
        mock_cancellable_subprocess_call.side_effect = OSError
        self.assertFalse(self.p.pg_rewind(r))

    def test_check_recovery_conf(self):
        self.p.write_recovery_conf({'primary_conninfo': 'foo'})
        self.assertFalse(self.p.check_recovery_conf(None))
        self.p.write_recovery_conf({})
        self.assertTrue(self.p.check_recovery_conf(None))

    @patch.object(Postgresql, 'start', Mock())
    @patch.object(Postgresql, 'can_rewind', PropertyMock(return_value=True))
    def test__get_local_timeline_lsn(self):
        self.p.trigger_check_diverged_lsn()
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'Database cluster state': 'shut down in recovery',
                                             'Minimum recovery ending location': '0/0',
                                             "Min recovery ending loc's timeline": '0'})):
            self.p.rewind_needed_and_possible(self.leader)
        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            with patch.object(MockCursor, 'fetchone', Mock(side_effect=[(False, ), Exception])):
                self.p.rewind_needed_and_possible(self.leader)

    @patch.object(Postgresql, 'start', Mock())
    @patch.object(Postgresql, 'can_rewind', PropertyMock(return_value=True))
    @patch.object(Postgresql, '_get_local_timeline_lsn', Mock(return_value=(2, '40159C1')))
    @patch.object(Postgresql, 'check_leader_is_not_in_recovery')
    def test__check_timeline_and_lsn(self, mock_check_leader_is_not_in_recovery):
        mock_check_leader_is_not_in_recovery.return_value = False
        self.p.trigger_check_diverged_lsn()
        self.assertFalse(self.p.rewind_needed_and_possible(self.leader))
        mock_check_leader_is_not_in_recovery.return_value = True
        self.assertFalse(self.p.rewind_needed_and_possible(self.leader))
        self.p.trigger_check_diverged_lsn()
        with patch('psycopg2.connect', Mock(side_effect=Exception)):
            self.assertFalse(self.p.rewind_needed_and_possible(self.leader))
        self.p.trigger_check_diverged_lsn()
        with patch.object(MockCursor, 'fetchone', Mock(side_effect=[('', 2, '0/0'), ('', b'3\t0/40159C0\tn\n')])):
            self.assertFalse(self.p.rewind_needed_and_possible(self.leader))
        self.p.trigger_check_diverged_lsn()
        with patch.object(MockCursor, 'fetchone', Mock(return_value=('', 1, '0/0'))):
            with patch.object(Postgresql, '_get_local_timeline_lsn', Mock(return_value=(1, '0/0'))):
                self.assertFalse(self.p.rewind_needed_and_possible(self.leader))
            self.p.trigger_check_diverged_lsn()
            self.assertTrue(self.p.rewind_needed_and_possible(self.leader))

    @patch.object(MockCursor, 'fetchone', Mock(side_effect=[(True,), Exception]))
    def test_check_leader_is_not_in_recovery(self):
        self.p.check_leader_is_not_in_recovery()
        self.p.check_leader_is_not_in_recovery()

    @patch.object(Postgresql, 'cancellable_subprocess_call', Mock(return_value=0))
    @patch.object(Postgresql, 'checkpoint', side_effect=['', '1'])
    @patch.object(Postgresql, 'stop', Mock(return_value=False))
    @patch.object(Postgresql, 'start', Mock())
    def test_rewind(self, mock_checkpoint):
        self.p.rewind(self.leader)
        with patch.object(Postgresql, 'pg_rewind', Mock(return_value=False)):
            mock_checkpoint.side_effect = ['1', '', '', '']
            self.p.rewind(self.leader)
            self.p.rewind(self.leader)
            with patch.object(Postgresql, 'check_leader_is_not_in_recovery', Mock(return_value=False)):
                self.p.rewind(self.leader)
            self.p.config['remove_data_directory_on_rewind_failure'] = False
            self.p.trigger_check_diverged_lsn()
            self.p.rewind(self.leader)
        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            self.p.rewind(self.leader)
            self.p.is_leader = Mock(return_value=False)
            self.p.rewind(self.leader)

    @patch.object(Postgresql, 'is_running', Mock(return_value=False))
    @patch.object(Postgresql, 'start', Mock())
    def test_follow(self):
        self.p.follow(None)

    @patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string))
    def test_can_rewind(self):
        with patch('subprocess.call', MagicMock(return_value=1)):
            self.assertFalse(self.p.can_rewind)
        with patch('subprocess.call', side_effect=OSError):
            self.assertFalse(self.p.can_rewind)
        with patch.object(Postgresql, 'controldata', Mock(return_value={'wal_log_hints setting': 'on'})):
            self.assertTrue(self.p.can_rewind)
        self.p.config['use_pg_rewind'] = False
        self.assertFalse(self.p.can_rewind)

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'cancellable_subprocess_call')
    @patch.object(Postgresql, 'remove_data_directory', Mock(return_value=True))
    def test_create_replica(self, mock_cancellable_subprocess_call):
        self.p.delete_trigger_file = Mock(side_effect=OSError)

        self.p.config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config['wale'] = {'command': 'foo'}
        mock_cancellable_subprocess_call.return_value = 0
        self.assertEquals(self.p.create_replica(self.leader), 0)
        del self.p.config['wale']
        self.assertEquals(self.p.create_replica(self.leader), 0)

        mock_cancellable_subprocess_call.return_value = 1
        self.assertEquals(self.p.create_replica(self.leader), 1)

        mock_cancellable_subprocess_call.side_effect = Exception('foo')
        self.assertEquals(self.p.create_replica(self.leader), 1)

        mock_cancellable_subprocess_call.side_effect = [1, 0]
        self.assertEquals(self.p.create_replica(self.leader), 0)

        mock_cancellable_subprocess_call.side_effect = [Exception(), 0]
        self.assertEquals(self.p.create_replica(self.leader), 0)

        self.p.cancel()
        self.assertEquals(self.p.create_replica(self.leader), 1)

    def test_basebackup(self):
        self.p.cancel()
        self.p.basebackup(None, None)

    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_sync_replication_slots(self):
        self.p.start()
        cluster = Cluster(True, None, self.leader, 0, [self.me, self.other, self.leadermem], None, None, None)
        with mock.patch('patroni.postgresql.Postgresql._query', Mock(side_effect=psycopg2.OperationalError)):
            self.p.sync_replication_slots(cluster)
        self.p.sync_replication_slots(cluster)
        with mock.patch('patroni.postgresql.Postgresql.role', new_callable=PropertyMock(return_value='replica')):
            self.p.sync_replication_slots(cluster)
        with mock.patch('patroni.postgresql.logger.error', new_callable=Mock()) as errorlog_mock:
            self.p.query = Mock()
            alias1 = Member(0, 'test-3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            alias2 = Member(0, 'test.3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            cluster.members.extend([alias1, alias2])
            self.p.sync_replication_slots(cluster)
            errorlog_mock.assert_called_once()
            assert "test-3" in errorlog_mock.call_args[0][1]
            assert "test.3" in errorlog_mock.call_args[0][1]

    @patch.object(MockCursor, 'execute', Mock(side_effect=psycopg2.OperationalError))
    def test__query(self):
        self.assertRaises(PostgresConnectionException, self.p._query, 'blabla')
        self.p._state = 'restarting'
        self.assertRaises(RetryFailedError, self.p._query, 'blabla')

    def test_query(self):
        self.p.query('select 1')
        self.assertRaises(PostgresConnectionException, self.p.query, 'RetryFailedError')
        self.assertRaises(psycopg2.ProgrammingError, self.p.query, 'blabla')

    @patch.object(Postgresql, 'pg_isready', Mock(return_value=STATE_REJECT))
    def test_is_leader(self):
        self.assertTrue(self.p.is_leader())
        self.p.reset_cluster_info_state()
        with patch.object(Postgresql, '_query', Mock(side_effect=RetryFailedError(''))):
            self.assertRaises(PostgresConnectionException, self.p.is_leader)

    def test_reload(self):
        self.assertTrue(self.p.reload())

    @patch.object(Postgresql, 'is_running')
    def test_is_healthy(self, mock_is_running):
        mock_is_running.return_value = True
        self.assertTrue(self.p.is_healthy())
        mock_is_running.return_value = False
        self.assertFalse(self.p.is_healthy())

    def test_promote(self):
        self.p.set_role('replica')
        self.assertIsNone(self.p.promote(0))
        self.assertTrue(self.p.promote(0))

    def test_timeline_wal_position(self):
        self.assertEquals(self.p.timeline_wal_position(), (1, 2))
        Thread(target=self.p.timeline_wal_position).start()

    @patch.object(PostmasterProcess, 'from_pidfile')
    def test_is_running(self, mock_frompidfile):
        # Cached postmaster running
        mock_postmaster = self.p._postmaster_proc = MockPostmaster()
        self.assertEquals(self.p.is_running(), mock_postmaster)

        # Cached postmaster not running, no postmaster running
        mock_postmaster.is_running.return_value = False
        mock_frompidfile.return_value = None
        self.assertEquals(self.p.is_running(), None)
        self.assertEquals(self.p._postmaster_proc, None)

        # No cached postmaster, postmaster running
        mock_frompidfile.return_value = mock_postmaster2 = MockPostmaster()
        self.assertEquals(self.p.is_running(), mock_postmaster2)
        self.assertEquals(self.p._postmaster_proc, mock_postmaster2)

    @patch('shlex.split', Mock(side_effect=OSError))
    def test_call_nowait(self):
        self.p.set_role('replica')
        self.assertIsNone(self.p.call_nowait('on_start'))
        self.p.bootstrapping = True
        self.assertIsNone(self.p.call_nowait('on_start'))

    def test_non_existing_callback(self):
        self.assertFalse(self.p.call_nowait('foobar'))

    @patch.object(Postgresql, 'is_running', Mock(return_value=MockPostmaster()))
    def test_is_leader_exception(self):
        self.p.start()
        self.p.query = Mock(side_effect=psycopg2.OperationalError("not supported"))
        self.assertTrue(self.p.stop())

    @patch('os.rename', Mock())
    @patch('os.path.isdir', Mock(return_value=True))
    def test_move_data_directory(self):
        self.p.move_data_directory()
        with patch('os.rename', Mock(side_effect=OSError)):
            self.p.move_data_directory()

    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_bootstrap(self):
        with patch('subprocess.call', Mock(return_value=1)):
            self.assertFalse(self.p.bootstrap({}))

        config = {'users': {'replicator': {'password': 'rep-pass', 'options': ['replication']}}}

        self.p.bootstrap(config)
        with open(os.path.join(self.config_dir, 'pg_hba.conf')) as f:
            lines = f.readlines()
            self.assertTrue('host all all 0.0.0.0/0 md5\n' in lines)

        self.p.config.pop('pg_hba')
        config.update({'post_init': '/bin/false',
                       'pg_hba': ['host replication replicator 127.0.0.1/32 md5',
                                  'hostssl all all 0.0.0.0/0 md5',
                                  'host all all 0.0.0.0/0 md5']})
        self.p.bootstrap(config)
        with open(os.path.join(self.data_dir, 'pg_hba.conf')) as f:
            lines = f.readlines()
            self.assertTrue('host replication replicator 127.0.0.1/32 md5\n' in lines)

    @patch.object(Postgresql, 'cancellable_subprocess_call')
    def test_custom_bootstrap(self, mock_cancellable_subprocess_call):
        config = {'method': 'foo', 'foo': {'command': 'bar'}}

        mock_cancellable_subprocess_call.return_value = 1
        self.assertFalse(self.p.bootstrap(config))

        mock_cancellable_subprocess_call.return_value = 0
        with patch('subprocess.Popen', Mock(side_effect=Exception("42"))),\
                patch('os.path.isfile', Mock(return_value=True)),\
                patch('os.unlink', Mock()),\
                patch.object(Postgresql, 'save_configuration_files', Mock()),\
                patch.object(Postgresql, 'restore_configuration_files', Mock()),\
                patch.object(Postgresql, 'write_recovery_conf', Mock()):
            with self.assertRaises(Exception) as e:
                self.p.bootstrap(config)
            self.assertEqual(str(e.exception), '42')

            config['foo']['recovery_conf'] = {'foo': 'bar'}

            with self.assertRaises(Exception) as e:
                self.p.bootstrap(config)
            self.assertEqual(str(e.exception), '42')

        mock_cancellable_subprocess_call.side_effect = Exception
        self.assertFalse(self.p.bootstrap(config))

    @patch('time.sleep', Mock())
    @patch('os.unlink', Mock())
    @patch.object(Postgresql, 'run_bootstrap_post_init', Mock(return_value=True))
    @patch.object(Postgresql, '_custom_bootstrap', Mock(return_value=True))
    @patch.object(Postgresql, 'start', Mock(return_value=True))
    def test_post_bootstrap(self):
        config = {'method': 'foo', 'foo': {'command': 'bar'}}
        self.p.bootstrap(config)

        task = CriticalTask()
        with patch.object(Postgresql, 'create_or_update_role', Mock(side_effect=Exception)):
            self.p.post_bootstrap({}, task)
            self.assertFalse(task.result)

        self.p.config.pop('pg_hba')
        self.p.post_bootstrap({}, task)
        self.assertTrue(task.result)

        self.p.bootstrap(config)
        self.p.set_state('stopped')
        self.p.reload_config({'authentication': {'superuser': {'username': 'p', 'password': 'p'},
                                                 'replication': {'username': 'r', 'password': 'r'}},
                              'listen': '*', 'retry_timeout': 10, 'parameters': {'hba_file': 'foo'}})
        with patch.object(Postgresql, 'restart', Mock()) as mock_restart:
            self.p.post_bootstrap({}, task)
            mock_restart.assert_called_once()

    @patch.object(Postgresql, 'cancellable_subprocess_call')
    def test_run_bootstrap_post_init(self, mock_cancellable_subprocess_call):
        mock_cancellable_subprocess_call.return_value = 1
        self.assertFalse(self.p.run_bootstrap_post_init({'post_init': '/bin/false'}))

        mock_cancellable_subprocess_call.return_value = 0
        self.p._superuser.pop('username')
        self.assertTrue(self.p.run_bootstrap_post_init({'post_init': '/bin/false'}))
        mock_cancellable_subprocess_call.assert_called()
        args, kwargs = mock_cancellable_subprocess_call.call_args
        self.assertTrue('PGPASSFILE' in kwargs['env'])
        self.assertEquals(args[0], ['/bin/false', 'postgres://127.0.0.2:5432/postgres'])

        mock_cancellable_subprocess_call.reset_mock()
        self.p._local_address.pop('host')
        self.assertTrue(self.p.run_bootstrap_post_init({'post_init': '/bin/false'}))
        mock_cancellable_subprocess_call.assert_called()
        self.assertEquals(mock_cancellable_subprocess_call.call_args[0][0], ['/bin/false', 'postgres://:5432/postgres'])

        mock_cancellable_subprocess_call.side_effect = OSError
        self.assertFalse(self.p.run_bootstrap_post_init({'post_init': '/bin/false'}))

    @patch('patroni.postgresql.Postgresql.create_replica', Mock(return_value=0))
    def test_clone(self):
        self.p.clone(self.leader)

    @patch('os.listdir', Mock(return_value=['recovery.conf']))
    @patch('os.path.exists', Mock(return_value=True))
    def test_get_postgres_role_from_data_directory(self):
        self.assertEquals(self.p.get_postgres_role_from_data_directory(), 'replica')

    def test_remove_data_directory(self):
        self.p.remove_data_directory()
        open(self.data_dir, 'w').close()
        self.p.remove_data_directory()
        os.symlink('unexisting', self.data_dir)
        with patch('os.unlink', Mock(side_effect=OSError)):
            self.p.remove_data_directory()
        self.p.remove_data_directory()

    @patch('patroni.postgresql.Postgresql._version_file_exists', Mock(return_value=True))
    def test_controldata(self):
        with patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string)):
            data = self.p.controldata()
            self.assertEquals(len(data), 50)
            self.assertEquals(data['Database cluster state'], 'shut down in recovery')
            self.assertEquals(data['wal_log_hints setting'], 'on')
            self.assertEquals(int(data['Database block size']), 8192)

        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, ''))):
            self.assertEquals(self.p.controldata(), {})

    @patch('patroni.postgresql.Postgresql._version_file_exists', Mock(return_value=True))
    @patch('subprocess.check_output', MagicMock(return_value=0, side_effect=pg_controldata_string))
    def test_sysid(self):
        self.assertEqual(self.p.sysid, "6200971513092291716")

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_save_configuration_files(self):
        self.p.save_configuration_files()

    @patch('os.path.isfile', Mock(side_effect=[False, True]))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_restore_configuration_files(self):
        self.p.restore_configuration_files()

    def test_can_create_replica_without_replication_connection(self):
        self.p.config['create_replica_method'] = []
        self.assertFalse(self.p.can_create_replica_without_replication_connection())
        self.p.config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config['wale'] = {'command': 'foo', 'no_master': 1}
        self.assertTrue(self.p.can_create_replica_without_replication_connection())

    def test_replica_method_can_work_without_replication_connection(self):
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('basebackup'))
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foobar'))
        self.p.config['foo'] = {'command': 'bar', 'no_master': 1}
        self.assertTrue(self.p.replica_method_can_work_without_replication_connection('foo'))
        self.p.config['foo'] = {'command': 'bar'}
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foo'))

    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_reload_config(self):
        parameters = self._PARAMETERS.copy()
        parameters.pop('f.oo')
        config = {'pg_hba': [''], 'use_unix_socket': True, 'authentication': {},
                  'retry_timeout': 10, 'listen': '*', 'parameters': parameters}
        self.p.reload_config(config)
        parameters['b.ar'] = 'bar'
        self.p.reload_config(config)
        parameters['autovacuum'] = 'on'
        self.p.reload_config(config)
        parameters['autovacuum'] = 'off'
        parameters.pop('search_path')
        config['listen'] = '*:5433'
        self.p.reload_config(config)
        parameters['unix_socket_directories'] = '.'
        self.p.reload_config(config)
        self.p.resolve_connection_addresses()

    @patch.object(Postgresql, '_version_file_exists', Mock(return_value=True))
    def test_get_major_version(self):
        with patch.object(builtins, 'open', mock_open(read_data='9.4')):
            self.assertEquals(self.p.get_major_version(), 90400)
        with patch.object(builtins, 'open', Mock(side_effect=Exception)):
            self.assertEquals(self.p.get_major_version(), 0)

    def test_postmaster_start_time(self):
        with patch.object(MockCursor, "fetchone", Mock(return_value=('foo', True, '', '', '', '', False))):
            self.assertEqual(self.p.postmaster_start_time(), 'foo')
        with patch.object(MockCursor, "execute", side_effect=psycopg2.Error):
            self.assertIsNone(self.p.postmaster_start_time())

    def test_check_for_startup(self):
        with patch('subprocess.call', return_value=0):
            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'running')

        with patch('subprocess.call', return_value=1):
            self.p._state = 'starting'
            self.assertTrue(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'starting')

        with patch('subprocess.call', return_value=2):
            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'start failed')

        with patch('subprocess.call', return_value=0):
            self.p._state = 'running'
            self.assertFalse(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'running')

        with patch('subprocess.call', return_value=127):
            self.p._state = 'running'
            self.assertFalse(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'running')

            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEquals(self.p.state, 'running')

    def test_wait_for_startup(self):
        state = {'sleeps': 0, 'num_rejects': 0, 'final_return': 0}

        def increment_sleeps(*args):
            print("Sleep")
            state['sleeps'] += 1

        def isready_return(*args):
            ret = 1 if state['sleeps'] < state['num_rejects'] else state['final_return']
            print("Isready {0} {1}".format(ret, state))
            return ret

        def time_in_state(*args):
            return state['sleeps']

        with patch('subprocess.call', side_effect=isready_return):
            with patch('time.sleep', side_effect=increment_sleeps):
                self.p.time_in_state = Mock(side_effect=time_in_state)

                self.p._state = 'stopped'
                self.assertTrue(self.p.wait_for_startup())
                self.assertEquals(state['sleeps'], 0)

                self.p._state = 'starting'
                state['num_rejects'] = 5
                self.assertTrue(self.p.wait_for_startup())
                self.assertEquals(state['sleeps'], 5)

                self.p._state = 'starting'
                state['sleeps'] = 0
                state['final_return'] = 2
                self.assertFalse(self.p.wait_for_startup())

                self.p._state = 'starting'
                state['sleeps'] = 0
                state['final_return'] = 0
                self.assertFalse(self.p.wait_for_startup(timeout=2))
                self.assertEquals(state['sleeps'], 3)

        with patch.object(Postgresql, 'check_startup_state_changed', Mock(return_value=False)):
            self.p.cancel()
            self.p._state = 'starting'
            self.assertIsNone(self.p.wait_for_startup())

    def test_read_pid_file(self):
        pidfile = os.path.join(self.data_dir, 'postmaster.pid')
        if os.path.exists(pidfile):
            os.remove(pidfile)
        self.assertEquals(self.p._read_pid_file(), {})
        with open(pidfile, 'w') as fd:
            fd.write("123\n/foo/bar\n123456789\n5432")
        self.assertEquals(self.p._read_pid_file(), {"pid": "123", "data_dir": "/foo/bar",
                                                    "start_time": "123456789", "port": "5432"})

    def test_pick_sync_standby(self):
        cluster = Cluster(True, None, self.leader, 0, [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name), None)

        with patch.object(Postgresql, "query", return_value=[
                    (self.leadermem.name, 'streaming', 'sync'),
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEquals(self.p.pick_synchronous_standby(cluster), (self.leadermem.name, True))

        with patch.object(Postgresql, "query", return_value=[
                    (self.me.name, 'streaming', 'async'),
                    (self.leadermem.name, 'streaming', 'potential'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEquals(self.p.pick_synchronous_standby(cluster), (self.leadermem.name, False))

        with patch.object(Postgresql, "query", return_value=[
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEquals(self.p.pick_synchronous_standby(cluster), (self.me.name, False))

        with patch.object(Postgresql, "query", return_value=[
                    ('missing', 'streaming', 'sync'),
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEquals(self.p.pick_synchronous_standby(cluster), (self.me.name, False))

        with patch.object(Postgresql, "query", return_value=[]):
            self.assertEquals(self.p.pick_synchronous_standby(cluster), (None, False))

    def test_set_sync_standby(self):
        def value_in_conf():
            with open(os.path.join(self.data_dir, 'postgresql.conf')) as f:
                for line in f:
                    if line.startswith('synchronous_standby_names'):
                        return line.strip()

        mock_reload = self.p.reload = Mock()
        self.p.set_synchronous_standby('n1')
        self.assertEquals(value_in_conf(), "synchronous_standby_names = 'n1'")
        mock_reload.assert_called()

        mock_reload.reset_mock()
        self.p.set_synchronous_standby('n1')
        mock_reload.assert_not_called()
        self.assertEquals(value_in_conf(), "synchronous_standby_names = 'n1'")

        self.p.set_synchronous_standby('n2')
        mock_reload.assert_called()
        self.assertEquals(value_in_conf(), "synchronous_standby_names = 'n2'")

        mock_reload.reset_mock()
        self.p.set_synchronous_standby(None)
        mock_reload.assert_called()
        self.assertEquals(value_in_conf(), None)

    def test_get_server_parameters(self):
        config = {'synchronous_mode': True, 'parameters': {'wal_level': 'hot_standby'}, 'listen': '0'}
        self.p.get_server_parameters(config)
        config['synchronous_mode_strict'] = True
        self.p.get_server_parameters(config)
        self.p.set_synchronous_standby('foo')
        self.p.get_server_parameters(config)

    @patch('time.sleep', Mock())
    def test__wait_for_connection_close(self):
        mock_postmaster = MockPostmaster()
        with patch.object(Postgresql, 'is_running', Mock(return_value=mock_postmaster)):
            mock_postmaster.is_running.side_effect = [True, False, False]
            mock_callback = Mock()
            self.p.stop(on_safepoint=mock_callback)

            mock_postmaster.is_running.side_effect = [True, False, False]
            with patch.object(MockCursor, "execute", Mock(side_effect=psycopg2.Error)):
                self.p.stop(on_safepoint=mock_callback)

    def test_terminate_starting_postmaster(self):
        mock_postmaster = MockPostmaster()
        self.p.terminate_starting_postmaster(mock_postmaster)
        mock_postmaster.signal_stop.assert_called()
        mock_postmaster.wait.assert_called()

    def test_read_postmaster_opts(self):
        m = mock_open(read_data='/usr/lib/postgres/9.6/bin/postgres "-D" "data/postgresql0" \
"--listen_addresses=127.0.0.1" "--port=5432" "--hot_standby=on" "--wal_level=hot_standby" \
"--wal_log_hints=on" "--max_wal_senders=5" "--max_replication_slots=5"\n')
        with patch.object(builtins, 'open', m):
            data = self.p.read_postmaster_opts()
            self.assertEquals(data['wal_level'], 'hot_standby')
            self.assertEquals(int(data['max_replication_slots']), 5)
            self.assertEqual(data.get('D'), None)

            m.side_effect = IOError
            data = self.p.read_postmaster_opts()
            self.assertEqual(data, dict())

    @patch('subprocess.Popen')
    def test_single_user_mode(self, subprocess_popen_mock):
        subprocess_popen_mock.return_value.wait.return_value = 0
        self.assertEquals(self.p.single_user_mode('CHECKPOINT', {'archive_mode': 'on'}), 0)

    @patch('os.listdir', Mock(side_effect=[OSError, ['a', 'b']]))
    @patch('os.unlink', Mock(side_effect=OSError))
    @patch('os.remove', Mock())
    @patch('os.path.islink', Mock(side_effect=[True, False]))
    @patch('os.path.isfile', Mock(return_value=True))
    def test_cleanup_archive_status(self):
        self.p.cleanup_archive_status()
        self.p.cleanup_archive_status()

    @patch('os.unlink', Mock())
    @patch('os.path.isfile', Mock(return_value=True))
    @patch.object(Postgresql, 'single_user_mode', Mock(return_value=0))
    def test_fix_cluster_state(self):
        self.assertTrue(self.p.fix_cluster_state())

    def test_replica_cached_timeline(self):
        self.assertEquals(self.p.replica_cached_timeline(1), 2)

    def test_get_master_timeline(self):
        self.assertEquals(self.p.get_master_timeline(), 1)

    def test_cancellable_subprocess_call(self):
        self.p.cancel()
        self.assertRaises(PostgresException, self.p.cancellable_subprocess_call, communicate_input=None)

    @patch('patroni.postgresql.polling_loop', Mock(return_value=[0, 0]))
    def test_cancel(self):
        self.p._cancellable = Mock()
        self.p._cancellable.returncode = None
        self.p.cancel()
        type(self.p._cancellable).returncode = PropertyMock(side_effect=[None, -15])
        self.p.cancel()
