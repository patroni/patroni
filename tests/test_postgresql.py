import mock  # for the mock.call method, importing it without a namespace breaks python3
import os
import psycopg2
import re
import subprocess
import time

from mock import Mock, MagicMock, PropertyMock, patch, mock_open
from patroni.async_executor import CriticalTask
from patroni.dcs import Cluster, ClusterConfig, Member, RemoteMember, SyncState
from patroni.exceptions import PostgresConnectionException, PatroniException
from patroni.postgresql import Postgresql, STATE_REJECT, STATE_NO_RESPONSE
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.postgresql.slots import SlotsHandler
from patroni.utils import RetryFailedError
from six.moves import builtins
from threading import Thread, current_thread

from . import BaseTestPostgresql, MockCursor, MockPostmaster, psycopg2_connect


mtime_ret = {}


def mock_mtime(filename):
    if filename not in mtime_ret:
        mtime_ret[filename] = time.time()
    else:
        mtime_ret[filename] += 1
    return mtime_ret[filename]


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


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
class TestPostgresql(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=120000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def setUp(self):
        super(TestPostgresql, self).setUp()
        self.p.config.write_postgresql_conf()
        self.p._callback_executor = Mock()

    @patch('subprocess.Popen')
    @patch.object(Postgresql, 'wait_for_startup')
    @patch.object(Postgresql, 'wait_for_port_open')
    @patch.object(Postgresql, 'is_running')
    @patch.object(Postgresql, 'controldata', Mock())
    def test_start(self, mock_is_running, mock_wait_for_port_open, mock_wait_for_startup, mock_popen):
        mock_is_running.return_value = MockPostmaster()
        mock_wait_for_port_open.return_value = True
        mock_wait_for_startup.return_value = False
        mock_popen.return_value.stdout.readline.return_value = '123'
        self.assertTrue(self.p.start())
        mock_is_running.return_value = None

        mock_postmaster = MockPostmaster()
        with patch.object(PostmasterProcess, 'start', return_value=mock_postmaster):
            pg_conf = os.path.join(self.p.data_dir, 'postgresql.conf')
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

        self.p.cancellable.cancel()
        self.assertFalse(self.p.start())
        with patch('patroni.postgresql.config.ConfigHandler.effective_configuration',
                   PropertyMock(side_effect=Exception)):
            self.assertIsNone(self.p.start())

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
        self.p.cancellable.cancel()
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
        self.assertEqual(self.p.state, 'restart failed (restarting)')

    @patch('os.chmod', Mock())
    @patch.object(builtins, 'open', MagicMock())
    def test_write_pgpass(self):
        self.p.config.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo'})
        self.p.config.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo', 'password': 'bar'})

    def test_checkpoint(self):
        with patch.object(MockCursor, 'fetchone', Mock(return_value=(True, ))):
            self.assertEqual(self.p.checkpoint({'user': 'postgres'}), 'is_in_recovery=true')
        with patch.object(MockCursor, 'execute', Mock(return_value=None)):
            self.assertIsNone(self.p.checkpoint())
        self.assertEqual(self.p.checkpoint(), 'not accessible or not healty')

    @patch('patroni.postgresql.config.mtime', mock_mtime)
    @patch('patroni.postgresql.config.ConfigHandler._get_pg_settings')
    def test_check_recovery_conf(self, mock_get_pg_settings):
        mock_get_pg_settings.return_value = {
            'primary_conninfo': ['primary_conninfo', 'foo=', None, 'string', 'postmaster', self.p.config._auto_conf],
            'recovery_min_apply_delay': ['recovery_min_apply_delay', '0', 'ms', 'integer', 'sighup', 'foo']
        }
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        self.p.config.write_recovery_conf({'standby_mode': 'on'})
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        mock_get_pg_settings.return_value['primary_conninfo'][1] = ''
        mock_get_pg_settings.return_value['recovery_min_apply_delay'][1] = '1'
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
        mock_get_pg_settings.return_value['recovery_min_apply_delay'][5] = self.p.config._auto_conf
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, False))
        mock_get_pg_settings.return_value['recovery_min_apply_delay'][1] = '0'
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
        conninfo = {'host': '1', 'password': 'bar'}
        with patch('patroni.postgresql.config.ConfigHandler.primary_conninfo_params', Mock(return_value=conninfo)):
            mock_get_pg_settings.return_value['recovery_min_apply_delay'][1] = '1'
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
            mock_get_pg_settings.return_value['primary_conninfo'][1] = 'host=1 passfile='\
                + re.sub(r'([\'\\ ])', r'\\\1', self.p.config._pgpass)
            mock_get_pg_settings.return_value['recovery_min_apply_delay'][1] = '0'
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
            self.p.config.write_recovery_conf({'standby_mode': 'on', 'primary_conninfo': conninfo.copy()})
            self.p.config.write_postgresql_conf()
            self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=120000))
    @patch.object(Postgresql, 'is_running', MockPostmaster)
    @patch.object(MockPostmaster, 'create_time', Mock(return_value=1234567), create=True)
    @patch('patroni.postgresql.config.ConfigHandler._get_pg_settings')
    def test__read_recovery_params(self, mock_get_pg_settings):
        mock_get_pg_settings.return_value = {'primary_conninfo': ['primary_conninfo', '', None, 'string',
                                                                  'postmaster', self.p.config._postgresql_conf]}
        self.p.config.write_recovery_conf({'standby_mode': 'on', 'primary_conninfo': {'password': 'foo'}})
        self.p.config.write_postgresql_conf()
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
        mock_get_pg_settings.side_effect = Exception
        with patch('patroni.postgresql.config.mtime', mock_mtime):
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=100000))
    def test__read_recovery_params_pre_v12(self):
        self.p.config.write_recovery_conf({'standby_mode': 'on', 'primary_conninfo': {'password': 'foo'}})
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        self.p.config.write_recovery_conf({'standby_mode': '\n'})
        with patch('patroni.postgresql.config.mtime', mock_mtime):
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))

    def test_write_postgresql_and_sanitize_auto_conf(self):
        read_data = 'primary_conninfo = foo\nfoo = bar\n'
        with open(os.path.join(self.p.data_dir, 'postgresql.auto.conf'), 'w') as f:
            f.write(read_data)

        mock_read_auto = mock_open(read_data=read_data)
        mock_read_auto.return_value.__iter__ = lambda o: iter(o.readline, '')
        with patch.object(builtins, 'open', Mock(side_effect=[mock_open()(), mock_read_auto(), IOError])),\
                patch('os.chmod', Mock()):
            self.p.config.write_postgresql_conf()

        with patch.object(builtins, 'open', Mock(side_effect=[mock_open()(), IOError])), patch('os.chmod', Mock()):
            self.p.config.write_postgresql_conf()
        self.p.config.write_recovery_conf({'foo': 'bar'})
        self.p.config.write_postgresql_conf()

    @patch.object(Postgresql, 'is_running', Mock(return_value=False))
    @patch.object(Postgresql, 'start', Mock())
    def test_follow(self):
        self.p.call_nowait('on_start')
        m = RemoteMember('1', {'restore_command': '2', 'primary_slot_name': 'foo', 'conn_kwargs': {'host': 'bar'}})
        self.p.follow(m)

    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_sync_replication_slots(self):
        self.p.start()
        config = ClusterConfig(1, {'slots': {'test_3': {'database': 'a', 'plugin': 'b'},
                                             'A': 0, 'ls': 0, 'b': {'type': 'logical', 'plugin': '1'}}}, 1)
        cluster = Cluster(True, config, self.leader, 0, [self.me, self.other, self.leadermem], None, None, None)
        with mock.patch('patroni.postgresql.Postgresql._query', Mock(side_effect=psycopg2.OperationalError)):
            self.p.slots_handler.sync_replication_slots(cluster)
        self.p.slots_handler.sync_replication_slots(cluster)
        with mock.patch('patroni.postgresql.Postgresql.role', new_callable=PropertyMock(return_value='replica')):
            self.p.slots_handler.sync_replication_slots(cluster)
        with patch.object(SlotsHandler, 'drop_replication_slot', Mock(return_value=True)),\
                patch('patroni.dcs.logger.error', new_callable=Mock()) as errorlog_mock:
            alias1 = Member(0, 'test-3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            alias2 = Member(0, 'test.3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            cluster.members.extend([alias1, alias2])
            self.p.slots_handler.sync_replication_slots(cluster)
            self.assertEqual(errorlog_mock.call_count, 5)
            ca = errorlog_mock.call_args_list[0][0][1]
            self.assertTrue("test-3" in ca, "non matching {0}".format(ca))
            self.assertTrue("test.3" in ca, "non matching {0}".format(ca))

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
        self.assertEqual(self.p.timeline_wal_position(), (1, 2, 1))
        Thread(target=self.p.timeline_wal_position).start()

    @patch.object(PostmasterProcess, 'from_pidfile')
    def test_is_running(self, mock_frompidfile):
        # Cached postmaster running
        mock_postmaster = self.p._postmaster_proc = MockPostmaster()
        self.assertEqual(self.p.is_running(), mock_postmaster)

        # Cached postmaster not running, no postmaster running
        mock_postmaster.is_running.return_value = False
        mock_frompidfile.return_value = None
        self.assertEqual(self.p.is_running(), None)
        self.assertEqual(self.p._postmaster_proc, None)

        # No cached postmaster, postmaster running
        mock_frompidfile.return_value = mock_postmaster2 = MockPostmaster()
        self.assertEqual(self.p.is_running(), mock_postmaster2)
        self.assertEqual(self.p._postmaster_proc, mock_postmaster2)

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

    @patch('os.listdir', Mock(return_value=['recovery.conf']))
    @patch('os.path.exists', Mock(return_value=True))
    @patch.object(Postgresql, 'controldata', Mock())
    def test_get_postgres_role_from_data_directory(self):
        self.assertEqual(self.p.get_postgres_role_from_data_directory(), 'replica')

    def test_remove_data_directory(self):
        def _symlink(src, dst):
            try:
                os.symlink(src, dst)
            except OSError:
                if os.name == 'nt':  # os.symlink under Windows needs admin rights skip it
                    pass
        os.makedirs(os.path.join(self.p.data_dir, 'foo'))
        _symlink('foo', os.path.join(self.p.data_dir, 'pg_wal'))
        self.p.remove_data_directory()
        open(self.p.data_dir, 'w').close()
        self.p.remove_data_directory()
        _symlink('unexisting', self.p.data_dir)
        with patch('os.unlink', Mock(side_effect=OSError)):
            self.p.remove_data_directory()
        self.p.remove_data_directory()

    @patch('patroni.postgresql.Postgresql._version_file_exists', Mock(return_value=True))
    def test_controldata(self):
        with patch('subprocess.check_output', Mock(return_value=0, side_effect=pg_controldata_string)):
            data = self.p.controldata()
            self.assertEqual(len(data), 50)
            self.assertEqual(data['Database cluster state'], 'shut down in recovery')
            self.assertEqual(data['wal_log_hints setting'], 'on')
            self.assertEqual(int(data['Database block size']), 8192)

        with patch('subprocess.check_output', Mock(side_effect=subprocess.CalledProcessError(1, ''))):
            self.assertEqual(self.p.controldata(), {})

    @patch('patroni.postgresql.Postgresql._version_file_exists', Mock(return_value=True))
    @patch('subprocess.check_output', MagicMock(return_value=0, side_effect=pg_controldata_string))
    def test_sysid(self):
        self.assertEqual(self.p.sysid, "6200971513092291716")

    @patch('os.path.isfile', Mock(return_value=True))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_save_configuration_files(self):
        self.p.config.save_configuration_files()

    @patch('os.path.isfile', Mock(side_effect=[False, True]))
    @patch('shutil.copy', Mock(side_effect=IOError))
    def test_restore_configuration_files(self):
        self.p.config.restore_configuration_files()

    def test_can_create_replica_without_replication_connection(self):
        self.p.config._config['create_replica_method'] = []
        self.assertFalse(self.p.can_create_replica_without_replication_connection())
        self.p.config._config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config._config['wale'] = {'command': 'foo', 'no_master': 1}
        self.assertTrue(self.p.can_create_replica_without_replication_connection())

    def test_replica_method_can_work_without_replication_connection(self):
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('basebackup'))
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foobar'))
        self.p.config._config['foo'] = {'command': 'bar', 'no_master': 1}
        self.assertTrue(self.p.replica_method_can_work_without_replication_connection('foo'))
        self.p.config._config['foo'] = {'command': 'bar'}
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foo'))

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    @patch.object(MockCursor, 'fetchone')
    def test_reload_config(self, mock_fetchone):
        mock_fetchone.return_value = (1,)
        parameters = self._PARAMETERS.copy()
        parameters.pop('f.oo')
        parameters['wal_buffers'] = '512'
        config = {'pg_hba': [''], 'pg_ident': [''], 'use_unix_socket': True, 'authentication': {},
                  'retry_timeout': 10, 'listen': '*', 'krbsrvname': 'postgres', 'parameters': parameters}
        self.p.reload_config(config)
        mock_fetchone.side_effect = Exception
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
        self.p.config.resolve_connection_addresses()

    @patch.object(Postgresql, '_version_file_exists', Mock(return_value=True))
    def test_get_major_version(self):
        with patch.object(builtins, 'open', mock_open(read_data='9.4')):
            self.assertEqual(self.p.get_major_version(), 90400)
        with patch.object(builtins, 'open', Mock(side_effect=Exception)):
            self.assertEqual(self.p.get_major_version(), 0)

    def test_postmaster_start_time(self):
        with patch.object(MockCursor, "fetchone", Mock(return_value=('foo', True, '', '', '', '', False))):
            self.assertEqual(self.p.postmaster_start_time(), 'foo')
            t = Thread(target=self.p.postmaster_start_time)
            t.start()
            t.join()

        with patch.object(MockCursor, "execute", side_effect=psycopg2.Error):
            self.assertIsNone(self.p.postmaster_start_time())

    def test_check_for_startup(self):
        with patch('subprocess.call', return_value=0):
            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'running')

        with patch('subprocess.call', return_value=1):
            self.p._state = 'starting'
            self.assertTrue(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'starting')

        with patch('subprocess.call', return_value=2):
            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'start failed')

        with patch('subprocess.call', return_value=0):
            self.p._state = 'running'
            self.assertFalse(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'running')

        with patch('subprocess.call', return_value=127):
            self.p._state = 'running'
            self.assertFalse(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'running')

            self.p._state = 'starting'
            self.assertFalse(self.p.check_for_startup())
            self.assertEqual(self.p.state, 'running')

    def test_wait_for_startup(self):
        state = {'sleeps': 0, 'num_rejects': 0, 'final_return': 0}
        self.__thread_ident = current_thread().ident

        def increment_sleeps(*args):
            if current_thread().ident == self.__thread_ident:
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
                self.assertEqual(state['sleeps'], 0)

                self.p._state = 'starting'
                state['num_rejects'] = 5
                self.assertTrue(self.p.wait_for_startup())
                self.assertEqual(state['sleeps'], 5)

                self.p._state = 'starting'
                state['sleeps'] = 0
                state['final_return'] = 2
                self.assertFalse(self.p.wait_for_startup())

                self.p._state = 'starting'
                state['sleeps'] = 0
                state['final_return'] = 0
                self.assertFalse(self.p.wait_for_startup(timeout=2))
                self.assertEqual(state['sleeps'], 3)

        with patch.object(Postgresql, 'check_startup_state_changed', Mock(return_value=False)):
            self.p.cancellable.cancel()
            self.p._state = 'starting'
            self.assertIsNone(self.p.wait_for_startup())

    def test_pick_sync_standby(self):
        cluster = Cluster(True, None, self.leader, 0, [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name), None)

        with patch.object(Postgresql, "query", return_value=[
                    (self.leadermem.name, 'streaming', 'sync'),
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEqual(self.p.pick_synchronous_standby(cluster), (self.leadermem.name, True))

        with patch.object(Postgresql, "query", return_value=[
                    (self.me.name, 'streaming', 'async'),
                    (self.leadermem.name, 'streaming', 'potential'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEqual(self.p.pick_synchronous_standby(cluster), (self.leadermem.name, False))

        with patch.object(Postgresql, "query", return_value=[
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEqual(self.p.pick_synchronous_standby(cluster), (self.me.name, False))

        with patch.object(Postgresql, "query", return_value=[
                    ('missing', 'streaming', 'sync'),
                    (self.me.name, 'streaming', 'async'),
                    (self.other.name, 'streaming', 'async'),
                ]):
            self.assertEqual(self.p.pick_synchronous_standby(cluster), (self.me.name, False))

        with patch.object(Postgresql, "query", return_value=[]):
            self.assertEqual(self.p.pick_synchronous_standby(cluster), (None, False))

    def test_set_sync_standby(self):
        def value_in_conf():
            with open(os.path.join(self.p.data_dir, 'postgresql.conf')) as f:
                for line in f:
                    if line.startswith('synchronous_standby_names'):
                        return line.strip()

        mock_reload = self.p.reload = Mock()
        self.p.config.set_synchronous_standby('n1')
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")
        mock_reload.assert_called()

        mock_reload.reset_mock()
        self.p.config.set_synchronous_standby('n1')
        mock_reload.assert_not_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")

        self.p.config.set_synchronous_standby('n2')
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n2'")

        mock_reload.reset_mock()
        self.p.config.set_synchronous_standby(None)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), None)

    def test_get_server_parameters(self):
        config = {'synchronous_mode': True, 'parameters': {'wal_level': 'hot_standby'}, 'listen': '0'}
        self.p.config.get_server_parameters(config)
        config['synchronous_mode_strict'] = True
        self.p.config.get_server_parameters(config)
        self.p.config.set_synchronous_standby('foo')
        self.assertTrue(str(self.p.config.get_server_parameters(config)).startswith('{'))

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
            self.assertEqual(data['wal_level'], 'hot_standby')
            self.assertEqual(int(data['max_replication_slots']), 5)
            self.assertEqual(data.get('D'), None)

            m.side_effect = IOError
            data = self.p.read_postmaster_opts()
            self.assertEqual(data, dict())

    @patch('psutil.Popen')
    def test_single_user_mode(self, subprocess_popen_mock):
        subprocess_popen_mock.return_value.wait.return_value = 0
        self.assertEqual(self.p.single_user_mode('CHECKPOINT', {'archive_mode': 'on'}), 0)

    @patch('os.listdir', Mock(side_effect=[OSError, ['a', 'b']]))
    @patch('os.unlink', Mock(side_effect=OSError))
    @patch('os.remove', Mock())
    @patch('os.path.islink', Mock(side_effect=[True, False]))
    @patch('os.path.isfile', Mock(return_value=True))
    def test_cleanup_archive_status(self):
        self.p.cleanup_archive_status()
        self.p.cleanup_archive_status()

    @patch('os.unlink', Mock())
    @patch('os.listdir', Mock(return_value=[]))
    @patch('os.path.isfile', Mock(return_value=True))
    @patch.object(Postgresql, 'read_postmaster_opts', Mock(return_value={}))
    @patch.object(Postgresql, 'single_user_mode', Mock(return_value=0))
    def test_fix_cluster_state(self):
        self.assertTrue(self.p.fix_cluster_state())

    def test_replica_cached_timeline(self):
        self.assertEqual(self.p.replica_cached_timeline(1), 2)

    def test_get_master_timeline(self):
        self.assertEqual(self.p.get_master_timeline(), 1)

    @patch.object(Postgresql, 'get_postgres_role_from_data_directory', Mock(return_value='replica'))
    def test__build_effective_configuration(self):
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'max_connections setting': '200',
                                             'max_worker_processes setting': '20',
                                             'max_prepared_xacts setting': '100',
                                             'max_locks_per_xact setting': '100',
                                             'max_wal_senders setting': 10})):
            self.p.cancellable.cancel()
            self.assertFalse(self.p.start())
            self.assertTrue(self.p.pending_restart)

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isfile', Mock(return_value=False))
    def test_pgpass_is_dir(self):
        self.assertRaises(PatroniException, self.setUp)
