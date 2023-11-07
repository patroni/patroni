import datetime
import os
import psutil
import re
import subprocess
import time

from mock import Mock, MagicMock, PropertyMock, patch, mock_open

import patroni.psycopg as psycopg

from patroni.async_executor import CriticalTask
from patroni.collections import CaseInsensitiveSet
from patroni.config import GlobalConfig
from patroni.dcs import RemoteMember
from patroni.exceptions import PostgresConnectionException, PatroniException
from patroni.postgresql import Postgresql, STATE_REJECT, STATE_NO_RESPONSE
from patroni.postgresql.bootstrap import Bootstrap
from patroni.postgresql.callback_executor import CallbackAction
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.postgresql.validator import (ValidatorFactoryNoType, ValidatorFactoryInvalidType,
                                          ValidatorFactoryInvalidSpec, ValidatorFactory, InvalidGucValidatorsFile,
                                          _get_postgres_guc_validators, _read_postgres_gucs_validators_file,
                                          _load_postgres_gucs_validators, Bool, Integer, Real, Enum, EnumBool, String)
from patroni.utils import RetryFailedError
from threading import Thread, current_thread

from . import BaseTestPostgresql, MockCursor, MockPostmaster, psycopg_connect, mock_available_gucs


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
@patch('patroni.psycopg.connect', psycopg_connect)
@patch.object(Postgresql, 'available_gucs', mock_available_gucs)
class TestPostgresql(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=140000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    @patch.object(Postgresql, 'available_gucs', mock_available_gucs)
    def setUp(self):
        super(TestPostgresql, self).setUp()
        self.p.config.write_postgresql_conf()

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

        with patch.object(Postgresql, 'ensure_major_version_is_known', Mock(return_value=False)):
            self.assertIsNone(self.p.start())

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
        mock_postmaster = MockPostmaster()
        mock_postmaster.is_running.return_value = None

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
    @patch('patroni.postgresql.cancellable.CancellableSubprocess.call')
    def test_stop(self, mock_cancellable_call, mock_is_running):
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

        # Timed out waiting for fast shutdown triggers immediate shutdown
        mock_postmaster.wait.side_effect = [psutil.TimeoutExpired(30), psutil.TimeoutExpired(30), Mock()]
        mock_callback.reset_mock()
        self.assertTrue(self.p.stop(on_safepoint=mock_callback, stop_timeout=30))
        mock_callback.assert_called()
        mock_postmaster.signal_stop.assert_called()

        # Immediate shutdown succeeded
        mock_postmaster.wait.side_effect = [psutil.TimeoutExpired(30), Mock()]
        self.assertTrue(self.p.stop(on_safepoint=mock_callback, stop_timeout=30))

        # Ensure before_stop script is called when configured to
        self.p.config._config['before_stop'] = ':'
        mock_postmaster.wait.side_effect = [psutil.TimeoutExpired(30), Mock()]
        mock_cancellable_call.return_value = 0
        with patch('patroni.postgresql.logger.info') as mock_logger:
            self.p.stop(on_safepoint=mock_callback, stop_timeout=30)
            self.assertEqual(mock_logger.call_args[0], ('before_stop script `%s` exited with %s', ':', 0))
        mock_postmaster.wait.side_effect = [psutil.TimeoutExpired(30), Mock()]
        mock_cancellable_call.side_effect = Exception
        with patch('patroni.postgresql.logger.error') as mock_logger:
            self.p.stop(on_safepoint=mock_callback, stop_timeout=30)
            self.assertEqual(mock_logger.call_args_list[1][0][0], 'Exception when calling `%s`: %r')

        # Stop signal failed
        mock_postmaster.signal_stop.return_value = False
        self.assertFalse(self.p.stop())

        # Stop signal failed to find process
        mock_postmaster.signal_stop.return_value = True
        mock_callback.reset_mock()
        self.assertTrue(self.p.stop(on_safepoint=mock_callback))
        mock_callback.assert_called()

        # Fast shutdown is timed out but when immediate postmaster is already gone
        mock_postmaster.wait.side_effect = [psutil.TimeoutExpired(30), Mock()]
        mock_postmaster.signal_stop.side_effect = [None, True]
        self.assertTrue(self.p.stop(on_safepoint=mock_callback, stop_timeout=30))

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_running', MockPostmaster)
    @patch.object(Postgresql, '_wait_for_connection_close', Mock())
    @patch.object(Postgresql, 'latest_checkpoint_location', Mock(return_value='7'))
    def test__do_stop(self):
        mock_callback = Mock()
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'Database cluster state': 'shut down',
                                             "Latest checkpoint's TimeLineID": '1',
                                             'Latest checkpoint location': '1/1'})):
            self.assertTrue(self.p.stop(on_shutdown=mock_callback, stop_timeout=3))
            mock_callback.assert_called()
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'Database cluster state': 'shut down in recovery'})):
            self.assertTrue(self.p.stop(on_shutdown=mock_callback, stop_timeout=3))
        with patch.object(Postgresql, 'controldata', Mock(return_value={'Database cluster state': 'shutting down'})):
            self.assertTrue(self.p.stop(on_shutdown=mock_callback, stop_timeout=3))

    def test_restart(self):
        self.p.start = Mock(return_value=False)
        self.assertFalse(self.p.restart())
        self.assertEqual(self.p.state, 'restart failed (restarting)')

    @patch('os.chmod', Mock())
    @patch('builtins.open', MagicMock())
    def test_write_pgpass(self):
        self.p.config.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo'})
        self.p.config.write_pgpass({'host': 'localhost', 'port': '5432', 'user': 'foo', 'password': 'bar'})

    def test_checkpoint(self):
        with patch.object(MockCursor, 'fetchone', Mock(return_value=(True, ))):
            self.assertEqual(self.p.checkpoint({'user': 'postgres'}), 'is_in_recovery=true')
        with patch.object(MockCursor, 'execute', Mock(return_value=None)):
            self.assertIsNone(self.p.checkpoint())
        self.assertEqual(self.p.checkpoint(timeout=10), 'not accessible or not healty')

    @patch('patroni.postgresql.config.mtime', mock_mtime)
    @patch('patroni.postgresql.config.ConfigHandler._get_pg_settings')
    def test_check_recovery_conf(self, mock_get_pg_settings):
        self.p.call_nowait(CallbackAction.ON_START)
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
            mock_get_pg_settings.return_value['primary_conninfo'][1] = 'host=1 target_session_attrs=read-write'\
                + ' passfile=' + re.sub(r'([\'\\ ])', r'\\\1', self.p.config._pgpass)
            mock_get_pg_settings.return_value['recovery_min_apply_delay'][1] = '0'
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
            self.p.config.write_recovery_conf({'standby_mode': 'on', 'primary_conninfo': conninfo.copy()})
            self.p.config.write_postgresql_conf()
            self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
            with patch.object(Postgresql, 'primary_conninfo', Mock(return_value='host=1')):
                mock_get_pg_settings.return_value['primary_slot_name'] = [
                    'primary_slot_name', '', '', 'string', 'postmaster', self.p.config._postgresql_conf]
                self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=120000))
    @patch.object(Postgresql, 'is_running', MockPostmaster)
    @patch.object(MockPostmaster, 'create_time', Mock(return_value=1234567), create=True)
    @patch('patroni.postgresql.config.ConfigHandler._get_pg_settings')
    def test__read_recovery_params(self, mock_get_pg_settings):
        self.p.call_nowait(CallbackAction.ON_START)
        mock_get_pg_settings.return_value = {'primary_conninfo': ['primary_conninfo', '', None, 'string',
                                                                  'postmaster', self.p.config._postgresql_conf]}
        self.p.config.write_recovery_conf({'standby_mode': 'on', 'primary_conninfo': {'password': 'foo'}})
        self.p.config.write_postgresql_conf()
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))
        self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))

        # Config files changed, but can't connect to postgres
        mock_get_pg_settings.side_effect = PostgresConnectionException('')
        with patch('patroni.postgresql.config.mtime', mock_mtime):
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))

        # Config files didn't change, but postgres crashed or in crash recovery
        with patch.object(MockPostmaster, 'create_time', Mock(return_value=1234568), create=True):
            self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))

        # Any other exception raised when executing the query
        mock_get_pg_settings.side_effect = Exception
        with patch('patroni.postgresql.config.mtime', mock_mtime):
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        with patch.object(Postgresql, 'is_starting', Mock(return_value=True)):
            self.assertEqual(self.p.config.check_recovery_conf(None), (False, False))

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=100000))
    @patch.object(Postgresql, 'primary_conninfo', Mock(return_value='host=1'))
    def test__read_recovery_params_pre_v12(self):
        self.p.config.write_recovery_conf({'standby_mode': 'off', 'primary_conninfo': {'password': 'foo'}})
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))
        self.p.config.write_recovery_conf({'restore_command': '\n'})
        with patch('patroni.postgresql.config.mtime', mock_mtime):
            self.assertEqual(self.p.config.check_recovery_conf(None), (True, True))

    def test_write_postgresql_and_sanitize_auto_conf(self):
        read_data = 'primary_conninfo = foo\nfoo = bar\n'
        with open(os.path.join(self.p.data_dir, 'postgresql.auto.conf'), 'w') as f:
            f.write(read_data)

        mock_read_auto = mock_open(read_data=read_data)
        mock_read_auto.return_value.__iter__ = lambda o: iter(o.readline, '')
        with patch('builtins.open', Mock(side_effect=[mock_open()(), mock_read_auto(), IOError])), \
                patch('os.chmod', Mock()):
            self.p.config.write_postgresql_conf()

        with patch('builtins.open', Mock(side_effect=[mock_open()(), IOError])), patch('os.chmod', Mock()):
            self.p.config.write_postgresql_conf()
        self.p.config.write_recovery_conf({'foo': 'bar'})
        self.p.config.write_postgresql_conf()

    @patch.object(Postgresql, 'is_running', Mock(return_value=False))
    @patch.object(Postgresql, 'start', Mock())
    def test_follow(self):
        self.p.call_nowait(CallbackAction.ON_START)
        m = RemoteMember('1', {'restore_command': '2', 'primary_slot_name': 'foo', 'conn_kwargs': {'host': 'bar'}})
        self.p.follow(m)
        with patch.object(Postgresql, 'ensure_major_version_is_known', Mock(return_value=False)):
            self.assertIsNone(self.p.follow(m))

    @patch.object(MockCursor, 'execute', Mock(side_effect=psycopg.OperationalError))
    def test__query(self):
        self.assertRaises(PostgresConnectionException, self.p._query, 'blabla')
        self.p._state = 'restarting'
        self.assertRaises(RetryFailedError, self.p._query, 'blabla')

    def test_query(self):
        self.p.query('select 1')
        self.assertRaises(PostgresConnectionException, self.p.query, 'RetryFailedError')
        self.assertRaises(psycopg.ProgrammingError, self.p.query, 'blabla')

    @patch.object(Postgresql, 'pg_isready', Mock(return_value=STATE_REJECT))
    def test_is_primary(self):
        self.assertTrue(self.p.is_primary())
        self.p.reset_cluster_info_state(None)
        with patch.object(Postgresql, '_query', Mock(side_effect=RetryFailedError(''))):
            self.assertFalse(self.p.is_primary())

    @patch.object(Postgresql, 'controldata', Mock(return_value={'Database cluster state': 'shut down',
                                                                'Latest checkpoint location': '0/1ADBC18',
                                                                "Latest checkpoint's TimeLineID": '1'}))
    @patch('subprocess.Popen')
    def test_latest_checkpoint_location(self, mock_popen):
        mock_popen.return_value.communicate.return_value = (None, None)
        self.assertEqual(self.p.latest_checkpoint_location(), 28163096)
        with patch.object(Postgresql, 'controldata', Mock(return_value={'Database cluster state': 'shut down',
                                                                        'Latest checkpoint location': 'k/1ADBC18',
                                                                        "Latest checkpoint's TimeLineID": '1'})):
            self.assertIsNone(self.p.latest_checkpoint_location())
        # 9.3 and 9.4 format
        mock_popen.return_value.communicate.side_effect = [
            (b'rmgr: XLOG        len (rec/tot):     72/   104, tx:          0, lsn: 0/01ADBC18, prev 0/01ADBBB8, '
             + b'bkp: 0000, desc: checkpoint: redo 0/1ADBC18; tli 1; prev tli 1; fpw true; xid 0/727; oid 16386; multi'
             + b' 1; offset 0; oldest xid 715 in DB 1; oldest multi 1 in DB 1; oldest running xid 0; shutdown', None),
            (b'rmgr: Transaction len (rec/tot):     64/    96, tx:        726, lsn: 0/01ADBBB8, prev 0/01ADBB70, '
             + b'bkp: 0000, desc: commit: 2021-02-26 11:19:37.900918 CET; inval msgs: catcache 11 catcache 10', None)]
        self.assertEqual(self.p.latest_checkpoint_location(), 28163096)
        mock_popen.return_value.communicate.side_effect = [
            (b'rmgr: XLOG        len (rec/tot):     72/   104, tx:          0, lsn: 0/01ADBC18, prev 0/01ADBBB8, '
             + b'bkp: 0000, desc: checkpoint: redo 0/1ADBC18; tli 1; prev tli 1; fpw true; xid 0/727; oid 16386; multi'
             + b' 1; offset 0; oldest xid 715 in DB 1; oldest multi 1 in DB 1; oldest running xid 0; shutdown', None),
            (b'rmgr: XLOG        len (rec/tot):      0/    32, tx:          0, lsn: 0/01ADBBB8, prev 0/01ADBBA0, '
             + b'bkp: 0000, desc: xlog switch ', None)]
        self.assertEqual(self.p.latest_checkpoint_location(), 28163000)
        # 9.5+ format
        mock_popen.return_value.communicate.side_effect = [
            (b'rmgr: XLOG        len (rec/tot):    114/   114, tx:          0, lsn: 0/01ADBC18, prev 0/018260F8, '
             + b'desc: CHECKPOINT_SHUTDOWN redo 0/1825ED8; tli 1; prev tli 1; fpw true; xid 0:494; oid 16387; multi 1'
             + b'; offset 0; oldest xid 479 in DB 1; oldest multi 1 in DB 1; oldest/newest commit timestamp xid: 0/0;'
             + b' oldest running xid 0; shutdown', None),
            (b'rmgr: XLOG        len (rec/tot):     24/    24, tx:          0, lsn: 0/018260F8, prev 0/01826080, '
             + b'desc: SWITCH ', None)]
        self.assertEqual(self.p.latest_checkpoint_location(), 25321720)

    def test_reload(self):
        self.assertTrue(self.p.reload())

    @patch.object(Postgresql, 'is_running')
    def test_is_healthy(self, mock_is_running):
        mock_is_running.return_value = True
        self.assertTrue(self.p.is_healthy())
        mock_is_running.return_value = False
        self.assertFalse(self.p.is_healthy())

    @patch('psutil.Popen')
    def test_promote(self, mock_popen):
        mock_popen.return_value.wait.return_value = 0
        task = CriticalTask()
        self.assertTrue(self.p.promote(0, task))

        self.p.set_role('replica')
        self.p.config._config['pre_promote'] = 'test'
        with patch('patroni.postgresql.cancellable.CancellableSubprocess.is_cancelled', PropertyMock(return_value=1)):
            self.assertFalse(self.p.promote(0, task))

        mock_popen.side_effect = Exception
        self.assertFalse(self.p.promote(0, task))
        task.reset()
        task.cancel()
        self.assertFalse(self.p.promote(0, task))

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
        self.assertIsNone(self.p.call_nowait(CallbackAction.ON_START))
        self.p.bootstrapping = True
        self.assertIsNone(self.p.call_nowait(CallbackAction.ON_START))

    @patch.object(Postgresql, 'is_running', Mock(return_value=MockPostmaster()))
    def test_is_primary_exception(self):
        self.p.start()
        self.p.query = Mock(side_effect=psycopg.OperationalError("not supported"))
        self.assertTrue(self.p.stop())

    @patch('os.rename', Mock())
    @patch('os.path.exists', Mock(return_value=True))
    @patch('shutil.rmtree', Mock())
    @patch('os.path.isdir', Mock(return_value=True))
    @patch('os.unlink', Mock())
    @patch('os.symlink', Mock())
    @patch('patroni.postgresql.Postgresql.pg_wal_realpath', Mock(return_value={'pg_wal': '/mnt/pg_wal'}))
    @patch('patroni.postgresql.Postgresql.pg_tblspc_realpaths', Mock(return_value={'42': '/mnt/tablespaces/archive'}))
    def test_move_data_directory(self):
        self.p.move_data_directory()
        with patch('os.rename', Mock(side_effect=OSError)):
            self.p.move_data_directory()

    @patch('os.listdir', Mock(return_value=['recovery.conf']))
    @patch('os.path.exists', Mock(return_value=True))
    @patch.object(Postgresql, 'controldata', Mock())
    def test_get_postgres_role_from_data_directory(self):
        self.assertEqual(self.p.get_postgres_role_from_data_directory(), 'replica')

    @patch('os.remove', Mock())
    @patch('shutil.rmtree', Mock())
    @patch('os.unlink', Mock(side_effect=OSError))
    @patch('os.path.isdir', Mock(return_value=True))
    @patch('os.path.exists', Mock(return_value=True))
    def test_remove_data_directory(self):
        with patch('os.path.islink', Mock(return_value=True)):
            self.p.remove_data_directory()
        with patch('os.path.isfile', Mock(return_value=True)):
            self.p.remove_data_directory()
        with patch('os.path.islink', Mock(side_effect=[False, False, True, True])), \
                patch('os.listdir', Mock(return_value=['12345'])), \
                patch('os.path.realpath', Mock(side_effect=['../foo', '../foo_tsp'])):
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

    @patch('os.path.isfile', Mock(side_effect=[False, True, False, True]))
    @patch('shutil.copy', Mock(side_effect=[None, IOError]))
    @patch('os.chmod', Mock())
    def test_restore_configuration_files(self):
        self.p.config.restore_configuration_files()

    def test_can_create_replica_without_replication_connection(self):
        self.p.config._config['create_replica_method'] = []
        self.assertFalse(self.p.can_create_replica_without_replication_connection(None))
        self.p.config._config['create_replica_method'] = ['wale', 'basebackup']
        self.p.config._config['wale'] = {'command': 'foo', 'no_leader': 1}
        self.assertTrue(self.p.can_create_replica_without_replication_connection(None))

    def test_replica_method_can_work_without_replication_connection(self):
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('basebackup'))
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foobar'))
        self.p.config._config['foo'] = {'command': 'bar', 'no_leader': 1}
        self.assertTrue(self.p.replica_method_can_work_without_replication_connection('foo'))
        self.p.config._config['foo'] = {'command': 'bar'}
        self.assertFalse(self.p.replica_method_can_work_without_replication_connection('foo'))

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_reload_config(self):
        parameters = self._PARAMETERS.copy()
        parameters.pop('f.oo')
        parameters['wal_buffers'] = '512'
        config = {'pg_hba': [''], 'pg_ident': [''], 'use_unix_socket': True, 'use_unix_socket_repl': True,
                  'authentication': {},
                  'retry_timeout': 10, 'listen': '*', 'krbsrvname': 'postgres', 'parameters': parameters}
        self.p.reload_config(config)
        parameters['b.ar'] = 'bar'
        with patch.object(MockCursor, 'fetchall',
                          Mock(side_effect=[[('wal_block_size', '8191', None, 'integer', 'internal'),
                                             ('wal_segment_size', '2048', '8kB', 'integer', 'internal'),
                                             ('shared_buffers', '16384', '8kB', 'integer', 'postmaster'),
                                             ('wal_buffers', '-1', '8kB', 'integer', 'postmaster'),
                                             ('port', '5433', None, 'integer', 'postmaster')], Exception])):
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

    def test_resolve_connection_addresses(self):
        self.p.config._config['use_unix_socket'] = self.p.config._config['use_unix_socket_repl'] = True
        self.p.config.resolve_connection_addresses()
        self.assertEqual(self.p.config.local_replication_address, {'host': '/tmp', 'port': '5432'})
        self.p.config._server_parameters.pop('unix_socket_directories')
        self.p.config.resolve_connection_addresses()
        self.assertEqual(self.p.connection_pool.conn_kwargs, {'connect_timeout': 3, 'dbname': 'postgres',
                                                              'fallback_application_name': 'Patroni',
                                                              'options': '-c statement_timeout=2000',
                                                              'password': 'test', 'port': '5432', 'user': 'foo'})

    @patch.object(Postgresql, '_version_file_exists', Mock(return_value=True))
    def test_get_major_version(self):
        with patch('builtins.open', mock_open(read_data='9.4')):
            self.assertEqual(self.p.get_major_version(), 90400)
        with patch('builtins.open', Mock(side_effect=Exception)):
            self.assertEqual(self.p.get_major_version(), 0)

    def test_postmaster_start_time(self):
        now = datetime.datetime.now()
        with patch.object(MockCursor, "fetchall", Mock(return_value=[(now, True, '', '', '', '', False)])):
            self.assertEqual(self.p.postmaster_start_time(), now.isoformat(sep=' '))
            t = Thread(target=self.p.postmaster_start_time)
            t.start()
            t.join()

        with patch.object(MockCursor, "execute", side_effect=psycopg.Error):
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

    def test_get_server_parameters(self):
        config = {'parameters': {'wal_level': 'hot_standby', 'max_prepared_transactions': 100}, 'listen': '0'}
        self.p._global_config = GlobalConfig({'synchronous_mode': True})
        self.p.config.get_server_parameters(config)
        self.p._global_config = GlobalConfig({'synchronous_mode': True, 'synchronous_mode_strict': True})
        self.p.config.get_server_parameters(config)
        self.p.config.set_synchronous_standby_names('foo')
        self.assertTrue(str(self.p.config.get_server_parameters(config)).startswith('<CaseInsensitiveDict'))

    @patch('time.sleep', Mock())
    def test__wait_for_connection_close(self):
        mock_postmaster = MockPostmaster()
        with patch.object(Postgresql, 'is_running', Mock(return_value=mock_postmaster)):
            mock_postmaster.is_running.side_effect = [True, False, False]
            mock_callback = Mock()
            self.p.stop(on_safepoint=mock_callback)

            mock_postmaster.is_running.side_effect = [True, False, False]
            with patch.object(MockCursor, "execute", Mock(side_effect=psycopg.Error)):
                self.p.stop(on_safepoint=mock_callback)

    def test_terminate_starting_postmaster(self):
        mock_postmaster = MockPostmaster()
        self.p.terminate_starting_postmaster(mock_postmaster)
        mock_postmaster.signal_stop.assert_called()
        mock_postmaster.wait.assert_called()

    def test_replica_cached_timeline(self):
        self.assertEqual(self.p.replica_cached_timeline(2), 3)

    def test_get_primary_timeline(self):
        self.assertEqual(self.p.get_primary_timeline(), 1)

    @patch.object(Postgresql, 'get_postgres_role_from_data_directory', Mock(return_value='replica'))
    @patch.object(Postgresql, 'is_running', Mock(return_value=False))
    @patch.object(Bootstrap, 'running_custom_bootstrap', PropertyMock(return_value=True))
    @patch.object(Postgresql, 'controldata', Mock(return_value={'max_connections setting': '200',
                                                                'max_worker_processes setting': '20',
                                                                'max_locks_per_xact setting': '100',
                                                                'max_wal_senders setting': 10}))
    @patch('patroni.postgresql.config.logger.warning')
    def test_effective_configuration(self, mock_logger):
        self.p.cancellable.cancel()
        self.p.config.write_recovery_conf({'pause_at_recovery_target': 'false'})
        self.assertFalse(self.p.start())
        mock_logger.assert_called_once()
        self.assertTrue('is missing from pg_controldata output' in mock_logger.call_args[0][0])

        self.assertTrue(self.p.pending_restart)
        with patch.object(Bootstrap, 'keep_existing_recovery_conf', PropertyMock(return_value=True)):
            self.assertFalse(self.p.start())
            self.assertTrue(self.p.pending_restart)

    @patch('os.path.exists', Mock(return_value=True))
    @patch('os.path.isfile', Mock(return_value=False))
    def test_pgpass_is_dir(self):
        self.assertRaises(PatroniException, self.setUp)

    @patch.object(Postgresql, '_query', Mock(side_effect=RetryFailedError('')))
    def test_received_timeline(self):
        self.p.set_role('standby_leader')
        self.p.reset_cluster_info_state(None)
        self.assertRaises(PostgresConnectionException, self.p.received_timeline)

    def test__write_recovery_params(self):
        self.p.config._write_recovery_params(Mock(), {'pause_at_recovery_target': 'false'})
        with patch.object(Postgresql, 'major_version', PropertyMock(return_value=90400)):
            self.p.config._write_recovery_params(Mock(), {'recovery_target_action': 'PROMOTE'})

    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def test_set_enforce_hot_standby_feedback(self):
        self.p.set_enforce_hot_standby_feedback(True)

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=140000))
    @patch.object(Postgresql, '_cluster_info_state_get', Mock(return_value=True))
    def test_handle_parameter_change(self):
        self.p.handle_parameter_change()

    def test_validator_factory(self):
        # validator with no type
        validator = {
            'version_from': 90300,
            'version_till': None,
        }
        with self.assertRaises(ValidatorFactoryNoType) as e:
            ValidatorFactory(validator)
        self.assertEqual(str(e.exception), 'Validator contains no type.')

        # validator with invalid type
        validator = {
            'type': 'Random',
            'version_from': 90300,
            'version_till': None,
        }
        with self.assertRaises(ValidatorFactoryInvalidType) as e:
            ValidatorFactory(validator)
        self.assertEqual(str(e.exception), f'Unexpected validator type: `{validator["type"]}`.')

        # validator with missing attributes
        validator = {
            'type': 'Integer',
            'version_from': 90300,
            'min_val': 0,
        }
        with self.assertRaises(ValidatorFactoryInvalidSpec) as e:
            ValidatorFactory(validator)
        type_ = validator.pop('type')
        self.assertRegex(
            str(e.exception),
            rf"Failed to parse `{type_}` validator \(`{validator}`\): `(Number\.)?__init__\(\) missing 1 "
            "required keyword-only argument: 'max_val'`."
        )

        # valid validators
        # Bool
        validator = {
            'type': 'Bool',
            'version_from': 90300,
            'version_till': None,
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, Bool)
        self.assertEqual(
            ret.__dict__,
            Bool(version_from=validator['version_from'], version_till=validator['version_till']).__dict__,
        )

        # Integer
        validator = {
            'type': 'Integer',
            'version_from': 90300,
            'version_till': None,
            'min_val': 1,
            'max_val': 100,
            'unit': None,
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, Integer)
        self.assertEqual(
            ret.__dict__,
            Integer(version_from=validator['version_from'], version_till=validator['version_till'],
                    min_val=validator['min_val'], max_val=validator['max_val'], unit=validator['unit']).__dict__,
        )

        # Real
        validator = {
            'type': 'Real',
            'version_from': 90300,
            'version_till': None,
            'min_val': 1.0,
            'max_val': 100.0,
            'unit': None,
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, Real)
        self.assertEqual(
            ret.__dict__,
            Real(version_from=validator['version_from'], version_till=validator['version_till'],
                 min_val=validator['min_val'], max_val=validator['max_val'], unit=validator['unit']).__dict__,
        )

        # Enum
        validator = {
            'type': 'Enum',
            'version_from': 90300,
            'version_till': None,
            'possible_values': ('abc', 'def'),
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, Enum)
        self.assertEqual(
            ret.__dict__,
            Enum(version_from=validator['version_from'], version_till=validator['version_till'],
                 possible_values=validator['possible_values']).__dict__,
        )

        # EnumBool
        validator = {
            'type': 'EnumBool',
            'version_from': 90300,
            'version_till': None,
            'possible_values': ('abc', 'def'),
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, EnumBool)
        self.assertEqual(
            ret.__dict__,
            EnumBool(version_from=validator['version_from'], version_till=validator['version_till'],
                     possible_values=validator['possible_values']).__dict__,
        )

        # String
        validator = {
            'type': 'String',
            'version_from': 90300,
            'version_till': None,
        }
        ret = ValidatorFactory(validator)
        self.assertIsInstance(ret, String)
        self.assertEqual(
            ret.__dict__,
            String(version_from=validator['version_from'], version_till=validator['version_till']).__dict__,
        )

    def test__get_postgres_guc_validators(self):
        # normal run
        parameter = 'my_parameter'

        config = {
            parameter: [{
                'type': 'Bool',
                'version_from': 90300,
                'version_till': 90500,
            }, {
                'type': 'EnumBool',
                'version_from': 90500,
                'version_till': 90600,
                'possible_values': [
                    'always',
                ],
            }]
        }
        ret = _get_postgres_guc_validators(config, parameter)
        self.assertIsInstance(ret, tuple)
        self.assertEqual(len(ret), 2)
        self.assertIsInstance(ret[0], Bool)
        self.assertIsInstance(ret[1], EnumBool)

        # log exceptions
        del config[parameter][0]['type']

        with patch('patroni.postgresql.validator.logger.warning') as mock_logger:
            ret = _get_postgres_guc_validators(config, parameter)
            self.assertIsInstance(ret, tuple)
            self.assertEqual(len(ret), 1)
            self.assertIsInstance(ret[0], EnumBool)

            mock_logger.assert_called_once()
            mock_call = mock_logger.call_args[0]
            self.assertEqual(mock_call[0], 'Faced an issue while parsing a validator for parameter `%s`: `%r`')
            self.assertEqual(mock_call[1], parameter)
            self.assertIsInstance(mock_call[2], ValidatorFactoryNoType)

    def test__read_postgres_gucs_validators_file(self):
        # raise exception
        with self.assertRaises(InvalidGucValidatorsFile) as exc:
            _read_postgres_gucs_validators_file('random_file.yaml')
        self.assertEqual(
            str(exc.exception),
            "Unexpected issue while reading parameters file `random_file.yaml`: `[Errno 2] No such file or directory: "
            "'random_file.yaml'`."
        )

    def test__load_postgres_gucs_validators(self):
        # log messages
        with patch('os.walk', Mock(return_value=iter([('.', [], ['file.txt', 'random.yaml'])]))), \
             patch('patroni.postgresql.validator.logger.info') as mock_info, \
             patch('patroni.postgresql.validator.logger.warning') as mock_warning:
            _load_postgres_gucs_validators()
            mock_info.assert_called_once_with('Ignored a non-YAML file found under `available_parameters` directory: '
                                              '`%s`.', os.path.join('.', 'file.txt'))
            mock_warning.assert_called_once()
            self.assertIn(
                "Unexpected issue while reading parameters file `{0}`: `[Errno 2] No such file or "
                "directory:".format(os.path.join('.', 'random.yaml')),
                mock_warning.call_args[0][0]
            )


@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
class TestPostgresql2(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=140000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    @patch.object(Postgresql, 'is_primary', Mock(return_value=False))
    def setUp(self):
        super(TestPostgresql2, self).setUp()

    @patch('subprocess.check_output', Mock(return_value='\n'.join(mock_available_gucs.return_value).encode('utf-8')))
    def test_available_gucs(self):
        gucs = self.p.available_gucs
        self.assertIsInstance(gucs, CaseInsensitiveSet)
        self.assertEqual(gucs, mock_available_gucs.return_value)

    def test_cluster_info_query(self):
        self.assertIn('diff(pg_catalog.pg_current_wal_flush_lsn(', self.p.cluster_info_query)
        self.p._major_version = 90600
        self.assertIn('diff(pg_catalog.pg_current_xlog_flush_location(', self.p.cluster_info_query)
        self.p._major_version = 90500
        self.assertIn('diff(pg_catalog.pg_current_xlog_location(', self.p.cluster_info_query)
