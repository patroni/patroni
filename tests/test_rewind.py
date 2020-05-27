from mock import Mock, PropertyMock, patch, mock_open

from patroni.postgresql import Postgresql
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.rewind import Rewind
from six.moves import builtins

from . import BaseTestPostgresql, MockCursor, psycopg2_connect


class MockThread(object):

    def __init__(self, target, args):
        self._target = target
        self._args = args

    def start(self):
        self._target(*self._args)


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
class TestRewind(BaseTestPostgresql):

    def setUp(self):
        super(TestRewind, self).setUp()
        self.r = Rewind(self.p)

    def test_can_rewind(self):
        with patch.object(Postgresql, 'controldata', Mock(return_value={'wal_log_hints setting': 'on'})):
            self.assertTrue(self.r.can_rewind)
        with patch('subprocess.call', Mock(return_value=1)):
            self.assertFalse(self.r.can_rewind)
        with patch('subprocess.call', side_effect=OSError):
            self.assertFalse(self.r.can_rewind)
        self.p.config._config['use_pg_rewind'] = False
        self.assertFalse(self.r.can_rewind)

    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=130000))
    @patch.object(CancellableSubprocess, 'call')
    def test_pg_rewind(self, mock_cancellable_subprocess_call):
        r = {'user': '', 'host': '', 'port': '', 'database': '', 'password': ''}
        mock_cancellable_subprocess_call.return_value = 0
        with patch('subprocess.check_output', Mock(return_value=b'foo')):
            self.assertTrue(self.r.pg_rewind(r))
        mock_cancellable_subprocess_call.side_effect = OSError
        with patch('subprocess.check_output', Mock(side_effect=Exception)):
            self.assertFalse(self.r.pg_rewind(r))

    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    def test__get_local_timeline_lsn(self):
        self.r.trigger_check_diverged_lsn()
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'Database cluster state': 'shut down in recovery',
                                             'Minimum recovery ending location': '0/0',
                                             "Min recovery ending loc's timeline": '0',
                                             'Latest checkpoint location': '0/'})):
            self.r.rewind_or_reinitialize_needed_and_possible(self.leader)

        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            with patch.object(MockCursor, 'fetchone', Mock(side_effect=[(0, 0, 1, 1,), Exception])):
                self.r.rewind_or_reinitialize_needed_and_possible(self.leader)

    @patch.object(CancellableSubprocess, 'call', Mock(return_value=0))
    @patch.object(Postgresql, 'checkpoint', side_effect=['', '1'],)
    @patch.object(Postgresql, 'stop', Mock(return_value=False))
    @patch.object(Postgresql, 'start', Mock())
    def test_execute(self, mock_checkpoint):
        self.r.execute(self.leader)
        with patch.object(Rewind, 'pg_rewind', Mock(return_value=False)):
            mock_checkpoint.side_effect = ['1', '', '', '']
            self.r.execute(self.leader)
            self.r.execute(self.leader)
            with patch.object(Rewind, 'check_leader_is_not_in_recovery', Mock(return_value=False)):
                self.r.execute(self.leader)
            self.p.config._config['remove_data_directory_on_rewind_failure'] = False
            self.r.trigger_check_diverged_lsn()
            self.r.execute(self.leader)

        self.leader.member.data.update(version='1.5.7', checkpoint_after_promote=False, role='master')
        self.assertIsNone(self.r.execute(self.leader))

        del self.leader.member.data['checkpoint_after_promote']
        with patch.object(Rewind, 'check_leader_is_not_in_recovery', Mock(return_value=False)):
            self.assertIsNone(self.r.execute(self.leader))

        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            self.r.execute(self.leader)

    @patch('patroni.postgresql.rewind.logger.info')
    def test__log_master_history(self, mock_logger):
        history = [[n, n, ''] for n in range(1, 10)]
        self.r._log_master_history(history, 1)
        expected = '\n'.join(['{0}\t0/{0}\t'.format(n) for n in range(1, 4)] + ['...', '9\t0/9\t'])
        self.assertEqual(mock_logger.call_args[0][1], expected)

    @patch.object(Postgresql, 'start', Mock())
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    @patch.object(Rewind, '_get_local_timeline_lsn')
    @patch.object(Rewind, 'check_leader_is_not_in_recovery')
    def test__check_timeline_and_lsn(self, mock_check_leader_is_not_in_recovery, mock_get_local_timeline_lsn):
        mock_get_local_timeline_lsn.return_value = (True, 2, 67197377)
        mock_check_leader_is_not_in_recovery.return_value = False
        self.r.trigger_check_diverged_lsn()
        self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        self.leader = self.leader.member
        self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        mock_check_leader_is_not_in_recovery.return_value = True
        self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        self.r.trigger_check_diverged_lsn()
        with patch('psycopg2.connect', Mock(side_effect=Exception)):
            self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        self.r.trigger_check_diverged_lsn()
        with patch.object(MockCursor, 'fetchone', Mock(side_effect=[('', 3, '0/0'), ('', b'3\t0/40159C0\tn\n')])):
            self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        self.r.trigger_check_diverged_lsn()
        with patch.object(MockCursor, 'fetchone', Mock(return_value=('', 1, '0/0'))):
            with patch.object(Rewind, '_get_local_timeline_lsn', Mock(return_value=(True, 1, '0/0'))):
                self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
            self.r.trigger_check_diverged_lsn()
            self.assertTrue(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))

        self.r.reset_state()
        self.r.trigger_check_diverged_lsn()
        mock_get_local_timeline_lsn.return_value = (False, 2, 67296664)
        self.assertTrue(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))

        with patch('subprocess.Popen') as mock_popen:
            mock_popen.return_value.communicate.return_value = (
                b'0, lsn: 0/040159C1, prev 0/\n',
                b'pg_waldump: fatal: error in WAL record at 0/40159C1: invalid record length at /: wanted 24, got 0\n'
            )
            self.r.reset_state()
            self.r.trigger_check_diverged_lsn()
            mock_get_local_timeline_lsn.return_value = (False, 2, 67197377)
            self.assertTrue(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
            self.r.reset_state()
            self.r.trigger_check_diverged_lsn()
            mock_popen.side_effect = Exception
            self.assertTrue(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))

    @patch.object(MockCursor, 'fetchone', Mock(side_effect=[(True,), Exception]))
    def test_check_leader_is_not_in_recovery(self):
        self.r.check_leader_is_not_in_recovery()
        self.r.check_leader_is_not_in_recovery()

    def test_read_postmaster_opts(self):
        m = mock_open(read_data='/usr/lib/postgres/9.6/bin/postgres "-D" "data/postgresql0" \
"--listen_addresses=127.0.0.1" "--port=5432" "--hot_standby=on" "--wal_level=hot_standby" \
"--wal_log_hints=on" "--max_wal_senders=5" "--max_replication_slots=5"\n')
        with patch.object(builtins, 'open', m):
            data = self.r.read_postmaster_opts()
            self.assertEqual(data['wal_level'], 'hot_standby')
            self.assertEqual(int(data['max_replication_slots']), 5)
            self.assertEqual(data.get('D'), None)

            m.side_effect = IOError
            data = self.r.read_postmaster_opts()
            self.assertEqual(data, dict())

    @patch('psutil.Popen')
    def test_single_user_mode(self, subprocess_popen_mock):
        subprocess_popen_mock.return_value.wait.return_value = 0
        self.assertEqual(self.r.single_user_mode('CHECKPOINT', {'archive_mode': 'on'}), 0)

    @patch('os.listdir', Mock(side_effect=[OSError, ['a', 'b']]))
    @patch('os.unlink', Mock(side_effect=OSError))
    @patch('os.remove', Mock())
    @patch('os.path.islink', Mock(side_effect=[True, False]))
    @patch('os.path.isfile', Mock(return_value=True))
    def test_cleanup_archive_status(self):
        self.r.cleanup_archive_status()
        self.r.cleanup_archive_status()

    @patch('os.unlink', Mock())
    @patch('os.listdir', Mock(return_value=[]))
    @patch('os.path.isfile', Mock(return_value=True))
    @patch.object(Rewind, 'read_postmaster_opts', Mock(return_value={}))
    @patch.object(Rewind, 'single_user_mode', Mock(return_value=0))
    def test_ensure_clean_shutdown(self):
        self.assertTrue(self.r.ensure_clean_shutdown())

    @patch('patroni.postgresql.rewind.Thread', MockThread)
    @patch.object(Postgresql, 'controldata')
    @patch.object(Postgresql, 'checkpoint')
    def test_ensure_checkpoint_after_promote(self, mock_checkpoint, mock_controldata):
        mock_checkpoint.return_value = None
        self.r.ensure_checkpoint_after_promote(Mock())
        self.r.ensure_checkpoint_after_promote(Mock())

        self.r.reset_state()
        mock_controldata.return_value = {"Latest checkpoint's TimeLineID": 1}
        mock_checkpoint.side_effect = Exception
        self.r.ensure_checkpoint_after_promote(Mock())
        self.r.ensure_checkpoint_after_promote(Mock())

        self.r.reset_state()
        mock_controldata.side_effect = TypeError
        self.r.ensure_checkpoint_after_promote(Mock())
        self.r.ensure_checkpoint_after_promote(Mock())
