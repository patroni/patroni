from mock import Mock, PropertyMock, patch

from patroni.postgresql import Postgresql
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.rewind import Rewind

from . import BaseTestPostgresql, MockCursor, psycopg2_connect


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

    @patch.object(CancellableSubprocess, 'call')
    def test_pg_rewind(self, mock_cancellable_subprocess_call):
        r = {'user': '', 'host': '', 'port': '', 'database': '', 'password': ''}
        mock_cancellable_subprocess_call.return_value = 0
        self.assertTrue(self.r.pg_rewind(r))
        mock_cancellable_subprocess_call.side_effect = OSError
        self.assertFalse(self.r.pg_rewind(r))

    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    def test__get_local_timeline_lsn(self):
        self.r.trigger_check_diverged_lsn()
        with patch.object(Postgresql, 'controldata',
                          Mock(return_value={'Database cluster state': 'shut down in recovery',
                                             'Minimum recovery ending location': '0/0',
                                             "Min recovery ending loc's timeline": '0'})):
            self.r.rewind_or_reinitialize_needed_and_possible(self.leader)

        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            with patch.object(MockCursor, 'fetchone', Mock(side_effect=[(False, ), Exception])):
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

        self.leader.member.data.update(version='1.5.7', checkpoint_after_promote=False)
        self.assertIsNone(self.r.execute(self.leader))

        self.leader.member.data['checkpoint_after_promote'] = True
        with patch.object(Rewind, 'check_leader_is_not_in_recovery', Mock(return_value=False)):
            self.assertIsNone(self.r.execute(self.leader))

        with patch.object(Postgresql, 'is_running', Mock(return_value=True)):
            self.r.execute(self.leader)

    @patch.object(Postgresql, 'start', Mock())
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    @patch.object(Rewind, '_get_local_timeline_lsn', Mock(return_value=(2, '40159C1')))
    @patch.object(Rewind, 'check_leader_is_not_in_recovery')
    def test__check_timeline_and_lsn(self, mock_check_leader_is_not_in_recovery):
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
        with patch.object(MockCursor, 'fetchone', Mock(side_effect=[('', 2, '0/0'), ('', b'3\t0/40159C0\tn\n')])):
            self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
        self.r.trigger_check_diverged_lsn()
        with patch.object(MockCursor, 'fetchone', Mock(return_value=('', 1, '0/0'))):
            with patch.object(Rewind, '_get_local_timeline_lsn', Mock(return_value=(1, '0/0'))):
                self.assertFalse(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))
            self.r.trigger_check_diverged_lsn()
            self.assertTrue(self.r.rewind_or_reinitialize_needed_and_possible(self.leader))

    @patch.object(MockCursor, 'fetchone', Mock(side_effect=[(True,), Exception]))
    def test_check_leader_is_not_in_recovery(self):
        self.r.check_leader_is_not_in_recovery()
        self.r.check_leader_is_not_in_recovery()

    @patch.object(Postgresql, 'controldata', Mock(return_value={"Latest checkpoint's TimeLineID": 1}))
    def test_check_for_checkpoint_after_promote(self):
        self.r.check_for_checkpoint_after_promote()
