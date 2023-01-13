import os

from mock import Mock, patch

from patroni.dcs import Cluster, SyncState
from patroni.postgresql import Postgresql

from . import BaseTestPostgresql, psycopg_connect


@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
class TestSync(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=140000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def setUp(self):
        super(TestSync, self).setUp()
        self.p.config.write_postgresql_conf()
        self.s = self.p.sync_handler

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_pick_sync_standby(self):
        cluster = Cluster(True, None, self.leader, 0, [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name), None, None, None)

        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[
                    'on',
                    [{'application_name': self.leadermem.name, 'sync_state': 'sync', 'flush_lsn': 1},
                     {'application_name': self.me.name, 'sync_state': 'async', 'flush_lsn': 2},
                     {'application_name': self.other.name, 'sync_state': 'async', 'flush_lsn': 2}]
                ]):
            self.assertEqual(self.s.current_state(cluster), ([self.leadermem.name], [self.leadermem.name]))

        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[
                    'remote_write',
                    [{'application_name': self.leadermem.name, 'sync_state': 'potential', 'write_lsn': 1},
                     {'application_name': self.me.name, 'sync_state': 'async', 'write_lsn': 2},
                     {'application_name': self.other.name, 'sync_state': 'async', 'write_lsn': 2}]
                ]):
            self.assertEqual(self.s.current_state(cluster), ([self.leadermem.name], []))

        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[
                    'remote_apply',
                    [{'application_name': self.me.name.upper(), 'sync_state': 'async', 'replay_lsn': 2},
                     {'application_name': self.other.name, 'sync_state': 'async', 'replay_lsn': 1}]
                ]):
            self.assertEqual(self.s.current_state(cluster), ([self.me.name], []))

        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[
                    'remote_apply',
                    [{'application_name': 'missing', 'sync_state': 'sync', 'replay_lsn': 3},
                     {'application_name': self.me.name, 'sync_state': 'async', 'replay_lsn': 2},
                     {'application_name': self.other.name, 'sync_state': 'async', 'replay_lsn': 1}]
                ]):
            self.assertEqual(self.s.current_state(cluster), ([self.me.name], []))

        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['remote_apply', []]):
            self.p._major_version = 90400
            self.assertEqual(self.s.current_state(cluster), ([], []))

    def test_set_sync_standby(self):
        def value_in_conf():
            with open(os.path.join(self.p.data_dir, 'postgresql.conf')) as f:
                for line in f:
                    if line.startswith('synchronous_standby_names'):
                        return line.strip()

        mock_reload = self.p.reload = Mock()
        self.s.set_synchronous_standby_names(['n1'])
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")
        mock_reload.assert_called()

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['n1'])
        mock_reload.assert_not_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")

        self.s.set_synchronous_standby_names(['n1', 'n2'])
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '2 (n1,n2)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names([])
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), None)
