import mock
import psycopg2

from mock import Mock, PropertyMock, patch

from patroni.dcs import Cluster, ClusterConfig, Member
from patroni.postgresql import Postgresql
from patroni.postgresql.slots import SlotsHandler

from . import BaseTestPostgresql, psycopg2_connect, MockCursor


@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch.object(Postgresql, 'is_running', Mock(return_value=True))
class TestSlotsHandler(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=130000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    def setUp(self):
        super(TestSlotsHandler, self).setUp()
        self.s = self.p.slots_handler
        self.p.start()

    def test_sync_replication_slots(self):
        config = ClusterConfig(1, {'slots': {'test_3': {'database': 'a', 'plugin': 'b'},
                                             'A': 0, 'ls': 0, 'b': {'type': 'logical', 'plugin': '1'}},
                                   'ignore_slots': [{'name': 'blabla'}]}, 1)
        cluster = Cluster(True, config, self.leader, 0,
                          [self.me, self.other, self.leadermem], None, None, None, {'test_3': 10})
        with mock.patch('patroni.postgresql.Postgresql._query', Mock(side_effect=psycopg2.OperationalError)):
            self.s.sync_replication_slots(cluster)
        self.p.set_role('standby_leader')
        self.s.sync_replication_slots(cluster)
        self.p.set_role('replica')
        with patch.object(Postgresql, 'is_leader', Mock(return_value=False)):
            self.s.sync_replication_slots(cluster)
        self.p.set_role('master')
        with mock.patch('patroni.postgresql.Postgresql.role', new_callable=PropertyMock(return_value='replica')):
            self.s.sync_replication_slots(cluster)
        with patch.object(SlotsHandler, 'drop_replication_slot', Mock(return_value=True)),\
                patch('patroni.dcs.logger.error', new_callable=Mock()) as errorlog_mock:
            alias1 = Member(0, 'test-3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            alias2 = Member(0, 'test.3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            cluster.members.extend([alias1, alias2])
            self.s.sync_replication_slots(cluster)
            self.assertEqual(errorlog_mock.call_count, 5)
            ca = errorlog_mock.call_args_list[0][0][1]
            self.assertTrue("test-3" in ca, "non matching {0}".format(ca))
            self.assertTrue("test.3" in ca, "non matching {0}".format(ca))
            with patch.object(Postgresql, 'major_version', PropertyMock(return_value=90618)):
                self.s.sync_replication_slots(cluster)

    def test_process_permanent_slots(self):
        config = ClusterConfig(1, {'slots': {'ls': {'database': 'a', 'plugin': 'b'}},
                                   'ignore_slots': [{'name': 'blabla'}]}, 1)
        cluster = Cluster(True, config, self.leader, 0, [self.me, self.other, self.leadermem], None, None, None, None)

        self.s.sync_replication_slots(cluster)
        with patch.object(Postgresql, '_query') as mock_query:
            self.p.reset_cluster_info_state(None)
            mock_query.return_value.fetchone.return_value = (
                1, 0, 0, 0, 0, 0, 0, 0, 0,
                [{"slot_name": "ls", "type": "logical", "datoid": 5, "plugin": "b", "confirmed_flush_lsn": 12345}])
            self.assertEqual(self.p.slots(), {'ls': 12345})

            self.p.reset_cluster_info_state(None)
            mock_query.return_value.fetchone.return_value = (
                1, 0, 0, 0, 0, 0, 0, 0, 0,
                [{"slot_name": "ls", "type": "logical", "datoid": 6, "plugin": "b", "confirmed_flush_lsn": 12345}])
            self.assertEqual(self.p.slots(), {})

    @patch.object(Postgresql, 'is_leader', Mock(return_value=False))
    def test__ensure_logical_slots_replica(self):
        self.p.set_role('replica')
        config = ClusterConfig(1, {'slots': {'ls': {'database': 'a', 'plugin': 'b'}}}, 1)
        cluster = Cluster(True, config, self.leader, 0,
                          [self.me, self.other, self.leadermem], None, None, None, {'ls': 12346})
        with patch.object(MockCursor, 'fetchone'):
            self.assertEqual(self.s.sync_replication_slots(cluster), [])
        self.assertEqual(self.s.sync_replication_slots(cluster), [])
        cluster.slots['ls'] = 'a'
        self.assertEqual(self.s.sync_replication_slots(cluster), [])

    @patch.object(MockCursor, 'execute', Mock(side_effect=psycopg2.OperationalError))
    def test_copy_logical_slots(self):
        self.s.copy_logical_slots(self.leader, ['foo'])
