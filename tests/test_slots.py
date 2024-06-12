import mock
import os
import unittest


from mock import Mock, PropertyMock, patch
from threading import Thread

from patroni import global_config, psycopg
from patroni.dcs import Cluster, ClusterConfig, Member, Status, SyncState
from patroni.postgresql import Postgresql
from patroni.postgresql.misc import fsync_dir
from patroni.postgresql.slots import SlotsAdvanceThread, SlotsHandler
from patroni.tags import Tags

from . import BaseTestPostgresql, psycopg_connect, MockCursor


class TestTags(Tags):

    @property
    def tags(self):
        return {}


@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch.object(Thread, 'start', Mock())
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
        config = ClusterConfig(1, {'slots': {'ls': {'database': 'a', 'plugin': 'b'}, 'ls2': None}}, 1)
        self.cluster = Cluster(True, config, self.leader, Status(0, {'ls': 12345, 'ls2': 12345}),
                               [self.me, self.other, self.leadermem], None, SyncState.empty(), None, None)
        global_config.update(self.cluster)
        self.tags = TestTags()

    def test_sync_replication_slots(self):
        config = ClusterConfig(1, {'slots': {'test_3': {'database': 'a', 'plugin': 'b'},
                                             'A': 0, 'ls': 0, 'b': {'type': 'logical', 'plugin': '1'}},
                                   'ignore_slots': [{'name': 'blabla'}]}, 1)
        cluster = Cluster(True, config, self.leader, Status(0, {'test_3': 10}),
                          [self.me, self.other, self.leadermem], None, SyncState.empty(), None, None)
        global_config.update(cluster)
        with mock.patch('patroni.postgresql.Postgresql._query', Mock(side_effect=psycopg.OperationalError)):
            self.s.sync_replication_slots(cluster, self.tags)
        self.p.set_role('standby_leader')
        with patch.object(SlotsHandler, 'drop_replication_slot', Mock(return_value=(True, False))), \
                patch.object(global_config.__class__, 'is_standby_cluster', PropertyMock(return_value=True)), \
                patch('patroni.postgresql.slots.logger.debug') as mock_debug:
            self.s.sync_replication_slots(cluster, self.tags)
            mock_debug.assert_called_once()
        self.p.set_role('replica')
        with patch.object(Postgresql, 'is_primary', Mock(return_value=False)), \
                patch.object(global_config.__class__, 'is_paused', PropertyMock(return_value=True)), \
                patch.object(SlotsHandler, 'drop_replication_slot') as mock_drop:
            config.data['slots'].pop('ls')
            self.s.sync_replication_slots(cluster, self.tags)
            mock_drop.assert_not_called()
        self.p.set_role('primary')
        with mock.patch('patroni.postgresql.Postgresql.role', new_callable=PropertyMock(return_value='replica')):
            self.s.sync_replication_slots(cluster, self.tags)
        with patch('patroni.dcs.logger.error', new_callable=Mock()) as errorlog_mock:
            alias1 = Member(0, 'test-3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            alias2 = Member(0, 'test.3', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
            cluster.members.extend([alias1, alias2])
            self.s.sync_replication_slots(cluster, self.tags)
            self.assertEqual(errorlog_mock.call_count, 5)
            ca = errorlog_mock.call_args_list[0][0][1]
            self.assertTrue("test-3" in ca, "non matching {0}".format(ca))
            self.assertTrue("test.3" in ca, "non matching {0}".format(ca))
            with patch.object(Postgresql, 'major_version', PropertyMock(return_value=90618)):
                self.s.sync_replication_slots(cluster, self.tags)
                self.p.set_role('replica')
                self.s.sync_replication_slots(cluster, self.tags)

    def test_cascading_replica_sync_replication_slots(self):
        """Test sync with a cascading replica so physical slots are present on a replica."""
        config = ClusterConfig(1, {'slots': {'ls': {'database': 'a', 'plugin': 'b'}}}, 1)
        cascading_replica = Member(0, 'test-2', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'postgresql0'}
        })
        cluster = Cluster(True, config, self.leader, Status(0, {'ls': 10}),
                          [self.me, self.other, self.leadermem, cascading_replica], None, SyncState.empty(), None, None)
        self.p.set_role('replica')
        with patch.object(Postgresql, '_query') as mock_query, \
                patch.object(Postgresql, 'is_primary', Mock(return_value=False)):
            mock_query.return_value = [('ls', 'logical', 104, 'b', 'a', 5, 12345, 105)]
            ret = self.s.sync_replication_slots(cluster, self.tags)
        self.assertEqual(ret, [])

    def test_process_permanent_slots(self):
        config = ClusterConfig(1, {'slots': {'ls': {'database': 'a', 'plugin': 'b'}, 'blabla': {'type': 'physical'}},
                                   'ignore_slots': [{'name': 'blabla'}]}, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(), [self.me, self.other, self.leadermem],
                          None, SyncState.empty(), None, None)
        global_config.update(cluster)

        self.s.sync_replication_slots(cluster, self.tags)
        with patch.object(Postgresql, '_query') as mock_query:
            self.p.reset_cluster_info_state(None)
            mock_query.return_value = [(
                1, 0, 0, 0, 0, 0, 0, 0, 0, None, None,
                [{"slot_name": "ls", "type": "logical", "datoid": 5, "plugin": "b",
                  "confirmed_flush_lsn": 12345, "catalog_xmin": 105, "restart_lsn": 12344},
                 {"slot_name": "blabla", "type": "physical", "datoid": None, "plugin": None,
                  "confirmed_flush_lsn": None, "catalog_xmin": 105, "restart_lsn": 12344}])]
            self.assertEqual(self.p.slots(), {'ls': 12345, 'blabla': 12344})

            self.p.reset_cluster_info_state(None)
            mock_query.return_value = [(
                1, 0, 0, 0, 0, 0, 0, 0, 0, None, None,
                [{"slot_name": "ls", "type": "logical", "datoid": 6, "plugin": "b",
                  "confirmed_flush_lsn": 12345, "catalog_xmin": 105}])]
            self.assertEqual(self.p.slots(), {})

    def test_nostream_slot_processing(self):
        config = ClusterConfig(
            1, {'slots': {'foo': {'type': 'logical', 'database': 'a', 'plugin': 'b'}, 'bar': {'type': 'physical'}}}, 1)
        nostream_node = Member(0, 'test-2', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'nostream': 'True'}
        })
        cascade_node = Member(0, 'test-3', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'test-2'}
        })
        stream_node = Member(0, 'test-4', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres'})
        cluster = Cluster(
            True, config, self.leader, Status.empty(),
            [self.leadermem, nostream_node, cascade_node, stream_node], None, SyncState.empty(), None, None)
        global_config.update(cluster)

        # sanity for primary
        self.p.name = self.leadermem.name
        self.assertEqual(
            cluster._get_permanent_slots(self.p, self.leadermem, 'primary'),
            {'foo': {'type': 'logical', 'database': 'a', 'plugin': 'b'}, 'bar': {'type': 'physical'}})
        self.assertEqual(
            cluster._get_members_slots(self.p.name, 'primary'),
            {'test_4': {'type': 'physical'}})

        # nostream node must not have slot on primary
        self.p.name = nostream_node.name
        # permanent logical slots are not allowed on nostream node
        self.assertEqual(
            cluster._get_permanent_slots(self.p, nostream_node, 'replica'),
            {'bar': {'type': 'physical'}})
        self.assertEqual(
            cluster.get_slot_name_on_primary(self.p.name, nostream_node),
            None)

        # check cascade member-slot existence on nostream node
        self.assertEqual(
            cluster._get_members_slots(nostream_node.name, 'replica'),
            {'test_3': {'type': 'physical'}})

        # cascade also does not entitled to have logical slot on itself ...
        self.p.name = cascade_node.name
        self.assertEqual(
            cluster._get_permanent_slots(self.p, cascade_node, 'replica'),
            {'bar': {'type': 'physical'}})
        # ... and member-slot on primary
        self.assertEqual(
            cluster.get_slot_name_on_primary(self.p.name, cascade_node),
            None)

        # simple replica must have every permanent slot ...
        self.p.name = stream_node.name
        self.assertEqual(
            cluster._get_permanent_slots(self.p, stream_node, 'replica'),
            {'foo': {'type': 'logical', 'database': 'a', 'plugin': 'b'}, 'bar': {'type': 'physical'}})
        # ... and member-slot on primary
        self.assertEqual(
            cluster.get_slot_name_on_primary(self.p.name, stream_node),
            'test_4')

    def test_get_slot_name_on_primary(self):
        node1 = Member(0, 'node1', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'node2'}
        })
        node2 = Member(0, 'node2', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'node1'}
        })
        cluster = Cluster(True, None, self.leader, Status.empty(), [self.leadermem, node1, node2],
                          None, SyncState.empty(), None, None)
        self.assertIsNone(cluster.get_slot_name_on_primary('node1', node1))

    def test_should_enforce_hot_standby_feedback(self):
        node1 = Member(0, 'postgresql0', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'postgresql1'}
        })
        node2 = Member(0, 'postgresql1', 28, {
            'state': 'running', 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
            'tags': {'replicatefrom': 'postgresql0'}
        })
        cluster = Cluster(True, None, self.leader, Status.empty(), [self.leadermem, node1, node2],
                          None, SyncState.empty(), None, None)
        self.assertFalse(cluster.should_enforce_hot_standby_feedback(self.p, node1))

    @patch.object(Postgresql, 'is_primary', Mock(return_value=False))
    def test__ensure_logical_slots_replica(self):
        self.p.set_role('replica')
        self.cluster.slots['ls'] = 12346
        with patch.object(SlotsHandler, 'check_logical_slots_readiness', Mock(return_value=False)):
            self.assertEqual(self.s.sync_replication_slots(self.cluster, self.tags), [])
        with patch.object(SlotsHandler, '_query', Mock(return_value=[('ls', 'logical', 499, 'b', 'a', 5, 100, 500)])), \
                patch.object(MockCursor, 'execute', Mock(side_effect=psycopg.OperationalError)), \
                patch.object(SlotsAdvanceThread, 'schedule', Mock(return_value=(True, ['ls']))), \
                patch.object(psycopg.OperationalError, 'diag') as mock_diag:
            type(mock_diag).sqlstate = PropertyMock(return_value='58P01')
            self.assertEqual(self.s.sync_replication_slots(self.cluster, self.tags), ['ls'])
        self.cluster.slots['ls'] = 'a'
        self.assertEqual(self.s.sync_replication_slots(self.cluster, self.tags), [])
        self.cluster.config.data['slots']['ls']['database'] = 'b'
        self.cluster.slots['ls'] = '500'
        with patch.object(MockCursor, 'rowcount', PropertyMock(return_value=1), create=True):
            self.assertEqual(self.s.sync_replication_slots(self.cluster, self.tags), ['ls'])

    def test_copy_logical_slots(self):
        self.cluster.config.data['slots']['ls']['database'] = 'b'
        self.s.copy_logical_slots(self.cluster, self.tags, ['ls'])
        with patch.object(MockCursor, 'execute', Mock(side_effect=psycopg.OperationalError)):
            self.s.copy_logical_slots(self.cluster, self.tags, ['foo'])
        with patch.object(Cluster, 'leader', PropertyMock(return_value=None)):
            self.s.copy_logical_slots(self.cluster, self.tags, ['foo'])

    @patch.object(Postgresql, 'stop', Mock(return_value=True))
    @patch.object(Postgresql, 'start', Mock(return_value=True))
    @patch.object(Postgresql, 'is_primary', Mock(return_value=False))
    def test_check_logical_slots_readiness(self):
        self.s.copy_logical_slots(self.cluster, self.tags, ['ls'])
        with patch.object(MockCursor, '__iter__', Mock(return_value=iter([('postgresql0', None)]))), \
                patch.object(MockCursor, 'fetchall', Mock(side_effect=Exception)):
            self.assertFalse(self.s.check_logical_slots_readiness(self.cluster, self.tags))
        with patch.object(MockCursor, '__iter__', Mock(return_value=iter([('postgresql0', None)]))), \
                patch.object(MockCursor, 'fetchall', Mock(return_value=[(False,)])):
            self.assertFalse(self.s.check_logical_slots_readiness(self.cluster, self.tags))
        with patch.object(MockCursor, '__iter__', Mock(return_value=iter([('ls', 100)]))):
            self.s.check_logical_slots_readiness(self.cluster, self.tags)

    @patch.object(Postgresql, 'stop', Mock(return_value=True))
    @patch.object(Postgresql, 'start', Mock(return_value=True))
    @patch.object(Postgresql, 'is_primary', Mock(return_value=False))
    def test_on_promote(self):
        self.s.schedule_advance_slots({'foo': {'bar': 100}})
        self.s.copy_logical_slots(self.cluster, self.tags, ['ls'])
        self.s.on_promote()

    @unittest.skipIf(os.name == 'nt', "Windows not supported")
    @patch('os.open', Mock())
    @patch('os.close', Mock())
    @patch('os.fsync', Mock(side_effect=OSError))
    def test_fsync_dir(self):
        self.assertRaises(OSError, fsync_dir, 'foo')

    def test_slots_advance_thread(self):
        with patch.object(MockCursor, 'execute', Mock(side_effect=psycopg.OperationalError)), \
                patch.object(psycopg.OperationalError, 'diag') as mock_diag:
            type(mock_diag).sqlstate = PropertyMock(return_value='58P01')
            self.s.schedule_advance_slots({'foo': {'bar': 100}})
            self.s._advance.sync_slots()

        with patch.object(SlotsAdvanceThread, 'sync_slots', Mock(side_effect=Exception)):
            self.s._advance._condition.wait = Mock()
            self.assertRaises(Exception, self.s._advance.run)

        with patch.object(SlotsHandler, 'get_local_connection_cursor', Mock(side_effect=Exception)):
            self.s.schedule_advance_slots({'foo': {'bar': 100}})
            self.s._advance.sync_slots()

    @patch.object(Postgresql, 'is_primary', Mock(return_value=False))
    def test_advance_physical_slots(self):
        config = ClusterConfig(1, {'slots': {'blabla': {'type': 'physical'}, 'leader': None}}, 1)
        cluster = Cluster(True, config, self.leader, Status(0, {'blabla': 12346}),
                          [self.me, self.other, self.leadermem], None, SyncState.empty(), None, None)
        global_config.update(cluster)
        self.s.sync_replication_slots(cluster, self.tags)
        with patch.object(SlotsHandler, '_query', Mock(side_effect=[[('blabla', 'physical', 12345, None, None, None,
                                                                      None, None)], Exception])) as mock_query, \
                patch('patroni.postgresql.slots.logger.error') as mock_error:
            self.s.sync_replication_slots(cluster, self.tags)
            self.assertEqual(mock_query.call_args[0],
                             ("SELECT pg_catalog.pg_replication_slot_advance(%s, %s)", "blabla", '0/303A'))
            self.assertEqual(mock_error.call_args[0][0],
                             "Error while advancing replication slot %s to position '%s': %r")
