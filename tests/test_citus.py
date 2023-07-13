import time
import unittest

from copy import deepcopy
from mock import Mock, patch
from typing import List
from patroni.postgresql.citus import CitusHandler, PgDistGroup, PgDistNode

from . import BaseTestPostgresql, MockCursor, psycopg_connect, SleepException
from .test_ha import get_cluster_initialized_with_leader


@patch('patroni.postgresql.citus.Thread', Mock())
@patch('patroni.psycopg.connect', psycopg_connect)
class TestCitus(BaseTestPostgresql):

    def setUp(self):
        super(TestCitus, self).setUp()
        self.c = self.p.citus_handler
        self.c.set_conn_kwargs({'host': 'localhost', 'dbname': 'postgres'})
        self.cluster = get_cluster_initialized_with_leader()
        self.cluster.workers[1] = self.cluster

    @patch('time.time', Mock(side_effect=[100, 130, 160, 190, 220, 250, 280, 310, 340, 370]))
    @patch('patroni.postgresql.citus.logger.exception', Mock(side_effect=SleepException))
    @patch('patroni.postgresql.citus.logger.warning')
    @patch('patroni.postgresql.citus.PgDistTask.wait', Mock())
    @patch.object(CitusHandler, 'is_alive', Mock(return_value=True))
    def test_run(self, mock_logger_warning):
        # `before_demote` or `before_promote` REST API calls starting a
        # transaction. We want to make sure that it finishes during
        # certain timeout. In case if it is not, we want to roll it back
        # in order to not block other workers that want to update
        # `pg_dist_node`.
        self.c._condition.wait = Mock(side_effect=[Mock(), Mock(), Mock(), SleepException])

        self.c.handle_event(self.cluster, {'type': 'before_demote', 'group': 1,
                                           'leader': 'leader', 'timeout': 30, 'cooldown': 10})
        self.c.add_task('after_promote', 2, self.cluster, self.cluster.leader_name, 'postgres://host3:5432/postgres')
        self.assertRaises(SleepException, self.c.run)
        mock_logger_warning.assert_called_once()
        self.assertTrue(mock_logger_warning.call_args[0][0].startswith('Rolling back transaction'))
        self.assertTrue(repr(mock_logger_warning.call_args[0][1]).startswith('PgDistTask'))

    @patch.object(CitusHandler, 'is_alive', Mock(return_value=False))
    @patch.object(CitusHandler, 'start', Mock())
    def test_sync_pg_dist_node(self):
        with patch.object(CitusHandler, 'is_enabled', Mock(return_value=False)):
            self.c.sync_pg_dist_node(self.cluster)
        self.c.sync_pg_dist_node(self.cluster)

    def test_handle_event(self):
        self.c.handle_event(self.cluster, {})
        with patch.object(CitusHandler, 'is_alive', Mock(return_value=True)):
            self.c.handle_event(self.cluster, {'type': 'after_promote', 'group': 2,
                                               'leader': 'leader', 'timeout': 30, 'cooldown': 10})

    def test_add_task(self):
        with patch('patroni.postgresql.citus.logger.error') as mock_logger,\
                patch('patroni.postgresql.citus.urlparse', Mock(side_effect=Exception)):
            self.c.add_task('', 1, self.cluster, '', None)
            mock_logger.assert_called_once()

        with patch('patroni.postgresql.citus.logger.debug') as mock_logger:
            self.c.add_task('before_demote', 1, self.cluster,
                            self.cluster.leader_name, 'postgres://host:5432/postgres', 30)
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Adding the new task:'))

        with patch('patroni.postgresql.citus.logger.debug') as mock_logger:
            self.c.add_task('before_promote', 1, self.cluster,
                            self.cluster.leader_name, 'postgres://host:5432/postgres', 30)
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Overriding existing task:'))

        # add_task called from sync_pg_dist_node should not override already scheduled or in flight task until deadline
        self.assertIsNotNone(self.c.add_task('after_promote', 1, self.cluster,
                                             self.cluster.leader_name, 'postgres://host:5432/postgres', 30))
        self.assertIsNone(self.c.add_task('after_promote', 1, self.cluster,
                                          self.cluster.leader_name, 'postgres://host:5432/postgres'))
        self.c._in_flight = self.c._tasks.pop()
        self.c._in_flight.deadline = self.c._in_flight.timeout + time.time()
        self.assertIsNone(self.c.add_task('after_promote', 1, self.cluster,
                                          self.cluster.leader_name, 'postgres://host:5432/postgres'))
        self.c._in_flight.deadline = 0
        self.assertIsNotNone(self.c.add_task('after_promote', 1, self.cluster,
                                             self.cluster.leader_name, 'postgres://host:5432/postgres'))

        # If there is no transaction in progress and cached pg_dist_node matching desired state task should not be added
        self.c._schedule_load_pg_dist_node = False
        self.c._pg_dist_group[self.c._in_flight.group] = self.c._in_flight
        self.c._in_flight = None
        self.assertIsNone(self.c.add_task('after_promote', 1, self.cluster,
                                          self.cluster.leader_name, 'postgres://host:5432/postgres'))

    def test_pick_task(self):
        self.c.add_task('after_promote', 0, self.cluster, self.cluster.leader_name, 'postgres://host1:5432/postgres')
        with patch.object(CitusHandler, 'update_node') as mock_update_node:
            self.c.process_tasks()
            # process_task() shouln't be called because pick_task double checks with _pg_dist_group
            mock_update_node.assert_not_called()

    def test_process_task(self):
        self.c.add_task('after_promote', 1, self.cluster, self.cluster.leader_name, 'postgres://host2:5432/postgres')
        task = self.c.add_task('before_promote', 1, self.cluster,
                               self.cluster.leader_name, 'postgres://host4:5432/postgres', 30)
        self.c.process_tasks()
        self.assertTrue(task._event.is_set())

        # the after_promote should result only in COMMIT
        task = self.c.add_task('after_promote', 1, self.cluster,
                               self.cluster.leader_name, 'postgres://host4:5432/postgres', 30)
        with patch.object(CitusHandler, 'query') as mock_query:
            self.c.process_tasks()
            mock_query.assert_called_once()
            self.assertEqual(mock_query.call_args[0][0], 'COMMIT')

    def test_process_tasks(self):
        self.c.add_task('after_promote', 0, self.cluster, self.cluster.leader_name, 'postgres://host2:5432/postgres')
        self.c.process_tasks()

        self.c.add_task('after_promote', 0, self.cluster, self.cluster.leader_name, 'postgres://host3:5432/postgres')
        with patch('patroni.postgresql.citus.logger.error') as mock_logger,\
                patch.object(CitusHandler, 'query', Mock(side_effect=Exception)):
            self.c.process_tasks()
            mock_logger.assert_called_once()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Exception when working with pg_dist_node: '))

    def test_on_demote(self):
        self.c.on_demote()

    @patch('patroni.postgresql.citus.logger.error')
    @patch.object(MockCursor, 'execute', Mock(side_effect=Exception))
    def test_load_pg_dist_group(self, mock_logger):
        # load_pg_dist_group) triggers, query fails and exception is property handled
        self.c.process_tasks()
        self.assertTrue(self.c._schedule_load_pg_dist_group)
        mock_logger.assert_called_once()
        self.assertTrue(mock_logger.call_args[0][0].startswith('Exception when executing query'))
        self.assertTrue(mock_logger.call_args[0][1].startswith('SELECT groupid, nodename, '))

    def test_wait(self):
        task = self.c.add_task('before_demote', 1, self.cluster,
                               self.cluster.leader_name, u'postgres://host:5432/postgres', 30)
        task._event.wait = Mock()
        task.wait()

    def test_adjust_postgres_gucs(self):
        parameters = {'max_connections': 101,
                      'max_prepared_transactions': 0,
                      'shared_preload_libraries': 'foo , citus, bar '}
        self.c.adjust_postgres_gucs(parameters)
        self.assertEqual(parameters['max_prepared_transactions'], 202)
        self.assertEqual(parameters['shared_preload_libraries'], 'citus,foo,bar')
        self.assertEqual(parameters['wal_level'], 'logical')

    def test_bootstrap(self):
        self.c._config = None
        self.c.bootstrap()

    def test_ignore_replication_slot(self):
        self.assertFalse(self.c.ignore_replication_slot({'name': 'foo', 'type': 'physical',
                                                         'database': 'bar', 'plugin': 'wal2json'}))
        self.assertFalse(self.c.ignore_replication_slot({'name': 'foo', 'type': 'logical',
                                                         'database': 'bar', 'plugin': 'wal2json'}))
        self.assertFalse(self.c.ignore_replication_slot({'name': 'foo', 'type': 'logical',
                                                         'database': 'bar', 'plugin': 'pgoutput'}))
        self.assertFalse(self.c.ignore_replication_slot({'name': 'foo', 'type': 'logical',
                                                         'database': 'citus', 'plugin': 'pgoutput'}))
        self.assertTrue(self.c.ignore_replication_slot({'name': 'citus_shard_move_slot_1_2_3',
                                                        'type': 'logical', 'database': 'citus', 'plugin': 'pgoutput'}))
        self.assertFalse(self.c.ignore_replication_slot({'name': 'citus_shard_move_slot_1_2_3',
                                                         'type': 'logical', 'database': 'citus', 'plugin': 'citus'}))
        self.assertFalse(self.c.ignore_replication_slot({'name': 'citus_shard_split_slot_1_2_3',
                                                         'type': 'logical', 'database': 'citus', 'plugin': 'pgoutput'}))
        self.assertTrue(self.c.ignore_replication_slot({'name': 'citus_shard_split_slot_1_2_3',
                                                        'type': 'logical', 'database': 'citus', 'plugin': 'citus'}))


class TestGroupTransition(unittest.TestCase):
    nodeid = 100

    def map_to_sql(self, group: int, transition: PgDistNode) -> str:
        if transition.role not in ('primary', 'demoted', 'secondary'):
            return "citus_remove_node('{0}', {1})".format(transition.host, transition.port)
        elif transition.nodeid:
            host = transition.host + ('-demoted' if transition.role == 'demoted' else '')
            return "citus_update_node({0}, '{1}', {2})".format(transition.nodeid, host, transition.port)
        else:
            transition.nodeid = self.nodeid
            self.nodeid += 1

            return "citus_add_node('{0}', {1}, {2}, '{3}')".format(transition.host, transition.port,
                                                                   group, transition.role)

    def check_transitions(self, old_topology: PgDistGroup, new_topology: PgDistGroup,
                          expected_transitions: List[str]) -> None:
        check_topology = deepcopy(old_topology)

        transitions: List[str] = []
        for node in new_topology.transition(old_topology):
            self.assertTrue(node not in check_topology or (check_topology.get(node) or node).role == 'demoted')
            old_node = node.nodeid and next(iter(v for v in check_topology if v.nodeid == node.nodeid), None)
            if old_node:
                check_topology.discard(old_node)
            transitions.append(self.map_to_sql(new_topology.group, node))
            check_topology.add(node)
        self.assertEqual(transitions, expected_transitions)

    def test_new_topology(self):
        old = PgDistGroup(0)
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'primary'),
                              PgDistNode('2', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=100),
                                   PgDistNode('2', 5432, 'secondary', nodeid=101)})
        self.check_transitions(old, new,
                               ["citus_add_node('1', 5432, 0, 'primary')",
                                "citus_add_node('2', 5432, 0, 'secondary')"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary'),
                              PgDistNode('2', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('2', 5432, 'primary', nodeid=1),
                                   PgDistNode('1', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new,
                               ["citus_update_node(1, '1-demoted', 5432)",
                                "citus_update_node(2, '1', 5432)",
                                "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_failover(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('2', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('2', 5432, 'primary', nodeid=1),
                                   PgDistNode('1', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new,
                               ["citus_update_node(1, '1-demoted', 5432)",
                                "citus_update_node(2, '1', 5432)",
                                "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_failover_and_new_secondary(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('2', 5432, 'primary'),
                              PgDistNode('3', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('2', 5432, 'primary', nodeid=1),
                                   PgDistNode('3', 5432, 'secondary', nodeid=2)})
        # the secondary record is used to add the new standby and primary record is updated with the new hostname
        self.check_transitions(old, new, ["citus_update_node(2, '3', 5432)", "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_and_new_secondary_primary_gone(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('2', 5432, 'primary'),
                              PgDistNode('3', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('2', 5432, 'primary', nodeid=1),
                                   PgDistNode('3', 5432, 'secondary', nodeid=2)})
        # the secondary record is used to add the new standby and primary record is updated with the new hostname
        self.check_transitions(old, new, ["citus_update_node(2, '3', 5432)", "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_secondary_replaced(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'primary'),
                              PgDistNode('3', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                                   PgDistNode('3', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new, ["citus_update_node(2, '3', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_secondary_repmoved(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2),
                              PgDistNode('3', 5432, 'secondary', nodeid=3)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'primary'),
                              PgDistNode('3', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                                   PgDistNode('3', 5432, 'secondary', nodeid=3)})
        self.check_transitions(old, new, ["citus_remove_node('2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_and_secondary_removed(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2),
                              PgDistNode('3', 5432, 'secondary', nodeid=3)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary'),
                              PgDistNode('2', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary', nodeid=2),
                                   PgDistNode('2', 5432, 'primary', nodeid=1),
                                   PgDistNode('3', 5432, 'secondary', nodeid=3)})
        self.check_transitions(old, new,
                               ["citus_update_node(1, '1-demoted', 5432)",
                                "citus_update_node(2, '1', 5432)",
                                "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_and_new_secondary(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary'),
                              PgDistNode('2', 5432, 'primary'),
                              PgDistNode('3', 5432, 'secondary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary', nodeid=2),
                                   PgDistNode('2', 5432, 'primary', nodeid=1)})
        self.check_transitions(old, new,
                               ["citus_update_node(1, '1-demoted', 5432)",
                                "citus_update_node(2, '1', 5432)",
                                "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_failover_to_new_node_secondary_remains(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('2', 5432, 'secondary'),
                              PgDistNode('3', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('3', 5432, 'primary', nodeid=1),
                                   PgDistNode('2', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new, ["citus_update_node(1, '3', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_failover_to_new_node_secondary_removed(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('3', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('3', 5432, 'primary', nodeid=1),
                                   PgDistNode('2', 5432, 'secondary', nodeid=2)})
        # the secondary record needs to be removed before we update the primary record
        self.check_transitions(old, new, ["citus_update_node(1, '3', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_to_new_node_and_secondary_removed(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary'),
                              PgDistNode('3', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('3', 5432, 'primary', nodeid=1),
                                   PgDistNode('1', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new, ["citus_update_node(1, '3', 5432)", "citus_update_node(2, '1', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_with_pause(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'primary', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted', nodeid=1),
                                   PgDistNode('2', 5432, 'secondary', nodeid=2)})
        self.check_transitions(old, new, ["citus_update_node(1, '1-demoted', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_after_paused_connections(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('2', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary', nodeid=2),
                                   PgDistNode('2', 5432, 'primary', nodeid=1)})
        self.check_transitions(old, new, ["citus_update_node(2, '1', 5432)", "citus_update_node(1, '2', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_to_new_node_after_paused_connections(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('3', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('1', 5432, 'secondary', nodeid=2),
                                   PgDistNode('3', 5432, 'primary', nodeid=1)})
        self.check_transitions(old, new, ["citus_update_node(1, '3', 5432)", "citus_update_node(2, '1', 5432)"])
        self.assertTrue(new.equals(expected, True))

    def test_switchover_to_new_node_after_paused_connections_secondary_added(self):
        old = PgDistGroup(0, {PgDistNode('1', 5432, 'demoted', nodeid=1),
                              PgDistNode('2', 5432, 'secondary', nodeid=2)})
        new = PgDistGroup(0, {PgDistNode('4', 5432, 'secondary'),
                              PgDistNode('3', 5432, 'primary')})
        expected = PgDistGroup(0, {PgDistNode('4', 5432, 'secondary', nodeid=2),
                                   PgDistNode('3', 5432, 'primary', nodeid=1)})
        self.check_transitions(old, new, ["citus_update_node(1, '3', 5432)", "citus_update_node(2, '4', 5432)"])
        self.assertTrue(new.equals(expected, True))
