import os

from concurrent.futures import Future
from unittest.mock import Mock, patch, PropertyMock

from patroni import global_config
from patroni.collections import CaseInsensitiveSet
from patroni.dcs import Cluster, ClusterConfig, Status, SyncState
from patroni.postgresql import Postgresql, PostgresqlState
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.slots import SlotsHandler

from . import BaseTestPostgresql, mock_available_gucs, psycopg_connect


@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch.object(Postgresql, 'available_gucs', mock_available_gucs)
class TestSync(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
    @patch('subprocess.check_output', Mock(return_value=b'postgres (PostgreSQL) 19.0'))
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.CallbackExecutor', Mock())
    @patch.object(Postgresql, 'get_major_version', Mock(return_value=140000))
    @patch.object(Postgresql, 'is_running', Mock(return_value=True))
    @patch.object(Postgresql, 'available_gucs', mock_available_gucs)
    def setUp(self):
        super(TestSync, self).setUp()
        self.p.config.write_postgresql_conf()
        self.s = self.p.sync_handler
        config = ClusterConfig(1, {'synchronous_mode': True}, 1)
        self.cluster = Cluster(True, config, self.leader, Status.empty(), [self.me, self.other, self.leadermem],
                               None, SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(self.cluster)

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_pick_sync_standby(self):
        pg_stat_replication = [
            {'pid': 100, 'application_name': self.leadermem.name, 'sync_state': 'sync', 'flush_lsn': 1},
            {'pid': 101, 'application_name': self.me.name, 'sync_state': 'async', 'flush_lsn': 2},
            {'pid': 102, 'application_name': self.other.name, 'sync_state': 'async', 'flush_lsn': 2}]

        # sync node is a bit behind of async, but we prefer it anyway
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[self.leadermem.name, pg_stat_replication,
                                                                              'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster), ('priority', 1,
                                                                  CaseInsensitiveSet([self.leadermem.name]),
                                                                  CaseInsensitiveSet([self.leadermem.name]),
                                                                  CaseInsensitiveSet([self.leadermem.name])))

        # prefer node with sync_state='potential', even if it is slightly behind of async
        pg_stat_replication[0]['sync_state'] = 'potential'
        for r in pg_stat_replication:
            r['write_lsn'] = r.pop('flush_lsn')
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['', 'remote_write', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster), ('off', 0, CaseInsensitiveSet(), CaseInsensitiveSet(),
                                                                  CaseInsensitiveSet([self.leadermem.name])))

        # when there are no sync or potential candidates we pick async with the minimal replication lag
        for i, r in enumerate(pg_stat_replication):
            r.update(replay_lsn=3 - i, application_name=r['application_name'].upper())
        missing = pg_stat_replication.pop(0)
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['', 'remote_apply', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster), ('off', 0, CaseInsensitiveSet(), CaseInsensitiveSet(),
                                                                  CaseInsensitiveSet([self.me.name])))

        # unknown sync node is ignored
        missing.update(application_name='missing', sync_state='sync')
        pg_stat_replication.insert(0, missing)
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['', 'remote_apply', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster), ('off', 0, CaseInsensitiveSet(), CaseInsensitiveSet(),
                                                                  CaseInsensitiveSet([self.me.name])))

        # invalid synchronous_standby_names and empty pg_stat_replication
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['a b', 'remote_apply', None]):
            self.p._major_version = 90400
            self.assertEqual(self.s.current_state(self.cluster), ('off', 0, CaseInsensitiveSet(), CaseInsensitiveSet(),
                                                                  CaseInsensitiveSet()))

        # synchronous_standby_names contains '*'
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['*', None, 'remote_apply', None]):
            self.p._major_version = 90400
            self.assertEqual(self.s.current_state(self.cluster),
                             ('priority', 1,
                              CaseInsensitiveSet(['__patroni_strict_sync_replica_placeholder__']),
                              CaseInsensitiveSet(), CaseInsensitiveSet()))

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_current_state_quorum(self):
        self.cluster.config.data['synchronous_mode'] = 'quorum'
        global_config.update(self.cluster)

        pg_stat_replication = [
            {'pid': 100, 'application_name': self.leadermem.name, 'sync_state': 'quorum', 'flush_lsn': 1},
            {'pid': 101, 'application_name': self.me.name, 'sync_state': 'quorum', 'flush_lsn': 2}]

        # sync node is a bit behind of async, but we prefer it anyway
        with patch.object(Postgresql, "_cluster_info_state_get",
                          side_effect=['ANY 1 ({0},"{1}")'.format(self.leadermem.name, self.me.name),
                                       pg_stat_replication, 'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster),
                             ('quorum', 1, CaseInsensitiveSet([self.me.name, self.leadermem.name]),
                              CaseInsensitiveSet([self.me.name, self.leadermem.name]),
                              CaseInsensitiveSet([self.leadermem.name, self.me.name])))

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_current_state_cascading(self):
        pg_stat_replication = [
            {'pid': 100, 'application_name': self.me.name, 'sync_state': 'async', 'flush_lsn': 1},
            {'pid': 101, 'application_name': self.other.name, 'sync_state': 'sync', 'flush_lsn': 2}]

        # nodes that are supposed to replicate from other standby nodes are not
        # returned if at least one standby in a chain is streaming from primary
        self.leadermem.data['tags'] = {'replicatefrom': self.me.name}
        with patch.object(Postgresql, "_cluster_info_state_get",
                          side_effect=['2 ({0},"{1}")'.format(self.leadermem.name, self.other.name),
                                       pg_stat_replication, 'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster),
                             ('priority', 2, CaseInsensitiveSet([self.other.name, self.leadermem.name]),
                              CaseInsensitiveSet(), CaseInsensitiveSet([self.me.name])))

    def test_current_state_waiting(self):
        pg_stat_replication = [
            {'pid': 100, 'application_name': self.me.name, 'sync_state': 'sync', 'flush_lsn': 1},
            {'pid': 101, 'application_name': self.other.name, 'sync_state': 'sync', 'flush_lsn': 2}]

        # some nodes were removed from synchronous_standby_names
        with patch.object(Postgresql, "_cluster_info_state_get",
                          side_effect=['1 ("{0}")'.format(self.other.name), pg_stat_replication]):
            self.assertIsNone(self.s.current_state(self.cluster))

    @patch.object(Postgresql, 'last_operation', Mock(return_value=100))
    def test_current_state_waiting_after_removal(self):
        self.cluster.config.data['synchronous_mode'] = 'quorum'
        global_config.update(self.cluster)
        a, b = self.leadermem.name, self.me.name
        replication = [
            {'pid': 100, 'application_name': a, 'sync_state': 'quorum', 'flush_lsn': 100},
            {'pid': 101, 'application_name': b, 'sync_state': 'quorum', 'flush_lsn': 100}]

        with patch.object(Postgresql, 'synchronous_standby_names',
                          return_value='ANY 1 ({0},{1})'.format(a, b)) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication):
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a, b]))

            # SHOW changed, but b is still reported as quorum.
            mock_ssn.return_value = 'ANY 1 ({0})'.format(a)
            for _ in range(2):
                self.assertIsNone(self.s.current_state(self.cluster))

            replication[1]['sync_state'] = 'async'
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

    def test_current_state_overlapping_changes(self):
        self.cluster.config.data['synchronous_node_count'] = 3
        global_config.update(self.cluster)
        self.other.data['tags'] = {}
        a, b, c = self.leadermem.name, self.me.name, self.other.name
        replication = [
            {'pid': 100, 'application_name': a, 'sync_state': 'sync', 'flush_lsn': 100},
            {'pid': 101, 'application_name': b, 'sync_state': 'async', 'flush_lsn': 100}]

        with patch.object(Postgresql, 'synchronous_standby_names', return_value=a) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication), \
                patch.object(Postgresql, 'last_operation', return_value=100) as mock_lsn:
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            mock_ssn.return_value = '2 ({0},{1})'.format(a, b)
            mock_lsn.return_value = 200
            replication[0]['flush_lsn'] = 200
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            # Add c before the previous configuration was verified.
            mock_ssn.return_value = '3 ({0},{1},"{2}")'.format(a, b, c)
            mock_lsn.return_value = 300
            replication[0]['flush_lsn'] = 300
            replication[1]['sync_state'] = 'sync'
            replication.append({'pid': 102, 'application_name': c, 'sync_state': 'async', 'flush_lsn': 100})
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            # All names now match, but b and c have not reached the new WAL boundary.
            mock_lsn.return_value = 400
            replication[0]['flush_lsn'] = 400
            replication[2]['sync_state'] = 'sync'
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))
            replication[1]['flush_lsn'] = 300
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            # Catching up to that boundary is sufficient even if the primary advances.
            mock_lsn.return_value = 500
            replication[0]['flush_lsn'] = 500
            replication[1]['flush_lsn'] = replication[2]['flush_lsn'] = 400
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a, b, c]))

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_current_state_wait_for_checkpointer(self):
        a = self.leadermem.name
        replication = [{'pid': 100, 'application_name': a, 'sync_state': 'sync', 'flush_lsn': 1}]
        futures = [Future(), Future(), Future()]
        executor = Mock()
        executor.submit.side_effect = futures

        with patch.object(Postgresql, 'synchronous_standby_names', return_value=a) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication), \
                patch.object(Postgresql, 'query', return_value=[(False,)]) as mock_query, \
                patch('patroni.thread_pool.get_executor', Mock(return_value=executor)):
            # walsender already reports sync_state=sync, but checkpointer might still not enforce it
            self.assertIsNone(self.s.current_state(self.cluster))
            self.assertIsNone(self.s.current_state(self.cluster))
            executor.submit.assert_called_once_with(self.p.checkpoint)

            futures[0].set_result('not accessible or not healthy')
            self.assertIsNone(self.s.current_state(self.cluster))
            self.assertIsNone(self.s.current_state(self.cluster))
            self.assertEqual(executor.submit.call_count, 2)

            futures[1].set_result(None)
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            # synchronous_standby_names became empty and non-empty again, the last checkpoint started after reload
            mock_ssn.return_value = ''
            self.s.current_state(self.cluster)
            mock_ssn.return_value = a
            mock_query.return_value = [(True,)]
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))
            self.assertEqual(executor.submit.call_count, 2)

            # requested checkpoint started (CheckpointDone), there is no need to wait for its completion
            mock_ssn.return_value = ''
            self.s.current_state(self.cluster)
            mock_ssn.return_value = a
            mock_query.return_value = [(False,)]
            self.assertIsNone(self.s.current_state(self.cluster))
            self.assertEqual(executor.submit.call_count, 3)
            mock_query.return_value = [(True,)]
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))
            self.assertFalse(futures[2].done())

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_primary', Mock(return_value=True))
    @patch.object(Postgresql, 'get_primary_timeline', Mock(return_value=1))
    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    @patch.object(Postgresql, 'state', PropertyMock(return_value=PostgresqlState.RUNNING))
    def test_set_sync_standby_slow_reload(self):
        a, b = self.leadermem.name, self.me.name
        replication = [{'pid': 100, 'application_name': a, 'sync_state': 'sync', 'flush_lsn': 1},
                       {'pid': 101, 'application_name': b, 'sync_state': 'async', 'flush_lsn': 1}]

        with patch.object(Postgresql, 'synchronous_standby_names', return_value=a) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication), \
                patch.object(ConfigHandler, 'set_synchronous_standby_names', Mock(return_value=True)) as mock_set:
            self.assertFalse(self.s.set_synchronous_standby_names([a]))

            # postgresql.conf was updated, but the new value is not yet visible
            self.assertTrue(self.s.set_synchronous_standby_names([a, b]))
            mock_set.return_value = None  # postgresql.conf already has the new value
            self.assertTrue(self.s.set_synchronous_standby_names([a, b]))

            mock_ssn.return_value = '2 ({0},{1})'.format(a, b)
            replication[1]['sync_state'] = 'sync'
            self.assertFalse(self.s.set_synchronous_standby_names([a, b]))
            self.assertIsNone(self.s.set_synchronous_standby_names([a, b]))

    @patch('time.sleep', Mock())
    @patch.object(Postgresql, 'is_primary', Mock(return_value=True))
    @patch.object(Postgresql, 'get_primary_timeline', Mock(return_value=1))
    @patch.object(Postgresql, 'state', PropertyMock(return_value=PostgresqlState.RUNNING))
    def test_current_state_quorum_catchup(self):
        self.cluster.config.data['synchronous_mode'] = 'quorum'
        global_config.update(self.cluster)
        self.other.data['tags'] = {}
        a, b, c = self.leadermem.name, self.me.name, self.other.name
        replication = [{'pid': 100 + i, 'application_name': name, 'sync_state': 'quorum', 'flush_lsn': 100}
                       for i, name in enumerate((a, b, c))]

        with patch.object(Postgresql, 'synchronous_standby_names',
                          return_value='ANY 1 ({0},{1},"{2}")'.format(a, b, c)) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication), \
                patch.object(Postgresql, 'last_operation', return_value=100) as mock_lsn, \
                patch.object(ConfigHandler, 'set_synchronous_standby_names', Mock(return_value=True)):
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a, b, c]))

            # c was removed, transactions acknowledged only by c must reach one of remaining nodes
            mock_ssn.return_value = 'ANY 1 ({0},{1})'.format(a, b)
            mock_lsn.return_value = 200
            replication[2]['sync_state'] = 'async'
            self.assertTrue(self.s.set_synchronous_standby_names([a, b], 1))
            self.assertIsNone(self.s.current_state(self.cluster))
            replication[1]['flush_lsn'] = 200
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([b]))

            # num was increased, transactions acknowledged by one node must reach two nodes
            self.cluster.config.data['synchronous_node_count'] = 2
            global_config.update(self.cluster)
            mock_ssn.return_value = 'ANY 2 ({0},{1})'.format(a, b)
            mock_lsn.return_value = 300
            replication[0]['flush_lsn'] = 300
            self.assertIsNone(self.s.current_state(self.cluster))

            # don't wait when there are not enough eligible nodes and all of them caught up
            self.cluster.config.data['maximum_lag_on_syncnode'] = 50
            global_config.update(self.cluster)
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))
            self.cluster.config.data['maximum_lag_on_syncnode'] = -1
            global_config.update(self.cluster)
            self.assertIsNone(self.s.current_state(self.cluster))

            # synchronous_node_count was reduced, don't wait for more nodes than needed
            self.cluster.config.data['synchronous_node_count'] = 1
            global_config.update(self.cluster)
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))
            replication[1]['flush_lsn'] = 300
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a, b]))

    def test_current_state_quorum_catchup_degraded(self):
        self.cluster.config.data.update(synchronous_mode='quorum', synchronous_node_count=2)
        global_config.update(self.cluster)
        self.other.data['tags'] = {}
        a, b, c = self.leadermem.name, self.me.name, self.other.name
        replication = [{'pid': 100 + i, 'application_name': name, 'sync_state': 'quorum', 'flush_lsn': 100}
                       for i, name in enumerate((a, b, c))]

        with patch.object(Postgresql, 'synchronous_standby_names',
                          return_value='ANY 2 ({0},{1},"{2}")'.format(a, b, c)) as mock_ssn, \
                patch.object(Postgresql, 'pg_stat_replication', return_value=replication), \
                patch.object(Postgresql, 'last_operation', return_value=100) as mock_lsn:
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a, b, c]))

            # c was removed and b disconnected, transactions acknowledged by b and c must reach a
            mock_ssn.return_value = 'ANY 2 ({0},{1})'.format(a, b)
            mock_lsn.return_value = 200
            replication[2].update(sync_state='async', flush_lsn=200)
            del replication[1]
            self.assertIsNone(self.s.current_state(self.cluster))
            replication[0]['flush_lsn'] = 200
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

            # b was replaced with c, but c is not yet reporting the new sync_state
            mock_ssn.return_value = 'ANY 1 ({0},"{1}")'.format(a, c)
            mock_lsn.return_value = 300
            self.assertIsNone(self.s.current_state(self.cluster))
            replication[1]['sync_state'] = 'quorum'
            self.assertIsNone(self.s.current_state(self.cluster))
            replication[0]['flush_lsn'] = 300
            self.assertEqual(self.s.current_state(self.cluster).sync_confirmed, CaseInsensitiveSet([a]))

    @patch('time.sleep', Mock())
    def test_set_sync_standby(self):
        def value_in_conf():
            with open(os.path.join(self.p.data_dir, 'postgresql.conf')) as f:
                for line in f:
                    if line.startswith('synchronous_standby_names'):
                        return line.strip()

        mock_reload = self.p.reload = Mock()
        self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1']))
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")
        mock_reload.assert_called()

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1']))
        mock_reload.assert_not_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'n1'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1', 'n2']))
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '2 (n1,n2)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(CaseInsensitiveSet([]))
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), None)

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(CaseInsensitiveSet('*'))
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '__patroni_strict_sync_replica_placeholder__'")

        self.cluster.config.data['synchronous_mode'] = 'quorum'
        global_config.update(self.cluster)
        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names([], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(),
                         "synchronous_standby_names = 'ANY 1 (__patroni_strict_sync_replica_placeholder__)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['any', 'b'], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'ANY 1 (\"any\",b)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['a', 'b'], 3)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = 'ANY 3 (a,b)'")

        self.p._major_version = 90601
        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names([], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(),
                         "synchronous_standby_names = '1 (__patroni_strict_sync_replica_placeholder__)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['a', 'b'], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '1 (a,b)'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['a', 'b'], 3)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '3 (a,b)'")

        self.p._major_version = 90501
        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names([], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '__patroni_strict_sync_replica_placeholder__'")

        mock_reload.reset_mock()
        self.s.set_synchronous_standby_names(['a-1'], 1)
        mock_reload.assert_called()
        self.assertEqual(value_in_conf(), "synchronous_standby_names = '\"a-1\"'")

        # PG17+ with manage_synchronized_standby_slots: ensure a manual reload happens
        # when only synchronized_standby_slots changes (synchronous_standby_names unchanged).
        # We only need to check the update_synchronized_standby_slots/reload/write_postgresql_conf
        # calls here - how update_synchronized_standby_slots itself behaves is already covered by
        # test_update_synchronized_standby_slots in test_slots.py.
        self.p._major_version = 170000
        self.cluster.config.data['synchronous_mode'] = True
        self.cluster.config.data['manage_synchronized_standby_slots'] = True
        global_config.update(self.cluster)
        # Pre-populate SSN with the value set_synchronous_standby_names would produce (no change there).
        self.p.config._server_parameters['synchronous_standby_names'] = 'n1'
        mock_reload.reset_mock()
        with patch.object(global_config.__class__, 'manage_synchronized_standby_slots_enabled',
                          PropertyMock(return_value=True)), \
                patch.object(Postgresql, 'state', PropertyMock(return_value=PostgresqlState.RUNNING)), \
                patch.object(SlotsHandler, 'update_synchronized_standby_slots',
                             Mock(return_value=True)) as mock_update_slots, \
                patch.object(ConfigHandler, 'write_postgresql_conf', Mock()) as mock_write_conf:
            self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1']))
        mock_update_slots.assert_called_once_with(CaseInsensitiveSet(['n1']), None)
        # SSN didn't change, but slots did, so a manual reload (and config write) is expected.
        mock_write_conf.assert_called_once()
        mock_reload.assert_called()

        # Regression: member names containing characters that require quoting (e.g. dashes)
        # must NOT be passed through quote_standby_name() before being converted to slot names.
        # Otherwise slot_name_from_member_name() encodes the quotes as unicode codepoints
        # (e.g. u0034postgres_1u0034), yielding a slot name that doesn't match pg_replication_slots.
        self.p.config._server_parameters.pop('synchronous_standby_names', None)
        mock_reload.reset_mock()
        with patch.object(global_config.__class__, 'manage_synchronized_standby_slots_enabled',
                          PropertyMock(return_value=True)), \
                patch.object(SlotsHandler, 'update_synchronized_standby_slots',
                             Mock(return_value=False)) as mock_update_slots:
            self.s.set_synchronous_standby_names(CaseInsensitiveSet(['postgres-1', 'postgres-2']))
        mock_update_slots.assert_called_once_with(CaseInsensitiveSet(['postgres-1', 'postgres-2']), None)
        # synchronous_standby_names is built from QUOTED names, so we expect them quoted here:
        self.assertEqual(self.p.config._server_parameters.get('synchronous_standby_names'),
                         '2 ("postgres-1","postgres-2")')

        # Regression: same check with quorum mode (different sync_param building path).
        self.cluster.config.data['synchronous_mode'] = 'quorum'
        global_config.update(self.cluster)
        self.p.config._server_parameters.pop('synchronous_standby_names', None)
        mock_reload.reset_mock()
        with patch.object(global_config.__class__, 'manage_synchronized_standby_slots_enabled',
                          PropertyMock(return_value=True)), \
                patch.object(SlotsHandler, 'update_synchronized_standby_slots',
                             Mock(return_value=False)) as mock_update_slots:
            self.s.set_synchronous_standby_names(CaseInsensitiveSet(['postgres-1', 'postgres-2']), 1)
        mock_update_slots.assert_called_once_with(CaseInsensitiveSet(['postgres-1', 'postgres-2']), 1)
        self.assertEqual(self.p.config._server_parameters.get('synchronous_standby_names'),
                         'ANY 1 ("postgres-1","postgres-2")')

    def test_set_sync_standby_restores_slots_after_feature_toggled_off(self):
        # Regression: manage_synchronized_standby_slots is toggled off "in the background" (e.g. via
        # a dynamic configuration reload) and the next synchronous_standby_names update is triggered
        # by an unrelated transition (not a dedicated toggle check). The disabled state must still be
        # picked up and the user-configured value (or lack thereof) restored - see update_synchronized
        # _standby_slots in patroni/postgresql/slots.py for where this is actually detected.
        self.p._major_version = 170000
        self.cluster.config.data['synchronous_mode'] = True
        self.cluster.config.data['manage_synchronized_standby_slots'] = True
        global_config.update(self.cluster)

        with patch.object(global_config.__class__, 'manage_synchronized_standby_slots_enabled',
                          PropertyMock(return_value=True)):
            self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1']))
        self.assertEqual(self.p.config._server_parameters.get('synchronized_standby_slots'), 'n1')

        # Feature toggled off without going through a dedicated toggle-check call site.
        self.p.config._config['parameters'] = {'synchronized_standby_slots': 'user_slot'}
        with patch.object(global_config.__class__, 'manage_synchronized_standby_slots_enabled',
                          PropertyMock(return_value=False)):
            self.s.set_synchronous_standby_names(CaseInsensitiveSet(['n1', 'n2']))
        self.assertEqual(self.p.config._server_parameters.get('synchronized_standby_slots'), 'user_slot')

    @patch.object(Postgresql, 'last_operation', Mock(return_value=1))
    def test_do_not_prick_yourself(self):
        self.p.name = self.leadermem.name
        cluster = Cluster(True, None, self.leader, 0, [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)

        pg_stat_replication = [
            {'pid': 100, 'application_name': self.leadermem.name, 'sync_state': 'sync', 'flush_lsn': 1},
            {'pid': 101, 'application_name': self.me.name, 'sync_state': 'async', 'flush_lsn': 2},
            {'pid': 102, 'application_name': self.other.name, 'sync_state': 'async', 'flush_lsn': 2}]

        # Faulty case, application_name of the current primary is in synchronous_standby_names and in
        # the pg_stat_replication. We need to check that primary is not selected as the synchronous node.
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[self.leadermem.name, pg_stat_replication,
                                                                              'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(cluster), ('priority', 1, CaseInsensitiveSet([self.leadermem.name]),
                                                             CaseInsensitiveSet(), CaseInsensitiveSet([self.me.name])))
