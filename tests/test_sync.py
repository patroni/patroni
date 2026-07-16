import os

from unittest.mock import Mock, patch, PropertyMock

from patroni import global_config
from patroni.collections import CaseInsensitiveSet
from patroni.dcs import Cluster, ClusterConfig, Status, SyncState
from patroni.postgresql import Postgresql

from . import BaseTestPostgresql, mock_available_gucs, psycopg_connect


@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch.object(Postgresql, 'available_gucs', mock_available_gucs)
class TestSync(BaseTestPostgresql):

    @patch('subprocess.call', Mock(return_value=0))
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
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[self.leadermem.name,
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
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=['*', 'remote_apply', None]):
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
                                       'on', pg_stat_replication]):
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
                                       'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(self.cluster),
                             ('priority', 2, CaseInsensitiveSet([self.other.name, self.leadermem.name]),
                              CaseInsensitiveSet(), CaseInsensitiveSet([self.me.name])))

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
        with patch.object(Postgresql, "_cluster_info_state_get", side_effect=[self.leadermem.name,
                                                                              'on', pg_stat_replication]):
            self.assertEqual(self.s.current_state(cluster), ('priority', 1, CaseInsensitiveSet([self.leadermem.name]),
                                                             CaseInsensitiveSet(), CaseInsensitiveSet([self.me.name])))

    def test_apply_topology_filter(self):
        """Full branch coverage for _ReplicaList._apply_topology_filter."""
        from patroni.postgresql.sync import _ReplicaList
        from patroni.collections import CaseInsensitiveDict
        from patroni.dcs import Member
        from patroni.postgresql.misc import PostgresqlState

        # Helper: create a Member with tags
        def make_member(name, tags=None, state=PostgresqlState.RUNNING):
            data = {'state': state, 'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5432/postgres'}
            if tags:
                data['tags'] = tags
            return Member(0, name, 28, data)

        # Helper: create a Cluster with a specific leader member in members list
        def make_cluster(leader_name, leader_tags=None, extra_members=None):
            leader_member = make_member(leader_name, leader_tags)
            member_list = [leader_member] + (extra_members or [])
            return Cluster(True, None, None, Status.empty(), member_list, None, None, None, None, None)

        self.p.name = 'primary1'

        with patch.object(global_config.__class__, 'synchronous_node_topology', new_callable=PropertyMock) as mock_topology:
            # ─── Branch 1: topology is None (not configured) → return members unchanged ───
            mock_topology.return_value = None
            m1 = make_member('m1', {'dc': 'dc-a'})
            m2 = make_member('m2', {'dc': 'dc-b'})
            members = CaseInsensitiveDict({'m1': m1, 'm2': m2})
            cluster = make_cluster('primary1', {'dc': 'dc-a'})

            result = _ReplicaList._apply_topology_filter(members, cluster, self.p)
            self.assertEqual(len(result), 2)
            self.assertIn('m1', result)
            self.assertIn('m2', result)

            # ─── Branch 2: topology is empty dict → returns None from property → return members ───
            mock_topology.return_value = {}
            result = _ReplicaList._apply_topology_filter(members, cluster, self.p)
            self.assertEqual(len(result), 2)

            # ─── Branch 3: primary member not found in cluster.members → return members ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            self.p.name = 'ghost_primary'  # not in cluster.members
            result = _ReplicaList._apply_topology_filter(members, cluster, self.p)
            self.assertEqual(len(result), 2)
            self.p.name = 'primary1'  # restore

            # ─── Branch 4: primary member has no tag for the key → log warning, return members ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            cluster_no_tag = make_cluster('primary1', {})  # primary has no 'dc' tag
            with patch('patroni.postgresql.sync.logger.warning') as mock_warn:
                result = _ReplicaList._apply_topology_filter(members, cluster_no_tag, self.p)
                self.assertEqual(len(result), 2)
                mock_warn.assert_called_once()
                self.assertIn('does not have tag', mock_warn.call_args[0][0])

            # ─── Branch 5: primary tag value is empty string → log warning, return members ───
            cluster_empty_tag = make_cluster('primary1', {'dc': ''})
            with patch('patroni.postgresql.sync.logger.warning') as mock_warn:
                result = _ReplicaList._apply_topology_filter(members, cluster_empty_tag, self.p)
                self.assertEqual(len(result), 2)
                mock_warn.assert_called_once()

            # ─── Branch 6: strategy='different' → keeps only members with different tag value ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            cluster = make_cluster('primary1', {'dc': 'dc-a'})
            m_same = make_member('m_same', {'dc': 'dc-a'})
            m_diff = make_member('m_diff', {'dc': 'dc-b'})
            m_diff2 = make_member('m_diff2', {'dc': 'dc-c'})
            members_multi = CaseInsensitiveDict({'m_same': m_same, 'm_diff': m_diff, 'm_diff2': m_diff2})
            with patch('patroni.postgresql.sync.logger.debug') as mock_debug:
                result = _ReplicaList._apply_topology_filter(members_multi, cluster, self.p)
                self.assertEqual(len(result), 2)
                self.assertIn('m_diff', result)
                self.assertIn('m_diff2', result)
                self.assertNotIn('m_same', result)
                mock_debug.assert_called_once()
                self.assertIn('selected 2 of 3', mock_debug.call_args[0][0] % mock_debug.call_args[0][1:])

            # ─── Branch 7: strategy='same' → keeps only members with same tag value ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'same'}
            result = _ReplicaList._apply_topology_filter(members_multi, cluster, self.p)
            self.assertEqual(len(result), 1)
            self.assertIn('m_same', result)

            # ─── Branch 8: strategy='different', member has no tag → member is skipped (not included) ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            m_no_tag = make_member('m_no_tag')  # no tags at all
            m_no_dc = make_member('m_no_dc', {'zone': 'z1'})  # has tags but not 'dc'
            m_valid = make_member('m_valid', {'dc': 'dc-b'})
            members_mixed = CaseInsensitiveDict({'m_no_tag': m_no_tag, 'm_no_dc': m_no_dc, 'm_valid': m_valid})
            result = _ReplicaList._apply_topology_filter(members_mixed, cluster, self.p)
            self.assertEqual(len(result), 1)
            self.assertIn('m_valid', result)
            self.assertNotIn('m_no_tag', result)
            self.assertNotIn('m_no_dc', result)

            # ─── Branch 9: strategy='same', member has empty tag → treated as '' != primary tag ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'same'}
            m_empty = make_member('m_empty', {'dc': ''})
            m_match = make_member('m_match', {'dc': 'dc-a'})
            members_empty_tag = CaseInsensitiveDict({'m_empty': m_empty, 'm_match': m_match})
            result = _ReplicaList._apply_topology_filter(members_empty_tag, cluster, self.p)
            self.assertEqual(len(result), 1)
            self.assertIn('m_match', result)

            # ─── Branch 10: FALLBACK - all candidates filtered out → return original members ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'same'}
            m_only_diff = make_member('m_only_diff', {'dc': 'dc-b'})
            members_all_diff = CaseInsensitiveDict({'m_only_diff': m_only_diff})
            with patch('patroni.postgresql.sync.logger.warning') as mock_warn:
                result = _ReplicaList._apply_topology_filter(members_all_diff, cluster, self.p)
                self.assertEqual(len(result), 1)
                self.assertIn('m_only_diff', result)  # fallback: original list returned
                mock_warn.assert_called_once()
                self.assertIn('falling back', mock_warn.call_args[0][0])

            # ─── Branch 11: FALLBACK with strategy='different' ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            m_all_same = make_member('m_all_same', {'dc': 'dc-a'})
            members_no_diff = CaseInsensitiveDict({'m_all_same': m_all_same})
            with patch('patroni.postgresql.sync.logger.warning') as mock_warn:
                result = _ReplicaList._apply_topology_filter(members_no_diff, cluster, self.p)
                self.assertEqual(len(result), 1)
                self.assertIn('m_all_same', result)  # fallback
                mock_warn.assert_called_once()

            # ─── Branch 12: Case-insensitive primary name matching ───
            mock_topology.return_value = {'key': 'dc', 'strategy': 'different'}
            self.p.name = 'PRIMARY1'  # uppercase name
            cluster_lower = make_cluster('primary1', {'dc': 'dc-a'})  # lowercase in cluster
            m_cross = make_member('m_cross', {'dc': 'dc-b'})
            members_ci = CaseInsensitiveDict({'m_cross': m_cross})
            result = _ReplicaList._apply_topology_filter(members_ci, cluster_lower, self.p)
            self.assertEqual(len(result), 1)
            self.assertIn('m_cross', result)
            self.p.name = 'primary1'  # restore

            # ─── Branch 13: Multiple members, mixed tags, some match some don't ───
            mock_topology.return_value = {'key': 'zone', 'strategy': 'different'}
            cluster_zone = make_cluster('primary1', {'zone': 'us-east-1a'})
            m_z1 = make_member('z1', {'zone': 'us-east-1a'})    # same → excluded
            m_z2 = make_member('z2', {'zone': 'us-east-1b'})    # different → included
            m_z3 = make_member('z3', {'zone': 'eu-west-1a'})    # different → included
            m_z4 = make_member('z4', {'rack': 'r1'})            # no 'zone' tag → excluded
            members_zones = CaseInsensitiveDict({'z1': m_z1, 'z2': m_z2, 'z3': m_z3, 'z4': m_z4})
            result = _ReplicaList._apply_topology_filter(members_zones, cluster_zone, self.p)
            self.assertEqual(len(result), 2)
            self.assertIn('z2', result)
            self.assertIn('z3', result)

    def test_global_config_synchronous_node_topology_property(self):
        """Full branch coverage for GlobalConfig.synchronous_node_topology property."""
        from patroni.dcs import ClusterConfig

        # ─── Case 1: Not configured at all → None ───
        config = ClusterConfig(1, {'synchronous_mode': True}, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 2: Valid configuration with 'different' ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'key': 'dc', 'strategy': 'different'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        result = global_config.synchronous_node_topology
        self.assertIsNotNone(result)
        self.assertEqual(result['key'], 'dc')
        self.assertEqual(result['strategy'], 'different')

        # ─── Case 3: Valid configuration with 'same' ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'key': 'zone', 'strategy': 'same'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        result = global_config.synchronous_node_topology
        self.assertIsNotNone(result)
        self.assertEqual(result['key'], 'zone')
        self.assertEqual(result['strategy'], 'same')

        # ─── Case 4: Invalid - not a dict (string) → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': 'invalid_string'
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 5: Invalid - missing 'key' → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'strategy': 'different'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 6: Invalid - missing 'strategy' → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'key': 'dc'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 7: Invalid - wrong strategy value → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'key': 'dc', 'strategy': 'nearest'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 8: Invalid - empty key → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': {'key': '', 'strategy': 'different'}
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 9: Invalid - not a dict (list) → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': ['dc', 'different']
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

        # ─── Case 10: Invalid - not a dict (integer) → None ───
        config = ClusterConfig(1, {
            'synchronous_mode': True,
            'synchronous_node_topology': 42
        }, 1)
        cluster = Cluster(True, config, self.leader, Status.empty(),
                          [self.me, self.other, self.leadermem], None,
                          SyncState(0, self.me.name, self.leadermem.name, 0), None, None, None)
        global_config.update(cluster)
        self.assertIsNone(global_config.synchronous_node_topology)

