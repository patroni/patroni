import datetime
import etcd
import os
import sys

from mock import Mock, MagicMock, PropertyMock, patch, mock_open
from patroni.collections import CaseInsensitiveSet
from patroni.config import Config
from patroni.dcs import Cluster, ClusterConfig, Failover, Leader, Member, get_dcs, Status, SyncState, TimelineHistory
from patroni.dcs.etcd import AbstractEtcdClientWithFailover
from patroni.exceptions import DCSError, PostgresConnectionException, PatroniFatalException
from patroni.ha import Ha, _MemberStatus
from patroni.postgresql import Postgresql
from patroni.postgresql.bootstrap import Bootstrap
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.postgresql.rewind import Rewind
from patroni.postgresql.slots import SlotsHandler
from patroni.utils import tzutc
from patroni.watchdog import Watchdog

from . import PostgresInit, MockPostmaster, psycopg_connect, requests_get
from .test_etcd import socket_getaddrinfo, etcd_read, etcd_write

SYSID = '12345678901'


def true(*args, **kwargs):
    return True


def false(*args, **kwargs):
    return False


def get_cluster(initialize, leader, members, failover, sync, cluster_config=None, failsafe=None):
    t = datetime.datetime.now().isoformat()
    history = TimelineHistory(1, '[[1,67197376,"no recovery target specified","' + t + '","foo"]]',
                              [(1, 67197376, 'no recovery target specified', t, 'foo')])
    cluster_config = cluster_config or ClusterConfig(1, {'check_timeline': True}, 1)
    return Cluster(initialize, cluster_config, leader, Status(10, None), members, failover, sync, history, failsafe)


def get_cluster_not_initialized_without_leader(cluster_config=None):
    return get_cluster(None, None, [], None, SyncState.empty(), cluster_config)


def get_cluster_bootstrapping_without_leader(cluster_config=None):
    return get_cluster("", None, [], None, SyncState.empty(), cluster_config)


def get_cluster_initialized_without_leader(leader=False, failover=None, sync=None, cluster_config=None, failsafe=False):
    m1 = Member(0, 'leader', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres',
                                  'api_url': 'http://127.0.0.1:8008/patroni', 'xlog_location': 4,
                                  'role': 'primary', 'state': 'running'})
    leader = Leader(0, 0, m1 if leader else Member(0, '', 28, {}))
    m2 = Member(0, 'other', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
                                 'api_url': 'http://127.0.0.1:8011/patroni',
                                 'state': 'running',
                                 'pause': True,
                                 'tags': {'clonefrom': True},
                                 'scheduled_restart': {'schedule': "2100-01-01 10:53:07.560445+00:00",
                                                       'postgres_version': '99.0.0'}})
    syncstate = SyncState(0 if sync else None, sync and sync[0], sync and sync[1])
    failsafe = {m.name: m.api_url for m in (m1, m2)} if failsafe else None
    return get_cluster(SYSID, leader, [m1, m2], failover, syncstate, cluster_config, failsafe)


def get_cluster_initialized_with_leader(failover=None, sync=None):
    return get_cluster_initialized_without_leader(leader=True, failover=failover, sync=sync)


def get_cluster_initialized_with_only_leader(failover=None, cluster_config=None):
    leader = get_cluster_initialized_without_leader(leader=True, failover=failover).leader
    return get_cluster(True, leader, [leader.member], failover, SyncState.empty(), cluster_config)


def get_standby_cluster_initialized_with_only_leader(failover=None, sync=None):
    return get_cluster_initialized_with_only_leader(
        cluster_config=ClusterConfig(1, {
            "standby_cluster": {
                "host": "localhost",
                "port": 5432,
                "primary_slot_name": "",
            }}, 1)
    )


def get_cluster_initialized_with_leader_and_failsafe():
    return get_cluster_initialized_without_leader(leader=True, failsafe=True,
                                                  cluster_config=ClusterConfig(1, {'failsafe_mode': True}, 1))


def get_node_status(reachable=True, in_recovery=True, dcs_last_seen=0,
                    timeline=2, wal_position=10, nofailover=False,
                    watchdog_failed=False, failover_priority=1):
    def fetch_node_status(e):
        tags = {}
        if nofailover:
            tags['nofailover'] = True
        tags['failover_priority'] = failover_priority
        return _MemberStatus(e, reachable, in_recovery, wal_position,
                             {'tags': tags, 'watchdog_failed': watchdog_failed,
                              'dcs_last_seen': dcs_last_seen, 'timeline': timeline})
    return fetch_node_status


future_restart_time = datetime.datetime.now(tzutc) + datetime.timedelta(days=5)
postmaster_start_time = datetime.datetime.now(tzutc)


class MockPatroni(object):

    def __init__(self, p, d):
        os.environ[Config.PATRONI_CONFIG_VARIABLE] = """
restapi:
  listen: 0.0.0.0:8008
bootstrap:
  users:
    replicator:
      password: rep-pass
      options:
        - replication
postgresql:
  name: foo
  data_dir: data/postgresql0
  pg_rewind:
    username: postgres
    password: postgres
watchdog:
  mode: off
zookeeper:
  exhibitor:
    hosts: [localhost]
    port: 8181
"""
        # We rely on sys.argv in Config, so it's necessary to reset
        # all the extra values that are coming from py.test
        sys.argv = sys.argv[:1]

        self.config = Config(None)
        self.version = '1.5.7'
        self.postgresql = p
        self.dcs = d
        self.api = Mock()
        self.tags = {'foo': 'bar'}
        self.nofailover = None
        self.replicatefrom = None
        self.api.connection_string = 'http://127.0.0.1:8008'
        self.clonefrom = None
        self.nosync = False
        self.scheduled_restart = {'schedule': future_restart_time,
                                  'postmaster_start_time': str(postmaster_start_time)}
        self.watchdog = Watchdog(self.config)
        self.request = lambda *args, **kwargs: requests_get(args[0].api_url, *args[1:], **kwargs)
        self.failover_priority = 1


def run_async(self, func, args=()):
    self.reset_scheduled_action()
    if args:
        func(*args)
    else:
        func()


@patch.object(Postgresql, 'is_running', Mock(return_value=MockPostmaster()))
@patch.object(Postgresql, 'is_primary', Mock(return_value=True))
@patch.object(Postgresql, 'timeline_wal_position', Mock(return_value=(1, 10, 1)))
@patch.object(Postgresql, '_cluster_info_state_get', Mock(return_value=10))
@patch.object(Postgresql, 'slots', Mock(return_value={'l': 100}))
@patch.object(Postgresql, 'data_directory_empty', Mock(return_value=False))
@patch.object(Postgresql, 'controldata', Mock(return_value={
    'Database system identifier': SYSID,
    'Database cluster state': 'shut down',
    'Latest checkpoint location': '0/12345678',
    "Latest checkpoint's TimeLineID": '2'}))
@patch.object(SlotsHandler, 'load_replication_slots', Mock(side_effect=Exception))
@patch.object(ConfigHandler, 'append_pg_hba', Mock())
@patch.object(ConfigHandler, 'write_pgpass', Mock(return_value={}))
@patch.object(ConfigHandler, 'write_recovery_conf', Mock())
@patch.object(ConfigHandler, 'write_postgresql_conf', Mock())
@patch.object(Postgresql, 'query', Mock())
@patch.object(Postgresql, 'checkpoint', Mock())
@patch.object(CancellableSubprocess, 'call', Mock(return_value=0))
@patch.object(Postgresql, 'get_replica_timeline', Mock(return_value=2))
@patch.object(Postgresql, 'get_primary_timeline', Mock(return_value=2))
@patch.object(Postgresql, 'get_major_version', Mock(return_value=140000))
@patch.object(Postgresql, 'resume_wal_replay', Mock())
@patch.object(ConfigHandler, 'restore_configuration_files', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
@patch.object(etcd.Client, 'delete', Mock(side_effect=etcd.EtcdException))
@patch('patroni.postgresql.polling_loop', Mock(return_value=range(1)))
@patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=False))
@patch('patroni.async_executor.AsyncExecutor.run_async', run_async)
@patch('patroni.postgresql.rewind.Thread', Mock())
@patch('patroni.postgresql.citus.CitusHandler.start', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('time.sleep', Mock())
class TestHa(PostgresInit):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch('patroni.dcs.dcs_modules', Mock(return_value=['patroni.dcs.etcd']))
    @patch.object(etcd.Client, 'read', etcd_read)
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    def setUp(self):
        super(TestHa, self).setUp()
        self.p.set_state('running')
        self.p.set_role('replica')
        self.p.postmaster_start_time = MagicMock(return_value=str(postmaster_start_time))
        self.p.can_create_replica_without_replication_connection = MagicMock(return_value=False)
        self.e = get_dcs({'etcd': {'ttl': 30, 'host': 'ok:2379', 'scope': 'test',
                                   'name': 'foo', 'retry_timeout': 10},
                          'citus': {'database': 'citus', 'group': None}})
        self.ha = Ha(MockPatroni(self.p, self.e))
        self.ha.old_cluster = self.e.get_cluster()
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.ha.load_cluster_from_dcs = Mock()

    def test_update_lock(self):
        self.ha.is_failsafe_mode = true
        self.p.last_operation = Mock(side_effect=PostgresConnectionException(''))
        self.ha.dcs.update_leader = Mock(side_effect=[DCSError(''), Exception])
        self.assertRaises(DCSError, self.ha.update_lock)
        self.assertFalse(self.ha.update_lock(True))

    @patch.object(Postgresql, 'received_timeline', Mock(return_value=None))
    def test_touch_member(self):
        self.p._major_version = 110000
        self.p.is_primary = false
        self.p.timeline_wal_position = Mock(return_value=(0, 1, 0))
        self.p.replica_cached_timeline = Mock(side_effect=Exception)
        with patch.object(Postgresql, '_cluster_info_state_get', Mock(return_value='streaming')):
            self.ha.touch_member()
        self.p.timeline_wal_position = Mock(return_value=(0, 1, 1))
        self.p.set_role('standby_leader')
        self.ha.touch_member()
        self.p.set_role('primary')
        self.ha.dcs.touch_member = true
        self.ha.touch_member()

    def test_is_leader(self):
        self.assertFalse(self.ha.is_leader())

    def test_start_as_replica(self):
        self.p.is_healthy = false
        self.assertEqual(self.ha.run_cycle(), 'starting as a secondary')

    @patch('patroni.dcs.etcd.Etcd.initialize', return_value=True)
    def test_bootstrap_as_standby_leader(self, initialize):
        self.p.data_directory_empty = true
        self.ha.cluster = get_cluster_not_initialized_without_leader(cluster_config=ClusterConfig(0, {}, 0))
        self.ha.patroni.config._dynamic_configuration = {"standby_cluster": {"port": 5432}}
        self.assertEqual(self.ha.run_cycle(), 'trying to bootstrap a new standby leader')

    def test_bootstrap_waiting_for_standby_leader(self):
        self.p.data_directory_empty = true
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.ha.cluster.config.data.update({'standby_cluster': {'port': 5432}})
        self.assertEqual(self.ha.run_cycle(), 'waiting for standby_leader to bootstrap')

    @patch.object(Cluster, 'get_clone_member',
                  Mock(return_value=Member(0, 'test', 1, {'api_url': 'http://127.0.0.1:8011/patroni',
                                                          'conn_url': 'postgres://127.0.0.1:5432/postgres'})))
    @patch.object(Bootstrap, 'create_replica', Mock(return_value=0))
    def test_start_as_cascade_replica_in_standby_cluster(self):
        self.p.data_directory_empty = true
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.assertEqual(self.ha.run_cycle(), "trying to bootstrap from replica 'test'")

    def test_recover_replica_failed(self):
        self.p.controldata = lambda: {'Database cluster state': 'in recovery', 'Database system identifier': SYSID}
        self.p.is_running = false
        self.p.follow = false
        self.assertEqual(self.ha.run_cycle(), 'starting as a secondary')
        self.assertEqual(self.ha.run_cycle(), 'failed to start postgres')

    def test_recover_raft(self):
        self.p.controldata = lambda: {'Database cluster state': 'in recovery', 'Database system identifier': SYSID}
        self.p.is_running = false
        self.p.follow = true
        self.assertEqual(self.ha.run_cycle(), 'starting as a secondary')
        self.p.is_running = true
        ha_dcs_orig_name = self.ha.dcs.__class__.__name__
        self.ha.dcs.__class__.__name__ = 'Raft'
        self.assertEqual(self.ha.run_cycle(), 'started as a secondary')
        self.ha.dcs.__class__.__name__ = ha_dcs_orig_name

    def test_recover_former_primary(self):
        self.p.follow = false
        self.p.is_running = false
        self.p.name = 'leader'
        self.p.set_role('demoted')
        self.p.controldata = lambda: {'Database cluster state': 'shut down', 'Database system identifier': SYSID}
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'starting as readonly because i had the session lock')

    def test_start_primary_after_failure(self):
        self.p.start = false
        self.p.is_running = false
        self.p.name = 'leader'
        self.p.set_role('primary')
        self.p.controldata = lambda: {'Database cluster state': 'in production', 'Database system identifier': SYSID}
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'starting primary after failure')

    @patch.object(Rewind, 'ensure_clean_shutdown', Mock())
    def test_crash_recovery(self):
        self.ha.has_lock = true
        self.p.is_running = false
        self.p.controldata = lambda: {'Database cluster state': 'in production', 'Database system identifier': SYSID}
        self.assertEqual(self.ha.run_cycle(), 'doing crash recovery in a single user mode')
        with patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=True)), \
                patch.object(Ha, 'check_timeline', Mock(return_value=False)):
            self.ha._async_executor.schedule('doing crash recovery in a single user mode')
            self.ha.state_handler.cancellable._process = Mock()
            self.ha._crash_recovery_started -= 600
            self.ha.cluster.config.data.update({'maximum_lag_on_failover': 10})
            self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
            self.assertEqual(self.ha.run_cycle(), 'terminated crash recovery because of startup timeout')

    @patch.object(Rewind, 'ensure_clean_shutdown', Mock())
    @patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True))
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    def test_crash_recovery_before_rewind(self):
        self.p.is_primary = false
        self.p.is_running = false
        self.p.controldata = lambda: {'Database cluster state': 'in archive recovery',
                                      'Database system identifier': SYSID}
        self.ha._rewind.trigger_check_diverged_lsn()
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'doing crash recovery in a single user mode')

    @patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True))
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    @patch('os.listdir', Mock(return_value=[]))
    @patch('patroni.postgresql.rewind.fsync_dir', Mock())
    def test_recover_with_rewind(self):
        self.p.is_running = false
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.cluster.leader.member.data.update(version='2.0.2', role='primary')
        self.ha._rewind.pg_rewind = true
        self.ha._rewind.check_leader_is_not_in_recovery = true
        with patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True)):
            self.assertEqual(self.ha.run_cycle(), 'running pg_rewind from leader')
        with patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=False)), \
                patch.object(Ha, 'is_synchronous_mode', Mock(return_value=True)):
            self.p.follow = true
            self.assertEqual(self.ha.run_cycle(), 'starting as a secondary')
            self.p.is_running = true
            self.ha.follow = Mock(return_value='fake')
            self.assertEqual(self.ha.run_cycle(), 'fake')

    @patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True))
    @patch.object(Rewind, 'should_remove_data_directory_on_diverged_timelines', PropertyMock(return_value=True))
    @patch.object(Bootstrap, 'create_replica', Mock(return_value=1))
    def test_recover_with_reinitialize(self):
        self.p.is_running = false
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'reinitializing due to diverged timelines')

    @patch('sys.exit', return_value=1)
    @patch('patroni.ha.Ha.sysid_valid', MagicMock(return_value=True))
    def test_sysid_no_match(self, exit_mock):
        self.p.controldata = lambda: {'Database cluster state': 'in recovery', 'Database system identifier': '123'}
        self.ha.run_cycle()
        exit_mock.assert_called_once_with(1)

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_start_as_readonly(self):
        self.p.is_primary = false
        self.p.is_healthy = true
        self.ha.has_lock = true
        self.p.controldata = lambda: {'Database cluster state': 'in production', 'Database system identifier': SYSID}
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader because I had the session lock')

    @patch('patroni.psycopg.connect', psycopg_connect)
    def test_acquire_lock_as_primary(self):
        self.assertEqual(self.ha.run_cycle(), 'acquired session lock as a leader')

    def test_leader_race_stale_primary(self):
        with patch.object(Postgresql, 'get_primary_timeline', Mock(return_value=1)), \
                patch('patroni.ha.logger.warning') as mock_logger:
            self.assertEqual(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')
            self.assertEqual(mock_logger.call_args[0][0], 'My timeline %s is behind last known cluster timeline %s')

    def test_promoted_by_acquiring_lock(self):
        self.ha.is_healthiest_node = true
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_promotion_cancelled_after_pre_promote_failed(self):
        self.p.is_primary = false
        self.p._pre_promote = false
        self.ha._is_healthiest_node = true
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
        self.assertEqual(self.ha.run_cycle(), 'Promotion cancelled because the pre-promote script failed')
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

    def test_lost_leader_lock_during_promote(self):
        with patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=True)):
            self.ha._async_executor.schedule('promote')
            self.assertEqual(self.ha.run_cycle(), 'lost leader before promote')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_long_promote(self):
        self.ha.has_lock = true
        self.p.is_primary = false
        self.p.set_role('primary')
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    def test_demote_after_failing_to_obtain_lock(self):
        self.ha.acquire_lock = false
        self.assertEqual(self.ha.run_cycle(), 'demoted self after trying and failing to obtain lock')

    def test_follow_new_leader_after_failing_to_obtain_lock(self):
        self.ha.is_healthiest_node = true
        self.ha.acquire_lock = false
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'following new leader after trying and failing to obtain lock')

    def test_demote_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
        self.assertEqual(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')

    def test_follow_new_leader_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_promote_because_have_lock(self):
        self.ha.has_lock = true
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader because I had the session lock')

    def test_promote_without_watchdog(self):
        self.ha.has_lock = true
        self.p.is_primary = true
        with patch.object(Watchdog, 'activate', Mock(return_value=False)):
            self.assertEqual(self.ha.run_cycle(), 'Demoting self because watchdog could not be activated')
            self.p.is_primary = false
            self.assertEqual(self.ha.run_cycle(), 'Not promoting self because watchdog could not be activated')

    def test_leader_with_lock(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    def test_coordinator_leader_with_lock(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    @patch.object(Postgresql, '_wait_for_connection_close', Mock())
    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_demote_because_not_having_lock(self):
        with patch.object(Watchdog, 'is_running', PropertyMock(return_value=True)):
            self.assertEqual(self.ha.run_cycle(), 'demoting self because I do not have the lock and I was a leader')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_demote_because_update_lock_failed(self):
        self.ha.has_lock = true
        self.ha.update_lock = false
        self.assertEqual(self.ha.run_cycle(), 'demoted self because failed to update leader lock in DCS')
        with patch.object(Ha, '_get_node_to_follow', Mock(side_effect=DCSError('foo'))):
            self.assertEqual(self.ha.run_cycle(), 'demoted self because failed to update leader lock in DCS')
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'not promoting because failed to update leader lock in DCS')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_follow(self):
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), a secondary, and following a leader ()')
        self.ha.patroni.replicatefrom = "foo"
        self.p.config.check_recovery_conf = Mock(return_value=(True, False))
        self.ha.cluster.config.data.update({'slots': {'l': {'database': 'a', 'plugin': 'b'}}})
        self.ha.cluster.members[1].data['tags']['replicatefrom'] = 'postgresql0'
        self.ha.patroni.nofailover = True
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), a secondary, and following a leader ()')
        del self.ha.cluster.config.data['slots']
        self.ha.cluster.config.data.update({'postgresql': {'use_slots': False}})
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), a secondary, and following a leader ()')
        del self.ha.cluster.config.data['postgresql']['use_slots']

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_follow_in_pause(self):
        self.ha.is_paused = true
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: continue to run as primary without lock')
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: no action. I am (postgresql0)')

    @patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True))
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    def test_follow_triggers_rewind(self):
        self.p.is_primary = false
        self.ha._rewind.trigger_check_diverged_lsn()
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'running pg_rewind from leader')

    def test_no_dcs_connection_primary_demote(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.assertEqual(self.ha.run_cycle(), 'demoting self because DCS is not accessible and I was a leader')
        self.ha._async_executor.schedule('dummy')
        self.assertEqual(self.ha.run_cycle(), 'demoted self because DCS is not accessible and I was a leader')

    def test_check_failsafe_topology(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.ha.cluster = get_cluster_initialized_with_leader_and_failsafe()
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        self.ha.dcs._last_failsafe = self.ha.cluster.failsafe
        self.assertEqual(self.ha.run_cycle(), 'demoting self because DCS is not accessible and I was a leader')
        self.ha.state_handler.name = self.ha.cluster.leader.name
        self.assertFalse(self.ha.failsafe_is_active())
        self.assertEqual(self.ha.run_cycle(),
                         'continue to run as a leader because failsafe mode is enabled and all members are accessible')
        self.assertTrue(self.ha.failsafe_is_active())
        with patch.object(Postgresql, 'slots', Mock(side_effect=Exception)):
            self.ha.patroni.request = Mock(side_effect=Exception)
            self.assertEqual(self.ha.run_cycle(), 'demoting self because DCS is not accessible and I was a leader')
            self.assertFalse(self.ha.failsafe_is_active())
        self.ha.dcs._last_failsafe.clear()
        self.ha.dcs._last_failsafe[self.ha.cluster.leader.name] = self.ha.cluster.leader.member.api_url
        self.assertEqual(self.ha.run_cycle(),
                         'continue to run as a leader because failsafe mode is enabled and all members are accessible')

    def test_no_dcs_connection_primary_failsafe(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.ha.cluster = get_cluster_initialized_with_leader_and_failsafe()
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        self.ha.dcs._last_failsafe = self.ha.cluster.failsafe
        self.ha.state_handler.name = self.ha.cluster.leader.name
        self.assertEqual(self.ha.run_cycle(),
                         'continue to run as a leader because failsafe mode is enabled and all members are accessible')

    def test_readonly_dcs_primary_failsafe(self):
        self.ha.cluster = get_cluster_initialized_with_leader_and_failsafe()
        self.ha.dcs.update_leader = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.ha.dcs._last_failsafe = self.ha.cluster.failsafe
        self.ha.state_handler.name = self.ha.cluster.leader.name
        self.assertEqual(self.ha.run_cycle(),
                         'continue to run as a leader because failsafe mode is enabled and all members are accessible')

    def test_no_dcs_connection_replica_failsafe(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.ha.cluster = get_cluster_initialized_with_leader_and_failsafe()
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        self.ha.update_failsafe({'name': 'leader', 'api_url': 'http://127.0.0.1:8008/patroni',
                                 'conn_url': 'postgres://127.0.0.1:5432/postgres', 'slots': {'foo': 1000}})
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'DCS is not accessible')

    def test_no_dcs_connection_replica_failsafe_not_enabled_but_active(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.update_failsafe({'name': 'leader', 'api_url': 'http://127.0.0.1:8008/patroni',
                                 'conn_url': 'postgres://127.0.0.1:5432/postgres', 'slots': {'foo': 1000}})
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'DCS is not accessible')

    def test_update_failsafe(self):
        self.assertRaises(Exception, self.ha.update_failsafe, {})
        self.p.set_role('primary')
        self.assertEqual(self.ha.update_failsafe({}), 'Running as a leader')

    @patch('time.sleep', Mock())
    def test_bootstrap_from_another_member(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.bootstrap(), 'trying to bootstrap from replica \'other\'')

    def test_bootstrap_waiting_for_leader(self):
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.assertEqual(self.ha.bootstrap(), 'waiting for leader to bootstrap')

    def test_bootstrap_without_leader(self):
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.p.can_create_replica_without_replication_connection = MagicMock(return_value=True)
        self.assertEqual(self.ha.bootstrap(), 'trying to bootstrap (without leader)')

    def test_bootstrap_not_running_concurrently(self):
        self.ha.cluster = get_cluster_bootstrapping_without_leader()
        self.p.can_create_replica_without_replication_connection = MagicMock(return_value=True)
        self.assertEqual(self.ha.bootstrap(), 'waiting for leader to bootstrap')

    def test_bootstrap_initialize_lock_failed(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.assertEqual(self.ha.bootstrap(), 'failed to acquire initialize lock')

    @patch('patroni.psycopg.connect', psycopg_connect)
    @patch('patroni.postgresql.citus.connect', psycopg_connect)
    @patch('patroni.postgresql.citus.quote_ident', Mock())
    @patch.object(Postgresql, 'connection', Mock(return_value=None))
    def test_bootstrap_initialized_new_cluster(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.assertEqual(self.ha.bootstrap(), 'trying to bootstrap a new cluster')
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(), 'waiting for end of recovery after bootstrap')
        self.p.is_primary = true
        self.ha.is_synchronous_mode = true
        self.assertEqual(self.ha.run_cycle(), 'running post_bootstrap')
        self.assertEqual(self.ha.run_cycle(), 'initialized a new cluster')

    def test_bootstrap_release_initialize_key_on_failure(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.ha.bootstrap()
        self.p.is_running = false
        self.assertRaises(PatroniFatalException, self.ha.post_bootstrap)

    @patch('patroni.psycopg.connect', psycopg_connect)
    @patch('patroni.postgresql.citus.connect', psycopg_connect)
    @patch('patroni.postgresql.citus.quote_ident', Mock())
    @patch.object(Postgresql, 'connection', Mock(return_value=None))
    def test_bootstrap_release_initialize_key_on_watchdog_failure(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.ha.bootstrap()
        self.p.is_primary = true
        with patch.object(Watchdog, 'activate', Mock(return_value=False)), \
                patch('patroni.ha.logger.error') as mock_logger:
            self.assertEqual(self.ha.post_bootstrap(), 'running post_bootstrap')
            self.assertRaises(PatroniFatalException, self.ha.post_bootstrap)
            self.assertTrue(mock_logger.call_args[0][0].startswith('Cancelling bootstrap because'
                                                                   ' watchdog activation failed'))

    @patch('patroni.psycopg.connect', psycopg_connect)
    def test_reinitialize(self):
        self.assertIsNotNone(self.ha.reinitialize())

        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertIsNone(self.ha.reinitialize(True))
        self.ha._async_executor.schedule('reinitialize')
        self.assertIsNotNone(self.ha.reinitialize())

        self.ha.state_handler.name = self.ha.cluster.leader.name
        self.assertIsNotNone(self.ha.reinitialize())

    @patch('time.sleep', Mock())
    def test_restart(self):
        self.assertEqual(self.ha.restart({}), (True, 'restarted successfully'))
        self.p.restart = Mock(return_value=None)
        self.assertEqual(self.ha.restart({}), (False, 'postgres is still starting'))
        self.p.restart = false
        self.assertEqual(self.ha.restart({}), (False, 'restart failed'))
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha._async_executor.schedule('reinitialize')
        self.assertEqual(self.ha.restart({}), (False, 'reinitialize already in progress'))
        with patch.object(self.ha, "restart_matches", return_value=False):
            self.assertEqual(self.ha.restart({'foo': 'bar'}), (False, "restart conditions are not satisfied"))

    @patch('time.sleep', Mock())
    @patch.object(ConfigHandler, 'replace_pg_hba', Mock())
    @patch.object(ConfigHandler, 'replace_pg_ident', Mock())
    @patch.object(PostmasterProcess, 'start', Mock(return_value=MockPostmaster()))
    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_worker_restart(self):
        self.ha.has_lock = true
        self.ha.patroni.request = Mock()
        self.p.is_running = Mock(side_effect=[Mock(), False])
        self.assertEqual(self.ha.restart({}), (True, 'restarted successfully'))
        self.ha.patroni.request.assert_called()
        self.assertEqual(self.ha.patroni.request.call_args_list[0][0][3]['type'], 'before_demote')
        self.assertEqual(self.ha.patroni.request.call_args_list[1][0][3]['type'], 'after_promote')

    @patch('os.kill', Mock())
    def test_restart_in_progress(self):
        with patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=True)):
            self.ha._async_executor.schedule('restart')
            self.assertTrue(self.ha.restart_scheduled())
            self.assertEqual(self.ha.run_cycle(), 'restart in progress')

            self.ha.cluster = get_cluster_initialized_with_leader()
            self.assertEqual(self.ha.run_cycle(), 'restart in progress')

            self.ha.has_lock = true
            self.assertEqual(self.ha.run_cycle(), 'updated leader lock during restart')

            self.ha.update_lock = false
            self.p.set_role('primary')
            with patch('patroni.async_executor.CriticalTask.cancel', Mock(return_value=False)), \
                    patch('patroni.async_executor.CriticalTask.result',
                          PropertyMock(return_value=PostmasterProcess(os.getpid())), create=True), \
                    patch('patroni.postgresql.Postgresql.terminate_starting_postmaster') as mock_terminate:
                self.assertEqual(self.ha.run_cycle(), 'lost leader lock during restart')
                mock_terminate.assert_called()

            self.ha.is_paused = true
            self.assertEqual(self.ha.run_cycle(), 'PAUSE: restart in progress')

    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_manual_failover_from_leader(self):
        self.ha.has_lock = true  # I am the leader

        # to me
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', self.p.name, None))
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            mock_warning.assert_called_with('%s: I am already the leader, no need to %s', 'manual failover', 'failover')

        # to a non-existent candidate
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', 'blabla', None))
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            mock_warning.assert_called_with(
                '%s: no healthy members found, %s is not possible', 'manual failover', 'failover')

        # to an existent candidate
        self.ha.fetch_node_status = get_node_status()
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', 'b', None))
        self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
        self.assertEqual(self.ha.run_cycle(), 'manual failover: demoting myself')

        # to a candidate on an older timeline
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(timeline=1)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0],
                             ('Timeline %s of member %s is behind the cluster timeline %s', 1, 'b', 2))

        # to a lagging candidate
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(wal_position=1)
            self.ha.cluster.config.data.update({'maximum_lag_on_failover': 5})
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0],
                             ('Member %s exceeds maximum replication lag', 'b'))
            self.ha.cluster.members.pop()

    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_manual_switchover_from_leader(self):
        self.ha.has_lock = true  # I am the leader

        self.ha.fetch_node_status = get_node_status()

        # different leader specified in failover key, no candidate
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', '', None))
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            mock_warning.assert_called_with(
                '%s: leader name does not match: %s != %s', 'switchover', 'blabla', 'postgresql0')

        # no candidate
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, '', None))
        self.assertEqual(self.ha.run_cycle(), 'switchover: demoting myself')

        self.ha._rewind.rewind_or_reinitialize_needed_and_possible = true
        self.assertEqual(self.ha.run_cycle(), 'switchover: demoting myself')

        # other members with failover_limitation_s
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(nofailover=True)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0], ('Member %s is %s', 'leader', 'not allowed to promote'))
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(watchdog_failed=True)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0], ('Member %s is %s', 'leader', 'not watchdog capable'))
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(timeline=1)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0],
                             ('Timeline %s of member %s is behind the cluster timeline %s', 1, 'leader', 2))
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(wal_position=1)
            self.ha.cluster.config.data.update({'maximum_lag_on_failover': 5})
            self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertEqual(mock_info.call_args_list[0][0], ('Member %s exceeds maximum replication lag', 'leader'))

    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_scheduled_switchover_from_leader(self):
        self.ha.has_lock = true  # I am the leader

        self.ha.fetch_node_status = get_node_status()

        # switchover scheduled time must include timezone
        with patch('patroni.ha.logger.warning') as mock_warning:
            scheduled = datetime.datetime.now()
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'blabla', scheduled))
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')
            self.assertIn('Incorrect value of scheduled_at: %s', mock_warning.call_args_list[0][0])

        # scheduled now
        scheduled = datetime.datetime.utcnow().replace(tzinfo=tzutc)
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'b', scheduled))
        self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
        self.assertEqual('switchover: demoting myself', self.ha.run_cycle())

        # scheduled in the future
        with patch('patroni.ha.logger.info') as mock_info:
            scheduled = scheduled + datetime.timedelta(seconds=30)
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'blabla', scheduled))
            self.assertEqual('no action. I am (postgresql0), the leader with the lock', self.ha.run_cycle())
            self.assertIn('Awaiting %s at %s (in %.0f seconds)', mock_info.call_args_list[0][0])

        # stale value
        with patch('patroni.ha.logger.warning') as mock_warning:
            scheduled = scheduled + datetime.timedelta(seconds=-600)
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'b', scheduled))
            self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
            self.assertEqual('no action. I am (postgresql0), the leader with the lock', self.ha.run_cycle())
            self.assertIn('Found a stale %s value, cleaning up: %s', mock_warning.call_args_list[0][0])

    def test_manual_switchover_from_leader_in_pause(self):
        self.ha.has_lock = true  # I am the leader
        self.ha.is_paused = true

        # no candidate
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, '', None))
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.assertEqual('PAUSE: no action. I am (postgresql0), the leader with the lock', self.ha.run_cycle())
            mock_warning.assert_called_with(
                '%s is possible only to a specific candidate in a paused state', 'Switchover')

    def test_manual_failover_from_leader_in_pause(self):
        self.ha.has_lock = true
        self.ha.fetch_node_status = get_node_status()
        self.ha.is_paused = true

        # failover from me, candidate is healthy
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, None, 'b', None))
        self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
        self.assertEqual('PAUSE: manual failover: demoting myself', self.ha.run_cycle())
        self.ha.cluster.members.pop()

    def test_manual_failover_from_leader_in_synchronous_mode(self):
        self.ha.is_synchronous_mode = true
        self.ha.process_sync_replication = Mock()
        self.ha.fetch_node_status = get_node_status()

        # I am the leader
        self.p.is_primary = true
        self.ha.has_lock = true

        # the candidate is not in sync members but we allow failover to an async candidate
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, None, 'b', None), sync=(self.p.name, 'a'))
        self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
        self.assertEqual('manual failover: demoting myself', self.ha.run_cycle())
        self.ha.cluster.members.pop()

    def test_manual_switchover_from_leader_in_synchronous_mode(self):
        self.ha.is_synchronous_mode = true
        self.ha.process_sync_replication = Mock()

        # I am the leader
        self.p.is_primary = true
        self.ha.has_lock = true

        # candidate specified is not in sync members
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'a', None),
                                                                  sync=(self.p.name, 'blabla'))
            self.assertEqual('no action. I am (postgresql0), the leader with the lock', self.ha.run_cycle())
            self.assertEqual(mock_warning.call_args_list[0][0],
                             ('%s candidate=%s does not match with sync_standbys=%s', 'Switchover', 'a', 'blabla'))

        # the candidate is in sync members and is healthy
        self.ha.fetch_node_status = get_node_status(wal_position=305419896)
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, 'a', None),
                                                              sync=(self.p.name, 'a'))
        self.ha.cluster.members.append(Member(0, 'a', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
        self.assertEqual('switchover: demoting myself', self.ha.run_cycle())

        # the candidate is in sync members but is not healthy
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(nofailover=true)
            self.assertEqual('no action. I am (postgresql0), the leader with the lock', self.ha.run_cycle())
            self.assertEqual(mock_info.call_args_list[0][0], ('Member %s is %s', 'a', 'not allowed to promote'))

    def test_manual_failover_process_no_leader(self):
        self.p.is_primary = false
        self.p.set_role('replica')

        # failover to another member, fetch_node_status for candidate fails
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'leader', None))
            self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
            self.assertEqual(mock_warning.call_args_list[1][0],
                             ('%s: member %s is %s', 'manual failover', 'leader', 'not reachable'))

        # failover to another member, candidate is accessible, in_recovery
        self.p.set_role('replica')
        self.ha.fetch_node_status = get_node_status()
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # set nofailover flag to True for all members of the cluster
        # this should elect the current member, as we are not going to call the API for it.
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None))
        self.ha.fetch_node_status = get_node_status(nofailover=True)
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

        # failover to me but I am set to nofailover. In no case I should be elected as a leader
        self.p.set_role('replica')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'postgresql0', None))
        self.ha.patroni.nofailover = True
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because I am not allowed to promote')

        self.ha.patroni.nofailover = False

        # failover to another member that is on an older timeline (only failover_limitation() is checked)
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'b', None))
            self.ha.cluster.members.append(Member(0, 'b', 28, {'api_url': 'http://127.0.0.1:8011/patroni'}))
            self.ha.fetch_node_status = get_node_status(timeline=1)
            self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')
            mock_info.assert_called_with('%s: to %s, i am %s', 'manual failover', 'b', 'postgresql0')

        # failover to another member lagging behind the cluster_lsn (only failover_limitation() is checked)
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.cluster.config.data.update({'maximum_lag_on_failover': 5})
            self.ha.fetch_node_status = get_node_status(wal_position=1)
            self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')
            mock_info.assert_called_with('%s: to %s, i am %s', 'manual failover', 'b', 'postgresql0')

    def test_manual_switchover_process_no_leader(self):
        self.p.is_primary = false
        self.p.set_role('replica')

        # I was the leader, other members are healthy
        self.ha.fetch_node_status = get_node_status()
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, self.p.name, '', None))
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # I was the leader, I am the only healthy member
        with patch('patroni.ha.logger.info') as mock_info:
            self.ha.fetch_node_status = get_node_status(reachable=False)  # inaccessible, in_recovery
            self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
            self.assertEqual(mock_info.call_args_list[0][0], ('Member %s is %s', 'leader', 'not reachable'))
            self.assertEqual(mock_info.call_args_list[1][0], ('Member %s is %s', 'other', 'not reachable'))

    def test_manual_failover_process_no_leader_in_synchronous_mode(self):
        self.ha.is_synchronous_mode = true
        self.p.is_primary = false
        self.ha.fetch_node_status = get_node_status(nofailover=True)  # other nodes are not healthy

        # manual failover when our name (postgresql0) isn't in the /sync key and the candidate node is not available
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None),
                                                                 sync=('leader1', 'blabla'))
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # manual failover when the candidate node isn't available but our name is in the /sync key
        # while other sync node is nofailover
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None),
                                                                     sync=('leader1', 'postgresql0'))
            self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(), CaseInsensitiveSet()))
            self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
            self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
            self.assertEqual(mock_warning.call_args_list[0][0],
                             ('%s: member %s is %s', 'manual failover', 'other', 'not allowed to promote'))

        # manual failover to our node (postgresql0),
        # which name is not in sync nodes list (some sync nodes are available)
        self.p.set_role('replica')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'postgresql0', None),
                                                                 sync=('leader1', 'other'))
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(['leader1']),
                                                               CaseInsensitiveSet(['leader1'])))
        self.assertEqual(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_manual_switchover_process_no_leader_in_synchronous_mode(self):
        self.ha.is_synchronous_mode = true
        self.p.is_primary = false

        # to a specific node, which name doesn't match our name (postgresql0)
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', 'other', None))
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # to our node (postgresql0), which name is not in sync nodes list
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', 'postgresql0', None),
                                                                 sync=('leader1', 'blabla'))
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # without candidate, our name (postgresql0) is not in the sync nodes list
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', '', None),
                                                                 sync=('leader', 'blabla'))
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

        # switchover from a specific leader, but the only sync node (us, postgresql0) has nofailover tag
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', '', None),
                                                                 sync=('postgresql0'))
        self.ha.patroni.nofailover = True
        self.assertEqual(self.ha.run_cycle(), 'following a different leader because I am not allowed to promote')

    def test_manual_failover_process_no_leader_in_pause(self):
        self.ha.is_paused = true

        # I am running as primary, cluster is unlocked, the candidate is allowed to promote
        # but we are in pause
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None))
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: continue to run as primary without lock')

    def test_manual_switchover_process_no_leader_in_pause(self):
        self.ha.is_paused = true

        # I am running as primary, cluster is unlocked, no candidate specified
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', '', None))
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: continue to run as primary without lock')

        # the candidate is not running
        with patch('patroni.ha.logger.warning') as mock_warning:
            self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', 'blabla', None))
            self.assertEqual('PAUSE: acquired session lock as a leader', self.ha.run_cycle())
            self.assertEqual(
                mock_warning.call_args_list[0][0],
                ('%s: removing failover key because failover candidate is not running', 'switchover'))

        # switchover to me, I am not leader
        self.p.is_primary = false
        self.p.set_role('replica')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', self.p.name, None))
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: promoted self to leader by acquiring session lock')

    def test_is_healthiest_node(self):
        self.ha.is_failsafe_mode = true
        self.ha.state_handler.is_primary = false
        self.ha.patroni.nofailover = False
        self.ha.fetch_node_status = get_node_status()
        self.ha.dcs._last_failsafe = {'foo': ''}
        self.assertFalse(self.ha.is_healthiest_node())
        self.ha.dcs._last_failsafe = {'postgresql0': ''}
        self.assertTrue(self.ha.is_healthiest_node())
        self.ha.dcs._last_failsafe = None
        with patch.object(Watchdog, 'is_healthy', PropertyMock(return_value=False)):
            self.assertFalse(self.ha.is_healthiest_node())
        self.ha.is_paused = true
        self.assertFalse(self.ha.is_healthiest_node())

    def test__is_healthiest_node(self):
        self.p.is_primary = false
        self.ha.cluster = get_cluster_initialized_without_leader(sync=('postgresql1', self.p.name))
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status(in_recovery=False)  # accessible, not in_recovery
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status(failover_priority=2)  # accessible, in_recovery, higher priority
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        # if there is a higher-priority node but it has a lower WAL position then this node should race
        self.ha.fetch_node_status = get_node_status(failover_priority=6, wal_position=9)
        self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status(wal_position=11)  # accessible, in_recovery, wal position ahead
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        # in synchronous_mode consider itself healthy if the former leader is accessible in read-only and ahead of us
        with patch.object(Ha, 'is_synchronous_mode', Mock(return_value=True)):
            self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.cluster.config.data.update({'maximum_lag_on_failover': 5})
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        with patch('patroni.postgresql.Postgresql.last_operation', return_value=1):
            self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        with patch('patroni.postgresql.Postgresql.replica_cached_timeline', return_value=None):
            self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        with patch('patroni.postgresql.Postgresql.replica_cached_timeline', return_value=1):
            self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.patroni.nofailover = True
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.patroni.nofailover = None
        self.ha.patroni.failover_priority = 0
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))

    def test_fetch_node_status(self):
        member = Member(0, 'test', 1, {'api_url': 'http://127.0.0.1:8011/patroni'})
        self.ha.fetch_node_status(member)
        member = Member(0, 'test', 1, {'api_url': 'http://localhost:8011/patroni'})
        self.ha.patroni.request = Mock()
        self.ha.patroni.request.return_value.data = b'{"wal":{"location":1},"role":"primary"}'
        ret = self.ha.fetch_node_status(member)
        self.assertFalse(ret.in_recovery)

    @patch.object(Rewind, 'pg_rewind', true)
    @patch.object(Rewind, 'check_leader_is_not_in_recovery', true)
    @patch('os.listdir', Mock(return_value=[]))
    @patch('patroni.postgresql.rewind.fsync_dir', Mock())
    def test_post_recover(self):
        self.p.is_running = false
        self.ha.has_lock = true
        self.p.set_role('primary')
        self.assertEqual(self.ha.post_recover(), 'removed leader key after trying and failing to start postgres')
        self.ha.has_lock = false
        self.assertEqual(self.ha.post_recover(), 'failed to start postgres')
        leader = Leader(0, 0, Member(0, 'l', 2, {"version": "1.6", "conn_url": "postgres://a", "role": "primary"}))
        self.ha._rewind.execute(leader)
        self.p.is_running = true
        self.assertIsNone(self.ha.post_recover())

    def test_schedule_future_restart(self):
        self.ha.patroni.scheduled_restart = {}
        # do the restart 2 times. The first one should succeed, the second one should fail
        self.assertTrue(self.ha.schedule_future_restart({'schedule': future_restart_time}))
        self.assertFalse(self.ha.schedule_future_restart({'schedule': future_restart_time}))

    def test_delete_future_restarts(self):
        self.ha.delete_future_restart()

    def test_evaluate_scheduled_restart(self):
        self.p.postmaster_start_time = Mock(return_value=str(postmaster_start_time))
        # restart already in progress
        with patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=True)):
            self.assertIsNone(self.ha.evaluate_scheduled_restart())
        # restart while the postmaster has been already restarted, fails
        with patch.object(self.ha,
                          'future_restart_scheduled',
                          Mock(return_value={'postmaster_start_time':
                                             str(postmaster_start_time - datetime.timedelta(days=1)),
                                             'schedule': str(future_restart_time)})):
            self.assertIsNone(self.ha.evaluate_scheduled_restart())
        with patch.object(self.ha,
                          'future_restart_scheduled',
                          Mock(return_value={'postmaster_start_time': str(postmaster_start_time),
                                             'schedule': str(future_restart_time)})):
            with patch.object(self.ha,
                              'should_run_scheduled_action', Mock(return_value=True)):
                # restart in the future, ok
                self.assertIsNotNone(self.ha.evaluate_scheduled_restart())
                with patch.object(self.ha, 'restart', Mock(return_value=(False, "Test"))):
                    # restart in the future, bit the actual restart failed
                    self.assertIsNone(self.ha.evaluate_scheduled_restart())

    def test_scheduled_restart(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        with patch.object(self.ha, "evaluate_scheduled_restart", Mock(return_value="restart scheduled")):
            self.assertEqual(self.ha.run_cycle(), "restart scheduled")

    def test_restart_matches(self):
        self.p._role = 'replica'
        self.p._connection.server_version = 90500
        self.p._pending_restart = True
        self.assertFalse(self.ha.restart_matches("primary", "9.5.0", True))
        self.assertFalse(self.ha.restart_matches("replica", "9.4.3", True))
        self.p._pending_restart = False
        self.assertFalse(self.ha.restart_matches("replica", "9.5.2", True))
        self.assertTrue(self.ha.restart_matches("replica", "9.5.2", False))

    def test_process_healthy_cluster_in_pause(self):
        self.p.is_primary = false
        self.ha.is_paused = true
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: removed leader lock because postgres is not running as primary')
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', self.p.name, None))
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: waiting to become primary after promote...')

    @patch('patroni.postgresql.mtime', Mock(return_value=1588316884))
    @patch('builtins.open', mock_open(read_data='1\t0/40159C0\tno recovery target specified\n'))
    def test_process_healthy_standby_cluster_as_standby_leader(self):
        self.p.is_primary = false
        self.p.name = 'leader'
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.p.config.check_recovery_conf = Mock(return_value=(False, False))
        self.ha._leader_timeline = 1
        self.assertEqual(self.ha.run_cycle(), 'promoted self to a standby leader because i had the session lock')
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (leader), the standby leader with the lock')
        self.p.set_role('replica')
        self.p.config.check_recovery_conf = Mock(return_value=(True, False))
        self.assertEqual(self.ha.run_cycle(), 'promoted self to a standby leader because i had the session lock')

    def test_process_healthy_standby_cluster_as_cascade_replica(self):
        self.p.is_primary = false
        self.p.name = 'replica'
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.assertEqual(self.ha.run_cycle(),
                         'no action. I am (replica), a secondary, and following a standby leader (leader)')
        with patch.object(Leader, 'conn_url', PropertyMock(return_value='')):
            self.assertEqual(self.ha.run_cycle(), 'continue following the old known standby leader')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=True))
    def test_process_unhealthy_standby_cluster_as_standby_leader(self):
        self.p.is_primary = false
        self.p.name = 'leader'
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.ha.sysid_valid = true
        self.p._sysid = True
        self.assertEqual(self.ha.run_cycle(), 'promoted self to a standby leader by acquiring session lock')

    @patch.object(Rewind, 'rewind_or_reinitialize_needed_and_possible', Mock(return_value=True))
    @patch.object(Rewind, 'can_rewind', PropertyMock(return_value=True))
    def test_process_unhealthy_standby_cluster_as_cascade_replica(self):
        self.p.is_primary = false
        self.p.name = 'replica'
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.assertTrue(self.ha.run_cycle().startswith('running pg_rewind from remote_member:'))

    def test_recover_unhealthy_leader_in_standby_cluster(self):
        self.p.is_primary = false
        self.p.name = 'leader'
        self.p.is_running = false
        self.p.follow = false
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.assertEqual(self.ha.run_cycle(), 'starting as a standby leader because i had the session lock')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=True))
    def test_recover_unhealthy_unlocked_standby_cluster(self):
        self.p.is_primary = false
        self.p.name = 'leader'
        self.p.is_running = false
        self.p.follow = false
        self.ha.cluster = get_standby_cluster_initialized_with_only_leader()
        self.ha.has_lock = false
        self.assertEqual(self.ha.run_cycle(), 'trying to follow a remote member because standby cluster is unhealthy')

    def test_failed_to_update_lock_in_pause(self):
        self.ha.update_lock = false
        self.ha.is_paused = true
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEqual(self.ha.run_cycle(),
                         'PAUSE: continue to run as primary after failing to update leader lock in DCS')

    def test_postgres_unhealthy_in_pause(self):
        self.ha.is_paused = true
        self.p.is_healthy = false
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: postgres is not running')
        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: removed leader lock because postgres is not running')

    def test_no_etcd_connection_in_pause(self):
        self.ha.is_paused = true
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: DCS is not accessible')

    @patch('patroni.ha.Ha.update_lock', return_value=True)
    @patch('patroni.ha.Ha.demote')
    def test_starting_timeout(self, demote, update_lock):
        def check_calls(seq):
            for mock, called in seq:
                if called:
                    mock.assert_called_once()
                else:
                    mock.assert_not_called()
                mock.reset_mock()
        self.ha.has_lock = true
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.p.check_for_startup = true
        self.p.time_in_state = lambda: 30
        self.assertEqual(self.ha.run_cycle(), 'PostgreSQL is still starting up, 270 seconds until timeout')
        check_calls([(update_lock, True), (demote, False)])

        self.p.time_in_state = lambda: 350
        self.ha.fetch_node_status = get_node_status(reachable=False)  # inaccessible, in_recovery
        self.assertEqual(self.ha.run_cycle(),
                         'primary start has timed out, but continuing to wait because failover is not possible')
        check_calls([(update_lock, True), (demote, False)])

        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEqual(self.ha.run_cycle(), 'stopped PostgreSQL because of startup timeout')
        check_calls([(update_lock, True), (demote, True)])

        update_lock.return_value = False
        self.assertEqual(self.ha.run_cycle(), 'stopped PostgreSQL while starting up because leader key was lost')
        check_calls([(update_lock, True), (demote, True)])

        self.ha.has_lock = false
        self.p.is_primary = false
        self.assertEqual(self.ha.run_cycle(),
                         'no action. I am (postgresql0), a secondary, and following a leader (leader)')
        check_calls([(update_lock, False), (demote, False)])

    def test_manual_failover_while_starting(self):
        self.ha.has_lock = true
        self.p.check_for_startup = true
        f = Failover(0, self.p.name, '', None)
        self.ha.cluster = get_cluster_initialized_with_leader(f)
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEqual(self.ha.run_cycle(), 'switchover: demoting myself')

    @patch('patroni.ha.Ha.demote')
    def test_failover_immediately_on_zero_primary_start_timeout(self, demote):
        self.p.is_running = false
        self.ha.cluster = get_cluster_initialized_with_leader(sync=(self.p.name, 'other'))
        self.ha.cluster.config.data.update({'synchronous_mode': True, 'primary_start_timeout': 0})
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        self.ha.has_lock = true
        self.ha.update_lock = true
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEqual(self.ha.run_cycle(), 'stopped PostgreSQL to fail over after a crash')
        demote.assert_called_once()

    def test_primary_stop_timeout(self):
        self.assertEqual(self.ha.primary_stop_timeout(), None)
        self.ha.cluster.config.data.update({'primary_stop_timeout': 30})
        self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
        with patch.object(Ha, 'is_synchronous_mode', Mock(return_value=True)):
            self.assertEqual(self.ha.primary_stop_timeout(), 30)
        with patch.object(Ha, 'is_synchronous_mode', Mock(return_value=False)):
            self.assertEqual(self.ha.primary_stop_timeout(), None)
            self.ha.cluster.config.data['primary_stop_timeout'] = None
            self.ha.global_config = self.ha.patroni.config.get_global_config(self.ha.cluster)
            self.assertEqual(self.ha.primary_stop_timeout(), None)

    @patch('patroni.postgresql.Postgresql.follow')
    def test_demote_immediate(self, follow):
        self.ha.has_lock = true
        self.e.get_cluster = Mock(return_value=get_cluster_initialized_without_leader())
        self.ha.demote('immediate')
        follow.assert_called_once_with(None)

    def test_process_sync_replication(self):
        self.ha.has_lock = true
        mock_set_sync = self.p.sync_handler.set_synchronous_standby_names = Mock()
        self.p.name = 'leader'

        # Test sync key removed when sync mode disabled
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        with patch.object(self.ha.dcs, 'delete_sync_state') as mock_delete_sync:
            self.ha.run_cycle()
            mock_delete_sync.assert_called_once()
            mock_set_sync.assert_called_once_with(CaseInsensitiveSet())

        mock_set_sync.reset_mock()
        # Test sync key not touched when not there
        self.ha.cluster = get_cluster_initialized_with_leader()
        with patch.object(self.ha.dcs, 'delete_sync_state') as mock_delete_sync:
            self.ha.run_cycle()
            mock_delete_sync.assert_not_called()
            mock_set_sync.assert_called_once_with(CaseInsensitiveSet())

        mock_set_sync.reset_mock()

        self.ha.is_synchronous_mode = true

        # Test sync standby not touched when picking the same node
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(['other']),
                                                               CaseInsensitiveSet(['other'])))
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.ha.run_cycle()
        mock_set_sync.assert_not_called()

        mock_set_sync.reset_mock()

        # Test sync standby is replaced when switching standbys
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(['other2']), CaseInsensitiveSet()))
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.ha.run_cycle()
        mock_set_sync.assert_called_once_with(CaseInsensitiveSet(['other2']))

        # Test sync standby is replaced when new standby is joined
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(['other2', 'other3']),
                                                               CaseInsensitiveSet(['other2'])))
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.ha.run_cycle()
        self.assertEqual(mock_set_sync.call_args_list[0][0], (CaseInsensitiveSet(['other2']),))
        self.assertEqual(mock_set_sync.call_args_list[1][0], (CaseInsensitiveSet(['other2', 'other3']),))

        mock_set_sync.reset_mock()
        # Test sync standby is not disabled when updating dcs fails
        self.ha.dcs.write_sync_state = Mock(return_value=None)
        self.ha.run_cycle()
        mock_set_sync.assert_not_called()

        mock_set_sync.reset_mock()
        # Test changing sync standby
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.ha.dcs.get_cluster = Mock(return_value=get_cluster_initialized_with_leader(sync=('leader', 'other')))
        # self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(['other2']),
                                                               CaseInsensitiveSet(['other2'])))
        self.ha.run_cycle()
        self.assertEqual(self.ha.dcs.write_sync_state.call_count, 2)

        # Test updating sync standby key failed due to race
        self.ha.dcs.write_sync_state = Mock(side_effect=[SyncState.empty(), None])
        self.ha.run_cycle()
        self.assertEqual(self.ha.dcs.write_sync_state.call_count, 2)

        # Test updating sync standby key failed due to DCS being not accessible
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.ha.dcs.get_cluster = Mock(side_effect=DCSError('foo'))
        self.ha.run_cycle()

        # Test changing sync standby failed due to race
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.ha.dcs.get_cluster = Mock(return_value=get_cluster_initialized_with_leader(sync=('somebodyelse', None)))
        self.ha.run_cycle()
        self.assertEqual(self.ha.dcs.write_sync_state.call_count, 2)

        # Test sync set to '*' when synchronous_mode_strict is enabled
        mock_set_sync.reset_mock()
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(), CaseInsensitiveSet()))
        with patch('patroni.config.GlobalConfig.is_synchronous_mode_strict', PropertyMock(return_value=True)):
            self.ha.run_cycle()
        mock_set_sync.assert_called_once_with(CaseInsensitiveSet('*'))

    def test_sync_replication_become_primary(self):
        self.ha.is_synchronous_mode = true

        mock_set_sync = self.p.sync_handler.set_synchronous_standby_names = Mock()
        self.p.is_primary = false
        self.p.set_role('replica')
        self.ha.has_lock = true
        mock_write_sync = self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('other', None))

        # When we just became primary nobody is sync
        self.assertEqual(self.ha.enforce_primary_role('msg', 'promote msg'), 'promote msg')
        mock_set_sync.assert_called_once_with(CaseInsensitiveSet())
        mock_write_sync.assert_called_once_with('leader', None, version=0)

        mock_set_sync.reset_mock()

        # When we just became primary nobody is sync
        self.p.set_role('replica')
        mock_write_sync.return_value = False
        self.assertTrue(self.ha.enforce_primary_role('msg', 'promote msg') != 'promote msg')
        mock_set_sync.assert_not_called()

    def test_unhealthy_sync_mode(self):
        self.ha.is_synchronous_mode = true

        self.p.is_primary = false
        self.p.set_role('replica')
        self.p.name = 'other'
        self.ha.cluster = get_cluster_initialized_without_leader(sync=('leader', 'other2'))
        mock_write_sync = self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        mock_acquire = self.ha.acquire_lock = Mock(return_value=True)
        mock_follow = self.p.follow = Mock()
        mock_promote = self.p.promote = Mock()

        # If we don't match the sync replica we are not allowed to acquire lock
        self.ha.run_cycle()
        mock_acquire.assert_not_called()
        mock_follow.assert_called_once()
        self.assertEqual(mock_follow.call_args[0][0], None)
        mock_write_sync.assert_not_called()

        mock_follow.reset_mock()
        # If we do match we will try to promote
        self.ha._is_healthiest_node = true

        self.ha.cluster = get_cluster_initialized_without_leader(sync=('leader', 'other'))
        self.ha.run_cycle()
        mock_acquire.assert_called_once()
        mock_follow.assert_not_called()
        mock_promote.assert_called_once()
        mock_write_sync.assert_called_once_with('other', None, version=0)

    def test_disable_sync_when_restarting(self):
        self.ha.is_synchronous_mode = true

        self.p.name = 'other'
        self.p.is_primary = false
        self.p.set_role('replica')
        mock_restart = self.p.restart = Mock(return_value=True)
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.ha.touch_member = Mock(return_value=True)
        self.ha.dcs.get_cluster = Mock(side_effect=[
            get_cluster_initialized_with_leader(sync=('leader', syncstandby))
            for syncstandby in ['other', None]])

        with patch('time.sleep') as mock_sleep:
            self.ha.restart({})
            mock_restart.assert_called_once()
            mock_sleep.assert_called()

        # Restart is still called when DCS connection fails
        mock_restart.reset_mock()
        self.ha.dcs.get_cluster = Mock(side_effect=DCSError("foo"))
        self.ha.restart({})

        mock_restart.assert_called_once()

        # We don't try to fetch the cluster state when touch_member fails
        mock_restart.reset_mock()
        self.ha.dcs.get_cluster.reset_mock()
        self.ha.touch_member = Mock(return_value=False)

        self.ha.restart({})

        mock_restart.assert_called_once()
        self.ha.dcs.get_cluster.assert_not_called()

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_enable_synchronous_mode(self):
        self.ha.is_synchronous_mode = true
        self.ha.has_lock = true
        self.p.name = 'leader'
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet(), CaseInsensitiveSet()))
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        with patch('patroni.ha.logger.info') as mock_logger:
            self.ha.run_cycle()
            self.assertEqual(mock_logger.call_args_list[0][0][0], 'Enabled synchronous replication')
        self.ha.dcs.write_sync_state = Mock(return_value=None)
        with patch('patroni.ha.logger.warning') as mock_logger:
            self.ha.run_cycle()
            self.assertEqual(mock_logger.call_args[0][0], 'Updating sync state failed')

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_inconsistent_synchronous_state(self):
        self.ha.is_synchronous_mode = true
        self.ha.has_lock = true
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_without_leader(sync=('leader', 'a'))
        self.p.sync_handler.current_state = Mock(return_value=(CaseInsensitiveSet('a'), CaseInsensitiveSet()))
        self.ha.dcs.write_sync_state = Mock(return_value=SyncState.empty())
        mock_set_sync = self.p.sync_handler.set_synchronous_standby_names = Mock()
        with patch('patroni.ha.logger.warning') as mock_logger:
            self.ha.run_cycle()
            mock_set_sync.assert_called_once()
            self.assertTrue(mock_logger.call_args_list[0][0][0].startswith('Inconsistent state between '))
        self.ha.dcs.write_sync_state = Mock(return_value=None)
        with patch('patroni.ha.logger.warning') as mock_logger:
            self.ha.run_cycle()
            self.assertEqual(mock_logger.call_args[0][0], 'Updating sync state failed')

    def test_effective_tags(self):
        self.ha._disable_sync = True
        self.assertEqual(self.ha.get_effective_tags(), {'foo': 'bar', 'nosync': True})
        self.ha._disable_sync = False
        self.assertEqual(self.ha.get_effective_tags(), {'foo': 'bar'})

    @patch('patroni.postgresql.mtime', Mock(return_value=1588316884))
    @patch('builtins.open', Mock(side_effect=Exception))
    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_restore_cluster_config(self):
        self.ha.cluster.config.data.clear()
        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    def test_watch(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.watch(0)

    def test_wakup(self):
        self.ha.wakeup()

    def test_shutdown(self):
        self.p.is_running = false
        self.ha.is_leader = true

        def stop(*args, **kwargs):
            kwargs['on_shutdown'](123, 120)

        self.p.stop = stop
        self.ha.shutdown()

        self.ha.is_failover_possible = true
        self.ha.shutdown()

    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_shutdown_citus_worker(self):
        self.ha.is_leader = true
        self.p.is_running = Mock(side_effect=[Mock(), False])
        self.ha.patroni.request = Mock()
        self.ha.shutdown()
        self.ha.patroni.request.assert_called()
        self.assertEqual(self.ha.patroni.request.call_args[0][2], 'citus')
        self.assertEqual(self.ha.patroni.request.call_args[0][3]['type'], 'before_demote')

    @patch('time.sleep', Mock())
    def test_leader_with_not_accessible_data_directory(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.has_lock = true
        self.p.data_directory_empty = Mock(side_effect=OSError(5, "Input/output error: '{}'".format(self.p.data_dir)))
        self.assertEqual(self.ha.run_cycle(),
                         'released leader key voluntarily as data dir not accessible and currently leader')
        self.assertEqual(self.p.role, 'uninitialized')

        # as has_lock is mocked out, we need to fake the leader key release
        self.ha.has_lock = false
        # will not say bootstrap because data directory is not accessible
        self.assertEqual(self.ha.run_cycle(),
                         "data directory is not accessible: [Errno 5] Input/output error: '{}'".format(self.p.data_dir))

    @patch('patroni.postgresql.mtime', Mock(return_value=1588316884))
    @patch('builtins.open', mock_open(read_data=('1\t0/40159C0\tno recovery target specified\n\n'
                                                 '2\t1/40159C0\tno recovery target specified\n')))
    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_update_cluster_history(self):
        self.ha.has_lock = true
        for tl in (1, 3):
            self.p.get_primary_timeline = Mock(return_value=tl)
            self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    @patch('sys.exit', return_value=1)
    def test_abort_join(self, exit_mock):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.p.is_primary = false
        self.ha.run_cycle()
        exit_mock.assert_called_once_with(1)

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_after_pause(self):
        self.ha.has_lock = true
        self.ha.is_paused = true
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: no action. I am (postgresql0), the leader with the lock')
        self.ha.is_paused = false
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (postgresql0), the leader with the lock')

    @patch('patroni.psycopg.connect', psycopg_connect)
    def test_permanent_logical_slots_after_promote(self):
        self.p._major_version = 110000
        config = ClusterConfig(1, {'slots': {'l': {'database': 'postgres', 'plugin': 'test_decoding'}}}, 1)
        self.p.name = 'other'
        self.ha.cluster = get_cluster_initialized_without_leader(cluster_config=config)
        self.assertEqual(self.ha.run_cycle(), 'acquired session lock as a leader')
        self.ha.cluster = get_cluster_initialized_without_leader(leader=True, cluster_config=config)
        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'no action. I am (other), the leader with the lock')

    @patch.object(Cluster, 'has_member', true)
    def test_run_cycle(self):
        self.ha.dcs.touch_member = Mock(side_effect=DCSError('foo'))
        self.assertEqual(self.ha.run_cycle(), 'Unexpected exception raised, please report it as a BUG')
        self.ha.dcs.touch_member = Mock(side_effect=PatroniFatalException('foo'))
        self.assertRaises(PatroniFatalException, self.ha.run_cycle)

    def test_empty_directory_in_pause(self):
        self.ha.is_paused = true
        self.p.data_directory_empty = true
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: running with empty data directory')
        self.assertEqual(self.p.role, 'uninitialized')

    @patch('patroni.ha.Ha.sysid_valid', MagicMock(return_value=True))
    def test_sysid_no_match_in_pause(self):
        self.ha.is_paused = true
        self.p.controldata = lambda: {'Database cluster state': 'in recovery', 'Database system identifier': '123'}
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: continue to run as primary without lock')

        self.ha.has_lock = true
        self.assertEqual(self.ha.run_cycle(), 'PAUSE: released leader key voluntarily due to the system ID mismatch')

    @patch('patroni.psycopg.connect', psycopg_connect)
    @patch('os.path.exists', Mock(return_value=True))
    @patch('shutil.rmtree', Mock())
    @patch('os.makedirs', Mock())
    @patch('os.open', Mock())
    @patch('os.fsync', Mock())
    @patch('os.close', Mock())
    @patch('os.chmod', Mock())
    @patch('os.rename', Mock())
    @patch('patroni.postgresql.Postgresql.is_starting', Mock(return_value=False))
    @patch('builtins.open', mock_open())
    @patch.object(ConfigHandler, 'check_recovery_conf', Mock(return_value=(False, False)))
    @patch.object(Postgresql, 'major_version', PropertyMock(return_value=130000))
    @patch.object(SlotsHandler, 'sync_replication_slots', Mock(return_value=['ls']))
    def test_follow_copy(self):
        self.ha.cluster.config.data['slots'] = {'ls': {'database': 'a', 'plugin': 'b'}}
        self.p.is_primary = false
        self.assertTrue(self.ha.run_cycle().startswith('Copying logical slots'))

    def test_acquire_lock(self):
        self.ha.dcs.attempt_to_acquire_leader = Mock(side_effect=[DCSError('foo'), Exception])
        self.assertRaises(DCSError, self.ha.acquire_lock)
        self.assertFalse(self.ha.acquire_lock())

    @patch('patroni.postgresql.citus.CitusHandler.is_coordinator', Mock(return_value=False))
    def test_notify_citus_coordinator(self):
        self.ha.patroni.request = Mock()
        self.ha.notify_citus_coordinator('before_demote')
        self.ha.patroni.request.assert_called_once()
        self.assertEqual(self.ha.patroni.request.call_args[1]['timeout'], 30)
        self.ha.patroni.request = Mock(side_effect=Exception)
        with patch('patroni.ha.logger.warning') as mock_logger:
            self.ha.notify_citus_coordinator('before_promote')
            self.assertEqual(self.ha.patroni.request.call_args[1]['timeout'], 2)
            mock_logger.assert_called()
            self.assertTrue(mock_logger.call_args[0][0].startswith('Request to Citus coordinator'))
