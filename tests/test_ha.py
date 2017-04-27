import datetime
import etcd
import os
import unittest

from mock import Mock, MagicMock, PropertyMock, patch
from patroni.config import Config
from patroni.dcs import Cluster, ClusterConfig, Failover, Leader, Member, get_dcs, SyncState
from patroni.dcs.etcd import Client
from patroni.exceptions import DCSError, PostgresException
from patroni.ha import Ha, _MemberStatus
from patroni.postgresql import Postgresql
from patroni.utils import tzutc
from test_etcd import socket_getaddrinfo, etcd_read, etcd_write, requests_get
from test_postgresql import psycopg2_connect


def true(*args, **kwargs):
    return True


def false(*args, **kwargs):
    return False


def get_cluster(initialize, leader, members, failover, sync):
    return Cluster(initialize, ClusterConfig(1, {1: 2}, 1), leader, 10, members, failover, sync)


def get_cluster_not_initialized_without_leader():
    return get_cluster(None, None, [], None, SyncState(None, None, None))


def get_cluster_initialized_without_leader(leader=False, failover=None, sync=None):
    m1 = Member(0, 'leader', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres',
                                  'api_url': 'http://127.0.0.1:8008/patroni', 'xlog_location': 4})
    l = Leader(0, 0, m1) if leader else None
    m2 = Member(0, 'other', 28, {'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5436/postgres',
                                 'api_url': 'http://127.0.0.1:8011/patroni',
                                 'state': 'running',
                                 'tags': {'clonefrom': True},
                                 'scheduled_restart': {'schedule': "2100-01-01 10:53:07.560445+00:00",
                                                       'postgres_version': '99.0.0'}})
    syncstate = SyncState(0 if sync else None, sync and sync[0], sync and sync[1])
    return get_cluster(True, l, [m1, m2], failover, syncstate)


def get_cluster_initialized_with_leader(failover=None, sync=None):
    return get_cluster_initialized_without_leader(leader=True, failover=failover, sync=sync)


def get_cluster_initialized_with_only_leader(failover=None):
    l = get_cluster_initialized_without_leader(leader=True, failover=failover).leader
    return get_cluster(True, l, [l], failover, None)


def get_node_status(reachable=True, in_recovery=True, xlog_location=10, nofailover=False):
    def fetch_node_status(e):
        tags = {}
        if nofailover:
            tags['nofailover'] = True
        return _MemberStatus(e, reachable, in_recovery, xlog_location, tags)
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
zookeeper:
  exhibitor:
    hosts: [localhost]
    port: 8181
"""
        self.config = Config()
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


def run_async(self, func, args=()):
    return func(*args) if args else func()


@patch.object(Postgresql, 'is_running', Mock(return_value=True))
@patch.object(Postgresql, 'is_leader', Mock(return_value=True))
@patch.object(Postgresql, 'xlog_position', Mock(return_value=10))
@patch.object(Postgresql, 'call_nowait', Mock(return_value=True))
@patch.object(Postgresql, 'data_directory_empty', Mock(return_value=False))
@patch.object(Postgresql, 'controldata', Mock(return_value={'Database system identifier': '1234567890'}))
@patch.object(Postgresql, 'sync_replication_slots', Mock())
@patch.object(Postgresql, 'write_pg_hba', Mock())
@patch.object(Postgresql, 'write_pgpass', Mock())
@patch.object(Postgresql, 'write_recovery_conf', Mock())
@patch.object(Postgresql, 'query', Mock())
@patch.object(Postgresql, 'checkpoint', Mock())
@patch.object(Postgresql, 'call_nowait', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
@patch.object(etcd.Client, 'delete', Mock(side_effect=etcd.EtcdException))
@patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=False))
@patch('patroni.async_executor.AsyncExecutor.run_async', run_async)
@patch('subprocess.call', Mock(return_value=0))
class TestHa(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch('psycopg2.connect', psycopg2_connect)
    @patch.object(etcd.Client, 'read', etcd_read)
    def setUp(self):
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            self.p = Postgresql({'name': 'postgresql0', 'scope': 'dummy', 'listen': '127.0.0.1:5432',
                                 'data_dir': 'data/postgresql0', 'retry_timeout': 10,
                                 'maximum_lag_on_failover': 5,
                                 'authentication': {'superuser': {'username': 'foo', 'password': 'bar'},
                                                    'replication': {'username': '', 'password': ''}},
                                 'parameters': {'wal_level': 'hot_standby', 'max_replication_slots': 5, 'foo': 'bar',
                                                'hot_standby': 'on', 'max_wal_senders': 5, 'wal_keep_segments': 8}})
            self.p.set_state('running')
            self.p.set_role('replica')
            self.p.postmaster_start_time = MagicMock(return_value=str(postmaster_start_time))
            self.p.can_create_replica_without_replication_connection = MagicMock(return_value=False)
            self.e = get_dcs({'etcd': {'ttl': 30, 'host': 'ok:2379', 'scope': 'test',
                                       'name': 'foo', 'retry_timeout': 10}})
            self.ha = Ha(MockPatroni(self.p, self.e))
            self.ha.old_cluster = self.e.get_cluster()
            self.ha.cluster = get_cluster_not_initialized_without_leader()
            self.ha.load_cluster_from_dcs = Mock()
            self.ha.is_synchronous_mode = false

    def test_update_lock(self):
        self.p.last_operation = Mock(side_effect=PostgresException(''))
        self.assertTrue(self.ha.update_lock(True))

    def test_touch_member(self):
        self.p.xlog_position = Mock(side_effect=Exception)
        self.ha.touch_member()

    def test_start_as_replica(self):
        self.p.is_healthy = false
        self.assertEquals(self.ha.run_cycle(), 'starting as a secondary')

    def test_recover_replica_failed(self):
        self.p.controldata = lambda: {'Database cluster state': 'in production'}
        self.p.is_healthy = false
        self.p.is_running = false
        self.p.follow = false
        self.assertEquals(self.ha.run_cycle(), 'starting as a secondary')
        self.assertEquals(self.ha.run_cycle(), 'failed to start postgres')

    def test_recover_master_failed(self):
        self.p.follow = false
        self.p.is_healthy = false
        self.p.is_running = false
        self.p.name = 'leader'
        self.p.set_role('master')
        self.p.controldata = lambda: {'Database cluster state': 'in production'}
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEquals(self.ha.run_cycle(), 'starting as readonly because i had the session lock')

    @patch('sys.exit', return_value=1)
    @patch('patroni.ha.Ha.sysid_valid', MagicMock(return_value=True))
    def test_sysid_no_match(self, exit_mock):
        self.ha.run_cycle()
        exit_mock.assert_called_once_with(1)

    @patch.object(Cluster, 'is_unlocked', Mock(return_value=False))
    def test_start_as_readonly(self):
        self.p.is_leader = false
        self.p.is_healthy = true
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_acquire_lock_as_master(self):
        self.assertEquals(self.ha.run_cycle(), 'acquired session lock as a leader')

    def test_promoted_by_acquiring_lock(self):
        self.ha.is_healthiest_node = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')

    def test_demote_after_failing_to_obtain_lock(self):
        self.ha.acquire_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoted self after trying and failing to obtain lock')

    def test_follow_new_leader_after_failing_to_obtain_lock(self):
        self.ha.is_healthiest_node = true
        self.ha.acquire_lock = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following new leader after trying and failing to obtain lock')

    def test_demote_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i am not the healthiest node')

    def test_follow_new_leader_because_not_healthiest(self):
        self.ha.is_healthiest_node = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')

    def test_promote_because_have_lock(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader because i had the session lock')

    def test_leader_with_lock(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')

    def test_demote_because_not_having_lock(self):
        self.ha.cluster.is_unlocked = false
        self.assertEquals(self.ha.run_cycle(), 'demoting self because i do not have the lock and i was a leader')

    def test_demote_because_update_lock_failed(self):
        self.ha.cluster.is_unlocked = false
        self.ha.has_lock = true
        self.ha.update_lock = false
        self.assertEquals(self.ha.run_cycle(), 'demoted self because failed to update leader lock in DCS')
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'not promoting because failed to update leader lock in DCS')

    def test_follow(self):
        self.ha.cluster.is_unlocked = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')
        self.ha.patroni.replicatefrom = "foo"
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')

    def test_follow_in_pause(self):
        self.ha.cluster.is_unlocked = false
        self.ha.is_paused = true
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: continue to run as master without lock')
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: no action')

    def test_no_etcd_connection_master_demote(self):
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.assertEquals(self.ha.run_cycle(), 'demoted self because DCS is not accessible and i was a leader')

    def test_bootstrap_from_another_member(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEquals(self.ha.bootstrap(), 'trying to bootstrap from replica \'other\'')

    def test_bootstrap_waiting_for_leader(self):
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.assertEquals(self.ha.bootstrap(), 'waiting for leader to bootstrap')

    def test_bootstrap_without_leader(self):
        self.ha.cluster = get_cluster_initialized_without_leader()
        self.p.can_create_replica_without_replication_connection = MagicMock(return_value=True)
        self.assertEquals(self.ha.bootstrap(), 'trying to bootstrap (without leader)')

    def test_bootstrap_initialize_lock_failed(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.assertEquals(self.ha.bootstrap(), 'failed to acquire initialize lock')

    def test_bootstrap_initialized_new_cluster(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.assertEquals(self.ha.bootstrap(), 'initialized a new cluster')

    def test_bootstrap_release_initialize_key_on_failure(self):
        self.ha.cluster = get_cluster_not_initialized_without_leader()
        self.e.initialize = true
        self.p.bootstrap = Mock(side_effect=PostgresException("Could not bootstrap master PostgreSQL"))
        self.assertRaises(PostgresException, self.ha.bootstrap)

    def test_reinitialize(self):
        self.assertIsNotNone(self.ha.reinitialize())

        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertIsNone(self.ha.reinitialize())

        self.assertIsNotNone(self.ha.reinitialize())

        self.ha.state_handler.name = self.ha.cluster.leader.name
        self.assertIsNotNone(self.ha.reinitialize())

    def test_restart(self):
        self.assertEquals(self.ha.restart({}), (True, 'restarted successfully'))
        self.p.restart = Mock(return_value=None)
        self.assertEquals(self.ha.restart({}), (False, 'postgres is still starting'))
        self.p.restart = false
        self.assertEquals(self.ha.restart({}), (False, 'restart failed'))
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.reinitialize()
        self.assertEquals(self.ha.restart({}), (False, 'reinitialize already in progress'))
        with patch.object(self.ha, "restart_matches", return_value=False):
            self.assertEquals(self.ha.restart({'foo': 'bar'}), (False, "restart conditions are not satisfied"))

    def test_restart_in_progress(self):
        with patch('patroni.async_executor.AsyncExecutor.busy', PropertyMock(return_value=True)):
            self.ha.restart({}, run_async=True)
            self.assertTrue(self.ha.restart_scheduled())
            self.assertEquals(self.ha.run_cycle(), 'not healthy enough for leader race')

            self.ha.cluster = get_cluster_initialized_with_leader()
            self.assertEquals(self.ha.run_cycle(), 'restart in progress')

            self.ha.has_lock = true
            self.assertEquals(self.ha.run_cycle(), 'updated leader lock during restart')

            self.ha.update_lock = false
            self.assertEquals(self.ha.run_cycle(), 'failed to update leader lock during restart')

    @patch('requests.get', requests_get)
    @patch('time.sleep', Mock())
    def test_manual_failover_from_leader(self):
        self.ha.fetch_node_status = get_node_status()
        self.ha.has_lock = true
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', '', None))
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', self.p.name, None))
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', 'blabla', None))
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')
        f = Failover(0, self.p.name, '', None)
        self.ha.cluster = get_cluster_initialized_with_leader(f)
        self.assertEquals(self.ha.run_cycle(), 'manual failover: demoting myself')
        self.ha.fetch_node_status = get_node_status(nofailover=True)
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')
        self.ha.fetch_node_status = get_node_status(xlog_location=1)
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')
        # manual failover from the previous leader to us won't happen if we hold the nofailover flag
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, None))
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')

        # Failover scheduled time must include timezone
        scheduled = datetime.datetime.now()
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.ha.run_cycle()

        scheduled = datetime.datetime.utcnow().replace(tzinfo=tzutc)
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.assertEquals('no action.  i am the leader with the lock', self.ha.run_cycle())

        scheduled = scheduled + datetime.timedelta(seconds=30)
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.assertEquals('no action.  i am the leader with the lock', self.ha.run_cycle())

        scheduled = scheduled + datetime.timedelta(seconds=-600)
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.assertEquals('no action.  i am the leader with the lock', self.ha.run_cycle())

        scheduled = None
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.assertEquals('no action.  i am the leader with the lock', self.ha.run_cycle())

    @patch('requests.get', requests_get)
    def test_manual_failover_from_leader_in_pause(self):
        self.ha.has_lock = true
        self.ha.is_paused = true
        scheduled = datetime.datetime.now()
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, 'blabla', self.p.name, scheduled))
        self.assertEquals('PAUSE: no action.  i am the leader with the lock', self.ha.run_cycle())
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, self.p.name, '', None))
        self.assertEquals('PAUSE: no action.  i am the leader with the lock', self.ha.run_cycle())

    @patch('requests.get', requests_get)
    @patch('time.sleep', Mock())
    def test_manual_failover_process_no_leader(self):
        self.p.is_leader = false
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', self.p.name, None))
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'leader', None))
        self.p.set_role('replica')
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, self.p.name, '', None))
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because i am not the healthiest node')
        self.ha.fetch_node_status = get_node_status(reachable=False)  # inaccessible, in_recovery
        self.p.set_role('replica')
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
        # set failover flag to True for all members of the cluster
        # this should elect the current member, as we are not going to call the API for it.
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None))
        self.ha.fetch_node_status = get_node_status(nofailover=True)  # accessible, in_recovery
        self.p.set_role('replica')
        self.assertEquals(self.ha.run_cycle(), 'promoted self to leader by acquiring session lock')
        # same as previous, but set the current member to nofailover. In no case it should be elected as a leader
        self.ha.patroni.nofailover = True
        self.assertEquals(self.ha.run_cycle(), 'following a different leader because I am not allowed to promote')

    @patch('time.sleep', Mock())
    def test_manual_failover_process_no_leader_in_pause(self):
        self.ha.is_paused = true
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, '', 'other', None))
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: continue to run as master without lock')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', '', None))
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: continue to run as master without lock')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', 'blabla', None))
        self.assertEquals('PAUSE: acquired session lock as a leader', self.ha.run_cycle())
        self.p.is_leader = false
        self.p.set_role('replica')
        self.ha.cluster = get_cluster_initialized_without_leader(failover=Failover(0, 'leader', self.p.name, None))
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: promoted self to leader by acquiring session lock')

    def test_is_healthiest_node(self):
        self.ha.state_handler.is_leader = false
        self.ha.patroni.nofailover = False
        self.ha.fetch_node_status = get_node_status()
        self.assertTrue(self.ha.is_healthiest_node())
        with patch('patroni.postgresql.Postgresql.is_starting', return_value=True):
            self.assertFalse(self.ha.is_healthiest_node())
        self.ha.is_paused = true
        self.assertFalse(self.ha.is_healthiest_node())

    def test__is_healthiest_node(self):
        self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.p.is_leader = false
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertTrue(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status(in_recovery=False)  # accessible, not in_recovery
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.fetch_node_status = get_node_status(xlog_location=11)  # accessible, in_recovery, xlog location ahead
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        with patch('patroni.postgresql.Postgresql.xlog_position', return_value=1):
            self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.patroni.nofailover = True
        self.assertFalse(self.ha._is_healthiest_node(self.ha.old_cluster.members))
        self.ha.patroni.nofailover = False

    @patch('requests.get', requests_get)
    def test_fetch_node_status(self):
        member = Member(0, 'test', 1, {'api_url': 'http://127.0.0.1:8011/patroni'})
        self.ha.fetch_node_status(member)
        member = Member(0, 'test', 1, {'api_url': 'http://localhost:8011/patroni'})
        self.ha.fetch_node_status(member)

    def test_post_recover(self):
        self.p.is_running = false
        self.ha.has_lock = true
        self.assertEqual(self.ha.post_recover(), 'removed leader key after trying and failing to start postgres')
        self.ha.has_lock = false
        self.assertEqual(self.ha.post_recover(), 'failed to start postgres')
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
        # restart already in progres
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
            self.assertEquals(self.ha.run_cycle(), "restart scheduled")

    def test_restart_matches(self):
        self.p._role = 'replica'
        self.p.server_version = 90500
        self.p._pending_restart = True
        self.assertFalse(self.ha.restart_matches("master", "9.5.0", True))
        self.assertFalse(self.ha.restart_matches("replica", "9.4.3", True))
        self.p._pending_restart = False
        self.assertFalse(self.ha.restart_matches("replica", "9.5.2", True))
        self.assertTrue(self.ha.restart_matches("replica", "9.5.2", False))

    def test_process_healthy_cluster_in_pause(self):
        self.p.is_leader = false
        self.ha.is_paused = true
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: removed leader lock because postgres is not running as master')
        self.ha.cluster = get_cluster_initialized_with_leader(Failover(0, '', self.p.name, None))
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: waiting to become master after promote...')

    def test_postgres_unhealthy_in_pause(self):
        self.ha.is_paused = true
        self.p.is_healthy = false
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: postgres is not running')
        self.ha.has_lock = true
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: removed leader lock because postgres is not running')

    def test_no_etcd_connection_in_pause(self):
        self.ha.is_paused = true
        self.ha.load_cluster_from_dcs = Mock(side_effect=DCSError('Etcd is not responding properly'))
        self.assertEquals(self.ha.run_cycle(), 'PAUSE: DCS is not accessible')

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
        self.assertEquals(self.ha.run_cycle(), 'PostgreSQL is still starting up, 270 seconds until timeout')
        check_calls([(update_lock, True), (demote, False)])

        self.p.time_in_state = lambda: 350
        self.ha.fetch_node_status = get_node_status(reachable=False)  # inaccessible, in_recovery
        self.assertEquals(self.ha.run_cycle(),
                          'master start has timed out, but continuing to wait because failover is not possible')
        check_calls([(update_lock, True), (demote, False)])

        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEquals(self.ha.run_cycle(), 'stopped PostgreSQL because of startup timeout')
        check_calls([(update_lock, True), (demote, True)])

        update_lock.return_value = False
        self.assertEquals(self.ha.run_cycle(), 'stopped PostgreSQL while starting up because leader key was lost')
        check_calls([(update_lock, True), (demote, True)])

        self.ha.has_lock = false
        self.p.is_leader = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am a secondary and i am following a leader')
        check_calls([(update_lock, False), (demote, False)])

    @patch('time.sleep', Mock())
    def test_manual_failover_while_starting(self):
        self.ha.has_lock = true
        self.p.check_for_startup = true
        f = Failover(0, self.p.name, '', None)
        self.ha.cluster = get_cluster_initialized_with_leader(f)
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEquals(self.ha.run_cycle(), 'manual failover: demoting myself')

    @patch('patroni.ha.Ha.demote')
    def test_failover_immediately_on_zero_master_start_timeout(self, demote):
        self.p.is_running = false
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.patroni.config.set_dynamic_configuration({'master_start_timeout': 0})
        self.ha.has_lock = true
        self.ha.update_lock = true
        self.ha.fetch_node_status = get_node_status()  # accessible, in_recovery
        self.assertEquals(self.ha.run_cycle(), 'stopped PostgreSQL to fail over after a crash')
        demote.assert_called_once()

    @patch('time.sleep', Mock())
    @patch('patroni.postgresql.Postgresql.follow')
    def test_demote_immediate(self, follow):
        self.ha.has_lock = true
        self.e.get_cluster = Mock(return_value=get_cluster_initialized_without_leader())
        self.ha.demote('immediate')
        follow.assert_called_once_with(None, None, True, None, True)

    @patch('time.sleep', Mock())
    def test_process_sync_replication(self):
        self.ha.has_lock = true
        mock_set_sync = self.p.set_synchronous_standby = Mock()
        self.p.name = 'leader'

        # Test sync key removed when sync mode disabled
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        with patch.object(self.ha.dcs, 'delete_sync_state') as mock_delete_sync:
            self.ha.run_cycle()
            mock_delete_sync.assert_called_once()
            mock_set_sync.assert_called_once_with(None)

        mock_set_sync.reset_mock()
        # Test sync key not touched when not there
        self.ha.cluster = get_cluster_initialized_with_leader()
        with patch.object(self.ha.dcs, 'delete_sync_state') as mock_delete_sync:
            self.ha.run_cycle()
            mock_delete_sync.assert_not_called()
            mock_set_sync.assert_called_once_with(None)

        mock_set_sync.reset_mock()

        self.ha.is_synchronous_mode = true

        # Test sync standby not touched when picking the same node
        self.p.pick_synchronous_standby = Mock(return_value=('other', True))
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.ha.run_cycle()
        mock_set_sync.assert_not_called()

        mock_set_sync.reset_mock()

        # Test sync standby is replaced when switching standbys
        self.p.pick_synchronous_standby = Mock(return_value=('other2', False))
        self.ha.dcs.write_sync_state = Mock(return_value=True)
        self.ha.run_cycle()
        mock_set_sync.assert_called_once_with('other2')

        mock_set_sync.reset_mock()
        # Test sync standby is not disabled when updating dcs fails
        self.ha.dcs.write_sync_state = Mock(return_value=False)
        self.ha.run_cycle()
        mock_set_sync.assert_not_called()

        mock_set_sync.reset_mock()
        # Test changing sync standby
        self.ha.dcs.write_sync_state = Mock(return_value=True)
        self.ha.dcs.get_cluster = Mock(return_value=get_cluster_initialized_with_leader(sync=('leader', 'other')))
        # self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.p.pick_synchronous_standby = Mock(return_value=('other2', True))
        self.ha.run_cycle()
        self.ha.dcs.get_cluster.assert_called_once()
        self.assertEquals(self.ha.dcs.write_sync_state.call_count, 2)

        # Test updating sync standby key failed due to race
        self.ha.dcs.write_sync_state = Mock(side_effect=[True, False])
        self.ha.run_cycle()
        self.assertEquals(self.ha.dcs.write_sync_state.call_count, 2)

        # Test changing sync standby failed due to race
        self.ha.dcs.write_sync_state = Mock(return_value=True)
        self.ha.dcs.get_cluster = Mock(return_value=get_cluster_initialized_with_leader(sync=('somebodyelse', None)))
        self.ha.run_cycle()
        self.assertEquals(self.ha.dcs.write_sync_state.call_count, 1)

        # Test sync set to '*' when synchronous_mode_strict is enabled
        mock_set_sync.reset_mock()
        self.ha.is_synchronous_mode_strict = true
        self.p.pick_synchronous_standby = Mock(return_value=(None, False))
        self.ha.run_cycle()
        mock_set_sync.assert_called_once_with('*')

    def test_sync_replication_become_master(self):
        self.ha.is_synchronous_mode = true

        mock_set_sync = self.p.set_synchronous_standby = Mock()
        self.p.is_leader = false
        self.p.set_role('replica')
        self.ha.has_lock = true
        mock_write_sync = self.ha.dcs.write_sync_state = Mock(return_value=True)
        self.p.name = 'leader'
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('other', None))

        # When we just became master nobody is sync
        self.assertEquals(self.ha.enforce_master_role('msg', 'promote msg'), 'promote msg')
        mock_set_sync.assert_called_once_with(None)
        mock_write_sync.assert_called_once_with('leader', None, index=0)

        mock_set_sync.reset_mock()

        # When we just became master nobody is sync
        self.p.set_role('replica')
        mock_write_sync.return_value = False
        self.assertTrue(self.ha.enforce_master_role('msg', 'promote msg') != 'promote msg')
        mock_set_sync.assert_not_called()

    def test_unhealthy_sync_mode(self):
        self.ha.is_synchronous_mode = true

        self.p.is_leader = false
        self.p.set_role('replica')
        self.p.name = 'other'
        self.ha.cluster = get_cluster_initialized_without_leader(sync=('leader', 'other2'))
        mock_write_sync = self.ha.dcs.write_sync_state = Mock(return_value=True)
        mock_acquire = self.ha.acquire_lock = Mock(return_value=True)
        mock_follow = self.p.follow = Mock()
        mock_promote = self.p.promote = Mock()

        # If we don't match the sync replica we are not allowed to acquire lock
        self.ha.run_cycle()
        mock_acquire.assert_not_called()
        mock_follow.assert_called_once()
        self.assertEquals(mock_follow.call_args[0][0], None)
        mock_write_sync.assert_not_called()

        mock_follow.reset_mock()
        # If we do match we will try to promote
        self.ha._is_healthiest_node = true

        self.ha.cluster = get_cluster_initialized_without_leader(sync=('leader', 'other'))
        self.ha.run_cycle()
        mock_acquire.assert_called_once()
        mock_follow.assert_not_called()
        mock_promote.assert_called_once()
        mock_write_sync.assert_called_once_with('other', None, index=0)

    @patch('time.sleep')
    def test_disable_sync_when_restarting(self, mock_sleep):
        self.ha.is_synchronous_mode = true

        self.p.name = 'other'
        self.p.is_leader = false
        self.p.set_role('replica')
        mock_restart = self.p.restart = Mock(return_value=True)
        self.ha.cluster = get_cluster_initialized_with_leader(sync=('leader', 'other'))
        self.ha.touch_member = Mock(return_value=True)
        self.ha.dcs.get_cluster = Mock(side_effect=[
            get_cluster_initialized_with_leader(sync=('leader', syncstandby))
            for syncstandby in ['other', None]])

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

    def test_effective_tags(self):
        self.ha._disable_sync = True
        self.assertEquals(self.ha.get_effective_tags(), {'foo': 'bar', 'nosync': True})
        self.ha._disable_sync = False
        self.assertEquals(self.ha.get_effective_tags(), {'foo': 'bar'})

    def test_restore_cluster_config(self):
        self.ha.cluster.config.data.clear()
        self.ha.has_lock = true
        self.ha.cluster.is_unlocked = false
        self.assertEquals(self.ha.run_cycle(), 'no action.  i am the leader with the lock')

    def test_watch(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.watch(0)

    def test_wakup(self):
        self.ha.wakeup()

    def test_leader_with_empty_directory(self):
        self.ha.cluster = get_cluster_initialized_with_leader()
        self.ha.has_lock = true
        self.p.data_directory_empty = true
        self.assertEquals(self.ha.run_cycle(), 'released leader key voluntarily as data dir empty and currently leader')

        # as has_lock is mocked out, we need to fake the leader key release
        self.ha.has_lock = false
        # will not say bootstrap from leader as replica can't self elect
        self.assertEquals(self.ha.run_cycle(), "trying to bootstrap from replica 'other'")
