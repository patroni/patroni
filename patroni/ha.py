import datetime
import functools
import json
import logging
import sys
import time
import uuid

from multiprocessing.pool import ThreadPool
from threading import RLock
from typing import Any, Callable, cast, Collection, Dict, List, NamedTuple, Optional, Tuple, TYPE_CHECKING, Union

from . import global_config, psycopg
from .__main__ import Patroni
from .async_executor import AsyncExecutor, CriticalTask
from .collections import CaseInsensitiveSet
from .dcs import AbstractDCS, Cluster, Leader, Member, RemoteMember, Status, SyncState
from .exceptions import DCSError, PatroniFatalException, PostgresConnectionException
from .postgresql.callback_executor import CallbackAction
from .postgresql.misc import postgres_version_to_int, PostgresqlRole, PostgresqlState
from .postgresql.postmaster import PostmasterProcess
from .postgresql.rewind import Rewind
from .quorum import QuorumStateResolver
from .tags import Tags
from .utils import parse_int, polling_loop, tzutc

logger = logging.getLogger(__name__)


class _MemberStatus(Tags, NamedTuple('_MemberStatus',
                                     [('member', Member),
                                      ('reachable', bool),
                                      ('in_recovery', Optional[bool]),
                                      ('wal_position', int),
                                      ('data', Dict[str, Any])])):
    """Node status distilled from API response.

    Consists of the following fields:

    :ivar member: :class:`~patroni.dcs.Member` object of the node.
    :ivar reachable: ``False`` if the node is not reachable or is not responding with correct JSON.
    :ivar in_recovery: ``False`` if the node is running as a primary (`if pg_is_in_recovery() == true`).
    :ivar wal_position: maximum value of ``replayed_location`` or ``received_location`` from JSON.
    :ivar data: the whole JSON response for future usage.
    """

    @classmethod
    def from_api_response(cls, member: Member, json: Dict[str, Any]) -> '_MemberStatus':
        """
        :param member: dcs.Member object
        :param json: RestApiHandler.get_postgresql_status() result
        :returns: _MemberStatus object
        """
        # If one of those is not in a response we want to count the node as not healthy/reachable
        wal: Dict[str, Any] = json.get('wal') or json['xlog']
        # abuse difference in primary/replica response format
        in_recovery = not (bool(wal.get('location'))
                           or json.get('role') in (PostgresqlRole.MASTER, PostgresqlRole.PRIMARY))
        lsn = int(in_recovery and max(wal.get('received_location', 0), wal.get('replayed_location', 0)))
        return cls(member, True, in_recovery, lsn, json)

    @property
    def tags(self) -> Dict[str, Any]:
        """Dictionary with values of different tags (i.e. nofailover)."""
        return self.data.get('tags', {})

    @property
    def timeline(self) -> int:
        """Timeline value from JSON."""
        return self.data.get('timeline', 0)

    @property
    def watchdog_failed(self) -> bool:
        """Indicates that watchdog is required by configuration but not available or failed."""
        return self.data.get('watchdog_failed', False)

    @classmethod
    def unknown(cls, member: Member) -> '_MemberStatus':
        """Create a new class instance with empty or null values."""
        return cls(member, False, None, 0, {})

    def failover_limitation(self) -> Optional[str]:
        """Returns reason why this node can't promote or None if everything is ok."""
        if not self.reachable:
            return 'not reachable'
        if self.nofailover:
            return 'not allowed to promote'
        if self.watchdog_failed:
            return 'not watchdog capable'
        return None


class _FailsafeResponse(NamedTuple):
    """Response on POST ``/failsafe`` API request.

    Consists of the following fields:

    :ivar member_name: member name.
    :ivar accepted: ``True`` if the member agrees that the current primary will continue running, ``False`` otherwise.
    :ivar lsn: absolute position of received/replayed location in bytes.
    """

    member_name: str
    accepted: bool
    lsn: Optional[int]


class Failsafe(object):
    """Object that represents failsafe state of the cluster."""

    def __init__(self, dcs: AbstractDCS) -> None:
        """Initialize the :class:`Failsafe` object.

        :param dcs: current DCS object, is used only to get current value of ``ttl``.
        """
        self._lock = RLock()
        self._dcs = dcs
        self._reset_state()

    def update_slots(self, slots: Dict[str, int]) -> None:
        """Assign value to :attr:`_slots`.

        .. note:: This method is only called on the primary node.

        :param slots: a :class:`dict` object with member names as keys and received/replayed LSNs as values.
        """
        with self._lock:
            self._slots = slots

    def update(self, data: Dict[str, Any]) -> None:
        """Update the :class:`Failsafe` object state.

        The last update time is stored and object will be invalidated after ``ttl`` seconds.

        .. note::
            This method is only called as a result of `POST /failsafe` REST API call.

        :param data: deserialized JSON document from REST API call that contains information about current leader.
        """
        with self._lock:
            self._last_update = time.time()
            self._name = data['name']
            self._conn_url = data['conn_url']
            self._api_url = data['api_url']
            self._slots = data.get('slots')

    def _reset_state(self) -> None:
        """Reset state of the :class:`Failsafe` object."""
        self._last_update = 0  # holds information when failsafe was triggered last time.
        self._name = ''  # name of the cluster leader
        self._conn_url = None  # PostgreSQL conn_url of the leader
        self._api_url = None  # Patroni REST api_url of the leader
        self._slots = None  # state of replication slots on the leader

    @property
    def leader(self) -> Optional[Leader]:
        """Return information about current cluster leader if the failsafe mode is active."""
        with self._lock:
            if self._last_update + self._dcs.ttl > time.time():
                return Leader('', '', RemoteMember(self._name, {'api_url': self._api_url,
                                                                'conn_url': self._conn_url,
                                                                'slots': self._slots}))

    def update_cluster(self, cluster: Cluster) -> Cluster:
        """Update and return provided :class:`Cluster` object with fresh values.

        .. note::
            This method is called when failsafe mode is active and is used to update cluster state
            with fresh values of replication ``slots`` status and ``xlog_location`` on member nodes.

        :returns: :class:`Cluster` object, either unchanged or updated.
        """
        # Enreach cluster with the real leader if there was a ping from it
        leader = self.leader
        if leader:
            # We rely on the strict order of fields in the namedtuple
            status = Status(cluster.status[0], leader.member.data['slots'], *cluster.status[2:])
            cluster = Cluster(*cluster[0:2], leader, status, *cluster[4:])
            # To advance LSN of replication slots on the primary for nodes that are doing cascading
            # replication from other nodes we need to update `xlog_location` on respective members.
            for member in cluster.members:
                if member.replicatefrom and status.slots and member.name in status.slots:
                    member.data['xlog_location'] = status.slots[member.name]
        return cluster

    def is_active(self) -> bool:
        """Check whether the failsafe mode is active.

        .. note:
            This method is called from the REST API to report whether the failsafe mode was activated.

            On primary the :attr:`_last_update` is updated from the :func:`set_is_active` method and always
            returns the correct value.

            On replicas the :attr:`_last_update` is updated at the moment when the primary performs
            ``POST /failsafe`` REST API calls.

            The side-effect - it is possible that replicas will show ``failsafe_is_active``
            values different from the primary.

        :returns: ``True`` if failsafe mode is active, ``False`` otherwise.
        """

        with self._lock:
            return self._last_update + self._dcs.ttl > time.time()

    def set_is_active(self, value: float) -> None:
        """Update :attr:`_last_update` value.

        .. note::
            This method is only called on the primary.
            Effectively it sets expiration time of failsafe mode.
            If the provided value is ``0``, it disables failsafe mode.

        :param value: time of the last update.
        """
        with self._lock:
            self._last_update = value
            if not value:
                self._reset_state()


class Ha(object):

    def __init__(self, patroni: Patroni):
        self.patroni = patroni
        self.state_handler = patroni.postgresql
        self._rewind = Rewind(self.state_handler)
        self.dcs = patroni.dcs
        self.cluster = Cluster.empty()
        self.old_cluster = Cluster.empty()
        self._leader_expiry = 0
        self._leader_expiry_lock = RLock()
        self._failsafe = Failsafe(patroni.dcs)
        self._was_paused = False
        self._promote_timestamp = 0
        self._leader_timeline = None
        self.recovering = False
        self._async_response = CriticalTask()
        self._crash_recovery_started = 0
        self._start_timeout = None
        self._async_executor = AsyncExecutor(self.state_handler.cancellable, self.wakeup)
        self.watchdog = patroni.watchdog

        # Each member publishes various pieces of information to the DCS using touch_member. This lock protects
        # the state and publishing procedure to have consistent ordering and avoid publishing stale values.
        self._member_state_lock = RLock()

        # The last know value of current receive/flush/replay LSN.
        # We update this value from update_lock() and touch_member() methods, because they fetch it anyway.
        # This value is used to notify the leader when the failsafe_mode is active without performing any queries.
        self._last_wal_lsn = None
        # The last known value of current timeline on this standby node.
        # We update this value from touch_member() and _is_healthiest_node() methods, because they fetch it anyway.
        # This value is used to detect cases of timeline bump with actual leader remaining on the same node
        # and trigger pg_rewind state machine.
        self._last_timeline = None

        # Count of concurrent sync disabling requests. Value above zero means that we don't want to be synchronous
        # standby. Changes protected by _member_state_lock.
        self._disable_sync = 0
        # Remember the last known member role and state written to the DCS in order to notify MPP coordinator
        self._last_state = None

        # We need following property to avoid shutdown of postgres when join of Patroni to the postgres
        # already running as replica was aborted due to cluster not being initialized in DCS.
        self._join_aborted = False

        # used only in backoff after failing a pre_promote script
        self._released_leader_key_timestamp = 0

    def primary_stop_timeout(self) -> Union[int, None]:
        """:returns: "primary_stop_timeout" from the global configuration or `None` when not in synchronous mode."""
        ret = global_config.primary_stop_timeout
        return ret if ret > 0 and self.is_synchronous_mode() else None

    def is_paused(self) -> bool:
        """:returns: `True` if in maintenance mode."""
        return global_config.is_paused

    def check_timeline(self) -> bool:
        """:returns: `True` if should check whether the timeline is latest during the leader race."""
        return global_config.check_mode('check_timeline')

    def is_standby_cluster(self) -> bool:
        """:returns: `True` if global configuration has a valid "standby_cluster" section."""
        return global_config.is_standby_cluster

    def is_leader(self) -> bool:
        """:returns: `True` if the current node is the leader, based on expiration set when it last held the key."""
        with self._leader_expiry_lock:
            return self._leader_expiry > time.time()

    def set_is_leader(self, value: bool) -> None:
        """Update the current node's view of it's own leadership status.

        Will update the expiry timestamp to match the dcs ttl if setting leadership to true,
        otherwise will set the expiry to the past to immediately invalidate.

        :param value: is the current node the leader.
        """
        with self._leader_expiry_lock:
            self._leader_expiry = time.time() + self.dcs.ttl if value else 0
            if not value:
                self._promote_timestamp = 0

    def sync_mode_is_active(self) -> bool:
        """Check whether synchronous replication is requested and already active.

        :returns: ``True`` if the primary already put its name into the ``/sync`` in DCS.
        """
        return self.is_synchronous_mode() and not self.cluster.sync.is_empty

    def quorum_commit_mode_is_active(self) -> bool:
        """Checks whether quorum replication is requested and already active.

        :returns: ``True`` if the primary already put its name into the ``/sync`` in DCS.
        """
        return self.is_quorum_commit_mode() and not self.cluster.sync.is_empty

    def _get_failover_action_name(self) -> str:
        """Return the currently requested manual failover action name or the default ``failover``.

        :returns: :class:`str` representing the manually requested action (``manual failover`` if no leader
            is specified in the ``/failover`` in DCS, ``switchover`` otherwise) or ``failover`` if
            ``/failover`` is empty.
        """
        if not self.cluster.failover:
            return 'failover'
        return 'switchover' if self.cluster.failover.leader else 'manual failover'

    def load_cluster_from_dcs(self) -> None:
        cluster = self.dcs.get_cluster()

        # We want to keep the state of cluster when it was healthy
        if not cluster.is_unlocked() or not self.old_cluster:
            self.old_cluster = cluster
        self.cluster = cluster

        if self.cluster.is_unlocked() and self.is_failsafe_mode():
            # If failsafe mode is enabled we want to inject the "real" leader to the cluster
            self.cluster = cluster = self._failsafe.update_cluster(cluster)

        if not self.has_lock(False):
            self.set_is_leader(False)

        self._leader_timeline = cluster.leader.timeline if cluster.leader else None

    def acquire_lock(self) -> bool:
        try:
            ret = self.dcs.acquire_leader_lock()
        except DCSError:
            raise
        except Exception:
            logger.exception('Unexpected exception raised from acquire_leader_lock, please report it as a BUG')
            ret = False
        self.set_is_leader(ret)
        return ret

    def _failsafe_config(self) -> Optional[Dict[str, str]]:
        if self.is_failsafe_mode():
            ret = {m.name: m.api_url for m in self.cluster.members if m.api_url}
            if self.state_handler.name not in ret:
                ret[self.state_handler.name] = self.patroni.api.connection_string
            return ret

    def update_lock(self, update_status: bool = False) -> bool:
        """Update the leader lock in DCS.

        .. note::
            After successful update of the leader key the :meth:`AbstractDCS.update_leader` method could also
            optionally update the ``/status`` and ``/failsafe`` keys.

            The ``/status`` key contains the last known LSN on the leader node and the last known state
            of permanent replication slots including permanent physical replication slot for the leader.

            Last, but not least, this method calls a :meth:`Watchdog.keepalive` method after the leader key
            was successfully updated.

        :param update_status: ``True`` if we also need to update the ``/status`` key in DCS, otherwise ``False``.

        :returns: ``True`` if the leader key was successfully updated and we can continue to run postgres
                  as a ``primary`` or as a ``standby_leader``, otherwise ``False``.
        """
        last_lsn = slots = None
        if update_status:
            try:
                last_lsn = self._last_wal_lsn = self.state_handler.last_operation()
                slots = self.cluster.maybe_filter_permanent_slots(self.state_handler, self.state_handler.slots())
            except Exception:
                logger.exception('Exception when called state_handler.last_operation()')
        try:
            ret = self.dcs.update_leader(self.cluster, last_lsn, slots, self._failsafe_config())
        except DCSError:
            raise
        except Exception:
            logger.exception('Unexpected exception raised from update_leader, please report it as a BUG')
            ret = False
        self.set_is_leader(ret)
        if ret:
            self.watchdog.keepalive()
        return ret

    def has_lock(self, info: bool = True) -> bool:
        lock_owner = self.cluster.leader and self.cluster.leader.name
        if info:
            logger.info('Lock owner: %s; I am %s', lock_owner, self.state_handler.name)
        return lock_owner == self.state_handler.name

    def get_effective_tags(self) -> Dict[str, Any]:
        """Return configuration tags merged with dynamically applied tags."""
        tags = self.patroni.tags.copy()
        # _disable_sync could be modified concurrently, but we don't care as attribute get and set are atomic.
        if self._disable_sync > 0:
            tags['nosync'] = True
            tags['sync_priority'] = 0
        return tags

    def notify_mpp_coordinator(self, event: str) -> None:
        """Send an event to the MPP coordinator.

        :param event: the type of event for coordinator to parse.
        """
        mpp_handler = self.state_handler.mpp_handler
        if mpp_handler.is_worker():
            coordinator = self.dcs.get_mpp_coordinator()
            if coordinator and coordinator.leader and coordinator.leader.conn_url:
                try:
                    data = {'type': event,
                            'group': mpp_handler.group,
                            'leader': self.state_handler.name,
                            'timeout': self.dcs.ttl,
                            'cooldown': self.patroni.config['retry_timeout']}
                    timeout = self.dcs.ttl if event == 'before_demote' else 2
                    endpoint = 'citus' if mpp_handler.type == 'Citus' else 'mpp'
                    self.patroni.request(coordinator.leader.member, 'post', endpoint, data, timeout=timeout, retries=0)
                except Exception as e:
                    logger.warning('Request to %s coordinator leader %s %s failed: %r', mpp_handler.type,
                                   coordinator.leader.name, coordinator.leader.member.api_url, e)

    def touch_member(self) -> bool:
        with self._member_state_lock:
            data: Dict[str, Any] = {
                'conn_url': self.state_handler.connection_string,
                'api_url': self.patroni.api.connection_string,
                'state': self.state_handler.state,
                'role': self.state_handler.role,
                'version': self.patroni.version
            }

            proxy_url = self.state_handler.proxy_url
            if proxy_url:
                data['proxy_url'] = proxy_url

            if self.is_leader() and not self._rewind.checkpoint_after_promote():
                data['checkpoint_after_promote'] = False
            tags = self.get_effective_tags()
            if tags:
                data['tags'] = tags
            if self.state_handler.pending_restart_reason:
                data['pending_restart'] = True
                data['pending_restart_reason'] = dict(self.state_handler.pending_restart_reason)
            if self._async_executor.scheduled_action in (None, 'promote') \
                    and data['state'] in [PostgresqlState.RUNNING, PostgresqlState.RESTARTING,
                                          PostgresqlState.STARTING]:
                try:
                    timeline, wal_position, pg_control_timeline, receive_lsn, replay_lsn =\
                        self.state_handler.timeline_wal_position()
                    data['xlog_location'] = self._last_wal_lsn = wal_position
                    if not timeline:  # running as a standby
                        if replay_lsn:
                            data['replay_lsn'] = replay_lsn
                        if receive_lsn:
                            data['receive_lsn'] = receive_lsn
                        replication_state = self.state_handler.replication_state()
                        if replication_state:
                            data['replication_state'] = replication_state
                        # try pg_stat_wal_receiver to get the timeline
                        timeline = self.state_handler.received_timeline()
                        if timeline:
                            self._last_timeline = timeline
                    if not timeline:
                        # So far the only way to get the current timeline on the standby is from
                        # the replication connection. In order to avoid opening the replication
                        # connection on every iteration of HA loop we will do it only when noticed
                        # that the timeline on the primary has changed.
                        # Unfortunately such optimization isn't possible on the standby_leader,
                        # therefore we will get the timeline from pg_control, either by calling
                        # pg_control_checkpoint() on 9.6+ or by parsing the output of pg_controldata.
                        if self.state_handler.role == PostgresqlRole.STANDBY_LEADER:
                            timeline = pg_control_timeline or self.state_handler.pg_control_timeline()
                        else:
                            timeline = self.state_handler.replica_cached_timeline(self._leader_timeline) or 0
                        if timeline:
                            self._last_timeline = timeline
                    if timeline:
                        data['timeline'] = timeline
                except Exception:
                    pass
            if self.patroni.scheduled_restart:
                scheduled_restart_data = self.patroni.scheduled_restart.copy()
                scheduled_restart_data['schedule'] = scheduled_restart_data['schedule'].isoformat()
                data['scheduled_restart'] = scheduled_restart_data

            if self.is_paused():
                data['pause'] = True

            ret = self.dcs.touch_member(data)
            if ret:
                new_state = (data['state'], data['role'])
                if self._last_state != new_state and new_state == (PostgresqlState.RUNNING, PostgresqlRole.PRIMARY):
                    self.notify_mpp_coordinator('after_promote')
                self._last_state = new_state
            return ret

    def clone(self, clone_member: Union[Leader, Member, None] = None, msg: str = '(without leader)',
              clone_from_leader: bool = False) -> Optional[bool]:
        if self.is_standby_cluster() and not isinstance(clone_member, RemoteMember):
            clone_member = self.get_remote_member(clone_member)

        self._rewind.reset_state()
        if self.state_handler.bootstrap.clone(clone_member, clone_from_leader):
            logger.info('bootstrapped %s', msg)
            cluster = self.dcs.get_cluster()
            node_to_follow = self._get_node_to_follow(cluster)
            return self.state_handler.follow(node_to_follow) is not False
        else:
            logger.error('failed to bootstrap %s', msg)
            self.state_handler.remove_data_directory()

    def bootstrap(self) -> str:
        # no initialize key and node is allowed to be primary and has 'bootstrap' section in a configuration file
        if self.cluster.is_unlocked() and self.cluster.initialize is None\
                and not self.patroni.nofailover and 'bootstrap' in self.patroni.config:
            if self.dcs.initialize(create_new=True):  # race for initialization
                self.state_handler.bootstrapping = True
                with self._async_response:
                    self._async_response.reset()

                if self.is_standby_cluster():
                    ret = self._async_executor.try_run_async('bootstrap_standby_leader', self.bootstrap_standby_leader)
                    return ret or 'trying to bootstrap a new standby leader'
                else:
                    ret = self._async_executor.try_run_async('bootstrap', self.state_handler.bootstrap.bootstrap,
                                                             args=(self.patroni.config['bootstrap'],))
                    return ret or 'trying to bootstrap a new cluster'
            else:
                return 'failed to acquire initialize lock'

        clone_member = self.cluster.get_clone_member(self.state_handler.name)
        # cluster already has a leader, we can bootstrap from it or from one of replicas (if they allow)
        if not self.cluster.is_unlocked() and clone_member:
            member_role = 'leader' if clone_member == self.cluster.leader else 'replica'
            msg = "from {0} '{1}'".format(member_role, clone_member.name)
            ret = self._async_executor.try_run_async('bootstrap {0}'.format(msg), self.clone, args=(clone_member, msg))
            return ret or 'trying to bootstrap {0}'.format(msg)

        # no leader, but configuration may allowed replica creation using backup tools
        create_replica_methods = global_config.get_standby_cluster_config().get('create_replica_methods', []) \
            if self.is_standby_cluster() else None
        can_bootstrap = self.state_handler.can_create_replica_without_replication_connection(create_replica_methods)
        concurrent_bootstrap = self.cluster.initialize == ""
        if can_bootstrap and not concurrent_bootstrap:
            msg = 'bootstrap (without leader)'
            return self._async_executor.try_run_async(msg, self.clone) or 'trying to ' + msg
        return 'waiting for {0}leader to bootstrap'.format('standby_' if self.is_standby_cluster() else '')

    def bootstrap_standby_leader(self) -> Optional[bool]:
        """ If we found 'standby' key in the configuration, we need to bootstrap
            not a real primary, but a 'standby leader', that will take base backup
            from a remote member and start follow it.
        """
        clone_source = self.get_remote_member()
        msg = 'clone from remote member {0}'.format(clone_source.conn_url)
        result = self.clone(clone_source, msg)
        with self._async_response:  # pretend that post_bootstrap was already executed
            self._async_response.complete(result)
        if result:
            self.state_handler.set_role(PostgresqlRole.STANDBY_LEADER)

        return result

    def _handle_crash_recovery(self) -> Optional[str]:
        if self._crash_recovery_started == 0 and (self.cluster.is_unlocked() or self._rewind.can_rewind):
            self._crash_recovery_started = time.time()
            msg = 'doing crash recovery in a single user mode'
            return self._async_executor.try_run_async(msg, self._rewind.ensure_clean_shutdown) or msg

    def _handle_rewind_or_reinitialize(self) -> Optional[str]:
        leader = self.get_remote_member() if self.is_standby_cluster() else self.cluster.leader
        if not self._rewind.rewind_or_reinitialize_needed_and_possible(leader) or not leader:
            return None

        if self._rewind.can_rewind:
            # rewind is required, but postgres wasn't shut down cleanly.
            if not self.state_handler.is_running() and \
                    self.state_handler.controldata().get('Database cluster state') == 'in archive recovery':
                msg = self._handle_crash_recovery()
                if msg:
                    return msg

            msg = 'running pg_rewind from ' + leader.name
            return self._async_executor.try_run_async(msg, self._rewind.execute, args=(leader,)) or msg

        if self._rewind.should_remove_data_directory_on_diverged_timelines and not self.is_standby_cluster():
            msg = 'reinitializing due to diverged timelines'
            return self._async_executor.try_run_async(msg, self._do_reinitialize, args=(self.cluster,)) or msg

    def recover(self) -> str:
        """Handle the case when postgres isn't running.

        Depending on the state of Patroni, DCS cluster view, and pg_controldata the following could happen:

          - if ``primary_start_timeout`` is 0 and this node owns the leader lock, the lock
            will be voluntarily released if there are healthy replicas to take it over.

          - if postgres was running as a ``primary`` and this node owns the leader lock, postgres is started as primary.

          - crash recover in a single-user mode is executed in the following cases:

            - postgres was running as ``primary`` wasn't ``shut down`` cleanly and there is no leader in DCS

            - postgres was running as ``replica`` wasn't ``shut down in recovery`` (cleanly)
              and we need to run ``pg_rewind`` to join back to the cluster.

          - ``pg_rewind`` is executed if it is necessary, or optionally, the data directory could
             be removed if it is allowed by configuration.

          - after ``crash recovery`` and/or ``pg_rewind`` are executed, postgres is started in recovery.

        :returns: action message, describing what was performed.
        """
        if self.has_lock() and self.update_lock():
            timeout = global_config.primary_start_timeout
            if timeout == 0:
                # We are requested to prefer failing over to restarting primary. But see first if there
                # is anyone to fail over to.
                if self.is_failover_possible():
                    self.watchdog.disable()
                    logger.info("Primary crashed. Failing over.")
                    self.demote('immediate')
                    return 'stopped PostgreSQL to fail over after a crash'
        else:
            timeout = None

        data = self.state_handler.controldata()
        logger.info('pg_controldata:\n%s\n', '\n'.join('  {0}: {1}'.format(k, v) for k, v in data.items()))

        # timeout > 0 indicates that we still have the leader lock, and it was just updated
        if timeout\
                and data.get('Database cluster state') in ('in production', 'in crash recovery',
                                                           'shutting down', 'shut down')\
                and self.state_handler.state == PostgresqlState.CRASHED\
                and self.state_handler.role == PostgresqlRole.PRIMARY\
                and not self.state_handler.config.recovery_conf_exists():
            # We know 100% that we were running as a primary a few moments ago, therefore could just start postgres
            msg = 'starting primary after failure'
            if self._async_executor.try_run_async(msg, self.state_handler.start,
                                                  args=(timeout, self._async_executor.critical_task)) is None:
                self.recovering = True
                return msg

        # Postgres is not running, and we will restart in standby mode. Watchdog is not needed until we promote.
        self.watchdog.disable()

        if data.get('Database cluster state') in ('in production', 'shutting down', 'in crash recovery'):
            msg = self._handle_crash_recovery()
            if msg:
                return msg

        self.load_cluster_from_dcs()

        role = PostgresqlRole.REPLICA
        if self.has_lock() and not self.is_standby_cluster():
            self._rewind.reset_state()  # we want to later trigger CHECKPOINT after promote
            msg = "starting as readonly because i had the session lock"
            node_to_follow = None
        else:
            if not self._rewind.executed:
                self._rewind.trigger_check_diverged_lsn()
            msg = self._handle_rewind_or_reinitialize()
            if msg:
                return msg

            if self.has_lock():  # in standby cluster
                msg = "starting as a standby leader because i had the session lock"
                role = PostgresqlRole.STANDBY_LEADER
                node_to_follow = self._get_node_to_follow(self.cluster)
            elif self.is_standby_cluster() and self.cluster.is_unlocked():
                msg = "trying to follow a remote member because standby cluster is unhealthy"
                node_to_follow = self.get_remote_member()
            else:
                msg = "starting as a secondary"
                node_to_follow = self._get_node_to_follow(self.cluster)

            if self.is_synchronous_mode():
                self.state_handler.sync_handler.set_synchronous_standby_names(CaseInsensitiveSet())

        if self._async_executor.try_run_async('restarting after failure', self.state_handler.follow,
                                              args=(node_to_follow, role, timeout)) is None:
            self.recovering = True
        return msg

    def _get_node_to_follow(self, cluster: Cluster) -> Union[Leader, Member, None]:
        """Determine the node to follow.

        :param cluster: the currently known cluster state from DCS.

        :returns: the node which we should be replicating from.
        """
        # nostream is set, the node must not use WAL streaming
        if self.patroni.nostream:
            return None
        # The standby leader or when there is no standby leader we want to follow
        # the remote member, except when there is no standby leader in pause.
        elif self.is_standby_cluster() \
                and (cluster.leader and cluster.leader.name and cluster.leader.name == self.state_handler.name
                     or cluster.is_unlocked() and not self.is_paused()):
            node_to_follow = self.get_remote_member()
        # If replicatefrom tag is set, try to follow the node mentioned there, otherwise, follow the leader.
        elif self.patroni.replicatefrom and self.patroni.replicatefrom != self.state_handler.name:
            node_to_follow = cluster.get_member(self.patroni.replicatefrom)
        else:
            node_to_follow = cluster.leader if cluster.leader and cluster.leader.name else None

        node_to_follow = node_to_follow if node_to_follow and node_to_follow.name != self.state_handler.name else None

        if node_to_follow and not isinstance(node_to_follow, RemoteMember):
            # we are going to abuse Member.data to pass following parameters
            params = ('restore_command', 'archive_cleanup_command')
            for param in params:  # It is highly unlikely to happen, but we want to protect from the case
                node_to_follow.data.pop(param, None)  # when above-mentioned params came from outside.
            if self.is_standby_cluster():
                standby_config = global_config.get_standby_cluster_config()
                node_to_follow.data.update({p: standby_config[p] for p in params if standby_config.get(p)})

        return node_to_follow

    def follow(self, demote_reason: str, follow_reason: str, refresh: bool = True) -> str:
        if refresh:
            self.load_cluster_from_dcs()

        is_primary = self.state_handler.is_primary()

        node_to_follow = self._get_node_to_follow(self.cluster)

        if self.is_paused():
            if not (self._rewind.is_needed and self._rewind.can_rewind_or_reinitialize_allowed)\
                    or self.cluster.is_unlocked():
                if is_primary:
                    self.state_handler.set_role(PostgresqlRole.PRIMARY)
                    return 'continue to run as primary without lock'
                elif self.state_handler.role != PostgresqlRole.STANDBY_LEADER:
                    self.state_handler.set_role(PostgresqlRole.REPLICA)

                if not node_to_follow:
                    return 'no action. I am ({0})'.format(self.state_handler.name)
        elif is_primary:
            if self.is_standby_cluster():
                self._async_executor.try_run_async('demoting to a standby cluster', self.demote, ('demote-cluster',))
            else:
                self.demote('immediate-nolock')
            return demote_reason

        if self.is_standby_cluster() and self._leader_timeline and \
                self.state_handler.get_history(self._leader_timeline + 1):
            self._rewind.trigger_check_diverged_lsn()

        if not self.state_handler.is_starting():
            msg = self._handle_rewind_or_reinitialize()
            if msg:
                return msg

        if not self.is_paused():
            self.state_handler.handle_parameter_change()

        role = PostgresqlRole.STANDBY_LEADER \
            if isinstance(node_to_follow, RemoteMember) and self.has_lock(False) else PostgresqlRole.REPLICA
        # It might happen that leader key in the standby cluster references non-exiting member.
        # In this case it is safe to continue running without changing recovery.conf
        if self.is_standby_cluster() and role == PostgresqlRole.REPLICA \
                and not (node_to_follow and node_to_follow.conn_url):
            return 'continue following the old known standby leader'
        else:
            change_required, restart_required = self.state_handler.config.check_recovery_conf(node_to_follow)
            if change_required:
                if restart_required:
                    self._async_executor.try_run_async('changing primary_conninfo and restarting',
                                                       self.state_handler.follow, args=(node_to_follow, role))
                else:
                    self.state_handler.follow(node_to_follow, role, do_reload=True)
                self._rewind.trigger_check_diverged_lsn()
            else:
                if role == PostgresqlRole.STANDBY_LEADER and self.state_handler.role != role:
                    self.state_handler.set_role(role)
                    self.state_handler.call_nowait(CallbackAction.ON_ROLE_CHANGE)

                if self._last_timeline and self._leader_timeline and self._last_timeline < self._leader_timeline:
                    self._rewind.trigger_check_diverged_lsn()
                    if not self.state_handler.is_starting():
                        msg = self._handle_rewind_or_reinitialize()
                        if msg:
                            return msg

        return follow_reason

    def is_synchronous_mode(self) -> bool:
        """:returns: `True` if synchronous replication is requested."""
        return global_config.is_synchronous_mode

    def is_quorum_commit_mode(self) -> bool:
        """``True`` if quorum commit replication is requested and "supported"."""
        return global_config.is_quorum_commit_mode and self.state_handler.supports_multiple_sync

    def is_failsafe_mode(self) -> bool:
        """:returns: `True` if failsafe_mode is enabled in global configuration."""
        return global_config.check_mode('failsafe_mode')

    def _maybe_enable_synchronous_mode(self) -> Optional[SyncState]:
        """Explicitly enable synchronous mode if not yet enabled.

        We are trying to solve a corner case: synchronous mode needs to be explicitly enabled
        by updating the ``/sync`` key with the current leader name and empty members. In opposite
        case it will never be automatically enabled if there are no eligible candidates.

        :returns: the latest version of :class:`~patroni.dcs.SyncState` object.
        """
        sync = self.cluster.sync
        if sync.is_empty:
            sync = self.dcs.write_sync_state(self.state_handler.name, None, 0, version=sync.version)
            if sync:
                logger.info("Enabled synchronous replication")
            else:
                logger.warning("Updating sync state failed")
        return sync

    def disable_synchronous_replication(self) -> None:
        """Cleans up ``/sync`` key in DCS and updates ``synchronous_standby_names``.

        .. note::
            We fall back to using the value configured by the user for ``synchronous_standby_names``, if any.
        """
        # If synchronous_mode was turned off, we need to update synchronous_standby_names in Postgres
        if not self.cluster.sync.is_empty and self.dcs.delete_sync_state(version=self.cluster.sync.version):
            logger.info("Disabled synchronous replication")
            self.state_handler.sync_handler.set_synchronous_standby_names(CaseInsensitiveSet())

        # As synchronous_mode is off, check if the user configured Postgres synchronous replication instead
        ssn = self.state_handler.config.synchronous_standby_names
        self.state_handler.config.set_synchronous_standby_names(ssn)

    def _process_quorum_replication(self) -> None:
        """Process synchronous replication state when quorum commit is requested.

        Synchronous standbys are registered in two places: ``postgresql.conf`` and DCS. The order of updating them must
        keep the invariant that ``quorum + sync >= len(set(quorum pool)|set(sync pool))``. This is done using
        :class:`QuorumStateResolver` that given a current state and set of desired synchronous nodes and replication
        level outputs changes to DCS and synchronous replication in correct order to reach the desired state.
        In case any of those steps causes an error we can just bail out and let next iteration rediscover the state
        and retry necessary transitions.
        """
        start_time = time.time()

        min_sync = global_config.min_synchronous_nodes
        sync_wanted = global_config.synchronous_node_count

        sync = self._maybe_enable_synchronous_mode()
        if not sync or not sync.leader:
            return

        leader = sync.leader

        def _check_timeout(offset: float = 0) -> bool:
            return time.time() - start_time + offset >= self.dcs.loop_wait

        while True:
            transition = 'break'  # we need define transition value if `QuorumStateResolver` produced no changes
            sync_state = self.state_handler.sync_handler.current_state(self.cluster)
            for transition, leader, num, nodes in QuorumStateResolver(leader=leader,
                                                                      quorum=sync.quorum,
                                                                      voters=sync.voters,
                                                                      numsync=sync_state.numsync,
                                                                      sync=sync_state.sync,
                                                                      numsync_confirmed=len(sync_state.sync_confirmed),
                                                                      active=sync_state.active,
                                                                      sync_wanted=sync_wanted,
                                                                      leader_wanted=self.state_handler.name):
                if _check_timeout():
                    return

                if transition == 'quorum':
                    logger.info("Setting leader to %s, quorum to %d of %d (%s)",
                                leader, num, len(nodes), ", ".join(sorted(nodes)))
                    sync = self.dcs.write_sync_state(leader, nodes, num, version=sync.version)
                    if not sync:
                        return logger.info('Synchronous replication key updated by someone else.')
                elif transition == 'sync':
                    logger.info("Setting synchronous replication to %d of %d (%s)",
                                num, len(nodes), ", ".join(sorted(nodes)))
                    # Bump up number of num nodes to meet minimum replication factor. Commits will have to wait until
                    # we have enough nodes to meet replication target.
                    if num < min_sync:
                        logger.warning("Replication factor %d requested, but %d synchronous standbys available."
                                       " Commits will be delayed.", min_sync + 1, num)
                        num = min_sync
                    self.state_handler.sync_handler.set_synchronous_standby_names(nodes, num)
            if transition != 'restart' or _check_timeout(1):
                return
            # synchronous_standby_names was transitioned from empty to non-empty and it may take
            # some time for nodes to become synchronous. In this case we want to restart state machine
            # hoping that we can update /sync key earlier than in loop_wait seconds.
            time.sleep(1)
            self.state_handler.reset_cluster_info_state(None)

    def _process_multisync_replication(self) -> None:
        """Process synchronous replication state with one or more sync standbys.

        Synchronous standbys are registered in two places postgresql.conf and DCS. The order of updating them must
        be right. The invariant that should be kept is that if a node is primary and sync_standby is set in DCS,
        then that node must have synchronous_standby set to that value. Or more simple, first set in postgresql.conf
        and then in DCS. When removing, first remove in DCS, then in postgresql.conf. This is so we only consider
        promoting standbys that were guaranteed to be replicating synchronously.
        """
        sync = self._maybe_enable_synchronous_mode()
        if not sync:
            return

        current_state = self.state_handler.sync_handler.current_state(self.cluster)
        picked = current_state.active
        allow_promote = current_state.sync_confirmed
        voters = CaseInsensitiveSet(sync.voters)

        if self.state_handler.name != sync.leader:
            logger.warning("Inconsistent state of /sync key detected, leader = %s doesn't match %s, "
                           "updating synchronous replication key", sync.leader, self.state_handler.name)
            sync = self.dcs.write_sync_state(self.state_handler.name, None, 0, version=sync.version)
            if not sync:
                return logger.warning("Updating sync state failed")
            voters = CaseInsensitiveSet()

        if picked == voters and voters != allow_promote:
            logger.warning('Inconsistent state between synchronous_standby_names = %s and /sync = %s key '
                           'detected, updating synchronous replication key...', list(allow_promote), list(voters))
            sync = self.dcs.write_sync_state(self.state_handler.name, allow_promote, 0, version=sync.version)
            if not sync:
                return logger.warning("Updating sync state failed")
            voters = CaseInsensitiveSet(sync.voters)

        if picked == voters == current_state.sync and current_state.numsync == len(picked):
            return

        # update synchronous standby list in dcs temporarily to point to common nodes in current and picked
        sync_common = voters & allow_promote
        if sync_common != voters:
            logger.info("Updating synchronous privilege temporarily from %s to %s",
                        list(voters), list(sync_common))
            sync = self.dcs.write_sync_state(self.state_handler.name, sync_common, 0, version=sync.version)
            if not sync:
                return logger.info('Synchronous replication key updated by someone else.')

        # When strict mode and no suitable replication connections put "*" to synchronous_standby_names
        if global_config.is_synchronous_mode_strict and not picked:
            picked = CaseInsensitiveSet('*')
            logger.warning("No standbys available!")

        # Update postgresql.conf and wait 2 secs for changes to become active
        logger.info("Assigning synchronous standby status to %s", list(picked))
        self.state_handler.sync_handler.set_synchronous_standby_names(picked)

        if picked and picked != CaseInsensitiveSet('*') and allow_promote != picked:
            # Wait for PostgreSQL to enable synchronous mode and see if we can immediately set sync_standby
            time.sleep(2)
            allow_promote = self.state_handler.sync_handler.current_state(self.cluster).sync_confirmed

        if allow_promote and allow_promote != sync_common:
            if self.dcs.write_sync_state(self.state_handler.name, allow_promote, 0, version=sync.version):
                logger.info("Synchronous standby status assigned to %s", list(allow_promote))
            else:
                logger.info("Synchronous replication key updated by someone else")

    def process_sync_replication(self) -> None:
        """Process synchronous replication behavior on the primary."""
        if self.is_quorum_commit_mode():
            # The synchronous_standby_names was adjusted right before promote.
            # After that, when postgres has become a primary, we need to reflect this change
            # in the /sync key. Further changes of synchronous_standby_names and /sync key should
            # be postponed for `loop_wait` seconds, to give a chance to some replicas to start streaming.
            # In opposite case the /sync key will end up without synchronous nodes.
            if self.state_handler.is_primary():
                if self._promote_timestamp == 0 or time.time() - self._promote_timestamp > self.dcs.loop_wait:
                    self._process_quorum_replication()
                if self._promote_timestamp == 0:
                    self._promote_timestamp = time.time()
        elif self.is_synchronous_mode():
            self._process_multisync_replication()
        else:
            self.disable_synchronous_replication()

    def process_sync_replication_prepromote(self) -> bool:
        """Handle sync replication state before promote.

        If quorum replication is requested, and we can keep syncing to enough nodes satisfying the quorum invariant
        we can promote immediately and let normal quorum resolver process handle any membership changes later.
        Otherwise, we will just reset DCS state to ourselves and add replicas as they connect.

        :returns: ``True`` if on success or ``False`` if failed to update /sync key in DCS.
        """
        if not self.is_synchronous_mode():
            self.disable_synchronous_replication()
            return True

        if self.quorum_commit_mode_is_active():
            sync = CaseInsensitiveSet(self.cluster.sync.members)
            numsync = len(sync) - self.cluster.sync.quorum - 1
            if self.state_handler.name not in sync:  # Node outside voters achieved quorum and got leader
                numsync += 1
            else:
                sync.discard(self.state_handler.name)
        else:
            sync = CaseInsensitiveSet()
            numsync = global_config.min_synchronous_nodes

        if not self.is_quorum_commit_mode() or not self.state_handler.supports_multiple_sync and numsync > 1:
            sync = CaseInsensitiveSet()
            numsync = global_config.min_synchronous_nodes

            # Just set ourselves as the authoritative source of truth for now. We don't want to wait for standbys
            # to connect. We will try finding a synchronous standby in the next cycle.
            if not self.dcs.write_sync_state(self.state_handler.name, None, 0, version=self.cluster.sync.version):
                return False

        self.state_handler.sync_handler.set_synchronous_standby_names(sync, numsync)
        return True

    def is_sync_standby(self, cluster: Cluster) -> bool:
        """:returns: `True` if the current node is a synchronous standby."""
        return bool(cluster.leader) and cluster.sync.leader_matches(cluster.leader.name) \
            and cluster.sync.matches(self.state_handler.name)

    def while_not_sync_standby(self, func: Callable[..., Any]) -> Any:
        """Runs specified action while trying to make sure that the node is not assigned synchronous standby status.

        When running in ``synchronous_mode`` with ``synchronous_node_count = 2``, shutdown or restart of a
        synchronous standby may cause a write downtime. Therefore we need to signal a primary that we don't want
        to by synchronous anymore and wait until it will replace our name from ``synchronous_standby_names``
        and ``/sync`` key in DCS with some other node. Once current node is not synchronous we will run the *func*.

        .. note::
            If the connection to DCS fails we run the *func* anyway, as this is only a hint.

            There is a small race window where this function runs between a primary picking us the sync standby
            and publishing it to the DCS. As the window is rather tiny consequences are holding up commits for
            one cycle period we don't worry about it here.

        :param func: the function to be executed.

        :returns: a return value of the *func*.
        """
        if self.is_leader() or not self.is_synchronous_mode() or self.patroni.nosync:
            return func()

        with self._member_state_lock:
            self._disable_sync += 1
        try:
            if self.touch_member():
                # Primary should notice the updated value during the next cycle. We will wait double that, if primary
                # hasn't noticed the value by then not disabling sync replication is not likely to matter.
                for _ in polling_loop(timeout=self.dcs.loop_wait * 2, interval=2):
                    try:
                        if not self.is_sync_standby(self.dcs.get_cluster()):
                            break
                    except DCSError:
                        logger.warning("Could not get cluster state, skipping synchronous standby disable")
                        break
                    logger.info("Waiting for primary to release us from synchronous standby")
            else:
                logger.warning("Updating member state failed, skipping synchronous standby disable")

            return func()
        finally:
            with self._member_state_lock:
                self._disable_sync -= 1

    def update_cluster_history(self) -> None:
        primary_timeline = self.state_handler.get_primary_timeline()
        cluster_history = self.cluster.history.lines if self.cluster.history else []
        if primary_timeline == 1:
            if cluster_history:
                self.dcs.set_history_value('[]')
        elif not cluster_history or cluster_history[-1][0] != primary_timeline - 1 or len(cluster_history[-1]) != 5:
            cluster_history_dict: Dict[int, List[Any]] = {line[0]: list(line) for line in cluster_history}
            history: List[List[Any]] = list(map(list, self.state_handler.get_history(primary_timeline)))
            if self.cluster.config:
                history = history[-global_config.max_timelines_history:]
            for line in history:
                # enrich current history with promotion timestamps stored in DCS
                cluster_history_line = cluster_history_dict.get(line[0], [])
                if len(line) == 3 and len(cluster_history_line) >= 4 and cluster_history_line[1] == line[1]:
                    line.append(cluster_history_line[3])
                    if len(cluster_history_line) == 5:
                        line.append(cluster_history_line[4])
            if history:
                self.dcs.set_history_value(json.dumps(history, separators=(',', ':')))

    def enforce_follow_remote_member(self, message: str) -> str:
        demote_reason = 'cannot be a real primary in standby cluster'
        return self.follow(demote_reason, message)

    def enforce_primary_role(self, message: str, promote_message: str) -> str:
        """
        Ensure the node that has won the race for the leader key meets criteria
        for promoting its PG server to the 'primary' role.
        """
        if not self.is_paused():
            if not self.watchdog.is_running and not self.watchdog.activate():
                if self.state_handler.is_primary():
                    self.demote('immediate')
                    return 'Demoting self because watchdog could not be activated'
                else:
                    self.release_leader_key_voluntarily()
                    return 'Not promoting self because watchdog could not be activated'

            with self._async_response:
                if self._async_response.result is False:
                    logger.warning("Releasing the leader key voluntarily because the pre-promote script failed")
                    self._released_leader_key_timestamp = time.time()
                    self.release_leader_key_voluntarily()
                    # discard the result of the failed pre-promote script to be able to re-try promote
                    self._async_response.reset()
                    return 'Promotion cancelled because the pre-promote script failed'

        if self.state_handler.is_primary():
            # Inform the state handler about its primary role.
            # It may be unaware of it if postgres is promoted manually.
            self.state_handler.set_role(PostgresqlRole.PRIMARY)
            self.process_sync_replication()
            self.update_cluster_history()
            self.state_handler.mpp_handler.sync_meta_data(self.cluster)
            return message
        elif self.state_handler.role in (PostgresqlRole.PRIMARY, PostgresqlRole.PROMOTED):
            self.process_sync_replication()
            return message
        else:
            if not self.process_sync_replication_prepromote():
                # Somebody else updated sync state, it may be due to us losing the lock. To be safe,
                # postpone promotion until next cycle. TODO: trigger immediate retry of run_cycle.
                return 'Postponing promotion because synchronous replication state was updated by somebody else'
            if self.state_handler.role not in (PostgresqlRole.PRIMARY, PostgresqlRole.PROMOTED):
                # reset failsafe state when promote
                self._failsafe.set_is_active(0)
                self._last_timeline = None

                def before_promote():
                    self._rewind.reset_state()  # make sure we will trigger checkpoint after promote
                    self.notify_mpp_coordinator('before_promote')

                with self._async_response:
                    self._async_response.reset()

                self._async_executor.try_run_async('promote', self.state_handler.promote,
                                                   args=(self.dcs.loop_wait, self._async_response, before_promote))
            return promote_message

    def fetch_node_status(self, member: Member) -> _MemberStatus:
        """Perform http get request on member.api_url to fetch its status.

        Usually this happens during the leader race and we can't afford to wait an indefinite time
        for a response, therefore the request timeout is hardcoded to 2 seconds, which seems to be a
        good compromise. The node which is slow to respond is most likely unhealthy.

        :returns: :class:`_MemberStatus` object
        """
        try:
            response = self.patroni.request(member, timeout=2, retries=0)
            data = response.data.decode('utf-8')
            logger.info('Got response from %s %s: %s', member.name, member.api_url, data)
            return _MemberStatus.from_api_response(member, json.loads(data))
        except Exception as e:
            logger.warning("Request failed to %s: GET %s (%s)", member.name, member.api_url, e)
        return _MemberStatus.unknown(member)

    def fetch_nodes_statuses(self, members: List[Member]) -> List[_MemberStatus]:
        if not members:
            return []
        pool = ThreadPool(len(members))
        results = pool.map(self.fetch_node_status, members)  # Run API calls on members in parallel
        pool.close()
        pool.join()
        return results

    def update_failsafe(self, data: Dict[str, Any]) -> Union[int, str, None]:
        """Update failsafe state.

        :param data: deserialized JSON document from REST API call that contains information about current leader.

        :returns: the reason why caller shouldn't continue as a primary or the current value of received/replayed LSN.
        """
        if self.state_handler.state == PostgresqlState.RUNNING and self.state_handler.role == PostgresqlRole.PRIMARY:
            return 'Running as a leader'
        self._failsafe.update(data)
        return self._last_wal_lsn

    def failsafe_is_active(self) -> bool:
        return self._failsafe.is_active()

    def call_failsafe_member(self, data: Dict[str, Any], member: Member) -> _FailsafeResponse:
        """Call ``POST /failsafe`` REST API request on provided member.

        :param data: data to be send in the POST request.

        :returns: a :class:`_FailsafeResponse` object.
        """
        endpoint = 'failsafe'
        url = member.get_endpoint_url(endpoint)
        try:
            response = self.patroni.request(member, 'post', endpoint, data, timeout=2, retries=1)
            response_data = response.data.decode('utf-8')
            logger.info('Got response from %s %s: %s', member.name, url, response_data)
            accepted = response.status == 200 and response_data == 'Accepted'
            # member may return its current received/replayed LSN in the "lsn" header.
            return _FailsafeResponse(member.name, accepted, parse_int(response.headers.get('lsn')))
        except Exception as e:
            logger.warning("Request failed to %s: POST %s (%s)", member.name, url, e)
        return _FailsafeResponse(member.name, False, None)

    def check_failsafe_topology(self) -> bool:
        """Check whether we could continue to run as a primary by calling all members from the failsafe topology.

        .. note::
            If the ``/failsafe`` key contains invalid data or if the ``name`` of our node is missing in
            the ``/failsafe`` key, we immediately give up and return ``False``.

            We send the JSON document in the POST request with the following fields:

            * ``name`` - the name of our node;
            * ``conn_url`` - connection URL to the postgres, which is reachable from other nodes;
            * ``api_url`` - connection URL to Patroni REST API on this node reachable from other nodes;
            * ``slots`` - a :class:`dict` with replication slots that exist on the leader node, including the primary
              itself with the last known LSN, because there could be a permanent physical slot on standby nodes.

            Standby nodes are using information from the ``slots`` dict to advance position of permanent
            replication slots while DCS is not accessible in order to avoid indefinite growth of ``pg_wal``.

            Standby nodes are returning their received/replayed location in the ``lsn`` header, which later are
            used by the primary to advance position of replication slots that for nodes that are doing cascading
            replication from other nodes. It is required to avoid indefinite growth of ``pg_wal``.

        :returns: ``True`` if all members from the ``/failsafe`` topology agree that this node could continue to
                  run as a ``primary``, or ``False`` if some of standby nodes are not accessible or don't agree.
        """
        failsafe = self.dcs.failsafe
        if not isinstance(failsafe, dict) or self.state_handler.name not in failsafe:
            return False
        data: Dict[str, Any] = {
            'name': self.state_handler.name,
            'conn_url': self.state_handler.connection_string,
            'api_url': self.patroni.api.connection_string,
        }
        try:
            data['slots'] = self.state_handler.slots()
        except Exception:
            logger.exception('Exception when called state_handler.slots()')

        members = [RemoteMember(name, {'api_url': url})
                   for name, url in failsafe.items() if name != self.state_handler.name]
        if not members:  # A single node cluster
            return True
        pool = ThreadPool(len(members))
        call_failsafe_member = functools.partial(self.call_failsafe_member, data)
        results: List[_FailsafeResponse] = pool.map(call_failsafe_member, members)
        pool.close()
        pool.join()
        ret = all(r.accepted for r in results)
        if ret:
            # The LSN feedback will be later used to advance position of replication slots
            # for nodes that are doing cascading replication from other nodes.
            self._failsafe.update_slots({r.member_name: r.lsn for r in results if r.lsn})
        return ret

    def is_lagging(self, wal_position: int) -> bool:
        """Check if node should consider itself unhealthy to be promoted due to replication lag.

        :param wal_position: Current wal position.

        :returns: ``True`` when node is lagging
        """
        lag = self.cluster.status.last_lsn - wal_position
        return lag > global_config.maximum_lag_on_failover

    def _is_healthiest_node(self, members: Collection[Member],
                            check_replication_lag: bool = True,
                            leader: Optional[Leader] = None) -> bool:
        """Determine whether the current node is healthy enough to become a new leader candidate.

        :param members: the list of nodes to check against
        :param check_replication_lag: whether to take the replication lag into account.
                                      If the lag exceeds configured threshold the node disqualifies itself.
        :param leader: the old cluster leader, it will be used to ignore its ``failover_priority`` value.
        :returns: ``True`` if the node is eligible to become the new leader. Since this method is executed
                  on multiple nodes independently it is possible that multiple nodes could count
                  themselves as the healthiest because they received/replayed up to the same LSN,
                  but this is totally fine.
        """
        cluster_timeline = self.cluster.timeline
        my_timeline = self.state_handler.replica_cached_timeline(cluster_timeline)
        if my_timeline:
            self._last_timeline = my_timeline
        my_wal_position = self.state_handler.last_operation()
        if check_replication_lag and self.is_lagging(my_wal_position):
            logger.info('My wal position exceeds maximum replication lag')
            return False  # Too far behind last reported wal position on primary

        if not self.is_standby_cluster() and self.check_timeline():
            if my_timeline is None:
                logger.info('Can not figure out my timeline')
                return False
            if my_timeline < cluster_timeline:
                logger.info('My timeline %s is behind last known cluster timeline %s', my_timeline, cluster_timeline)
                return False

        if self.quorum_commit_mode_is_active():
            quorum = self.cluster.sync.quorum
            voting_set = CaseInsensitiveSet(self.cluster.sync.members)
        else:
            quorum = 0
            voting_set = CaseInsensitiveSet()

        # Prepare list of nodes to run check against. If quorum commit is enabled
        # we also include members with nofailover tag if they are listed in voters.
        members = [m for m in members if m.name != self.state_handler.name
                   and m.api_url and (not m.nofailover or m.name in voting_set)]

        # If there is a quorum active then at least one of the quorum contains latest commit. A quorum member saying
        # their WAL position is not ahead counts as a vote saying we may become new leader. Note that a node doesn't
        # have to be a member of the voting set to gather the necessary votes.

        # Regardless of voting, if we observe a node that can become a leader and is ahead, we defer to that node.
        # This can lead to failure to act on quorum if there is asymmetric connectivity.
        quorum_votes = 0 if self.state_handler.name in voting_set else -1
        nodes_ahead = 0

        # we need to know the name of the former leader to ignore it if it has higher failover_priority
        if self.sync_mode_is_active():
            leader_name = self.cluster.sync.leader
        else:
            leader_name = leader and leader.name

        for st in self.fetch_nodes_statuses(members):
            if st.failover_limitation() is None:
                if st.in_recovery is False:
                    logger.warning('Primary (%s) is still alive', st.member.name)
                    return False
                if my_wal_position < st.wal_position:
                    nodes_ahead += 1
                    logger.info('Wal position of %s is ahead of my wal position', st.member.name)
                    # In synchronous mode the former leader might be still accessible and even be ahead of us.
                    # We should not disqualify himself from the leader race in such a situation.
                    if not self.sync_mode_is_active() or not self.cluster.sync.leader_matches(st.member.name):
                        return False
                    logger.info('Ignoring the former leader being ahead of us')
                elif st.wal_position > 0:  # we want to count votes only from nodes with postgres up and running!
                    quorum_vote = st.member.name in voting_set
                    low_priority = my_wal_position == st.wal_position \
                        and self.patroni.failover_priority < st.failover_priority

                    if low_priority and leader_name and leader_name == st.member.name:
                        logger.info('Ignoring former leader %s having priority %s higher than this nodes %s priority',
                                    leader_name, st.failover_priority, self.patroni.failover_priority)
                        low_priority = False

                    if low_priority and (not self.sync_mode_is_active() or quorum_vote):
                        # There's a higher priority non-lagging replica
                        logger.info(
                            '%s has equally tolerable WAL position and priority %s, while this node has priority %s',
                            st.member.name, st.failover_priority, self.patroni.failover_priority)
                        return False

                    if quorum_vote:
                        logger.info('Got quorum vote from %s', st.member.name)
                        quorum_votes += 1

        # When not in quorum commit we just want to return `True`.
        # In quorum commit the former leader is special and counted healthy even when there are no other nodes.
        # Otherwise check that the number of votes exceeds the quorum field from the /sync key.
        return not self.quorum_commit_mode_is_active() or quorum_votes >= quorum\
            or nodes_ahead == 0 and self.cluster.sync.leader == self.state_handler.name

    def is_failover_possible(self, *, cluster_lsn: int = 0, exclude_failover_candidate: bool = False) -> bool:
        """Checks whether any of the cluster members is allowed to promote and is healthy enough for that.

        :param cluster_lsn: to calculate replication lag and exclude member if it is lagging.
        :param exclude_failover_candidate: if ``True``, exclude :attr:`failover.candidate` from the members
                                           list against which the failover possibility checks are run.
        :returns: `True` if there are members eligible to become the new leader.
        """
        candidates = self.get_failover_candidates(exclude_failover_candidate)

        action = self._get_failover_action_name()
        if self.is_synchronous_mode() and self.cluster.failover and self.cluster.failover.candidate and not candidates:
            logger.warning('%s candidate=%s does not match with sync_standbys=%s',
                           action.title(), self.cluster.failover.candidate, self.cluster.sync.sync_standby)
        elif not candidates:
            logger.warning('%s: candidates list is empty', action)

        quorum_votes = -1
        cluster_timeline = self.cluster.timeline
        for st in self.fetch_nodes_statuses(candidates):
            not_allowed_reason = st.failover_limitation()
            if not_allowed_reason:
                logger.info('Member %s is %s', st.member.name, not_allowed_reason)
            elif cluster_lsn and st.wal_position < cluster_lsn or \
                    not cluster_lsn and self.is_lagging(st.wal_position):
                logger.info('Member %s exceeds maximum replication lag', st.member.name)
            elif self.check_timeline() and (not st.timeline or st.timeline < cluster_timeline):
                logger.info('Timeline %s of member %s is behind the cluster timeline %s',
                            st.timeline, st.member.name, cluster_timeline)
            else:
                quorum_votes += 1

        # In case of quorum replication we need to make sure that there is enough healthy synchronous replicas!
        # However, when failover candidate is set, we can ignore quorum requirements.
        check_quorum = self.quorum_commit_mode_is_active() and\
            not (self.cluster.failover and self.cluster.failover.candidate and not exclude_failover_candidate)
        if check_quorum and quorum_votes < self.cluster.sync.quorum:
            logger.info('Quorum requirement %d can not be reached', self.cluster.sync.quorum)
            return False

        return quorum_votes >= 0

    def manual_failover_process_no_leader(self) -> Optional[bool]:
        """Handles manual failover/switchover when the old leader already stepped down.

        :returns: - `True` if the current node is the best candidate to become the new leader
                  - `None` if the current node is running as a primary and requested candidate doesn't exist
        """
        failover = self.cluster.failover
        if TYPE_CHECKING:  # pragma: no cover
            assert failover is not None

        action = self._get_failover_action_name()

        if failover.candidate:  # manual failover/switchover to specific member
            if failover.candidate == self.state_handler.name:  # manual failover/switchover to me
                return True
            elif self.is_paused():
                # Remove failover key if the node to failover has terminated to avoid waiting for it indefinitely
                # In order to avoid attempts to delete this key from all nodes only the primary is allowed to do it.
                if not self.cluster.get_member(failover.candidate, fallback_to_leader=False)\
                        and self.state_handler.is_primary():
                    logger.warning("%s: removing failover key because failover candidate is not running", action)
                    self.dcs.manual_failover('', '', version=failover.version)
                    return None
                return False

            # in synchronous mode (except quorum commit!) when our name is not in the
            # /sync key we shouldn't take any action even if the candidate is unhealthy
            if self.is_synchronous_mode() and not self.is_quorum_commit_mode()\
                    and not self.cluster.sync.matches(self.state_handler.name, True):
                return False

            # find specific node and check that it is healthy
            member = self.cluster.get_member(failover.candidate, fallback_to_leader=False)
            if isinstance(member, Member):
                st = self.fetch_node_status(member)
                not_allowed_reason = st.failover_limitation()
                if not_allowed_reason is None:  # node is healthy
                    logger.info('%s: to %s, i am %s', action, st.member.name, self.state_handler.name)
                    return False
                # we wanted to failover/switchover to specific member but it is not healthy
                logger.warning('%s: member %s is %s', action, st.member.name, not_allowed_reason)

            # at this point we should consider all members as a candidates for failover/switchover
            # i.e. we assume that failover.candidate is None
        elif self.is_paused():
            return False

        # try to pick some other members for switchover and check that they are healthy
        if failover.leader:
            if self.state_handler.name == failover.leader:  # I was the leader
                # exclude desired member which is unhealthy if it was specified
                if self.is_failover_possible(exclude_failover_candidate=bool(failover.candidate)):
                    return False
                else:  # I was the leader and it looks like currently I am the only healthy member
                    return True

            # at this point we assume that our node is a candidate for a failover among all nodes except former leader

        # exclude former leader from the list (failover.leader can be None)
        members = [m for m in self.cluster.members if m.name != failover.leader]
        return self._is_healthiest_node(members, check_replication_lag=False)

    def is_healthiest_node(self) -> bool:
        """Performs a series of checks to determine that the current node is the best candidate.

        In case if manual failover/switchover is requested it calls :func:`manual_failover_process_no_leader` method.

        :returns: `True` if the current node is among the best candidates to become the new leader.
        """
        if time.time() - self._released_leader_key_timestamp < self.dcs.ttl:
            logger.info('backoff: skip leader race after pre_promote script failure and releasing the lock voluntarily')
            return False

        if self.is_paused() and not self.patroni.nofailover and \
                self.cluster.failover and not self.cluster.failover.scheduled_at:
            ret = self.manual_failover_process_no_leader()
            if ret is not None:  # continue if we just deleted the stale failover key as a leader
                return ret

        if self.state_handler.is_primary():
            if self.is_paused():
                # in pause leader is the healthiest only when no initialize or sysid matches with initialize!
                return not self.cluster.initialize or self.state_handler.sysid == self.cluster.initialize

            # We want to protect from the following scenario:
            # 1. node1 is stressed so much that heart-beat isn't running regularly and the leader lock expires.
            # 2. node2 promotes, gets heavy load and the situation described in 1 repeats.
            # 3. Patroni on node1 comes back, notices that Postgres is running as primary but there is
            #    no leader key and "happily" acquires the leader lock.
            # That is, node1 discarded promotion of node2. To avoid it we want to detect timeline change.
            my_timeline = self.state_handler.get_primary_timeline()
            if my_timeline < self.cluster.timeline:
                logger.warning('My timeline %s is behind last known cluster timeline %s',
                               my_timeline, self.cluster.timeline)
                return False
            return True

        if self.is_paused():
            return False

        if self.patroni.nofailover:  # nofailover tag makes node always unhealthy
            return False

        if self.cluster.failover:
            # When doing a switchover in synchronous mode only synchronous nodes and former leader are allowed to race
            if self.cluster.failover.leader and self.sync_mode_is_active() \
                    and not self.cluster.sync.matches(self.state_handler.name, True):
                return False
            return self.manual_failover_process_no_leader() or False

        if not self.watchdog.is_healthy:
            logger.warning('Watchdog device is not usable')
            return False

        all_known_members = self.old_cluster.members
        if self.is_failsafe_mode():
            failsafe_members = self.dcs.failsafe
            # We want to discard failsafe_mode if the /failsafe key contains garbage or empty.
            if isinstance(failsafe_members, dict):
                # If current node is missing in the /failsafe key we immediately disqualify it from the race.
                if failsafe_members and self.state_handler.name not in failsafe_members:
                    return False
                # Race among not only existing cluster members, but also all known members from the failsafe config
                all_known_members += [RemoteMember(name, {'api_url': url}) for name, url in failsafe_members.items()]
        all_known_members += self.cluster.members

        # Special handling if synchronous mode was requested and activated (the leader in /sync is not empty)
        if self.sync_mode_is_active():
            # In quorum commit mode we allow nodes outside of "voters" to take part in
            # the leader race. They just need to get enough votes to `reach quorum + 1`.
            if not self.is_quorum_commit_mode() and not self.cluster.sync.matches(self.state_handler.name, True):
                return False
            # pick between synchronous candidates so we minimize unnecessary failovers/demotions
            members = {m.name: m for m in all_known_members if self.cluster.sync.matches(m.name, True)}
        else:
            # run usual health check
            members = {m.name: m for m in all_known_members}

        return self._is_healthiest_node(members.values(), leader=self.old_cluster.leader)

    def _delete_leader(self, last_lsn: Optional[int] = None) -> None:
        self.set_is_leader(False)
        self.dcs.delete_leader(self.cluster.leader, last_lsn)
        self.dcs.reset_cluster()

    def release_leader_key_voluntarily(self, last_lsn: Optional[int] = None) -> None:
        self._delete_leader(last_lsn)
        self.touch_member()
        logger.info("Leader key released")

    def demote(self, mode: str) -> Optional[bool]:
        """Demote PostgreSQL running as primary.

        :param mode: One of offline, graceful, immediate or immediate-nolock.
                     ``offline`` is used when connection to DCS is not available.
                     ``graceful`` is used when failing over to another node due to user request. May only be called
                     running async.
                     ``immediate`` is used when we determine that we are not suitable for primary and want to failover
                     quickly without regard for data durability. May only be called synchronously.
                     ``immediate-nolock`` is used when find out that we have lost the lock to be primary. Need to bring
                     down PostgreSQL as quickly as possible without regard for data durability. May only be called
                     synchronously.
        """
        mode_control = {
            'offline':          dict(stop='fast',      checkpoint=False, release=False, offline=True,  async_req=False),  # noqa: E241,E501
            'graceful':         dict(stop='fast',      checkpoint=True,  release=True,  offline=False, async_req=False),  # noqa: E241,E501
            'immediate':        dict(stop='immediate', checkpoint=False, release=True,  offline=False, async_req=True),  # noqa: E241,E501
            'immediate-nolock': dict(stop='immediate', checkpoint=False, release=False, offline=False, async_req=True),  # noqa: E241,E501
            'demote-cluster':   dict(stop='fast',      checkpoint=False, release=True,  offline=False,  async_req=False),  # noqa: E241,E501

        }[mode]

        logger.info('Demoting self (%s)', mode)

        self._rewind.trigger_check_diverged_lsn()

        status = {'released': False}

        demote_cluster_with_archive = False
        archive_cmd = self._rewind.get_archive_command()
        if mode == 'demote-cluster' and archive_cmd is not None:
            # We need to send the shutdown checkpoint WAL file to archive to eliminate the need of rewind
            # from a promoted instance that was previously replicating from archive
            # When doing this, we disable stop timeout, do not run on_shutdown callback and do not release
            # leader key.
            demote_cluster_with_archive = True
            mode_control['release'] = False

        def on_shutdown(checkpoint_location: int, prev_location: int) -> None:
            # Postmaster is still running, but pg_control already reports clean "shut down".
            # It could happen if Postgres is still archiving the backlog of WAL files.
            # If we know that there are replicas that received the shutdown checkpoint
            # location, we can remove the leader key and allow them to start leader race.
            time.sleep(1)  # give replicas some more time to catch up
            if self.is_failover_possible(cluster_lsn=checkpoint_location):
                self.state_handler.set_role(PostgresqlRole.DEMOTED)
                # for demotion to a standby cluster we need shutdown checkpoint lsn to be written to optime,
                # not the prev one
                last_lsn = checkpoint_location if mode == 'demote-cluster' else prev_location
                with self._async_executor:
                    self.release_leader_key_voluntarily(last_lsn)
                    status['released'] = True

        def before_shutdown() -> None:
            if self.state_handler.mpp_handler.is_coordinator():
                self.state_handler.mpp_handler.on_demote()
            else:
                self.notify_mpp_coordinator('before_demote')

        self.state_handler.stop(str(mode_control['stop']), checkpoint=bool(mode_control['checkpoint']),
                                on_safepoint=self.watchdog.disable if self.watchdog.is_running else None,
                                on_shutdown=on_shutdown if mode_control['release'] else None,
                                before_shutdown=before_shutdown if mode == 'graceful' else None,
                                stop_timeout=None if demote_cluster_with_archive else self.primary_stop_timeout())
        self.state_handler.set_role(PostgresqlRole.DEMOTED)

        # for demotion to a standby cluster we need shutdown checkpoint lsn to be written to optime, not the prev one
        checkpoint_lsn, prev_lsn = self.state_handler.latest_checkpoint_locations() \
            if mode == 'graceful' else (None, None)

        is_standby_leader = mode == 'demote-cluster' and not status['released']
        if is_standby_leader:
            with self._async_executor:
                self.dcs.update_leader(self.cluster, checkpoint_lsn, None, self._failsafe_config())
            mode_control['release'] = False
        else:
            self.set_is_leader(False)

        if mode_control['release']:
            if not status['released']:
                with self._async_executor:
                    self.release_leader_key_voluntarily(prev_lsn)
            time.sleep(2)  # Give a time to somebody to take the leader lock

        if mode == 'demote-cluster':
            if demote_cluster_with_archive:
                self._rewind.archive_shutdown_checkpoint_wal(cast(str, archive_cmd))
            else:
                logger.info('Not archiving latest checkpoint WAL file. Archiving is not configured.')

        if mode_control['offline']:
            node_to_follow, leader = None, None
        else:
            try:
                cluster = self.dcs.get_cluster()
                node_to_follow, leader = self._get_node_to_follow(cluster), cluster.leader
            except Exception:
                node_to_follow, leader = None, None

        if self.is_synchronous_mode():
            self.state_handler.sync_handler.set_synchronous_standby_names(CaseInsensitiveSet())

        role = PostgresqlRole.STANDBY_LEADER if is_standby_leader else PostgresqlRole.REPLICA
        # FIXME: with mode offline called from DCS exception handler and handle_long_action_in_progress
        # there could be an async action already running, calling follow from here will lead
        # to racy state handler state updates.
        if mode_control['async_req']:
            self._async_executor.try_run_async('starting after demotion', self.state_handler.follow,
                                               (node_to_follow, role,))
        else:
            if self._rewind.rewind_or_reinitialize_needed_and_possible(leader):
                return False  # do not start postgres, but run pg_rewind on the next iteration
            return self.state_handler.follow(node_to_follow, role)

    def should_run_scheduled_action(self, action_name: str, scheduled_at: Optional[datetime.datetime],
                                    cleanup_fn: Callable[..., Any]) -> bool:
        if scheduled_at and not self.is_paused():
            # If the scheduled action is in the far future, we shouldn't do anything and just return.
            # If the scheduled action is in the past, we consider the value to be stale and we remove
            # the value.
            # If the value is close to now, we initiate the scheduled action
            # Additionally, if the scheduled action cannot be executed altogether, i.e. there is an error
            # or the action is in the past - we take care of cleaning it up.
            now = datetime.datetime.now(tzutc)
            try:
                delta = (scheduled_at - now).total_seconds()

                if delta > self.dcs.loop_wait:
                    logger.info('Awaiting %s at %s (in %.0f seconds)',
                                action_name, scheduled_at.isoformat(), delta)
                    return False
                elif delta < - int(self.dcs.loop_wait * 1.5):
                    # This means that if run_cycle gets delayed for 2.5x loop_wait we skip the
                    # scheduled action. Probably not a problem, if things are that bad we don't
                    # want to be restarting or failing over anyway.
                    logger.warning('Found a stale %s value, cleaning up: %s',
                                   action_name, scheduled_at.isoformat())
                    cleanup_fn()
                    return False

                # The value is very close to now
                time.sleep(max(delta, 0))
                logger.info('Manual scheduled {0} at %s'.format(action_name), scheduled_at.isoformat())
                return True
            except TypeError:
                logger.warning('Incorrect value of scheduled_at: %s', scheduled_at)
                cleanup_fn()
        return False

    def process_manual_failover_from_leader(self) -> Optional[str]:
        """Checks if manual failover is requested and takes action if appropriate.

        Cleans up failover key if failover conditions are not matched.

        :returns: action message if demote was initiated, None if no action was taken"""
        failover = self.cluster.failover
        # if there is no failover key or
        # I am holding the lock but am not primary = I am the standby leader,
        # then do nothing
        if not failover or (self.is_paused() and not self.state_handler.is_primary()):
            return

        action = self._get_failover_action_name()
        bare_action = action.replace('manual ', '')

        # it is not the time for the scheduled switchover yet, do nothing
        if (failover.scheduled_at and not
            self.should_run_scheduled_action(bare_action, failover.scheduled_at, lambda:
                                             self.dcs.manual_failover('', '', version=failover.version))):
            return

        if not failover.leader or failover.leader == self.state_handler.name:
            if not failover.candidate or failover.candidate != self.state_handler.name:
                if not failover.candidate and self.is_paused():
                    logger.warning('%s is possible only to a specific candidate in a paused state', action.title())
                elif self.is_failover_possible():
                    ret = self._async_executor.try_run_async(f'{action}: demote', self.demote, ('graceful',))
                    return ret or f'{action}: demoting myself'
                else:
                    logger.warning('%s: no healthy members found, %s is not possible',
                                   action, bare_action)
            else:
                logger.warning('%s: I am already the leader, no need to %s', action, bare_action)
        else:
            logger.warning('%s: leader name does not match: %s != %s', action, failover.leader, self.state_handler.name)

        logger.info('Cleaning up failover key')
        self.dcs.manual_failover('', '', version=failover.version)

    def process_unhealthy_cluster(self) -> str:
        """Cluster has no leader key"""

        if self.is_healthiest_node():
            if self.acquire_lock():
                failover = self.cluster.failover
                if failover:
                    if self.is_paused() and failover.leader and failover.candidate:
                        logger.info('Updating failover key after acquiring leader lock...')
                        self.dcs.manual_failover('', failover.candidate, failover.scheduled_at, failover.version)
                    else:
                        logger.info('Cleaning up failover key after acquiring leader lock...')
                        self.dcs.manual_failover('', '')
                self.load_cluster_from_dcs()

                if self.is_standby_cluster():
                    # standby leader disappeared, and this is the healthiest
                    # replica, so it should become a new standby leader.
                    # This implies we need to start following a remote member
                    msg = 'promoted self to a standby leader by acquiring session lock'
                    return self.enforce_follow_remote_member(msg)
                else:
                    return self.enforce_primary_role(
                        'acquired session lock as a leader',
                        'promoted self to leader by acquiring session lock'
                    )
            else:
                return self.follow('demoted self after trying and failing to obtain lock',
                                   'following new leader after trying and failing to obtain lock')
        else:
            # when we are doing manual failover there is no guaranty that new leader is ahead of any other node
            # node tagged as nofailover can be ahead of the new leader either, but it is always excluded from elections
            if bool(self.cluster.failover) or self.patroni.nofailover:
                self._rewind.trigger_check_diverged_lsn()
                time.sleep(2)  # Give a time to somebody to take the leader lock

            if self.patroni.nofailover:
                return self.follow('demoting self because I am not allowed to become primary',
                                   'following a different leader because I am not allowed to promote')
            return self.follow('demoting self because i am not the healthiest node',
                               'following a different leader because i am not the healthiest node')

    def process_healthy_cluster(self) -> str:
        if self.has_lock():
            if self.is_paused() and not self.state_handler.is_primary():
                if self.cluster.failover and self.cluster.failover.candidate == self.state_handler.name:
                    return 'waiting to become primary after promote...'

                if not self.is_standby_cluster():
                    self._delete_leader()
                    return 'removed leader lock because postgres is not running as primary'

            # update lock to avoid split-brain
            if self.update_lock(True):
                msg = self.process_manual_failover_from_leader()
                if msg is not None:
                    return msg

                # check if the node is ready to be used by pg_rewind
                self._rewind.ensure_checkpoint_after_promote(self.wakeup)

                if self.is_standby_cluster():
                    # in case of standby cluster we don't really need to
                    # enforce anything, since the leader is not a primary
                    # So just remind the role.
                    msg = 'no action. I am ({0}), the standby leader with the lock'.format(self.state_handler.name) \
                          if self.state_handler.role == PostgresqlRole.STANDBY_LEADER else \
                          'promoted self to a standby leader because i had the session lock'
                    return self.enforce_follow_remote_member(msg)
                else:
                    return self.enforce_primary_role(
                        'no action. I am ({0}), the leader with the lock'.format(self.state_handler.name),
                        'promoted self to leader because I had the session lock'
                    )
            else:
                # Either there is no connection to DCS or someone else acquired the lock
                logger.error('failed to update leader lock')
                if self.state_handler.is_primary():
                    if self.is_paused():
                        return 'continue to run as primary after failing to update leader lock in DCS'
                    self.demote('immediate-nolock')
                    return 'demoted self because failed to update leader lock in DCS'
                else:
                    return 'not promoting because failed to update leader lock in DCS'
        else:
            logger.debug('does not have lock')
        lock_owner = self.cluster.leader and self.cluster.leader.name
        if self.is_standby_cluster():
            return self.follow('cannot be a real primary in a standby cluster',
                               'no action. I am ({0}), a secondary, and following a standby leader ({1})'.format(
                                   self.state_handler.name, lock_owner), refresh=False)
        return self.follow('demoting self because I do not have the lock and I was a leader',
                           'no action. I am ({0}), a secondary, and following a leader ({1})'.format(
                               self.state_handler.name, lock_owner), refresh=False)

    def evaluate_scheduled_restart(self) -> Optional[str]:
        if self._async_executor.busy:  # Restart already in progress
            return None

        # restart if we need to
        restart_data = self.future_restart_scheduled()
        if restart_data:
            recent_time = self.state_handler.postmaster_start_time()
            request_time = restart_data['postmaster_start_time']
            # check if postmaster start time has changed since the last restart
            if recent_time and request_time and recent_time != request_time:
                logger.info("Cancelling scheduled restart: postgres restart has already happened at %s", recent_time)
                self.delete_future_restart()
                return None

        if restart_data\
                and self.should_run_scheduled_action('restart', restart_data['schedule'], self.delete_future_restart):
            try:
                ret, message = self.restart(restart_data, run_async=True)
                if not ret:
                    logger.warning("Scheduled restart: %s", message)
                    return None
                return message
            finally:
                self.delete_future_restart()

    def restart_matches(self, role: Optional[str], postgres_version: Optional[str], pending_restart: bool) -> bool:
        reason_to_cancel = ""
        # checking the restart filters here seem to be less ugly than moving them into the
        # run_scheduled_action.
        if role and role != self.state_handler.role:
            reason_to_cancel = "host role mismatch"

        if postgres_version and postgres_version_to_int(postgres_version) <= int(self.state_handler.server_version):
            reason_to_cancel = "postgres version mismatch"

        if pending_restart and not self.state_handler.pending_restart_reason:
            reason_to_cancel = "pending restart flag is not set"

        if not reason_to_cancel:
            return True
        else:
            logger.info("not proceeding with the restart: %s", reason_to_cancel)
        return False

    def schedule_future_restart(self, restart_data: Dict[str, Any]) -> bool:
        with self._async_executor:
            restart_data['postmaster_start_time'] = self.state_handler.postmaster_start_time()
            if not self.patroni.scheduled_restart:
                self.patroni.scheduled_restart = restart_data
                self.touch_member()
                return True
        return False

    def delete_future_restart(self) -> bool:
        ret = False
        with self._async_executor:
            if self.patroni.scheduled_restart:
                self.patroni.scheduled_restart = {}
                self.touch_member()
                ret = True
        return ret

    def future_restart_scheduled(self) -> Dict[str, Any]:
        return self.patroni.scheduled_restart.copy()

    def restart_scheduled(self) -> bool:
        return self._async_executor.scheduled_action == 'restart'

    def restart(self, restart_data: Dict[str, Any], run_async: bool = False) -> Tuple[bool, str]:
        """ conditional and unconditional restart """
        assert isinstance(restart_data, dict)

        if (not self.restart_matches(restart_data.get('role'),
                                     restart_data.get('postgres_version'),
                                     ('restart_pending' in restart_data))):
            return (False, "restart conditions are not satisfied")

        with self._async_executor:
            prev = self._async_executor.schedule('restart')
            if prev is not None:
                return (False, prev + ' already in progress')

            # Make the main loop to think that we were recovering dead postgres. If we fail
            # to start postgres after a specified timeout (see below), we need to remove
            # leader key (if it belong to us) rather than trying to start postgres once again.
            self.recovering = True

        # Now that restart is scheduled we can set timeout for startup, it will get reset
        # once async executor runs and main loop notices PostgreSQL as up.
        timeout = restart_data.get('timeout', global_config.primary_start_timeout)
        self.set_start_timeout(timeout)

        def before_shutdown() -> None:
            self.notify_mpp_coordinator('before_demote')

        def after_start() -> None:
            self.notify_mpp_coordinator('after_promote')

        # For non async cases we want to wait for restart to complete or timeout before returning.
        do_restart = functools.partial(self.state_handler.restart, timeout, self._async_executor.critical_task,
                                       before_shutdown=before_shutdown if self.has_lock() else None,
                                       after_start=after_start if self.has_lock() else None)
        if self.is_synchronous_mode() and not self.has_lock():
            do_restart = functools.partial(self.while_not_sync_standby, do_restart)

        if run_async:
            self._async_executor.run_async(do_restart)
            return (True, 'restart initiated')
        else:
            res = self._async_executor.run(do_restart)
            if res:
                return (True, 'restarted successfully')
            elif res is None:
                return (False, 'postgres is still starting')
            else:
                return (False, PostgresqlState.RESTART_FAILED)

    def _do_reinitialize(self, cluster: Cluster, from_leader: bool = False) -> Optional[bool]:
        self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
        # Commented redundant data directory cleanup here
        # self.state_handler.remove_data_directory()

        if from_leader:
            clone_member = cluster.leader
        else:
            clone_member = cluster.get_clone_member(self.state_handler.name)

        if clone_member:
            member_role = 'leader' if clone_member == cluster.leader else 'replica'
            return self.clone(clone_member, "from {0} '{1}'".format(member_role, clone_member.name), from_leader)

    def reinitialize(self, force: bool = False, from_leader: bool = False) -> Optional[str]:
        with self._async_executor:
            self.load_cluster_from_dcs()

            if self.cluster.is_unlocked():
                return 'Cluster has no leader, can not reinitialize'

            if self.has_lock(False):
                return 'I am the leader, can not reinitialize'

            cluster = self.cluster

        if force:
            self._async_executor.cancel()

        with self._async_executor:
            action = self._async_executor.schedule('reinitialize')
            if action is not None:
                return '{0} already in progress'.format(action)

        self._async_executor.run_async(self._do_reinitialize, args=(cluster, from_leader))

    def handle_long_action_in_progress(self) -> str:
        """Figure out what to do with the task AsyncExecutor is performing."""
        if self.has_lock() and self.update_lock():
            if self._async_executor.scheduled_action == 'doing crash recovery in a single user mode':
                time_left = global_config.primary_start_timeout - (time.time() - self._crash_recovery_started)
                if time_left <= 0 and self.is_failover_possible():
                    logger.info("Demoting self because crash recovery is taking too long")
                    self.state_handler.cancellable.cancel(True)
                    self.demote('immediate')
                    return 'terminated crash recovery because of startup timeout'

            return 'updated leader lock during {0}'.format(self._async_executor.scheduled_action)
        elif not self.state_handler.bootstrapping and not self.is_paused():
            # Don't have lock, make sure we are not promoting or starting up a primary in the background
            if self._async_executor.scheduled_action == 'promote':
                with self._async_response:
                    cancel = self._async_response.cancel()
                if cancel:
                    self.state_handler.cancellable.cancel()
                    return 'lost leader before promote'

            if self.state_handler.role == PostgresqlRole.PRIMARY:
                logger.info('Demoting primary during %s', self._async_executor.scheduled_action)
                if self._async_executor.scheduled_action in ('restart', 'starting primary after failure'):
                    # Restart needs a special interlocking cancel because postmaster may be just started in a
                    # background thread and has not even written a pid file yet.
                    with self._async_executor.critical_task as task:
                        if not task.cancel() and isinstance(task.result, PostmasterProcess):
                            self.state_handler.terminate_starting_postmaster(postmaster=task.result)
                self.demote('immediate-nolock')
                return 'lost leader lock during {0}'.format(self._async_executor.scheduled_action)
        if self.cluster.is_unlocked():
            logger.info('not healthy enough for leader race')

        return '{0} in progress'.format(self._async_executor.scheduled_action)

    @staticmethod
    def sysid_valid(sysid: Optional[str]) -> bool:
        # sysid does tv_sec << 32, where tv_sec is the number of seconds sine 1970,
        # so even 1 << 32 would have 10 digits.
        sysid = str(sysid)
        return len(sysid) >= 10 and sysid.isdigit()

    def post_recover(self) -> Optional[str]:
        if not self.state_handler.is_running():
            self.watchdog.disable()
            if self.has_lock():
                if self.state_handler.role in (PostgresqlRole.PRIMARY, PostgresqlRole.STANDBY_LEADER):
                    self.state_handler.set_role(PostgresqlRole.DEMOTED)
                    self.state_handler.call_nowait(CallbackAction.ON_ROLE_CHANGE)
                self._delete_leader()
                return 'removed leader key after trying and failing to start postgres'
            return 'failed to start postgres'
        return None

    def cancel_initialization(self) -> None:
        logger.info('removing initialize key after failed attempt to bootstrap the cluster')
        self.dcs.cancel_initialization()
        self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
        self.state_handler.move_data_directory()
        raise PatroniFatalException('Failed to bootstrap cluster')

    def post_bootstrap(self) -> str:
        with self._async_response:
            result = self._async_response.result
        # bootstrap has failed if postgres is not running
        if not self.state_handler.is_running() or result is False:
            self.cancel_initialization()

        if result is None:
            if not self.state_handler.is_primary():
                return 'waiting for end of recovery after bootstrap'

            self.state_handler.set_role(PostgresqlRole.PRIMARY)
            ret = self._async_executor.try_run_async('post_bootstrap', self.state_handler.bootstrap.post_bootstrap,
                                                     args=(self.patroni.config['bootstrap'], self._async_response))
            return ret or 'running post_bootstrap'

        self.state_handler.bootstrapping = False
        if not self.watchdog.activate():
            logger.error('Cancelling bootstrap because watchdog activation failed')
            self.cancel_initialization()

        self._rewind.ensure_checkpoint_after_promote(self.wakeup)
        self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)
        self.dcs.set_config_value(json.dumps(self.patroni.config.dynamic_configuration, separators=(',', ':')))
        self.dcs.take_leader()
        self.set_is_leader(True)
        if self.is_synchronous_mode():
            self.state_handler.sync_handler.set_synchronous_standby_names(
                CaseInsensitiveSet('*') if global_config.is_synchronous_mode_strict else CaseInsensitiveSet())
        self.state_handler.call_nowait(CallbackAction.ON_START)
        self.load_cluster_from_dcs()

        return 'initialized a new cluster'

    def handle_starting_instance(self) -> Optional[str]:
        """Starting up PostgreSQL may take a long time. In case we are the leader we may want to fail over to."""

        # Check if we are in startup, when paused defer to main loop for manual failovers.
        if not self.state_handler.check_for_startup() or self.is_paused():
            self.set_start_timeout(None)
            if self.is_paused():
                self.state_handler.set_state(PostgresqlState.RUNNING if self.state_handler.is_running()
                                             else PostgresqlState.STOPPED)
            return None

        # state_handler.state == 'starting' here
        if self.has_lock():
            if not self.update_lock():
                logger.info("Lost lock while starting up. Demoting self.")
                self.demote('immediate-nolock')
                return 'stopped PostgreSQL while starting up because leader key was lost'

            timeout = self._start_timeout or global_config.primary_start_timeout
            time_left = timeout - self.state_handler.time_in_state()

            if time_left <= 0:
                if self.is_failover_possible():
                    logger.info("Demoting self because primary startup is taking too long")
                    self.demote('immediate')
                    return 'stopped PostgreSQL because of startup timeout'
                else:
                    return 'primary start has timed out, but continuing to wait because failover is not possible'
            else:
                msg = self.process_manual_failover_from_leader()
                if msg is not None:
                    return msg

                return 'PostgreSQL is still starting up, {0:.0f} seconds until timeout'.format(time_left)
        else:
            # Use normal processing for standbys
            logger.info("Still starting up as a standby.")
            return None

    def set_start_timeout(self, value: Optional[int]) -> None:
        """Sets timeout for starting as primary before eligible for failover.

        Must be called when async_executor is busy or in the main thread.
        """
        self._start_timeout = value

    def _run_cycle(self) -> str:
        dcs_failed = False
        try:
            try:
                self.load_cluster_from_dcs()
                global_config.update(self.cluster)
                self.state_handler.reset_cluster_info_state(self.cluster, self.patroni)
            except Exception as exc1:
                self.state_handler.reset_cluster_info_state(None)
                if self.is_failsafe_mode():
                    # If DCS is not accessible we want to get the latest value of received/replayed LSN
                    # in order to have it immediately available if the failsafe mode is enabled.
                    try:
                        self._last_wal_lsn = self.state_handler.last_operation()
                    except Exception as exc2:
                        logger.debug('Failed to fetch current wal lsn: %r', exc2)
                raise exc1

            if self.is_paused():
                self.watchdog.disable()
                self._was_paused = True
            else:
                if self._was_paused:
                    self.state_handler.schedule_sanity_checks_after_pause()
                    # during pause people could manually do something with Postgres, therefore we want
                    # to double check rewind conditions on replicas and maybe run CHECKPOINT on the primary
                    self._rewind.reset_state()
                self._was_paused = False

            if not self.cluster.has_member(self.state_handler.name):
                self.touch_member()

            # cluster has leader key but not initialize key
            if self.has_lock(False) and not self.sysid_valid(self.cluster.initialize):
                self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)

            if self.has_lock(False) and not (self.cluster.config and self.cluster.config.data):
                self.dcs.set_config_value(json.dumps(self.patroni.config.dynamic_configuration, separators=(',', ':')))
                self.cluster = self.dcs.get_cluster()

            if self._async_executor.busy:
                return self.handle_long_action_in_progress()

            msg = self.handle_starting_instance()
            if msg is not None:
                return msg

            # we've got here, so any async action has finished.
            if self.state_handler.bootstrapping:
                return self.post_bootstrap()

            if self.recovering:
                self.recovering = False

                if not self._rewind.is_needed:
                    # Check if we tried to recover from postgres crash and failed
                    msg = self.post_recover()
                    if msg is not None:
                        return msg

                # Reset some states after postgres successfully started up
                self._crash_recovery_started = 0
                if self._rewind.executed and not self._rewind.failed:
                    self._rewind.reset_state()

                # The Raft cluster without a quorum takes a bit of time to stabilize.
                # Therefore we want to postpone the leader race if we just started up.
                if self.cluster.is_unlocked() and self.dcs.__class__.__name__ == 'Raft':
                    return 'started as a secondary'

            # is data directory empty?
            data_directory_error = ''
            data_directory_is_empty = None
            try:
                data_directory_is_empty = self.state_handler.data_directory_empty()
                data_directory_is_accessible = True
            except OSError as e:
                data_directory_is_accessible = False
                data_directory_error = e

            if not data_directory_is_accessible or data_directory_is_empty:
                self.state_handler.set_role(PostgresqlRole.UNINITIALIZED)
                self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
                # In case datadir went away while we were primary
                self.watchdog.disable()

                # is this instance the leader?
                if self.has_lock():
                    self.release_leader_key_voluntarily()
                    return 'released leader key voluntarily as data dir {0} and currently leader'.format(
                        'empty' if data_directory_is_accessible else 'not accessible')

                if not data_directory_is_accessible:
                    return 'data directory is not accessible: {0}'.format(data_directory_error)
                if self.is_paused():
                    return 'running with empty data directory'
                return self.bootstrap()  # new node
            else:
                # check if we are allowed to join
                data_sysid = self.state_handler.sysid
                if not self.sysid_valid(data_sysid):
                    # data directory is not empty, but no valid sysid, cluster must be broken, suggest reinit
                    return ("data dir for the cluster is not empty, "
                            "but system ID is invalid; consider doing reinitialize")

                if self.sysid_valid(self.cluster.initialize):
                    if self.cluster.initialize != data_sysid:
                        if self.is_paused():
                            logger.warning('system ID has changed while in paused mode. Patroni will exit when resuming'
                                           ' unless system ID is reset: %s != %s', self.cluster.initialize, data_sysid)
                            if self.has_lock():
                                self.release_leader_key_voluntarily()
                                return 'released leader key voluntarily due to the system ID mismatch'
                        else:
                            logger.fatal('system ID mismatch, node %s belongs to a different cluster: %s != %s',
                                         self.state_handler.name, self.cluster.initialize, data_sysid)
                            sys.exit(1)
                elif self.cluster.is_unlocked() and not self.is_paused() and not self.state_handler.cb_called:
                    # "bootstrap", but data directory is not empty
                    if self.state_handler.is_running() and not self.state_handler.is_primary():
                        self._join_aborted = True
                        logger.error('No initialize key in DCS and PostgreSQL is running as replica, aborting start')
                        logger.error('Please first start Patroni on the node running as primary')
                        sys.exit(1)
                    self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=data_sysid)

            if not self.state_handler.is_healthy():
                if self.is_paused():
                    self.state_handler.set_state(PostgresqlState.STOPPED)
                    if self.has_lock():
                        self._delete_leader()
                        return 'removed leader lock because postgres is not running'
                    # Normally we don't start Postgres in a paused state. We make an exception for the demoted primary
                    # that needs to be started after it had been stopped by demote. When there is no need to call rewind
                    # the demote code follows through to starting Postgres right away, however, in the rewind case
                    # it returns from demote and reaches this point to start PostgreSQL again after rewind. In that
                    # case it makes no sense to continue to recover() unless rewind has finished successfully.
                    elif self._rewind.failed or not self._rewind.executed and not \
                            (self._rewind.is_needed and self._rewind.can_rewind_or_reinitialize_allowed):
                        return 'postgres is not running'

                if self.state_handler.state in (PostgresqlState.RUNNING, PostgresqlState.STARTING):
                    self.state_handler.set_state(PostgresqlState.CRASHED)
                # try to start dead postgres
                return self.recover()

            if self.cluster.is_unlocked():
                ret = self.process_unhealthy_cluster()
            else:
                msg = self.process_healthy_cluster()
                ret = self.evaluate_scheduled_restart() or msg

            # We might not have a valid PostgreSQL connection here if AsyncExecutor is doing
            # something with PostgreSQL. Therefore we will sync replication slots only if no
            # asynchronous processes are running or we know that this is a standby being promoted.
            # But, we don't want to run pg_rewind checks or copy logical slots from itself,
            # therefore we have a couple additional `not is_promoting` checks.
            is_promoting = self._async_executor.scheduled_action == 'promote'
            if (not self._async_executor.busy or is_promoting) and not self.state_handler.is_starting():
                create_slots = self._sync_replication_slots(False)

                if not self.state_handler.cb_called:
                    if not is_promoting and not self.state_handler.is_primary():
                        self._rewind.trigger_check_diverged_lsn()
                    self.state_handler.call_nowait(CallbackAction.ON_START)

                if not is_promoting and create_slots and self.cluster.leader:
                    err = self._async_executor.try_run_async('copy_logical_slots',
                                                             self.state_handler.slots_handler.copy_logical_slots,
                                                             args=(self.cluster, self.patroni, create_slots))
                    if not err:
                        ret = 'Copying logical slots {0} from the primary'.format(create_slots)
            return ret
        except DCSError:
            dcs_failed = True
            logger.error('Error communicating with DCS')
            return self._handle_dcs_error()
        except (psycopg.Error, PostgresConnectionException):
            return 'Error communicating with PostgreSQL. Will try again later'
        finally:
            if not dcs_failed:
                if self.is_leader():
                    self._failsafe.set_is_active(0)
                self.touch_member()

    def _handle_dcs_error(self) -> str:
        if not self.is_paused() and self.state_handler.is_running():
            if self.state_handler.is_primary():
                if self.is_failsafe_mode() and self.check_failsafe_topology():
                    self.set_is_leader(True)
                    self._failsafe.set_is_active(time.time())
                    self.watchdog.keepalive()
                    self._sync_replication_slots(True)
                    return 'continue to run as a leader because failsafe mode is enabled and all members are accessible'
                self._failsafe.set_is_active(0)
                logger.info('demoting self because DCS is not accessible and I was a leader')
                self.demote('offline')
                return 'demoted self because DCS is not accessible and I was a leader'
            else:
                self._sync_replication_slots(True)
        return 'DCS is not accessible'

    def _sync_replication_slots(self, dcs_failed: bool) -> List[str]:
        """Handles replication slots.

        :param dcs_failed: bool, indicates that communication with DCS failed (get_cluster() or update_leader())

        :returns: list[str], replication slots names that should be copied from the primary
        """

        slots: List[str] = []

        # If dcs_failed we don't want to touch replication slots on a leader or replicas if failsafe_mode isn't enabled.
        if not self.cluster or dcs_failed and not self.is_failsafe_mode():
            return slots

        # It could be that DCS is read-only, or only the leader can't access it.
        # Only the second one could be handled by `load_cluster_from_dcs()`.
        # The first one affects advancing logical replication slots on replicas, therefore we rely on
        # Failsafe.update_cluster(), that will return "modified" Cluster if failsafe mode is active.
        cluster = self._failsafe.update_cluster(self.cluster) if self.is_failsafe_mode() else self.cluster
        if cluster:
            slots = self.state_handler.slots_handler.sync_replication_slots(cluster, self.patroni)
        # Don't copy replication slots if failsafe_mode is active
        return [] if self.failsafe_is_active() else slots

    def run_cycle(self) -> str:
        with self._async_executor:
            try:
                info = self._run_cycle()
                return (self.is_paused() and 'PAUSE: ' or '') + info
            except PatroniFatalException:
                raise
            except Exception:
                logger.exception('Unexpected exception')
                return 'Unexpected exception raised, please report it as a BUG'

    def shutdown(self) -> None:
        self._async_executor.cancel()
        if self.is_paused():
            logger.info('Leader key is not deleted and Postgresql is not stopped due paused state')
            self.watchdog.disable()
        elif not self._join_aborted:
            # FIXME: If stop doesn't reach safepoint quickly enough keepalive is triggered. If shutdown checkpoint
            # takes longer than ttl, then leader key is lost and replication might not have sent out all WAL.
            # This might not be the desired behavior of users, as a graceful shutdown of the host can mean lost data.
            # We probably need to something smarter here.
            disable_wd = self.watchdog.disable if self.watchdog.is_running else None

            status = {'deleted': False}

            def _on_shutdown(checkpoint_location: int, prev_location: int) -> None:
                if self.is_leader():
                    # Postmaster is still running, but pg_control already reports clean "shut down".
                    # It could happen if Postgres is still archiving the backlog of WAL files.
                    # If we know that there are replicas that received the shutdown checkpoint
                    # location, we can remove the leader key and allow them to start leader race.
                    time.sleep(1)  # give replicas some more time to catch up
                    if self.is_failover_possible(cluster_lsn=checkpoint_location):
                        self.dcs.delete_leader(self.cluster.leader, prev_location)
                        status['deleted'] = True
                    else:
                        self.dcs.write_leader_optime(prev_location)

            def _before_shutdown() -> None:
                self.notify_mpp_coordinator('before_demote')

            on_shutdown = _on_shutdown if self.is_leader() else None
            before_shutdown = _before_shutdown if self.is_leader() else None
            self.while_not_sync_standby(lambda: self.state_handler.stop(checkpoint=False, on_safepoint=disable_wd,
                                                                        on_shutdown=on_shutdown,
                                                                        before_shutdown=before_shutdown,
                                                                        stop_timeout=self.primary_stop_timeout()))
            if not self.state_handler.is_running():
                if self.is_leader() and not status['deleted']:
                    _, prev_location = self.state_handler.latest_checkpoint_locations()
                    self.dcs.delete_leader(self.cluster.leader, prev_location)
                self.touch_member()
            else:
                # XXX: what about when Patroni is started as the wrong user that has access to the watchdog device
                # but cannot shut down PostgreSQL. Root would be the obvious example. Would be nice to not kill the
                # system due to a bad config.
                logger.error("PostgreSQL shutdown failed, leader key not removed.%s",
                             (" Leaving watchdog running." if self.watchdog.is_running else ""))

    def watch(self, timeout: float) -> bool:
        # watch on leader key changes if the postgres is running and leader is known and current node is not lock owner
        if self._async_executor.busy or not self.cluster or self.cluster.is_unlocked() or self.has_lock(False):
            leader_version = None
        else:
            leader_version = self.cluster.leader.version if self.cluster.leader else None

        return self.dcs.watch(leader_version, timeout)

    def wakeup(self) -> None:
        """Trigger the next run of HA loop if there is no "active" leader watch request in progress.

        This usually happens on the leader or if the node is running async action"""
        self.dcs.event.set()

    def get_remote_member(self, member: Union[Leader, Member, None] = None) -> RemoteMember:
        """Get remote member node to stream from.

        In case of standby cluster this will tell us from which remote member to stream. Config can be both patroni
        config or cluster.config.data.
        """
        data: Dict[str, Any] = {}
        cluster_params = global_config.get_standby_cluster_config()

        if cluster_params:
            data.update({k: v for k, v in cluster_params.items() if k in RemoteMember.ALLOWED_KEYS})
            data['no_replication_slot'] = 'primary_slot_name' not in cluster_params
            conn_kwargs = member.conn_kwargs() if member else \
                {k: cluster_params[k] for k in ('host', 'port') if k in cluster_params}
            if conn_kwargs:
                data['conn_kwargs'] = conn_kwargs

        name = member.name if member else 'remote_member:{}'.format(uuid.uuid1())
        return RemoteMember(name, data)

    def get_failover_candidates(self, exclude_failover_candidate: bool) -> List[Member]:
        """Return a list of candidates for either manual or automatic failover.

        Exclude non-sync members when in synchronous mode, the current node (its checks are always performed earlier)
        and the candidate if required. If failover candidate exclusion is not requested and a candidate is specified
        in the /failover key, return the candidate only.
        The result is further evaluated in the caller :func:`Ha.is_failover_possible` to check if any member is actually
        healthy enough and is allowed to poromote.

        :param exclude_failover_candidate: if ``True``, exclude :attr:`failover.candidate` from the candidates.

        :returns: a list of :class:`Member` objects or an empty list if there is no candidate available.
        """
        failover = self.cluster.failover
        exclude = [self.state_handler.name] + ([failover.candidate] if failover and exclude_failover_candidate else [])

        def is_eligible(node: Member) -> bool:
            # in synchronous mode we allow failover (not switchover!) to async node
            if self.sync_mode_is_active() and not self.cluster.sync.matches(node.name)\
                    and not (failover and not failover.leader):
                return False
            # Don't spend time on "nofailover" nodes checking.
            # We also don't need nodes which we can't query with the api in the list.
            # And, if exclude_failover_candidate is True we want to skip  node.name == failover.candidate check.
            return node.name not in exclude and not node.nofailover and bool(node.api_url) and \
                (exclude_failover_candidate or not failover
                 or not failover.candidate or node.name == failover.candidate)

        return list(filter(is_eligible, self.cluster.members))
