import datetime
import functools
import json
import logging
import psycopg2
import sys
import time
import uuid

from collections import namedtuple
from multiprocessing.pool import ThreadPool
from patroni.async_executor import AsyncExecutor, CriticalTask
from patroni.exceptions import DCSError, PostgresConnectionException, PatroniFatalException
from patroni.postgresql import ACTION_ON_START, ACTION_ON_ROLE_CHANGE
from patroni.postgresql.misc import postgres_version_to_int
from patroni.postgresql.rewind import Rewind
from patroni.utils import polling_loop, tzutc, is_standby_cluster as _is_standby_cluster, parse_int
from patroni.dcs import RemoteMember
from threading import RLock

logger = logging.getLogger(__name__)


class _MemberStatus(namedtuple('_MemberStatus', ['member', 'reachable', 'in_recovery', 'timeline',
                                                 'wal_position', 'tags', 'watchdog_failed'])):
    """Node status distilled from API response:

        member - dcs.Member object of the node
        reachable - `!False` if the node is not reachable or is not responding with correct JSON
        in_recovery - `!True` if pg_is_in_recovery() == true
        timeline - timeline value from JSON
        wal_position - maximum value of `replayed_location` or `received_location` from JSON
        tags - dictionary with values of different tags (i.e. nofailover)
        watchdog_failed - indicates that watchdog is required by configuration but not available or failed
    """
    @classmethod
    def from_api_response(cls, member, json):
        is_master = json['role'] == 'master'
        timeline = json.get('timeline', 0)
        wal = not is_master and max(json['xlog'].get('received_location', 0), json['xlog'].get('replayed_location', 0))
        return cls(member, True, not is_master, timeline, wal, json.get('tags', {}), json.get('watchdog_failed', False))

    @classmethod
    def unknown(cls, member):
        return cls(member, False, None, 0, 0, {}, False)

    def failover_limitation(self):
        """Returns reason why this node can't promote or None if everything is ok."""
        if not self.reachable:
            return 'not reachable'
        if self.tags.get('nofailover', False):
            return 'not allowed to promote'
        if self.watchdog_failed:
            return 'not watchdog capable'
        return None


class Ha(object):

    def __init__(self, patroni):
        self.patroni = patroni
        self.state_handler = patroni.postgresql
        self._rewind = Rewind(self.state_handler)
        self.dcs = patroni.dcs
        self.cluster = None
        self.old_cluster = None
        self._is_leader = False
        self._is_leader_lock = RLock()
        self._was_paused = False
        self._leader_timeline = None
        self.recovering = False
        self._async_response = CriticalTask()
        self._crash_recovery_executed = False
        self._crash_recovery_started = None
        self._start_timeout = None
        self._async_executor = AsyncExecutor(self.state_handler.cancellable, self.wakeup)
        self.watchdog = patroni.watchdog

        # Each member publishes various pieces of information to the DCS using touch_member. This lock protects
        # the state and publishing procedure to have consistent ordering and avoid publishing stale values.
        self._member_state_lock = RLock()
        # Count of concurrent sync disabling requests. Value above zero means that we don't want to be synchronous
        # standby. Changes protected by _member_state_lock.
        self._disable_sync = 0

        # We need following property to avoid shutdown of postgres when join of Patroni to the postgres
        # already running as replica was aborted due to cluster not beeing initialized in DCS.
        self._join_aborted = False

        # used only in backoff after failing a pre_promote script
        self._released_leader_key_timestamp = 0

    def check_mode(self, mode):
        # Try to protect from the case when DCS was wiped out during pause
        if self.cluster and self.cluster.config and self.cluster.config.modify_index:
            return self.cluster.check_mode(mode)
        else:
            return self.patroni.config.check_mode(mode)

    def master_stop_timeout(self):
        """ Master stop timeout """
        ret = parse_int(self.patroni.config['master_stop_timeout'])
        return ret if ret and ret > 0 and self.is_synchronous_mode() else None

    def is_paused(self):
        return self.check_mode('pause')

    def check_timeline(self):
        return self.check_mode('check_timeline')

    def get_standby_cluster_config(self):
        if self.cluster and self.cluster.config and self.cluster.config.modify_index:
            config = self.cluster.config.data
        else:
            config = self.patroni.config.dynamic_configuration
        return config.get('standby_cluster')

    def is_standby_cluster(self):
        return _is_standby_cluster(self.get_standby_cluster_config())

    def is_leader(self):
        with self._is_leader_lock:
            return self._is_leader > time.time()

    def set_is_leader(self, value):
        with self._is_leader_lock:
            self._is_leader = time.time() + self.dcs.ttl if value else 0

    def load_cluster_from_dcs(self):
        cluster = self.dcs.get_cluster()

        # We want to keep the state of cluster when it was healthy
        if not cluster.is_unlocked() or not self.old_cluster:
            self.old_cluster = cluster
        self.cluster = cluster

        if not self.has_lock(False):
            self.set_is_leader(False)

        self._leader_timeline = None if cluster.is_unlocked() else cluster.leader.timeline

    def acquire_lock(self):
        ret = self.dcs.attempt_to_acquire_leader()
        self.set_is_leader(ret)
        return ret

    def update_lock(self, write_leader_optime=False):
        last_lsn = slots = None
        if write_leader_optime:
            try:
                last_lsn = self.state_handler.last_operation()
                slots = self.state_handler.slots()
            except Exception:
                logger.exception('Exception when called state_handler.last_operation()')
        try:
            ret = self.dcs.update_leader(last_lsn, slots)
        except Exception:
            logger.exception('Unexpected exception raised from update_leader, please report it as a BUG')
            ret = False
        self.set_is_leader(ret)
        if ret:
            self.watchdog.keepalive()
        return ret

    def has_lock(self, info=True):
        lock_owner = self.cluster.leader and self.cluster.leader.name
        if info:
            logger.info('Lock owner: %s; I am %s', lock_owner, self.state_handler.name)
        return lock_owner == self.state_handler.name

    def get_effective_tags(self):
        """Return configuration tags merged with dynamically applied tags."""
        tags = self.patroni.tags.copy()
        # _disable_sync could be modified concurrently, but we don't care as attribute get and set are atomic.
        if self._disable_sync > 0:
            tags['nosync'] = True
        return tags

    def touch_member(self):
        with self._member_state_lock:
            data = {
                'conn_url': self.state_handler.connection_string,
                'api_url': self.patroni.api.connection_string,
                'state': self.state_handler.state,
                'role': self.state_handler.role,
                'version': self.patroni.version
            }

            # following two lines are mainly necessary for consul, to avoid creation of master service
            if data['role'] == 'master' and not self.is_leader():
                data['role'] = 'promoted'
            if self.is_leader() and not self._rewind.checkpoint_after_promote():
                data['checkpoint_after_promote'] = False
            tags = self.get_effective_tags()
            if tags:
                data['tags'] = tags
            if self.state_handler.pending_restart:
                data['pending_restart'] = True
            if self._async_executor.scheduled_action in (None, 'promote') \
                    and data['state'] in ['running', 'restarting', 'starting']:
                try:
                    timeline, wal_position, pg_control_timeline = self.state_handler.timeline_wal_position()
                    data['xlog_location'] = wal_position
                    if not timeline:  # try pg_stat_wal_receiver to get the timeline
                        timeline = self.state_handler.received_timeline()
                    if not timeline:
                        # So far the only way to get the current timeline on the standby is from
                        # the replication connection. In order to avoid opening the replication
                        # connection on every iteration of HA loop we will do it only when noticed
                        # that the timeline on the primary has changed.
                        # Unfortunately such optimization isn't possible on the standby_leader,
                        # therefore we will get the timeline from pg_control, either by calling
                        # pg_control_checkpoint() on 9.6+ or by parsing the output of pg_controldata.
                        if self.state_handler.role == 'standby_leader':
                            timeline = pg_control_timeline or self.state_handler.pg_control_timeline()
                        else:
                            timeline = self.state_handler.replica_cached_timeline(self._leader_timeline)
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

            return self.dcs.touch_member(data)

    def clone(self, clone_member=None, msg='(without leader)'):
        if self.is_standby_cluster() and not isinstance(clone_member, RemoteMember):
            clone_member = self.get_remote_member(clone_member)

        self._rewind.reset_state()
        if self.state_handler.bootstrap.clone(clone_member):
            logger.info('bootstrapped %s', msg)
            cluster = self.dcs.get_cluster()
            node_to_follow = self._get_node_to_follow(cluster)
            return self.state_handler.follow(node_to_follow)
        else:
            logger.error('failed to bootstrap %s', msg)
            self.state_handler.remove_data_directory()

    def bootstrap(self):
        if not self.cluster.is_unlocked():  # cluster already has leader
            clone_member = self.cluster.get_clone_member(self.state_handler.name)
            member_role = 'leader' if clone_member == self.cluster.leader else 'replica'
            msg = "from {0} '{1}'".format(member_role, clone_member.name)
            ret = self._async_executor.try_run_async('bootstrap {0}'.format(msg), self.clone, args=(clone_member, msg))
            return ret or 'trying to bootstrap {0}'.format(msg)

        # no initialize key and node is allowed to be master and has 'bootstrap' section in a configuration file
        elif self.cluster.initialize is None and not self.patroni.nofailover and 'bootstrap' in self.patroni.config:
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
        else:
            create_replica_methods = self.get_standby_cluster_config().get('create_replica_methods', []) \
                                     if self.is_standby_cluster() else None
            if self.state_handler.can_create_replica_without_replication_connection(create_replica_methods):
                msg = 'bootstrap (without leader)'
                return self._async_executor.try_run_async(msg, self.clone) or 'trying to ' + msg
            return 'waiting for {0}leader to bootstrap'.format('standby_' if self.is_standby_cluster() else '')

    def bootstrap_standby_leader(self):
        """ If we found 'standby' key in the configuration, we need to bootstrap
            not a real master, but a 'standby leader', that will take base backup
            from a remote master and start follow it.
        """
        clone_source = self.get_remote_master()
        msg = 'clone from remote master {0}'.format(clone_source.conn_url)
        result = self.clone(clone_source, msg)
        with self._async_response:  # pretend that post_bootstrap was already executed
            self._async_response.complete(result)
        if result:
            self.state_handler.set_role('standby_leader')

        return result

    def _handle_rewind_or_reinitialize(self):
        leader = self.get_remote_master() if self.is_standby_cluster() else self.cluster.leader
        if not self._rewind.rewind_or_reinitialize_needed_and_possible(leader):
            return None

        if self._rewind.can_rewind:
            msg = 'running pg_rewind from ' + leader.name
            return self._async_executor.try_run_async(msg, self._rewind.execute, args=(leader,)) or msg

        # remove_data_directory_on_diverged_timelines is set
        if not self.is_standby_cluster():
            msg = 'reinitializing due to diverged timelines'
            return self._async_executor.try_run_async(msg, self._do_reinitialize, args=(self.cluster,)) or msg

    def recover(self):
        # Postgres is not running and we will restart in standby mode. Watchdog is not needed until we promote.
        self.watchdog.disable()

        if self.has_lock() and self.update_lock():
            timeout = self.patroni.config['master_start_timeout']
            if timeout == 0:
                # We are requested to prefer failing over to restarting master. But see first if there
                # is anyone to fail over to.
                members = self.cluster.members
                if self.is_synchronous_mode():
                    members = [m for m in members if self.cluster.sync.matches(m.name)]
                if self.is_failover_possible(members):
                    logger.info("Master crashed. Failing over.")
                    self.demote('immediate')
                    return 'stopped PostgreSQL to fail over after a crash'
        else:
            timeout = None

        data = self.state_handler.controldata()
        logger.info('pg_controldata:\n%s\n', '\n'.join('  {0}: {1}'.format(k, v) for k, v in data.items()))
        if data.get('Database cluster state') in ('in production', 'shutting down', 'in crash recovery') \
                and not self._crash_recovery_executed and \
                (self.cluster.is_unlocked() or self._rewind.can_rewind):
            self._crash_recovery_executed = True
            self._crash_recovery_started = time.time()
            msg = 'doing crash recovery in a single user mode'
            return self._async_executor.try_run_async(msg, self._rewind.ensure_clean_shutdown) or msg

        self.load_cluster_from_dcs()

        role = 'replica'
        if self.is_standby_cluster() or not self.has_lock():
            if not self._rewind.executed:
                self._rewind.trigger_check_diverged_lsn()
            msg = self._handle_rewind_or_reinitialize()
            if msg:
                return msg

            if self.has_lock():  # in standby cluster
                msg = "starting as a standby leader because i had the session lock"
                role = 'standby_leader'
                node_to_follow = self._get_node_to_follow(self.cluster)
            elif self.is_standby_cluster() and self.cluster.is_unlocked():
                msg = "trying to follow a remote master because standby cluster is unhealthy"
                node_to_follow = self.get_remote_master()
            else:
                msg = "starting as a secondary"
                node_to_follow = self._get_node_to_follow(self.cluster)
        elif self.has_lock():
            msg = "starting as readonly because i had the session lock"
            node_to_follow = None

        if self._async_executor.try_run_async('restarting after failure', self.state_handler.follow,
                                              args=(node_to_follow, role, timeout)) is None:
            self.recovering = True
        return msg

    def _get_node_to_follow(self, cluster):
        # determine the node to follow. If replicatefrom tag is set,
        # try to follow the node mentioned there, otherwise, follow the leader.
        standby_config = self.get_standby_cluster_config()
        is_standby_cluster = _is_standby_cluster(standby_config)
        if is_standby_cluster and (self.cluster.is_unlocked() or self.has_lock(False)):
            node_to_follow = self.get_remote_master()
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
            if is_standby_cluster:
                node_to_follow.data.update({p: standby_config[p] for p in params if standby_config.get(p)})

        return node_to_follow

    def follow(self, demote_reason, follow_reason, refresh=True):
        if refresh:
            self.load_cluster_from_dcs()

        is_leader = self.state_handler.is_leader()

        node_to_follow = self._get_node_to_follow(self.cluster)

        if self.is_paused():
            if not (self._rewind.is_needed and self._rewind.can_rewind_or_reinitialize_allowed)\
                    or self.cluster.is_unlocked():
                if is_leader:
                    self.state_handler.set_role('master')
                    return 'continue to run as master without lock'
                elif self.state_handler.role != 'standby_leader':
                    self.state_handler.set_role('replica')

                if not node_to_follow:
                    return 'no action'
        elif is_leader:
            self.demote('immediate-nolock')
            return demote_reason

        if self.is_standby_cluster() and self._leader_timeline and \
                self.state_handler.get_history(self._leader_timeline + 1):
            self._rewind.trigger_check_diverged_lsn()

        msg = self._handle_rewind_or_reinitialize()
        if msg:
            return msg

        role = 'standby_leader' if isinstance(node_to_follow, RemoteMember) and self.has_lock(False) else 'replica'
        # It might happen that leader key in the standby cluster references non-exiting member.
        # In this case it is safe to continue running without changing recovery.conf
        if self.is_standby_cluster() and role == 'replica' and not (node_to_follow and node_to_follow.conn_url):
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
            elif role == 'standby_leader' and self.state_handler.role != role:
                self.state_handler.set_role(role)
                self.state_handler.call_nowait(ACTION_ON_ROLE_CHANGE)

        return follow_reason

    def is_synchronous_mode(self):
        return self.check_mode('synchronous_mode')

    def is_synchronous_mode_strict(self):
        return self.check_mode('synchronous_mode_strict')

    def process_sync_replication(self):
        """Process synchronous standby beahvior.

        Synchronous standbys are registered in two places postgresql.conf and DCS. The order of updating them must
        be right. The invariant that should be kept is that if a node is master and sync_standby is set in DCS,
        then that node must have synchronous_standby set to that value. Or more simple, first set in postgresql.conf
        and then in DCS. When removing, first remove in DCS, then in postgresql.conf. This is so we only consider
        promoting standbys that were guaranteed to be replicating synchronously.
        """
        if self.is_synchronous_mode():
            sync_node_count = self.patroni.config['synchronous_node_count']
            current = self.cluster.sync.leader and self.cluster.sync.members or []
            picked, allow_promote = self.state_handler.pick_synchronous_standby(self.cluster, sync_node_count,
                                                                                self.patroni.config[
                                                                                    'maximum_lag_on_syncnode'])
            if set(picked) != set(current):
                # update synchronous standby list in dcs temporarily to point to common nodes in current and picked
                sync_common = list(set(current).intersection(set(allow_promote)))
                if set(sync_common) != set(current):
                    logger.info("Updating synchronous privilege temporarily from %s to %s", current, sync_common)
                    if not self.dcs.write_sync_state(self.state_handler.name,
                                                     sync_common or None,
                                                     index=self.cluster.sync.index):
                        logger.info('Synchronous replication key updated by someone else.')
                        return

                # Update  db param and wait for x secs
                if self.is_synchronous_mode_strict() and not picked:
                    picked = ['*']
                    logger.warning("No standbys available!")

                logger.info("Assigning synchronous standby status to %s", picked)
                self.state_handler.config.set_synchronous_standby(picked)

                if picked and picked[0] != '*' and set(allow_promote) != set(picked) and not allow_promote:
                    # Wait for PostgreSQL to enable synchronous mode and see if we can immediately set sync_standby
                    time.sleep(2)
                    _, allow_promote = self.state_handler.pick_synchronous_standby(self.cluster,
                                                                                   sync_node_count,
                                                                                   self.patroni.config[
                                                                                       'maximum_lag_on_syncnode'])
                if allow_promote and set(allow_promote) != set(sync_common):
                    try:
                        cluster = self.dcs.get_cluster()
                    except DCSError:
                        return logger.warning("Could not get cluster state from DCS during process_sync_replication()")
                    if cluster.sync.leader and cluster.sync.leader != self.state_handler.name:
                        logger.info("Synchronous replication key updated by someone else")
                        return
                    if not self.dcs.write_sync_state(self.state_handler.name, allow_promote, index=cluster.sync.index):
                        logger.info("Synchronous replication key updated by someone else")
                        return
                    logger.info("Synchronous standby status assigned to %s", allow_promote)
        else:
            if self.cluster.sync.leader and self.dcs.delete_sync_state(index=self.cluster.sync.index):
                logger.info("Disabled synchronous replication")
            self.state_handler.config.set_synchronous_standby([])

    def is_sync_standby(self, cluster):
        return cluster.leader and cluster.sync.leader == cluster.leader.name \
            and self.state_handler.name in cluster.sync.members

    def while_not_sync_standby(self, func):
        """Runs specified action while trying to make sure that the node is not assigned synchronous standby status.

        Tags us as not allowed to be a sync standby as we are going to go away, if we currently are wait for
        leader to notice and pick an alternative one or if the leader changes or goes away we are also free.

        If the connection to DCS fails we run the action anyway, as this is only a hint.

        There is a small race window where this function runs between a master picking us the sync standby and
        publishing it to the DCS. As the window is rather tiny consequences are holding up commits for one cycle
        period we don't worry about it here."""

        if not self.is_synchronous_mode() or self.patroni.nosync:
            return func()

        with self._member_state_lock:
            self._disable_sync += 1
        try:
            if self.touch_member():
                # Master should notice the updated value during the next cycle. We will wait double that, if master
                # hasn't noticed the value by then not disabling sync replication is not likely to matter.
                for _ in polling_loop(timeout=self.dcs.loop_wait*2, interval=2):
                    try:
                        if not self.is_sync_standby(self.dcs.get_cluster()):
                            break
                    except DCSError:
                        logger.warning("Could not get cluster state, skipping synchronous standby disable")
                        break
                    logger.info("Waiting for master to release us from synchronous standby")
            else:
                logger.warning("Updating member state failed, skipping synchronous standby disable")

            return func()
        finally:
            with self._member_state_lock:
                self._disable_sync -= 1

    def update_cluster_history(self):
        master_timeline = self.state_handler.get_master_timeline()
        cluster_history = self.cluster.history and self.cluster.history.lines
        if master_timeline == 1:
            if cluster_history:
                self.dcs.set_history_value('[]')
        elif not cluster_history or cluster_history[-1][0] != master_timeline - 1 or len(cluster_history[-1]) != 4:
            cluster_history = {line[0]: line for line in cluster_history or []}
            history = self.state_handler.get_history(master_timeline)
            if history and self.cluster.config:
                history = history[-self.cluster.config.max_timelines_history:]
                for line in history:
                    # enrich current history with promotion timestamps stored in DCS
                    if len(line) == 3 and line[0] in cluster_history \
                            and len(cluster_history[line[0]]) == 4 \
                            and cluster_history[line[0]][1] == line[1]:
                        line.append(cluster_history[line[0]][3])
                self.dcs.set_history_value(json.dumps(history, separators=(',', ':')))

    def enforce_follow_remote_master(self, message):
        demote_reason = 'cannot be a real master in standby cluster'
        return self.follow(demote_reason, message)

    def enforce_master_role(self, message, promote_message):
        """
        Ensure the node that has won the race for the leader key meets criteria
        for promoting its PG server to the 'master' role.
        """
        if not self.is_paused():
            if not self.watchdog.is_running and not self.watchdog.activate():
                if self.state_handler.is_leader():
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

        if self.state_handler.is_leader():
            # Inform the state handler about its master role.
            # It may be unaware of it if postgres is promoted manually.
            self.state_handler.set_role('master')
            self.process_sync_replication()
            self.update_cluster_history()
            return message
        elif self.state_handler.role == 'master':
            self.process_sync_replication()
            return message
        else:
            if self.is_synchronous_mode():
                # Just set ourselves as the authoritative source of truth for now. We don't want to wait for standbys
                # to connect. We will try finding a synchronous standby in the next cycle.
                if not self.dcs.write_sync_state(self.state_handler.name, None, index=self.cluster.sync.index):
                    # Somebody else updated sync state, it may be due to us losing the lock. To be safe, postpone
                    # promotion until next cycle. TODO: trigger immediate retry of run_cycle
                    return 'Postponing promotion because synchronous replication state was updated by somebody else'
                self.state_handler.config.set_synchronous_standby(['*'] if self.is_synchronous_mode_strict() else [])
            if self.state_handler.role != 'master':
                def on_success():
                    self._rewind.reset_state()
                    logger.info("cleared rewind state after becoming the leader")

                with self._async_response:
                    self._async_response.reset()
                self._async_executor.try_run_async('promote', self.state_handler.promote,
                                                   args=(self.dcs.loop_wait, self._async_response, on_success))
            return promote_message

    def fetch_node_status(self, member):
        """This function perform http get request on member.api_url and fetches its status
        :returns: `_MemberStatus` object
        """

        try:
            response = self.patroni.request(member, timeout=2, retries=0)
            data = response.data.decode('utf-8')
            logger.info('Got response from %s %s: %s', member.name, member.api_url, data)
            return _MemberStatus.from_api_response(member, json.loads(data))
        except Exception as e:
            logger.warning("Request failed to %s: GET %s (%s)", member.name, member.api_url, e)
        return _MemberStatus.unknown(member)

    def fetch_nodes_statuses(self, members):
        pool = ThreadPool(len(members))
        results = pool.map(self.fetch_node_status, members)  # Run API calls on members in parallel
        pool.close()
        pool.join()
        return results

    def is_lagging(self, wal_position):
        """Returns if instance with an wal should consider itself unhealthy to be promoted due to replication lag.

        :param wal_position: Current wal position.
        :returns True when node is lagging
        """
        lag = (self.cluster.last_lsn or 0) - wal_position
        return lag > self.patroni.config.get('maximum_lag_on_failover', 0)

    def _is_healthiest_node(self, members, check_replication_lag=True):
        """This method tries to determine whether I am healthy enough to became a new leader candidate or not."""

        my_wal_position = self.state_handler.last_operation()
        if check_replication_lag and self.is_lagging(my_wal_position):
            logger.info('My wal position exceeds maximum replication lag')
            return False  # Too far behind last reported wal position on master

        if not self.is_standby_cluster() and self.check_timeline():
            cluster_timeline = self.cluster.timeline
            my_timeline = self.state_handler.replica_cached_timeline(cluster_timeline)
            if my_timeline < cluster_timeline:
                logger.info('My timeline %s is behind last known cluster timeline %s', my_timeline, cluster_timeline)
                return False

        # Prepare list of nodes to run check against
        members = [m for m in members if m.name != self.state_handler.name and not m.nofailover and m.api_url]

        if members:
            for st in self.fetch_nodes_statuses(members):
                if st.failover_limitation() is None:
                    if not st.in_recovery:
                        logger.warning('Master (%s) is still alive', st.member.name)
                        return False
                    if my_wal_position < st.wal_position:
                        logger.info('Wal position of %s is ahead of my wal position', st.member.name)
                        # In synchronous mode the former leader might be still accessible and even be ahead of us.
                        # We should not disqualify himself from the leader race in such a situation.
                        if not self.is_synchronous_mode() or st.member.name != self.cluster.sync.leader:
                            return False
                        logger.info('Ignoring the former leader being ahead of us')
        return True

    def is_failover_possible(self, members):
        ret = False
        cluster_timeline = self.cluster.timeline
        members = [m for m in members if m.name != self.state_handler.name and not m.nofailover and m.api_url]
        if members:
            for st in self.fetch_nodes_statuses(members):
                not_allowed_reason = st.failover_limitation()
                if not_allowed_reason:
                    logger.info('Member %s is %s', st.member.name, not_allowed_reason)
                elif self.is_lagging(st.wal_position):
                    logger.info('Member %s exceeds maximum replication lag', st.member.name)
                elif self.check_timeline() and (not st.timeline or st.timeline < cluster_timeline):
                    logger.info('Timeline %s of member %s is behind the cluster timeline %s',
                                st.timeline, st.member.name, cluster_timeline)
                else:
                    ret = True
        else:
            logger.warning('manual failover: members list is empty')
        return ret

    def manual_failover_process_no_leader(self):
        failover = self.cluster.failover
        if failover.candidate:  # manual failover to specific member
            if failover.candidate == self.state_handler.name:  # manual failover to me
                return True
            elif self.is_paused():
                # Remove failover key if the node to failover has terminated to avoid waiting for it indefinitely
                # In order to avoid attempts to delete this key from all nodes only the master is allowed to do it.
                if (not self.cluster.get_member(failover.candidate, fallback_to_leader=False) and
                   self.state_handler.is_leader()):
                    logger.warning("manual failover: removing failover key because failover candidate is not running")
                    self.dcs.manual_failover('', '', index=self.cluster.failover.index)
                    return None
                return False

            # find specific node and check that it is healthy
            member = self.cluster.get_member(failover.candidate, fallback_to_leader=False)
            if member:
                st = self.fetch_node_status(member)
                not_allowed_reason = st.failover_limitation()
                if not_allowed_reason is None:  # node is healthy
                    logger.info('manual failover: to %s, i am %s', st.member.name, self.state_handler.name)
                    return False
                # we wanted to failover to specific member but it is not healthy
                logger.warning('manual failover: member %s is %s', st.member.name, not_allowed_reason)

            # at this point we should consider all members as a candidates for failover
            # i.e. we assume that failover.candidate is None
        elif self.is_paused():
            return False

        # try to pick some other members to failover and check that they are healthy
        if failover.leader:
            if self.state_handler.name == failover.leader:  # I was the leader
                # exclude me and desired member which is unhealthy (failover.candidate can be None)
                members = [m for m in self.cluster.members if m.name not in (failover.candidate, failover.leader)]
                if self.is_failover_possible(members):  # check that there are healthy members
                    return False
                else:  # I was the leader and it looks like currently I am the only healthy member
                    return True

            # at this point we assume that our node is a candidate for a failover among all nodes except former leader

        # exclude former leader from the list (failover.leader can be None)
        members = [m for m in self.cluster.members if m.name != failover.leader]
        return self._is_healthiest_node(members, check_replication_lag=False)

    def is_healthiest_node(self):
        if time.time() - self._released_leader_key_timestamp < self.dcs.ttl:
            logger.info('backoff: skip leader race after pre_promote script failure and releasing the lock voluntarily')
            return False

        if self.is_paused() and not self.patroni.nofailover and \
                self.cluster.failover and not self.cluster.failover.scheduled_at:
            ret = self.manual_failover_process_no_leader()
            if ret is not None:  # continue if we just deleted the stale failover key as a master
                return ret

        if self.state_handler.is_starting():  # postgresql still starting up is unhealthy
            return False

        if self.state_handler.is_leader():
            # in pause leader is the healthiest only when no initialize or sysid matches with initialize!
            return not self.is_paused() or not self.cluster.initialize\
                    or self.state_handler.sysid == self.cluster.initialize

        if self.is_paused():
            return False

        if self.patroni.nofailover:  # nofailover tag makes node always unhealthy
            return False

        if self.cluster.failover:
            return self.manual_failover_process_no_leader()

        if not self.watchdog.is_healthy:
            logger.warning('Watchdog device is not usable')
            return False

        # When in sync mode, only last known master and sync standby are allowed to promote automatically.
        all_known_members = self.cluster.members + self.old_cluster.members
        if self.is_synchronous_mode() and self.cluster.sync.leader:
            if not self.cluster.sync.matches(self.state_handler.name):
                return False
            # pick between synchronous candidates so we minimize unnecessary failovers/demotions
            members = {m.name: m for m in all_known_members if self.cluster.sync.matches(m.name)}
        else:
            # run usual health check
            members = {m.name: m for m in all_known_members}

        return self._is_healthiest_node(members.values())

    def _delete_leader(self, last_lsn=None):
        self.set_is_leader(False)
        self.dcs.delete_leader(last_lsn)
        self.dcs.reset_cluster()

    def release_leader_key_voluntarily(self, last_lsn=None):
        self._delete_leader(last_lsn)
        self.touch_member()
        logger.info("Leader key released")

    def demote(self, mode):
        """Demote PostgreSQL running as master.

        :param mode: One of offline, graceful or immediate.
            offline is used when connection to DCS is not available.
            graceful is used when failing over to another node due to user request. May only be called running async.
            immediate is used when we determine that we are not suitable for master and want to failover quickly
                without regard for data durability. May only be called synchronously.
            immediate-nolock is used when find out that we have lost the lock to be master. Need to bring down
                PostgreSQL as quickly as possible without regard for data durability. May only be called synchronously.
        """
        mode_control = {
            'offline':          dict(stop='fast', checkpoint=False, release=False, offline=True, async_req=False),
            'graceful':         dict(stop='fast', checkpoint=True, release=True, offline=False, async_req=False),
            'immediate':        dict(stop='immediate', checkpoint=False, release=True, offline=False, async_req=True),
            'immediate-nolock': dict(stop='immediate', checkpoint=False, release=False, offline=False, async_req=True),
        }[mode]

        self._rewind.trigger_check_diverged_lsn()
        self.state_handler.stop(mode_control['stop'], checkpoint=mode_control['checkpoint'],
                                on_safepoint=self.watchdog.disable if self.watchdog.is_running else None,
                                stop_timeout=self.master_stop_timeout())
        self.state_handler.set_role('demoted')
        self.set_is_leader(False)

        if mode_control['release']:
            checkpoint_location = self.state_handler.latest_checkpoint_location() if mode == 'graceful' else None
            with self._async_executor:
                self.release_leader_key_voluntarily(checkpoint_location)
            time.sleep(2)  # Give a time to somebody to take the leader lock
        if mode_control['offline']:
            node_to_follow, leader = None, None
        else:
            cluster = self.dcs.get_cluster()
            node_to_follow, leader = self._get_node_to_follow(cluster), cluster.leader

        # FIXME: with mode offline called from DCS exception handler and handle_long_action_in_progress
        # there could be an async action already running, calling follow from here will lead
        # to racy state handler state updates.
        if mode_control['async_req']:
            self._async_executor.try_run_async('starting after demotion', self.state_handler.follow, (node_to_follow,))
        else:
            if self.is_synchronous_mode():
                self.state_handler.config.set_synchronous_standby([])
            if self._rewind.rewind_or_reinitialize_needed_and_possible(leader):
                return False  # do not start postgres, but run pg_rewind on the next iteration
            self.state_handler.follow(node_to_follow)

    def should_run_scheduled_action(self, action_name, scheduled_at, cleanup_fn):
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

    def process_manual_failover_from_leader(self):
        """Checks if manual failover is requested and takes action if appropriate.

        Cleans up failover key if failover conditions are not matched.

        :returns: action message if demote was initiated, None if no action was taken"""
        failover = self.cluster.failover
        if not failover or (self.is_paused() and not self.state_handler.is_leader()):
            return

        if (failover.scheduled_at and not
            self.should_run_scheduled_action("failover", failover.scheduled_at, lambda:
                                             self.dcs.manual_failover('', '', index=failover.index))):
            return

        if not failover.leader or failover.leader == self.state_handler.name:
            if not failover.candidate or failover.candidate != self.state_handler.name:
                if not failover.candidate and self.is_paused():
                    logger.warning('Failover is possible only to a specific candidate in a paused state')
                else:
                    if self.is_synchronous_mode():
                        if failover.candidate and not self.cluster.sync.matches(failover.candidate):
                            logger.warning('Failover candidate=%s does not match with sync_standbys=%s',
                                           failover.candidate, self.cluster.sync.sync_standby)
                            members = []
                        else:
                            members = [m for m in self.cluster.members if self.cluster.sync.matches(m.name)]
                    else:
                        members = [m for m in self.cluster.members
                                   if not failover.candidate or m.name == failover.candidate]
                    if self.is_failover_possible(members):  # check that there are healthy members
                        ret = self._async_executor.try_run_async('manual failover: demote', self.demote, ('graceful',))
                        return ret or 'manual failover: demoting myself'
                    else:
                        logger.warning('manual failover: no healthy members found, failover is not possible')
            else:
                logger.warning('manual failover: I am already the leader, no need to failover')
        else:
            logger.warning('manual failover: leader name does not match: %s != %s',
                           failover.leader, self.state_handler.name)

        logger.info('Cleaning up failover key')
        self.dcs.manual_failover('', '', index=failover.index)

    def process_unhealthy_cluster(self):
        """Cluster has no leader key"""

        if self.is_healthiest_node():
            if self.acquire_lock():
                failover = self.cluster.failover
                if failover:
                    if self.is_paused() and failover.leader and failover.candidate:
                        logger.info('Updating failover key after acquiring leader lock...')
                        self.dcs.manual_failover('', failover.candidate, failover.scheduled_at, failover.index)
                    else:
                        logger.info('Cleaning up failover key after acquiring leader lock...')
                        self.dcs.manual_failover('', '')
                self.load_cluster_from_dcs()

                if self.is_standby_cluster():
                    # standby leader disappeared, and this is the healthiest
                    # replica, so it should become a new standby leader.
                    # This implies we need to start following a remote master
                    msg = 'promoted self to a standby leader by acquiring session lock'
                    return self.enforce_follow_remote_master(msg)
                else:
                    return self.enforce_master_role(
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
                return self.follow('demoting self because I am not allowed to become master',
                                   'following a different leader because I am not allowed to promote')
            return self.follow('demoting self because i am not the healthiest node',
                               'following a different leader because i am not the healthiest node')

    def process_healthy_cluster(self):
        if self.has_lock():
            if self.is_paused() and not self.state_handler.is_leader():
                if self.cluster.failover and self.cluster.failover.candidate == self.state_handler.name:
                    return 'waiting to become master after promote...'

                self._delete_leader()
                return 'removed leader lock because postgres is not running as master'

            if self.update_lock(True):
                msg = self.process_manual_failover_from_leader()
                if msg is not None:
                    return msg

                # check if the node is ready to be used by pg_rewind
                self._rewind.ensure_checkpoint_after_promote(self.wakeup)

                if self.is_standby_cluster():
                    # in case of standby cluster we don't really need to
                    # enforce anything, since the leader is not a master.
                    # So just remind the role.
                    msg = 'no action.  i am the standby leader with the lock' \
                          if self.state_handler.role == 'standby_leader' else \
                          'promoted self to a standby leader because i had the session lock'
                    return self.enforce_follow_remote_master(msg)
                else:
                    return self.enforce_master_role(
                        'no action.  i am the leader with the lock',
                        'promoted self to leader because i had the session lock'
                    )
            else:
                # Either there is no connection to DCS or someone else acquired the lock
                logger.error('failed to update leader lock')
                if self.state_handler.is_leader():
                    if self.is_paused():
                        return 'continue to run as master after failing to update leader lock in DCS'
                    self.demote('immediate-nolock')
                    return 'demoted self because failed to update leader lock in DCS'
                else:
                    return 'not promoting because failed to update leader lock in DCS'
        else:
            logger.info('does not have lock')
        if self.is_standby_cluster():
            return self.follow('cannot be a real master in standby cluster',
                               'no action.  i am a secondary and i am following a standby leader', refresh=False)
        return self.follow('demoting self because i do not have the lock and i was a leader',
                           'no action.  i am a secondary and i am following a leader', refresh=False)

    def evaluate_scheduled_restart(self):
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

        if (restart_data and
           self.should_run_scheduled_action('restart', restart_data['schedule'], self.delete_future_restart)):
            try:
                ret, message = self.restart(restart_data, run_async=True)
                if not ret:
                    logger.warning("Scheduled restart: %s", message)
                    return None
                return message
            finally:
                self.delete_future_restart()

    def restart_matches(self, role, postgres_version, pending_restart):
        reason_to_cancel = ""
        # checking the restart filters here seem to be less ugly than moving them into the
        # run_scheduled_action.
        if role and role != self.state_handler.role:
            reason_to_cancel = "host role mismatch"

        if postgres_version and postgres_version_to_int(postgres_version) <= int(self.state_handler.server_version):
            reason_to_cancel = "postgres version mismatch"

        if pending_restart and not self.state_handler.pending_restart:
            reason_to_cancel = "pending restart flag is not set"

        if not reason_to_cancel:
            return True
        else:
            logger.info("not proceeding with the restart: %s", reason_to_cancel)
        return False

    def schedule_future_restart(self, restart_data):
        with self._async_executor:
            restart_data['postmaster_start_time'] = self.state_handler.postmaster_start_time()
            if not self.patroni.scheduled_restart:
                self.patroni.scheduled_restart = restart_data
                self.touch_member()
                return True
        return False

    def delete_future_restart(self):
        ret = False
        with self._async_executor:
            if self.patroni.scheduled_restart:
                self.patroni.scheduled_restart = {}
                self.touch_member()
                ret = True
        return ret

    def future_restart_scheduled(self):
        return self.patroni.scheduled_restart.copy() if (self.patroni.scheduled_restart and
                                                         isinstance(self.patroni.scheduled_restart, dict)) else None

    def restart_scheduled(self):
        return self._async_executor.scheduled_action == 'restart'

    def restart(self, restart_data, run_async=False):
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
        timeout = restart_data.get('timeout', self.patroni.config['master_start_timeout'])
        self.set_start_timeout(timeout)

        # For non async cases we want to wait for restart to complete or timeout before returning.
        do_restart = functools.partial(self.state_handler.restart, timeout, self._async_executor.critical_task)
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
                return (False, 'restart failed')

    def _do_reinitialize(self, cluster):
        self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
        # Commented redundant data directory cleanup here
        # self.state_handler.remove_data_directory()

        clone_member = self.cluster.get_clone_member(self.state_handler.name)
        member_role = 'leader' if clone_member == self.cluster.leader else 'replica'
        return self.clone(clone_member, "from {0} '{1}'".format(member_role, clone_member.name))

    def reinitialize(self, force=False):
        with self._async_executor:
            self.load_cluster_from_dcs()

            if self.cluster.is_unlocked():
                return 'Cluster has no leader, can not reinitialize'

            if self.has_lock(False):
                return 'I am the leader, can not reinitialize'

        if force:
            self._async_executor.cancel()

        with self._async_executor:
            action = self._async_executor.schedule('reinitialize')
            if action is not None:
                return '{0} already in progress'.format(action)

        self._async_executor.run_async(self._do_reinitialize, args=(self.cluster, ))

    def handle_long_action_in_progress(self):
        """
        Figure out what to do with the task AsyncExecutor is performing.
        """
        if self.has_lock() and self.update_lock():
            if self._async_executor.scheduled_action == 'doing crash recovery in a single user mode':
                time_left = self.patroni.config['master_start_timeout'] - (time.time() - self._crash_recovery_started)
                if time_left <= 0 and self.is_failover_possible(self.cluster.members):
                    logger.info("Demoting self because crash recovery is taking too long")
                    self.state_handler.cancellable.cancel(True)
                    self.demote('immediate')
                    return 'terminated crash recovery because of startup timeout'

            return 'updated leader lock during ' + self._async_executor.scheduled_action
        elif not self.state_handler.bootstrapping and not self.is_paused():
            # Don't have lock, make sure we are not promoting or starting up a master in the background
            if self._async_executor.scheduled_action == 'promote':
                with self._async_response:
                    cancel = self._async_response.cancel()
                if cancel:
                    self.state_handler.cancellable.cancel()
                    return 'lost leader before promote'

            if self.state_handler.role == 'master':
                logger.info("Demoting master during " + self._async_executor.scheduled_action)
                if self._async_executor.scheduled_action == 'restart':
                    # Restart needs a special interlocking cancel because postmaster may be just started in a
                    # background thread and has not even written a pid file yet.
                    with self._async_executor.critical_task as task:
                        if not task.cancel():
                            self.state_handler.terminate_starting_postmaster(postmaster=task.result)
                self.demote('immediate-nolock')
                return 'lost leader lock during ' + self._async_executor.scheduled_action
        if self.cluster.is_unlocked():
            logger.info('not healthy enough for leader race')

        return self._async_executor.scheduled_action + ' in progress'

    @staticmethod
    def sysid_valid(sysid):
        # sysid does tv_sec << 32, where tv_sec is the number of seconds sine 1970,
        # so even 1 << 32 would have 10 digits.
        sysid = str(sysid)
        return len(sysid) >= 10 and sysid.isdigit()

    def post_recover(self):
        if not self.state_handler.is_running():
            self.watchdog.disable()
            if self.has_lock():
                if self.state_handler.role in ('master', 'standby_leader'):
                    self.state_handler.set_role('demoted')
                self._delete_leader()
                return 'removed leader key after trying and failing to start postgres'
            return 'failed to start postgres'
        self._crash_recovery_executed = False
        if self._rewind.executed and not self._rewind.failed:
            self._rewind.reset_state()
        return None

    def cancel_initialization(self):
        logger.info('removing initialize key after failed attempt to bootstrap the cluster')
        self.dcs.cancel_initialization()
        self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
        self.state_handler.move_data_directory()
        raise PatroniFatalException('Failed to bootstrap cluster')

    def post_bootstrap(self):
        with self._async_response:
            result = self._async_response.result
        # bootstrap has failed if postgres is not running
        if not self.state_handler.is_running() or result is False:
            self.cancel_initialization()

        if result is None:
            if not self.state_handler.is_leader():
                return 'waiting for end of recovery after bootstrap'

            self.state_handler.set_role('master')
            ret = self._async_executor.try_run_async('post_bootstrap', self.state_handler.bootstrap.post_bootstrap,
                                                     args=(self.patroni.config['bootstrap'], self._async_response))
            return ret or 'running post_bootstrap'

        self.state_handler.bootstrapping = False
        if not self.watchdog.activate():
            logger.error('Cancelling bootstrap because watchdog activation failed')
            self.cancel_initialization()
        self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)
        self.dcs.set_config_value(json.dumps(self.patroni.config.dynamic_configuration, separators=(',', ':')))
        self.dcs.take_leader()
        self.set_is_leader(True)
        self.state_handler.call_nowait(ACTION_ON_START)
        self.load_cluster_from_dcs()

        return 'initialized a new cluster'

    def handle_starting_instance(self):
        """Starting up PostgreSQL may take a long time. In case we are the leader we may want to
        fail over to."""

        # Check if we are in startup, when paused defer to main loop for manual failovers.
        if not self.state_handler.check_for_startup() or self.is_paused():
            self.set_start_timeout(None)
            if self.is_paused():
                self.state_handler.set_state(self.state_handler.is_running() and 'running' or 'stopped')
            return None

        # state_handler.state == 'starting' here
        if self.has_lock():
            if not self.update_lock():
                logger.info("Lost lock while starting up. Demoting self.")
                self.demote('immediate-nolock')
                return 'stopped PostgreSQL while starting up because leader key was lost'

            timeout = self._start_timeout or self.patroni.config['master_start_timeout']
            time_left = timeout - self.state_handler.time_in_state()

            if time_left <= 0:
                if self.is_failover_possible(self.cluster.members):
                    logger.info("Demoting self because master startup is taking too long")
                    self.demote('immediate')
                    return 'stopped PostgreSQL because of startup timeout'
                else:
                    return 'master start has timed out, but continuing to wait because failover is not possible'
            else:
                msg = self.process_manual_failover_from_leader()
                if msg is not None:
                    return msg

                return 'PostgreSQL is still starting up, {0:.0f} seconds until timeout'.format(time_left)
        else:
            # Use normal processing for standbys
            logger.info("Still starting up as a standby.")
            return None

    def set_start_timeout(self, value):
        """Sets timeout for starting as master before eligible for failover.

        Must be called when async_executor is busy or in the main thread."""
        self._start_timeout = value

    def _run_cycle(self):
        dcs_failed = False
        try:
            self.load_cluster_from_dcs()
            self.state_handler.reset_cluster_info_state(self.cluster, self.patroni.nofailover)

            if self.is_paused():
                self.watchdog.disable()
                self._was_paused = True
            else:
                if self._was_paused:
                    self.state_handler.schedule_sanity_checks_after_pause()
                self._was_paused = False

            if not self.cluster.has_member(self.state_handler.name):
                self.touch_member()

            # cluster has leader key but not initialize key
            if not (self.cluster.is_unlocked() or self.sysid_valid(self.cluster.initialize)) and self.has_lock():
                self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)

            if not (self.cluster.is_unlocked() or self.cluster.config and self.cluster.config.data) and self.has_lock():
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

            if self.recovering and not self._rewind.is_needed:
                self.recovering = False
                # Check if we tried to recover and failed
                msg = self.post_recover()
                if msg is not None:
                    return msg

            # is data directory empty?
            if self.state_handler.data_directory_empty():
                self.state_handler.set_role('uninitialized')
                self.state_handler.stop('immediate', stop_timeout=self.patroni.config['retry_timeout'])
                # In case datadir went away while we were master.
                self.watchdog.disable()

                # is this instance the leader?
                if self.has_lock():
                    self.release_leader_key_voluntarily()
                    return 'released leader key voluntarily as data dir empty and currently leader'

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
                elif self.cluster.is_unlocked() and not self.is_paused():
                    # "bootstrap", but data directory is not empty
                    if not self.state_handler.cb_called and self.state_handler.is_running() \
                            and not self.state_handler.is_leader():
                        self._join_aborted = True
                        logger.error('No initialize key in DCS and PostgreSQL is running as replica, aborting start')
                        logger.error('Please first start Patroni on the node running as master')
                        sys.exit(1)
                    self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=data_sysid)

            if not self.state_handler.is_healthy():
                if self.is_paused():
                    self.state_handler.set_state('stopped')
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

                if self.state_handler.state in ('running', 'starting'):
                    self.state_handler.set_state('crashed')
                # try to start dead postgres
                return self.recover()

            try:
                if self.cluster.is_unlocked():
                    ret = self.process_unhealthy_cluster()
                else:
                    msg = self.process_healthy_cluster()
                    ret = self.evaluate_scheduled_restart() or msg
            finally:
                # we might not have a valid PostgreSQL connection here if another thread
                # stops PostgreSQL, therefore, we only reload replication slots if no
                # asynchronous processes are running (should be always the case for the master)
                if not self._async_executor.busy and not self.state_handler.is_starting():
                    create_slots = self.state_handler.slots_handler.sync_replication_slots(self.cluster,
                                                                                           self.patroni.nofailover)
                    if not self.state_handler.cb_called:
                        if not self.state_handler.is_leader():
                            self._rewind.trigger_check_diverged_lsn()
                        self.state_handler.call_nowait(ACTION_ON_START)
                    if create_slots and self.cluster.leader:
                        err = self._async_executor.try_run_async('copy_logical_slots',
                                                                 self.state_handler.slots_handler.copy_logical_slots,
                                                                 args=(self.cluster.leader, create_slots))
                        if not err:
                            ret = 'Copying logical slots {0} from the primary'.format(create_slots)
            return ret
        except DCSError:
            dcs_failed = True
            logger.error('Error communicating with DCS')
            if not self.is_paused() and self.state_handler.is_running() and self.state_handler.is_leader():
                self.demote('offline')
                return 'demoted self because DCS is not accessible and i was a leader'
            return 'DCS is not accessible'
        except (psycopg2.Error, PostgresConnectionException):
            return 'Error communicating with PostgreSQL. Will try again later'
        finally:
            if not dcs_failed:
                self.touch_member()

    def run_cycle(self):
        with self._async_executor:
            try:
                info = self._run_cycle()
                return (self.is_paused() and 'PAUSE: ' or '') + info
            except PatroniFatalException:
                raise
            except Exception:
                logger.exception('Unexpected exception')
                return 'Unexpected exception raised, please report it as a BUG'

    def shutdown(self):
        if self.is_paused():
            logger.info('Leader key is not deleted and Postgresql is not stopped due paused state')
            self.watchdog.disable()
        elif not self._join_aborted:
            # FIXME: If stop doesn't reach safepoint quickly enough keepalive is triggered. If shutdown checkpoint
            # takes longer than ttl, then leader key is lost and replication might not have sent out all WAL.
            # This might not be the desired behavior of users, as a graceful shutdown of the host can mean lost data.
            # We probably need to something smarter here.
            disable_wd = self.watchdog.disable if self.watchdog.is_running else None
            self.while_not_sync_standby(lambda: self.state_handler.stop(checkpoint=False, on_safepoint=disable_wd,
                                                                        stop_timeout=self.master_stop_timeout()))
            if not self.state_handler.is_running():
                if self.is_leader():
                    checkpoint_location = self.state_handler.latest_checkpoint_location()
                    self.dcs.delete_leader(checkpoint_location)
                self.touch_member()
            else:
                # XXX: what about when Patroni is started as the wrong user that has access to the watchdog device
                # but cannot shut down PostgreSQL. Root would be the obvious example. Would be nice to not kill the
                # system due to a bad config.
                logger.error("PostgreSQL shutdown failed, leader key not removed." +
                             (" Leaving watchdog running." if self.watchdog.is_running else ""))

    def watch(self, timeout):
        # watch on leader key changes if the postgres is running and leader is known and current node is not lock owner
        if self._async_executor.busy or not self.cluster or self.cluster.is_unlocked() or self.has_lock(False):
            leader_index = None
        else:
            leader_index = self.cluster.leader.index

        return self.dcs.watch(leader_index, timeout)

    def wakeup(self):
        """Call of this method will trigger the next run of HA loop if there is
        no "active" leader watch request in progress.
        This usually happens on the master or if the node is running async action"""
        self.dcs.event.set()

    def get_remote_member(self, member=None):
        """ In case of standby cluster this will tel us from which remote
            master to stream. Config can be both patroni config or
            cluster.config.data
        """
        cluster_params = self.get_standby_cluster_config()

        if cluster_params:
            name = member.name if member else 'remote_master:{}'.format(uuid.uuid1())

            data = {k: v for k, v in cluster_params.items() if k in RemoteMember.allowed_keys()}
            data['no_replication_slot'] = 'primary_slot_name' not in cluster_params
            conn_kwargs = member.conn_kwargs() if member else \
                {k: cluster_params[k] for k in ('host', 'port') if k in cluster_params}
            if conn_kwargs:
                data['conn_kwargs'] = conn_kwargs

            return RemoteMember(name, data)

    def get_remote_master(self):
        return self.get_remote_member()
