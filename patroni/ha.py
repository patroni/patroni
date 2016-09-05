import json
import logging
import psycopg2
import requests
import sys
import datetime
import pytz

from multiprocessing.pool import ThreadPool
from patroni.async_executor import AsyncExecutor
from patroni.exceptions import DCSError, PostgresConnectionException
from patroni.postgresql import ACTION_ON_START
from patroni.utils import sleep

logger = logging.getLogger(__name__)


class Ha(object):

    def __init__(self, patroni):
        self.patroni = patroni
        self.state_handler = patroni.postgresql
        self.dcs = patroni.dcs
        self.cluster = None
        self.old_cluster = None
        self.recovering = False
        self._async_executor = AsyncExecutor()

    def is_paused(self):
        return self.cluster and self.cluster.is_paused()

    def load_cluster_from_dcs(self):
        cluster = self.dcs.get_cluster()

        # We want to keep the state of cluster when it was healthy
        if not cluster.is_unlocked() or not self.old_cluster:
            self.old_cluster = cluster
        self.cluster = cluster

    def acquire_lock(self):
        return self.dcs.attempt_to_acquire_leader()

    def update_lock(self):
        ret = self.dcs.update_leader()
        if ret and not self._async_executor.busy:
            try:
                self.dcs.write_leader_optime(self.state_handler.last_operation())
            except:
                pass
        return ret

    def has_lock(self):
        lock_owner = self.cluster.leader and self.cluster.leader.name
        logger.info('Lock owner: %s; I am %s', lock_owner, self.state_handler.name)
        return lock_owner == self.state_handler.name

    def touch_member(self):
        data = {
            'conn_url': self.state_handler.connection_string,
            'api_url': self.patroni.api.connection_string,
            'state': self.state_handler.state,
            'role': self.state_handler.role
        }
        if self.patroni.tags:
            data['tags'] = self.patroni.tags
        if self.state_handler.pending_restart:
            data['pending_restart'] = True
        if not self._async_executor.busy and data['state'] in ['running', 'restarting', 'starting']:
            try:
                data['xlog_location'] = self.state_handler.xlog_position()
            except:
                pass
        if self.patroni.scheduled_restart:
            scheduled_restart_data = self.patroni.scheduled_restart.copy()
            scheduled_restart_data['schedule'] = scheduled_restart_data['schedule'].isoformat()
            data['scheduled_restart'] = scheduled_restart_data

        self.dcs.touch_member(json.dumps(data, separators=(',', ':')))

    def clone(self, clone_member=None, msg='(without leader)'):
        if self.state_handler.clone(clone_member):
            logger.info('bootstrapped %s', msg)
            cluster = self.dcs.get_cluster()
            node_to_follow = self._get_node_to_follow(cluster)
            self.state_handler.follow(node_to_follow, cluster.leader, True)
        else:
            logger.error('failed to bootstrap %s', msg)
            self.state_handler.remove_data_directory()

    def bootstrap(self):
        if not self.cluster.is_unlocked():  # cluster already has leader
            clone_member = self.cluster.get_clone_member()
            member_role = 'leader' if clone_member == self.cluster.leader else 'replica'
            msg = "from {0} '{1}'".format(member_role, clone_member.name)
            self._async_executor.schedule('bootstrap {0}'.format(msg))
            self._async_executor.run_async(self.clone, args=(clone_member, msg))
            return 'trying to bootstrap {0}'.format(msg)
        # no initialize key and node is allowed to be master and has 'bootstrap' section in a configuration file
        elif self.cluster.initialize is None and not self.patroni.nofailover and 'bootstrap' in self.patroni.config:
            if self.dcs.initialize(create_new=True):  # race for initialization
                try:
                    self.state_handler.bootstrap(self.patroni.config['bootstrap'])
                    self.dcs.initialize(create_new=False, sysid=self.state_handler.sysid)
                except:  # initdb or start failed
                    # remove initialization key and give a chance to other members
                    logger.info("removing initialize key after failed attempt to initialize the cluster")
                    self.dcs.cancel_initialization()
                    self.state_handler.stop('immediate')
                    self.state_handler.move_data_directory()
                    raise
                self.dcs.set_config_value(json.dumps(self.patroni.config.dynamic_configuration, separators=(',', ':')))
                self.dcs.take_leader()
                self.load_cluster_from_dcs()
                return 'initialized a new cluster'
            else:
                return 'failed to acquire initialize lock'
        else:
            if self.state_handler.can_create_replica_without_replication_connection():
                self._async_executor.run_async(self.clone)
                return "trying to bootstrap (without leader)"
            return 'waiting for leader to bootstrap'

    def recover(self):
        self.recovering = True
        return self.follow("starting as readonly because i had the session lock", "starting as a secondary", True, True)

    def _get_node_to_follow(self, cluster):
        # determine the node to follow. If replicatefrom tag is set,
        # try to follow the node mentioned there, otherwise, follow the leader.
        if not self.patroni.replicatefrom or self.patroni.replicatefrom == self.state_handler.name:
            node_to_follow = cluster.leader
        else:
            node_to_follow = cluster.get_member(self.patroni.replicatefrom)

        return node_to_follow if node_to_follow and node_to_follow.name != self.state_handler.name else None

    def follow(self, demote_reason, follow_reason, refresh=True, recovery=False, need_rewind=None):
        if refresh:
            self.load_cluster_from_dcs()

        if recovery:
            ret = demote_reason if self.has_lock() else follow_reason
        else:
            is_leader = self.state_handler.is_leader()
            ret = demote_reason if is_leader else follow_reason

        node_to_follow = self._get_node_to_follow(self.cluster)

        if self.is_paused() and not self.state_handler.need_rewind:
            self.state_handler.set_role('master' if is_leader else 'replica')
            if is_leader:
                return 'continue to run as master without lock'
            elif not node_to_follow:
                return 'no action'

        self.state_handler.follow(node_to_follow, self.cluster.leader, recovery, self._async_executor, need_rewind)

        return ret

    def enforce_master_role(self, message, promote_message):
        if self.state_handler.is_leader() or self.state_handler.role == 'master':
            # Inform the state handler about its master role.
            # It may be unaware of it if postgres is promoted manually.
            self.state_handler.set_role('master')
            return message
        else:
            self.state_handler.promote()
            self.touch_member()
            return promote_message

    @staticmethod
    def fetch_node_status(member):
        """This function perform http get request on member.api_url and fetches its status
        :returns: tuple(`member`, reachable, in_recovery, xlog_location)

        reachable - `!False` if the node is not reachable or is not responding with correct JSON
        in_recovery - `!True` if pg_is_in_recovery() == true
        xlog_location - value of `replayed_location` or `location` from JSON, dependin on its role.
        tags - dictionary with values of different tags (i.e. nofailover)
        """

        try:
            response = requests.get(member.api_url, timeout=2, verify=False)
            logger.info('Got response from %s %s: %s', member.name, member.api_url, response.content)
            json = response.json()
            is_master = json['role'] == 'master'
            xlog_location = None if is_master else json['xlog']['replayed_location']
            return (member, True, not is_master, xlog_location, json.get('tags', {}))
        except Exception as e:
            logger.warning("request failed: GET %s (%s)", member.api_url, e)
        return (member, False, None, 0, {})

    def fetch_nodes_statuses(self, members):
        pool = ThreadPool(len(members))
        results = pool.map(self.fetch_node_status, members)  # Run API calls on members in parallel
        pool.close()
        pool.join()
        return results

    def _is_healthiest_node(self, members, check_replication_lag=True):
        """This method tries to determine whether I am healthy enough to became a new leader candidate or not."""

        if check_replication_lag and not self.state_handler.check_replication_lag(self.cluster.last_leader_operation):
            return False  # Too far behind last reported xlog location on master

        # Prepare list of nodes to run check against
        members = [m for m in members if m.name != self.state_handler.name and not m.nofailover and m.api_url]

        if members:
            my_xlog_location = self.state_handler.xlog_position()
            for member, reachable, in_recovery, xlog_location, tags in self.fetch_nodes_statuses(members):
                if reachable and not tags.get('nofailover', False):  # If the node is unreachable it's not healhy
                    if not in_recovery:
                        logger.warning('Master (%s) is still alive', member.name)
                        return False
                    if my_xlog_location < xlog_location:
                        return False
        return True

    def is_failover_possible(self, members):
        ret = False
        members = [m for m in members if m.name != self.state_handler.name and not m.nofailover and m.api_url]
        if members:
            for member, reachable, _, _, tags in self.fetch_nodes_statuses(members):
                if reachable and not tags.get('nofailover', False):
                    ret = True  # TODO: check xlog_location
                elif not reachable:
                    logger.info('Member %s is not reachable', member.name)
                elif tags.get('nofailover', False):
                    logger.info('Member %s is not allowed to promote', member.name)
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
                member, reachable, _, _, tags = self.fetch_node_status(member)
                if reachable and not tags.get('nofailover', False):  # node is healthy
                    logger.info('manual failover: to %s, i am %s', member.name, self.state_handler.name)
                    return False
                # we wanted to failover to specific member but it is not healthy
                if not reachable:
                    logger.warning('manual failover: member %s is unhealthy', member.name)
                elif tags.get('nofailover', False):
                    logger.warning('manual failover: member %s is not allowed to promote', member.name)

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
        if self.is_paused() and not self.patroni.nofailover and \
                self.cluster.failover and not self.cluster.failover.scheduled_at:
            ret = self.manual_failover_process_no_leader()
            if ret is not None:  # continue if we just deleted the stale failover key as a master
                return ret

        if self.state_handler.is_leader():  # leader is always the healthiest
            return True

        if self.is_paused():
            return False

        if self.patroni.nofailover:  # nofailover tag makes node always unhealthy
            return False

        if self.cluster.failover:
            return self.manual_failover_process_no_leader()

        # run usual health check
        members = {m.name: m for m in self.cluster.members + self.old_cluster.members}
        return self._is_healthiest_node(members.values())

    def demote(self, delete_leader=True):
        if delete_leader:
            self.state_handler.stop()
            self.state_handler.set_role('demoted')
            self.dcs.delete_leader()
            self.touch_member()
            self.dcs.reset_cluster()
            sleep(2)  # Give a time to somebody to take the leader lock
            cluster = self.dcs.get_cluster()
            node_to_follow = self._get_node_to_follow(cluster)
            self.state_handler.follow(node_to_follow, cluster.leader, recovery=True, need_rewind=True)
        else:
            self.state_handler.follow(None, None)

    def should_run_scheduled_action(self, action_name, scheduled_at, cleanup_fn):
        if scheduled_at and not self.is_paused():
            # If the scheduled action is in the far future, we shouldn't do anything and just return.
            # If the scheduled action is in the past, we consider the value to be stale and we remove
            # the value.
            # If the value is close to now, we initiate the scheduled action
            # Additionally, if the scheduled action cannot be executed altogether, i.e. there is an error
            # or the action is in the past - we take care of cleaning it up.
            now = datetime.datetime.now(pytz.utc)
            try:
                delta = (scheduled_at - now).total_seconds()

                if delta > self.dcs.loop_wait:
                    logger.info('Awaiting %s at %s (in %.0f seconds)',
                                action_name, scheduled_at.isoformat(), delta)
                    return False
                elif delta < - int(self.dcs.loop_wait * 1.5):
                    logger.warning('Found a stale %s value, cleaning up: %s',
                                   action_name, scheduled_at.isoformat())
                    cleanup_fn()
                    return False

                # The value is very close to now
                sleep(max(delta, 0))
                logger.info('Manual scheduled {0} at %s'.format(action_name), scheduled_at.isoformat())
                return True
            except TypeError:
                logger.warning('Incorrect value of scheduled_at: %s', scheduled_at)
                cleanup_fn()
        return False

    def process_manual_failover_from_leader(self):
        failover = self.cluster.failover

        if (failover.scheduled_at and not
            self.should_run_scheduled_action("failover", failover.scheduled_at, lambda:
                                             self.dcs.manual_failover('', '', index=failover.index))):
            return

        if not failover.leader or failover.leader == self.state_handler.name:
            if not failover.candidate or failover.candidate != self.state_handler.name:
                if not failover.candidate and self.is_paused():
                    logger.warning('Failover is possible only to a specific candidate in a paused state')
                else:
                    members = [m for m in self.cluster.members
                               if not failover.candidate or m.name == failover.candidate]
                    if self.is_failover_possible(members):  # check that there are healthy members
                        self._async_executor.schedule('manual failover: demote')
                        self._async_executor.run_async(self.demote)
                        return 'manual failover: demoting myself'
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
                return self.enforce_master_role('acquired session lock as a leader',
                                                'promoted self to leader by acquiring session lock')
            else:
                return self.follow('demoted self after trying and failing to obtain lock',
                                   'following new leader after trying and failing to obtain lock')
        else:
            # when we are doing manual failover there is no guaranty that new leader is ahead of any other node
            # node tagged as nofailover can be ahead of the new leader either, but it is always excluded from elections
            need_rewind = bool(self.cluster.failover) or self.patroni.nofailover
            if need_rewind:
                sleep(2)  # Give a time to somebody to take the leader lock

            if self.patroni.nofailover:
                return self.follow('demoting self because I am not allowed to become master',
                                   'following a different leader because I am not allowed to promote',
                                   need_rewind=need_rewind)
            return self.follow('demoting self because i am not the healthiest node',
                               'following a different leader because i am not the healthiest node',
                               need_rewind=need_rewind)

    def process_healthy_cluster(self):
        if self.has_lock():
            if self.cluster.failover and (not self.is_paused() or self.state_handler.is_leader()):
                msg = self.process_manual_failover_from_leader()
                if msg is not None:
                    return msg

            if self.is_paused() and not self.state_handler.is_leader():
                if self.cluster.failover and self.cluster.failover.candidate == self.state_handler.name:
                    return 'waiting to become master after promote...'

                self.dcs.delete_leader()
                self.dcs.reset_cluster()
                return 'removed leader lock because postgres is not running as master'

            if self.update_lock():
                return self.enforce_master_role('no action.  i am the leader with the lock',
                                                'promoted self to leader because i had the session lock')
            else:
                # Either there is no connection to DCS or someone else acquired the lock
                logger.error('failed to update leader lock')
                self.load_cluster_from_dcs()
        else:
            logger.info('does not have lock')
        return self.follow('demoting self because i do not have the lock and i was a leader',
                           'no action.  i am a secondary and i am following a leader', False)

    def evaluate_scheduled_restart(self):
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

        if (postgres_version and
           self.state_handler.postgres_version_to_int(postgres_version) <= int(self.state_handler.server_version)):
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

    def restart(self, restart_data=None, run_async=False):
        """ conditional and unconditional restart """
        if (restart_data and isinstance(restart_data, dict) and
            not self.restart_matches(restart_data.get('role'),
                                     restart_data.get('postgres_version'),
                                     ('restart_pending' in restart_data))):
            return (False, "restart conditions are not satisfied")

        with self._async_executor:
            prev = self._async_executor.schedule('restart')
            if prev is not None:
                return (False, prev + ' already in progress')

        if run_async:
            self._async_executor.run_async(self.state_handler.restart)
            return (True, 'restart initiated')
        elif self._async_executor.run(self.state_handler.restart):
            return (True, 'restarted successfully')
        else:
            return (False, 'restart failed')

    def _do_reinitialize(self, cluster):
        self.state_handler.stop('immediate')
        self.state_handler.remove_data_directory()

        clone_member = self.cluster.get_clone_member()
        member_role = 'leader' if clone_member == self.cluster.leader else 'replica'
        self.clone(clone_member, "from {0} '{1}'".format(member_role, clone_member.name))

    def reinitialize(self):
        with self._async_executor:
            self.load_cluster_from_dcs()

            if self.cluster.is_unlocked():
                return 'Cluster has no leader, can not reinitialize'

            if self.cluster.leader.name == self.state_handler.name:
                return 'I am the leader, can not reinitialize'

            action = self._async_executor.schedule('reinitialize', immediately=True)
            if action is not None:
                return '{0} already in progress'.format(action)

        self._async_executor.run_async(self._do_reinitialize, args=(self.cluster, ))

    def handle_long_action_in_progress(self):
        if self.has_lock():
            if self.update_lock():
                return 'updated leader lock during ' + self._async_executor.scheduled_action
            else:
                return 'failed to update leader lock during ' + self._async_executor.scheduled_action
        elif self.cluster.is_unlocked():
            return 'not healthy enough for leader race'
        else:
            return self._async_executor.scheduled_action + ' in progress'

    @staticmethod
    def sysid_valid(sysid):
        # sysid does tv_sec << 32, where tv_sec is the number of seconds sine 1970,
        # so even 1 << 32 would have 10 digits.
        sysid = str(sysid)
        return len(sysid) >= 10 and sysid.isdigit()

    def post_recover(self):
        if not self.state_handler.is_running():
            if self.has_lock():
                self.dcs.delete_leader()
                self.dcs.reset_cluster()
                return 'removed leader key after trying and failing to start postgres'
            return 'failed to start postgres'
        return None

    def _run_cycle(self):
        try:
            self.load_cluster_from_dcs()

            self.touch_member()

            # cluster has leader key but not initialize key
            if not (self.cluster.is_unlocked() or self.sysid_valid(self.cluster.initialize)) and self.has_lock():
                self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)

            if not (self.cluster.is_unlocked() or self.cluster.config and self.cluster.config.data) and self.has_lock():
                self.dcs.set_config_value(json.dumps(self.patroni.config.dynamic_configuration, separators=(',', ':')))

            if self._async_executor.busy:
                return self.handle_long_action_in_progress()

            # we've got here, so any async action has finished. Check if we tried to recover and failed
            if self.recovering and not self.state_handler.need_rewind:
                self.recovering = False
                msg = self.post_recover()
                if msg is not None:
                    return msg

            # is data directory empty?
            if self.state_handler.data_directory_empty():
                return self.bootstrap()  # new node
            # "bootstrap", but data directory is not empty
            elif not self.sysid_valid(self.cluster.initialize) and self.cluster.is_unlocked() and not self.is_paused():
                self.dcs.initialize(create_new=(self.cluster.initialize is None), sysid=self.state_handler.sysid)
            else:
                # check if we are allowed to join
                if self.sysid_valid(self.cluster.initialize) and self.cluster.initialize != self.state_handler.sysid:
                    logger.fatal("system ID mismatch, node %s belongs to a different cluster: %s != %s",
                                 self.state_handler.name, self.cluster.initialize, self.state_handler.sysid)
                    sys.exit(1)

            if not self.state_handler.is_healthy():
                if self.is_paused():
                    if self.has_lock():
                        self.dcs.delete_leader()
                        self.dcs.reset_cluster()
                        return 'removed leader lock because postgres is not running'
                    elif not self.state_handler.need_rewind:
                        return 'postgres is not running'

                # try to start dead postgres
                return self.recover()

            try:
                if self.cluster.is_unlocked():
                    return self.process_unhealthy_cluster()
                else:
                    msg = self.evaluate_scheduled_restart()
                    if msg is not None:
                        return msg
                    return self.process_healthy_cluster()
            finally:
                # we might not have a valid PostgreSQL connection here if another thread
                # stops PostgreSQL, therefore, we only reload replication slots if no
                # asynchronous processes are running (should be always the case for the master)
                if not self._async_executor.busy:
                    if not self.state_handler.cb_called:
                        self.state_handler.call_nowait(ACTION_ON_START)
                    self.state_handler.sync_replication_slots(self.cluster)
        except DCSError:
            logger.error('Error communicating with DCS')
            if not self.is_paused() and self.state_handler.is_running() and self.state_handler.is_leader():
                self.demote(delete_leader=False)
                return 'demoted self because DCS is not accessible and i was a leader'
            return 'DCS is not accessible'
        except (psycopg2.Error, PostgresConnectionException):
            return 'Error communicating with PostgreSQL. Will try again later'

    def run_cycle(self):
        with self._async_executor:
            info = self._run_cycle()
            return (self.is_paused() and 'PAUSE: ' or '') + info
