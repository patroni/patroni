import logging
import requests

from patroni.dcs import DCSError
from psycopg2 import InterfaceError, OperationalError
from multiprocessing.pool import ThreadPool

logger = logging.getLogger(__name__)


class Ha:

    def __init__(self, state_handler, dcs):
        self.state_handler = state_handler
        self.dcs = dcs
        self.cluster = None
        self.old_cluster = None

    def load_cluster_from_dcs(self):
        cluster = self.dcs.get_cluster()

        # We want to keep the state of cluster when it was healhy
        if cluster.is_unlocked() and self.cluster and not self.cluster.is_unlocked():
            self.old_cluster = self.cluster
        if not self.old_cluster:
            self.old_cluster = cluster
        self.cluster = cluster

    def acquire_lock(self):
        return self.dcs.attempt_to_acquire_leader()

    def update_lock(self):
        return self.dcs.update_leader(self.state_handler)

    def has_lock(self):
        lock_owner = self.cluster.leader and self.cluster.leader.name
        logger.info('Lock owner: %s; I am %s', lock_owner, self.state_handler.name)
        return lock_owner == self.state_handler.name

    def demote(self):
        return self.state_handler.demote(self.cluster.leader)

    def follow_the_leader(self):
        return self.state_handler.follow_the_leader(self.cluster.leader)

    @staticmethod
    def fetch_node_status(member):
        try:
            response = requests.get(member.api_url, timeout=2)
            json = response.json()
            logger.info('Got response from %s: %s', member.name, json)
            in_recovery = json.get('role', 'slave') != 'master'
            xlog_location = json.get(('replayed_location' if in_recovery else 'location'), 0)
            return (member, True, in_recovery, xlog_location)
        except:
            logging.exception('request failed: GET %s', member.api_url)
        return (member, False, None, 0)

    def is_healthiest_node(self):
        if self.state_handler.is_leader():
            return True

        if not self.state_handler.check_replication_lag(self.cluster.last_leader_operation):
            return False

        members = [m for m in self.old_cluster.members if m.name != self.state_handler.name and m.api_url]

        if members:
            pool = ThreadPool(len(members))
            results = pool.map(self.fetch_node_status, members)
            pool.close()
            pool.join()

            my_xlog_location = self.state_handler.xlog_position()
            for member, reachable, in_recovery, xlog_location in results:
                if reachable:
                    if not in_recovery:
                        logger.warning('Master (%s) is still alive', member.name)
                        return False
                    if my_xlog_location < xlog_location:
                        return False
        return True

    def run_cycle(self):
        try:
            self.load_cluster_from_dcs()
            if not self.state_handler.is_healthy():
                has_lock = self.has_lock()
                self.state_handler.write_recovery_conf(None if has_lock else self.cluster.leader)
                self.state_handler.start()
                if not has_lock:
                    return 'started as a secondary'
                logger.info('started as readonly because i had the session lock')
                self.load_cluster_from_dcs()

            if self.cluster.is_unlocked():
                if self.is_healthiest_node():
                    if self.acquire_lock():
                        if self.state_handler.is_leader() or self.state_handler.is_promoted:
                            return 'acquired session lock as a leader'
                        else:
                            self.state_handler.promote()
                            return 'promoted self to leader by acquiring session lock'
                    else:
                        self.load_cluster_from_dcs()
                        if self.state_handler.is_leader():
                            self.demote()
                            return 'demoted self due after trying and failing to obtain lock'
                        else:
                            self.follow_the_leader()
                            return 'following new leader after trying and failing to obtain lock'
                else:
                    self.load_cluster_from_dcs()
                    if self.state_handler.is_leader():
                        self.demote()
                        return 'demoting self because i am not the healthiest node'
                    else:
                        self.follow_the_leader()
                        return 'following a different leader because i am not the healthiest node'
            else:
                if self.has_lock() and self.update_lock():
                    if self.state_handler.is_leader() or self.state_handler.is_promoted:
                        return 'no action.  i am the leader with the lock'
                    else:
                        self.state_handler.promote()
                        return 'promoted self to leader because i had the session lock'
                else:
                    logger.info('does not have lock')
                    if self.state_handler.is_leader():
                        self.demote()
                        return 'demoting self because i do not have the lock and i was a leader'
                    else:
                        self.follow_the_leader()
                        return 'no action.  i am a secondary and i am following a leader'
        except DCSError:
            logger.error('Error communicating with DCS')
            if self.state_handler.is_leader():
                self.state_handler.demote(None)
                return 'demoted self because DCS is not accessible and i was a leader'
        except (InterfaceError, OperationalError):
            logger.error('Error communicating with Postgresql.  Will try again')
