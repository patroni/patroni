import logging

from helpers.errors import EtcdError, HealthiestMemberError
from psycopg2 import OperationalError

logger = logging.getLogger(__name__)


class Ha:

    def __init__(self, state_handler, etcd):
        self.state_handler = state_handler
        self.etcd = etcd
        self.cluster = None

    def load_cluster_from_etcd(self):
        self.cluster = self.etcd.get_cluster()

    def acquire_lock(self):
        return self.etcd.attempt_to_acquire_leader(self.state_handler.name)

    def update_lock(self):
        return self.etcd.update_leader(self.state_handler)

    def is_unlocked(self):
        return not (self.cluster.leader and self.cluster.leader.hostname)

    def has_lock(self):
        lock_owner = self.cluster.leader and self.cluster.leader.hostname
        logger.info('Lock owner: %s; I am %s', lock_owner, self.state_handler.name)
        return lock_owner == self.state_handler.name

    def demote(self):
        return self.state_handler.demote(self.cluster.leader)

    def follow_the_leader(self):
        return self.state_handler.follow_the_leader(self.cluster.leader)

    def run_cycle(self):
        try:
            self.load_cluster_from_etcd()
            if not self.state_handler.is_healthy():
                has_lock = self.has_lock()
                self.state_handler.write_recovery_conf(None if has_lock else self.cluster.leader)
                self.state_handler.start()
                if not has_lock:
                    return 'started as a secondary'
                logging.info('started as readonly because i had the session lock')
                self.load_cluster_from_etcd()

            if self.is_unlocked():
                if self.state_handler.is_healthiest_node(self.cluster):
                    if self.acquire_lock():
                        if not self.state_handler.is_leader():
                            self.state_handler.promote()
                            return 'promoted self to leader by acquiring session lock'
                        return 'acquired session lock as a leader'
                    else:
                        self.load_cluster_from_etcd()
                        if self.state_handler.is_leader():
                            self.demote()
                            return 'demoted self due after trying and failing to obtain lock'
                        else:
                            self.follow_the_leader()
                            return 'following new leader after trying and failing to obtain lock'
                else:
                    self.load_cluster_from_etcd()
                    if self.state_handler.is_leader():
                        self.demote()
                        return 'demoting self because i am not the healthiest node'
                    else:
                        self.follow_the_leader()
                        return 'following a different leader because i am not the healthiest node'
            else:
                if self.has_lock() and self.update_lock():
                    try:
                        if not self.state_handler.is_leader():
                            self.state_handler.promote()
                            return 'promoted self to leader because i had the session lock'
                        else:
                            return 'no action.  i am the leader with the lock'
                    finally:
                        # create replication slots
                        self.state_handler.create_replication_slots([m.hostname for m in self.cluster.members])
                else:
                    logger.info('does not have lock')
                    if self.state_handler.is_leader():
                        self.demote()
                        return 'demoting self because i do not have the lock and i was a leader'
                    else:
                        self.follow_the_leader()
                        return 'no action.  i am a secondary and i am following a leader'
        except EtcdError:
            logger.error('Error communicating with Etcd')
        except OperationalError:
            logger.error('Error communicating with Postgresql.  Will try again')
        except HealthiestMemberError:
            logger.error('failed to determine healthiest member fromt etcd')
