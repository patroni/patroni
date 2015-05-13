import sys, time, re, urllib2, json, psycopg2
import logging
from base64 import b64decode

import helpers.errors

import inspect

logger = logging.getLogger(__name__)


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno

class Ha:
    def __init__(self, state_handler, etcd):
        self.state_handler = state_handler
        self.etcd = etcd

    def acquire_lock(self):
        return self.etcd.attempt_to_acquire_leader(self.state_handler.name)

    def update_lock(self):
        return self.etcd.update_leader(self.state_handler)

    def update_last_leader_operation(self):
        return self.etcd.update_last_leader_operation(self.state_handler.last_operation)

    def is_unlocked(self):
        return self.etcd.leader_unlocked()

    def has_lock(self):
        return self.etcd.am_i_leader(self.state_handler.name)

    def fetch_current_leader(self):
            return self.etcd.current_leader()

    def run_cycle(self):
        try:
            if self.state_handler.is_healthy():
                if self.is_unlocked():
                    if self.state_handler.is_healthiest_node(self.etcd):
                        if self.acquire_lock():
                            if not self.state_handler.is_leader():
                                self.state_handler.promote()
                                return "promoted self to leader by acquiring session lock"

                            return "acquired session lock as a leader"
                        else:
                            if self.state_handler.is_leader():
                                self.state_handler.demote(self.fetch_current_leader())
                                return "demoted self due after trying and failing to obtain lock"
                            else:
                                self.state_handler.follow_the_leader(self.fetch_current_leader())
                                return "following new leader after trying and failing to obtain lock"
                    else:
                        if self.state_handler.is_leader():
                            self.state_handler.demote(self.fetch_current_leader())
                            return "demoting self because i am not the healthiest node"
                        elif self.fetch_current_leader() is None:
                            self.state_handler.follow_no_leader()
                            return "waiting on leader to be elected because i am not the healthiest node"
                        else:
                            self.state_handler.follow_the_leader(self.fetch_current_leader())
                            return "following a different leader because i am not the healthiest node"

                else:
                    if self.has_lock():
                        self.update_lock()

                        if not self.state_handler.is_leader():
                            self.state_handler.promote()
                            return "promoted self to leader because i had the session lock"
                        else:
                            return "no action.  i am the leader with the lock"
                    else:
                        logger.info("does not have lock")
                        if self.state_handler.is_leader():
                            self.state_handler.demote(self.fetch_current_leader())
                            return "demoting self because i do not have the lock and i was a leader"
                        else:
                            self.state_handler.follow_the_leader(self.fetch_current_leader())
                            return "no action.  i am a secondary and i am following a leader"
            else:
                if not self.state_handler.is_running():
                    self.state_handler.start()
                    return "postgresql was stopped.  starting again."
                return "no action.  not healthy enough to do anything."
        except helpers.errors.CurrentLeaderError:
            logger.error("failed to fetch current leader from etcd")
        except psycopg2.OperationalError:
            logger.error("Error communicating with Postgresql.  Will try again.")
        except helpers.errors.HealthiestMemberError:
            logger.error("failed to determine healthiest member fromt etcd")

    def run(self):
        while True:
            self.run_cycle()
            time.sleep(10)
