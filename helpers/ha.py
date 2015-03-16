import sys, time, re, urllib2, json, psycopg2
from base64 import b64decode

import helpers.errors

import inspect

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
        return self.etcd.update_leader(self.state_handler.name)

    def is_unlocked(self):
        return self.etcd.leader_unlocked()

    def has_lock(self):
        return self.etcd.am_i_leader(self.state_handler.name)

    def fetch_current_leader(self):
            return self.etcd.current_leader()

    def run_cycle(self):
        try:
            print lineno()
            if self.state_handler.is_healthy():
                print lineno()
                if self.is_unlocked():
                    print lineno()
                    if self.state_handler.is_healthiest_node():
                        print lineno()
                        if self.acquire_lock():
                            print lineno()
                            if not self.state_handler.is_leader():
                                print lineno()
                                self.state_handler.promote()
                                return "promoted self to leader by acquiring session lock"

                            print lineno()
                            return "acquired session lock as a leader"
                        else:
                            print lineno()
                            if self.state_handler.is_leader():
                                print lineno()
                                self.state_handler.demote(self.fetch_current_leader())
                                return "demoted self due after trying and failing to obtain lock"
                            else:
                                print lineno()
                                self.state_handler.follow_the_leader(self.fetch_current_leader())
                                return "following new leader after trying and failing to obtain lock"
                    else:
                        print lineno()
                        if self.state_handler.is_leader():
                            print lineno()
                            self.state_handler.demote(self.fetch_current_leader())
                            return "demoting self because i am not the healthiest node"
                        else:
                            print lineno()
                            self.state_handler.follow_the_leader(self.fetch_current_leader())
                            return "following a different leader because i am not the healthiest node"

                else:
                    print lineno()
                    if self.has_lock():
                        print lineno()
                        self.update_lock()

                        if not self.state_handler.is_leader():
                            print lineno()
                            self.state_handler.promote()
                            return "promoted self to leader because i had the session lock"
                        else:
                            print lineno()
                            return "no action.  i am the leader with the lock"
                    else:
                        print lineno()
                        print "does not have lock"
                        if self.state_handler.is_leader():
                            print lineno()
                            self.state_handler.demote(self.fetch_current_leader())
                            return "demoting self because i do not have the lock and i was a leader"
                        else:
                            print lineno()
                            self.state_handler.follow_the_leader(self.fetch_current_leader())
                            return "no action.  i am a secondary and i am following a leader"
            else:
                print lineno()
                return "no action.  not healthy enough to do anything."
        except helpers.errors.CurrentLeaderError:
            print lineno()
            print "failed to fetch current leader from etcd"
        except psycopg2.OperationalError:
            print lineno()
            print "Error communicating with Postgresql.  Will try again."
        except helpers.errors.HealthiestMemberError:
            print lineno()
            print "failed to determine healthiest member fromt etcd"

    def run(self):
        while True:
            self.run_cycle()
            time.sleep(10)
