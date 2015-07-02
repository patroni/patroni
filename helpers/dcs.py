import abc

from collections import namedtuple
from helpers.utils import calculate_ttl


class DCSError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Member(namedtuple('Member', 'index,name,conn_url,api_url,expiration,ttl')):

    def real_ttl(self):
        return calculate_ttl(self.expiration) or -1


class Cluster(namedtuple('Cluster', 'initialize,leader,last_leader_operation,members')):

    def is_unlocked(self):
        return not (self.leader and self.leader.name)


class AbstractDCS:

    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self._base_path = '/service/' + config['scope']

    def client_path(self, path):
        return self._base_path + path

    @abc.abstractmethod
    def get_cluster(self):
        raise NotImplementedError

    @abc.abstractmethod
    def update_leader(self, state_handler):
        raise NotImplementedError

    @abc.abstractmethod
    def attempt_to_acquire_leader(self, value):
        raise NotImplementedError

    def current_leader(self):
        try:
            cluster = self.get_cluster()
            return None if cluster.is_unlocked() else cluster.leader
        except DCSError:
            return None

    @abc.abstractmethod
    def touch_member(self, member, connection_string, ttl=None):
        raise NotImplementedError

    @abc.abstractmethod
    def take_leader(self, value):
        raise NotImplementedError

    @abc.abstractmethod
    def race(self, path, value):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_member(self, member):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_leader(self, value):
        raise NotImplementedError
