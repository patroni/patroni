import abc

from collections import namedtuple
from helpers.utils import calculate_ttl, sleep
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl


def parse_connection_string(value):
    scheme, netloc, path, params, query, fragment = urlparse(value)
    conn_url = urlunparse((scheme, netloc, path, params, '', fragment))
    api_url = ([v for n, v in parse_qsl(query) if n == 'application_name'] or [None])[0]
    return conn_url, api_url


class DCSError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        """
        >>> str(DCSError('foo'))
        "'foo'"
        """
        return repr(self.value)


class Member(namedtuple('Member', 'index,name,conn_url,api_url,expiration,ttl')):

    def real_ttl(self):
        return calculate_ttl(self.expiration) or -1


class Cluster(namedtuple('Cluster', 'initialize,leader,last_leader_operation,members')):

    def is_unlocked(self):
        return not (self.leader and self.leader.name)


class AbstractDCS:

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, config):
        self._name = name
        self._base_path = '/service/' + config['scope']

    def client_path(self, path):
        return self._base_path + path

    @abc.abstractmethod
    def get_cluster(self):
        """get_cluster"""

    @abc.abstractmethod
    def update_leader(self, state_handler):
        """update_leader"""

    @abc.abstractmethod
    def attempt_to_acquire_leader(self):
        """attempt_to_acquire_leader"""

    def current_leader(self):
        try:
            cluster = self.get_cluster()
            return None if cluster.is_unlocked() else cluster.leader
        except DCSError:
            return None

    @abc.abstractmethod
    def touch_member(self, connection_string, ttl=None):
        """touch_member"""

    @abc.abstractmethod
    def take_leader(self):
        """take_leader"""

    @abc.abstractmethod
    def race(self, path):
        """race"""

    @abc.abstractmethod
    def delete_leader(self):
        """delete_leader"""

    def sleep(self, timeout):
        sleep(timeout)
