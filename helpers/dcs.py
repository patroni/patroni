import abc

from collections import namedtuple
from helpers.utils import calculate_ttl, sleep
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl


def parse_connection_string(value):
    """Original Governor stores connection strings for each cluster members if a following format:
        postgres://{username}:{password}@{connect_address}/postgres
    Since each of our patroni instances provides own REST API endpoint it's good to store this information
    in DCS among with postgresql connection string. In order to not introduce new keys and be compatible with
    original Governor we decided to extend original connection string in a following way:
        postgres://{username}:{password}@{connect_address}/postgres?application_name={api_url}
    This way original Governor could use such connection string as it is, because of feature of `libpq` library.

    This method is able to split connection string stored in DCS into two parts, `conn_url` and `api_url`"""

    scheme, netloc, path, params, query, fragment = urlparse(value)
    conn_url = urlunparse((scheme, netloc, path, params, '', fragment))
    api_url = ([v for n, v in parse_qsl(query) if n == 'application_name'] or [None])[0]
    return conn_url, api_url


class DCSError(Exception):
    """Parent class for all kind of exceptions related to selected distributed configuration store"""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        """
        >>> str(DCSError('foo'))
        "'foo'"
        """
        return repr(self.value)


class Member(namedtuple('Member', 'index,name,conn_url,api_url,expiration,ttl')):
    """Immutable object (namedtuple) which represents single member of PostgreSQL cluster.
    Consists of the following fields:
    :param index: modification index of a given member key in a Configuration Store
    :param name: name of PostgreSQL cluster member
    :param conn_url: connection string containing host, user and password which could be used to access this member.
    :param api_url: REST API url of patroni instance
    :param expiration: expiration time of given member key
    :param ttl: ttl of given member key in seconds"""

    def real_ttl(self):
        return calculate_ttl(self.expiration) or -1


class Leader(namedtuple('Leader', 'index,expiration,ttl,member')):
    """Immutable object (namedtuple) which represents leader key.
    Consists of the following fields:
    :param index: modification index of a leader key in a Configuration Store
    :param expiration: expiration time of the leader key
    :param ttl: ttl of the leader key
    :param member: reference to a `Member` object which represents current leader (see `Cluster.members`)"""


class Cluster(namedtuple('Cluster', 'initialize,leader,last_leader_operation,members')):
    """Immutable object (namedtuple) which represents PostgreSQL cluster.
    Consists of the following fields:
    :param initialize: boolean, shows whether this cluster has initialization key stored in DC or not.
    :param leader: `Leader` object which represents current leader of the cluster
    :param last_leader_operation: int or long object containing position of last known leader operation.
        This value is stored in `/optime/leader` key
    :param members: list of Member object, all PostgreSQL cluster members including leader"""

    def is_unlocked(self):
        return not (self.leader and self.leader.member.name)


class AbstractDCS:

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, config):
        """
        :param name: name of current instance (the same value as `~Postgresql.name`)
        :param config: dict, reference to config section of selected DCS.
            i.e.: `zookeeper` for zookeeper, `etcd` for etcd, etc...
        """
        self._name = name
        self._base_path = '/service/' + config['scope']

    def client_path(self, path):
        return self._base_path + path

    @abc.abstractmethod
    def get_cluster(self):
        """:returns: `Cluster` object which represent current state and topology of the cluster
        raise `~DCSError` in case of communication or other problems with DCS. If current instance was
            running as a master and exception raised instance would be demoted."""

    @abc.abstractmethod
    def update_leader(self, state_handler):
        """Update leader key (or session) ttl and `/optime/leader` key in DCS.

        :param state_handler: reference to `Postgresql` object
        :returns: `!True` if leader key (or session) has been updated successfully.
            If not, `!False` must be returned and current instance would be demoted.

        If you failed to update `/optime/leader` this error is not critical and you can return `!True`
        You have to use CAS (Compare And Swap) operation in order to update leader key,
        for example for etcd `prevValue` parameter must be used."""

    @abc.abstractmethod
    def attempt_to_acquire_leader(self):
        """Attempt to acquire leader lock
        This method should create `/leader` key with value=`~self._name`
        :returns: `!True` if key has been created successfully.

        Key must be created atomically. In case if key already exists it should not be
        overwritten and `!False` must be returned"""

    def current_leader(self):
        try:
            cluster = self.get_cluster()
            return None if cluster.is_unlocked() else cluster.leader
        except DCSError:
            return None

    @abc.abstractmethod
    def touch_member(self, connection_string, ttl=None):
        """Update member key in DCS.
        This method should create or update key with the name = '/members/' + `~self._name`
        and value = connection_string in a given DCS.

        :param connection_string: how this instance can be accessed by other instances
        :param ttl: ttl for member key, optional parameter. If it is None `~self.member_ttl will be used`
        :returns: `!True` on success otherwise `!False`
        """

    @abc.abstractmethod
    def take_leader(self):
        """This method should create leader key with value = `~self._name` and ttl=`~self.ttl`
        Since it could be called only on initial cluster bootstrap it could create this key regardless,
        overwriting the key if necessary."""

    @abc.abstractmethod
    def race(self, path):
        """Race for cluster initialization.
        :param path: usually this is just '/initialize'
        :returns: `!True` if key has been created successfully.

        this method should create atomically `path` key and return `!True`
        otherwise it should return `!False`"""

    @abc.abstractmethod
    def delete_leader(self):
        """Voluntarily remove leader key from DCS
        This method should remove leader key if current instance is the leader"""

    def sleep(self, timeout):
        sleep(timeout)
