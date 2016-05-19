import abc
import dateutil
import importlib
import inspect
import json
import os
import six

from collections import namedtuple
from patroni.exceptions import PatroniException
from random import randint
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl
from threading import Event, Lock


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


def get_dcs(node_name, config):
    available_implementations = []
    for name in os.listdir(os.path.dirname(__file__)):
        if name.endswith('.py') and not name.startswith('__'):  # find module
            module = importlib.import_module(__package__ + '.' + name[:-3])
            for name in dir(module):  # iterate through module content
                if not name.startswith('__'):  # skip internal stuff
                    value = getattr(module, name)
                    name = name.lower()
                    # try to find implementation of AbstractDCS interface
                    if inspect.isclass(value) and issubclass(value, AbstractDCS):
                        available_implementations.append(name)
                        if name in config:  # which has configuration section in the config file
                            # propagate some parameters
                            config[name].update({p: config[p] for p in ('namespace', 'scope', 'ttl') if p in config})
                            return value(node_name, config[name])
    raise PatroniException("""Can not find suitable configuration of distributed configuration store
Available implementations: """ + ', '.join(available_implementations))


class Member(namedtuple('Member', 'index,name,session,data')):

    """Immutable object (namedtuple) which represents single member of PostgreSQL cluster.
    Consists of the following fields:
    :param index: modification index of a given member key in a Configuration Store
    :param name: name of PostgreSQL cluster member
    :param session: either session id or just ttl in seconds
    :param data: arbitrary data i.e. conn_url, api_url, xlog location, state, role, tags, etc...

    There are two mandatory keys in a data:
    conn_url: connection string containing host, user and password which could be used to access this member.
    api_url: REST API url of patroni instance"""

    @staticmethod
    def from_node(index, name, session, data):
        """
        >>> Member.from_node(-1, '', '', '{"conn_url": "postgres://foo@bar/postgres"}') is not None
        True
        >>> Member.from_node(-1, '', '', '{')
        Member(index=-1, name='', session='', data={})
        """
        if data.startswith('postgres'):
            conn_url, api_url = parse_connection_string(data)
            data = {'conn_url': conn_url, 'api_url': api_url}
        else:
            try:
                data = json.loads(data)
            except (TypeError, ValueError):
                data = {}
        return Member(index, name, session, data)

    @property
    def conn_url(self):
        return self.data.get('conn_url')

    @property
    def api_url(self):
        return self.data.get('api_url')

    @property
    def tags(self):
        return self.data.get('tags', {})

    @property
    def nofailover(self):
        return self.tags.get('nofailover', False)

    @property
    def replicatefrom(self):
        return self.tags.get('replicatefrom')

    @property
    def clonefrom(self):
        return self.tags.get('clonefrom', False)


class Leader(namedtuple('Leader', 'index,session,member')):

    """Immutable object (namedtuple) which represents leader key.
    Consists of the following fields:
    :param index: modification index of a leader key in a Configuration Store
    :param session: either session id or just ttl in seconds
    :param member: reference to a `Member` object which represents current leader (see `Cluster.members`)"""

    @property
    def name(self):
        return self.member.name

    @property
    def conn_url(self):
        return self.member.conn_url


class Failover(namedtuple('Failover', 'index,leader,candidate,scheduled_at')):

    """
    >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader"}'))
    True
    >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader", "member": "cluster_candidate"}'))
    True
    >>> Failover.from_node(1, 'null') is None
    True
    >>> n = '{"leader": "cluster_leader", "member": "cluster_candidate", "scheduled_at": "2016-01-14T10:09:57.1394Z"}'
    >>> 'tzinfo=' in str(Failover.from_node(1, n))
    True
    >>> Failover.from_node(1, None) is None
    True
    >>> Failover.from_node(1, '{}') is None
    True
    >>> 'abc' in Failover.from_node(1, 'abc:def')
    True
    """
    @staticmethod
    def from_node(index, value):
        if not value:
            return None

        try:
            data = json.loads(value)
            if not data:
                return None
        except ValueError:
            t = [a.strip() for a in value.split(':')]
            leader = t[0]
            candidate = t[1] if len(t) > 1 else None
            return Failover(index, leader, candidate, None) if leader or candidate else None

        if data.get('scheduled_at'):
            data['scheduled_at'] = dateutil.parser.parse(data['scheduled_at'])

        return Failover(index, data.get('leader'), data.get('member'), data.get('scheduled_at'))


class Cluster(namedtuple('Cluster', 'initialize,leader,last_leader_operation,members,failover')):

    """Immutable object (namedtuple) which represents PostgreSQL cluster.
    Consists of the following fields:
    :param initialize: boolean, shows whether this cluster has initialization key stored in DC or not.
    :param leader: `Leader` object which represents current leader of the cluster
    :param last_leader_operation: int or long object containing position of last known leader operation.
        This value is stored in `/optime/leader` key
    :param members: list of Member object, all PostgreSQL cluster members including leader
    :param failover: reference to `Failover` object"""

    def is_unlocked(self):
        return not (self.leader and self.leader.name)

    def has_member(self, member_name):
        return any(m for m in self.members if m.name == member_name)

    def get_member(self, member_name, fallback_to_leader=True):
        return ([m for m in self.members if m.name == member_name] or [self.leader if fallback_to_leader else None])[0]

    def get_clone_member(self):
        candidates = [m for m in self.members if m.clonefrom and (not self.leader or m.name != self.leader.name)]
        return candidates[randint(0, len(candidates) - 1)] if candidates else self.leader


@six.add_metaclass(abc.ABCMeta)
class AbstractDCS(object):

    _INITIALIZE = 'initialize'
    _LEADER = 'leader'
    _FAILOVER = 'failover'
    _MEMBERS = 'members/'
    _OPTIME = 'optime'
    _LEADER_OPTIME = _OPTIME + '/' + _LEADER

    def __init__(self, name, config):
        """
        :param name: name of current instance (the same value as `~Postgresql.name`)
        :param config: dict, reference to config section of selected DCS.
            i.e.: `zookeeper` for zookeeper, `etcd` for etcd, etc...
        """
        self._name = name
        self._namespace = '/{0}'.format(config.get('namespace', '/service/').strip('/'))
        self._base_path = '/'.join([self._namespace, config['scope']])

        self._cluster = None
        self._cluster_thread_lock = Lock()
        self.event = Event()

    def client_path(self, path):
        return '/'.join([self._base_path, path.lstrip('/')])

    @property
    def initialize_path(self):
        return self.client_path(self._INITIALIZE)

    @property
    def members_path(self):
        return self.client_path(self._MEMBERS)

    @property
    def member_path(self):
        return self.client_path(self._MEMBERS + self._name)

    @property
    def leader_path(self):
        return self.client_path(self._LEADER)

    @property
    def failover_path(self):
        return self.client_path(self._FAILOVER)

    @property
    def leader_optime_path(self):
        return self.client_path(self._LEADER_OPTIME)

    @abc.abstractmethod
    def _load_cluster(self):
        """Internally this method should build  `Cluster` object which
           represents current state and topology of the cluster in DCS.
           this method supposed to be called only by `get_cluster` method.

           raise `~DCSError` in case of communication or other problems with DCS.
           If the current node was running as a master and exception raised,
           instance would be demoted."""

    def get_cluster(self):
        with self._cluster_thread_lock:
            try:
                self._load_cluster()
            except:
                self._cluster = None
                raise
            return self._cluster

    @property
    def cluster(self):
        with self._cluster_thread_lock:
            return self._cluster

    def reset_cluster(self):
        with self._cluster_thread_lock:
            self._cluster = None

    @abc.abstractmethod
    def write_leader_optime(self, last_operation):
        """write current xlog location into `/optime/leader` key in DCS
        :param last_operation: absolute xlog location in bytes"""

    @abc.abstractmethod
    def update_leader(self):
        """Update leader key (or session) ttl

        :returns: `!True` if leader key (or session) has been updated successfully.
            If not, `!False` must be returned and current instance would be demoted.

        You have to use CAS (Compare And Swap) operation in order to update leader key,
        for example for etcd `prevValue` parameter must be used."""

    @abc.abstractmethod
    def attempt_to_acquire_leader(self):
        """Attempt to acquire leader lock
        This method should create `/leader` key with value=`~self._name`
        :returns: `!True` if key has been created successfully.

        Key must be created atomically. In case if key already exists it should not be
        overwritten and `!False` must be returned"""

    @abc.abstractmethod
    def set_failover_value(self, value, index=None):
        """Create or update `/failover` key"""

    def manual_failover(self, leader, candidate, scheduled_at=None, index=None):
        failover_value = {}
        if leader:
            failover_value['leader'] = leader

        if candidate:
            failover_value['member'] = candidate

        if scheduled_at:
            failover_value['scheduled_at'] = scheduled_at.isoformat()

        return self.set_failover_value(json.dumps(failover_value), index)

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
    def initialize(self, create_new=True, sysid=""):
        """Race for cluster initialization.

        :param create_new: False if the key should already exist (in the case we are setting the system_id)
        :param sysid: PostgreSQL cluster system identifier, if specified, is written to the key
        :returns: `!True` if key has been created successfully.

        this method should create atomically initialize key and return `!True`
        otherwise it should return `!False`"""

    @abc.abstractmethod
    def delete_leader(self):
        """Voluntarily remove leader key from DCS
        This method should remove leader key if current instance is the leader"""

    @abc.abstractmethod
    def cancel_initialization(self):
        """ Removes the initialize key for a cluster """

    @abc.abstractmethod
    def delete_cluster(self):
        """Delete cluster from DCS"""

    def watch(self, timeout):
        """If the current node is a master it should just sleep.
        Any other node should watch for changes of leader key with a given timeout

        :param timeout: timeout in seconds
        :returns: `!True` if you would like to reschedule the next run of ha cycle"""

        self.event.wait(timeout)
        return self.event.isSet()
