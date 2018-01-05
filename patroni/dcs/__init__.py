import abc
import dateutil
import importlib
import inspect
import json
import logging
import os
import pkgutil
import six
import sys

from collections import namedtuple
from patroni.exceptions import PatroniException
from random import randint
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl
from threading import Event, Lock

logger = logging.getLogger(__name__)


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


def dcs_modules():
    """Get names of DCS modules, depending on execution environment. If being packaged with PyInstaller,
    modules aren't discoverable dynamically by scanning source directory because `FrozenImporter` doesn't
    implement `iter_modules` method. But it is still possible to find all potential DCS modules by
    iterating through `toc`, which contains list of all "frozen" resources."""

    dcs_dirname = os.path.dirname(__file__)
    module_prefix = __package__ + '.'

    if getattr(sys, 'frozen', False):
        importer = pkgutil.get_importer(dcs_dirname)
        return [module for module in list(importer.toc) if module.startswith(module_prefix) and module.count('.') == 2]
    else:
        return [module_prefix + name for _, name, is_pkg in pkgutil.iter_modules([dcs_dirname]) if not is_pkg]


def get_dcs(config):
    available_implementations = set()
    for module_name in dcs_modules():
        try:
            module = importlib.import_module(module_name)
            for name in filter(lambda name: not name.startswith('__'), dir(module)):  # iterate through module content
                item = getattr(module, name)
                name = name.lower()
                # try to find implementation of AbstractDCS interface, class name must match with module_name
                if inspect.isclass(item) and issubclass(item, AbstractDCS) and __package__ + '.' + name == module_name:
                    available_implementations.add(name)
                    if name in config:  # which has configuration section in the config file
                        # propagate some parameters
                        config[name].update({p: config[p] for p in ('namespace', 'name', 'scope', 'loop_wait',
                                             'patronictl', 'ttl', 'retry_timeout') if p in config})
                        return item(config[name])
        except ImportError:
            if not config.get('patronictl'):
                logger.info('Failed to import %s', module_name)
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

    def conn_kwargs(self, auth=None):
        ret = self.data.get('conn_kwargs')
        if ret:
            ret = ret.copy()
        else:
            r = urlparse(self.conn_url)
            ret = {
                'host': r.hostname,
                'port': r.port or 5432,
                'database': r.path[1:]
            }
            self.data['conn_kwargs'] = ret.copy()

        if auth and isinstance(auth, dict):
            if 'username' in auth:
                ret['user'] = auth['username']
            if 'password' in auth:
                ret['password'] = auth['password']
        return ret

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
        return self.tags.get('clonefrom', False) and bool(self.conn_url)

    @property
    def state(self):
        return self.data.get('state', 'unknown')

    @property
    def is_running(self):
        return self.state == 'running'


class Leader(namedtuple('Leader', 'index,session,member')):

    """Immutable object (namedtuple) which represents leader key.
    Consists of the following fields:
    :param index: modification index of a leader key in a Configuration Store
    :param session: either session id or just ttl in seconds
    :param member: reference to a `Member` object which represents current leader (see `Cluster.members`)"""

    @property
    def name(self):
        return self.member.name

    def conn_kwargs(self, auth=None):
        return self.member.conn_kwargs(auth)

    @property
    def conn_url(self):
        return self.member.conn_url

    @property
    def timeline(self):
        return self.member.data.get('timeline')


class Failover(namedtuple('Failover', 'index,leader,candidate,scheduled_at')):

    """
    >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader"}'))
    True
    >>> 'Failover' in str(Failover.from_node(1, {"leader": "cluster_leader"}))
    True
    >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader", "member": "cluster_candidate"}'))
    True
    >>> Failover.from_node(1, 'null') is None
    False
    >>> n = '{"leader": "cluster_leader", "member": "cluster_candidate", "scheduled_at": "2016-01-14T10:09:57.1394Z"}'
    >>> 'tzinfo=' in str(Failover.from_node(1, n))
    True
    >>> Failover.from_node(1, None) is None
    False
    >>> Failover.from_node(1, '{}') is None
    False
    >>> 'abc' in Failover.from_node(1, 'abc:def')
    True
    """
    @staticmethod
    def from_node(index, value):
        if isinstance(value, dict):
            data = value
        elif value:
            try:
                data = json.loads(value)
                if not isinstance(data, dict):
                    data = {}
            except ValueError:
                t = [a.strip() for a in value.split(':')]
                leader = t[0]
                candidate = t[1] if len(t) > 1 else None
                return Failover(index, leader, candidate, None) if leader or candidate else None
        else:
            data = {}

        if data.get('scheduled_at'):
            data['scheduled_at'] = dateutil.parser.parse(data['scheduled_at'])

        return Failover(index, data.get('leader'), data.get('member'), data.get('scheduled_at'))

    def __len__(self):
        return int(bool(self.leader)) + int(bool(self.candidate))


class ClusterConfig(namedtuple('ClusterConfig', 'index,data,modify_index')):

    @staticmethod
    def from_node(index, data, modify_index=None):
        """
        >>> ClusterConfig.from_node(1, '{') is None
        True
        """

        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            return None
        return ClusterConfig(index, data, modify_index or index)


class SyncState(namedtuple('SyncState', 'index,leader,sync_standby')):
    """Immutable object (namedtuple) which represents last observed synhcronous replication state

    :param index: modification index of a synchronization key in a Configuration Store
    :param leader: reference to member that was leader
    :param sync_standby: standby that was last synchronized to leader
    """

    @staticmethod
    def from_node(index, value):
        """
        >>> SyncState.from_node(1, None).leader is None
        True
        >>> SyncState.from_node(1, '{}').leader is None
        True
        >>> SyncState.from_node(1, '{').leader is None
        True
        >>> SyncState.from_node(1, '[]').leader is None
        True
        >>> SyncState.from_node(1, '{"leader": "leader"}').leader == "leader"
        True
        >>> SyncState.from_node(1, {"leader": "leader"}).leader == "leader"
        True
        """
        if isinstance(value, dict):
            data = value
        elif value:
            try:
                data = json.loads(value)
                if not isinstance(data, dict):
                    data = {}
            except (TypeError, ValueError):
                data = {}
        else:
            data = {}
        return SyncState(index, data.get('leader'), data.get('sync_standby'))

    def matches(self, name):
        """
        Returns if a node name matches one of the nodes in the sync state

        >>> s = SyncState(1, 'foo', 'bar')
        >>> s.matches('foo')
        True
        >>> s.matches('bar')
        True
        >>> s.matches('baz')
        False
        >>> s.matches(None)
        False
        >>> SyncState(1, None, None).matches('foo')
        False
        """
        return name is not None and name in (self.leader, self.sync_standby)


class TimelineHistory(namedtuple('TimelineHistory', 'index,lines')):
    """Object representing timeline history file"""

    @staticmethod
    def from_node(index, value):
        """
        >>> h = TimelineHistory.from_node(1, 2)
        >>> h.lines
        []
        """
        try:
            lines = json.loads(value)
        except (TypeError, ValueError):
            lines = None
        if not isinstance(lines, list):
            lines = []
        return TimelineHistory(index, lines)


class Cluster(namedtuple('Cluster', 'initialize,config,leader,last_leader_operation,members,failover,sync,history')):

    """Immutable object (namedtuple) which represents PostgreSQL cluster.
    Consists of the following fields:
    :param initialize: shows whether this cluster has initialization key stored in DC or not.
    :param config: global dynamic configuration, reference to `ClusterConfig` object
    :param leader: `Leader` object which represents current leader of the cluster
    :param last_leader_operation: int or long object containing position of last known leader operation.
        This value is stored in `/optime/leader` key
    :param members: list of Member object, all PostgreSQL cluster members including leader
    :param failover: reference to `Failover` object
    :param sync: reference to `SyncState` object, last observed synchronous replication state.
    :param history: reference to `TimelineHistory` object
    """

    def is_unlocked(self):
        return not (self.leader and self.leader.name)

    def has_member(self, member_name):
        return any(m for m in self.members if m.name == member_name)

    def get_member(self, member_name, fallback_to_leader=True):
        return ([m for m in self.members if m.name == member_name] or [self.leader if fallback_to_leader else None])[0]

    def get_clone_member(self, exclude):
        exclude = [exclude] + [self.leader.name] if self.leader else []
        candidates = [m for m in self.members if m.clonefrom and m.is_running and m.name not in exclude]
        return candidates[randint(0, len(candidates) - 1)] if candidates else self.leader

    def is_paused(self):
        return self.config and self.config.data.get('pause', False) or False

    def is_synchronous_mode(self):
        return bool(self.config and self.config.data.get('synchronous_mode'))

    def is_synchronous_mode_strict(self):
        return bool(self.config and self.config.data.get('synchronous_mode_strict'))


@six.add_metaclass(abc.ABCMeta)
class AbstractDCS(object):

    _INITIALIZE = 'initialize'
    _CONFIG = 'config'
    _LEADER = 'leader'
    _FAILOVER = 'failover'
    _HISTORY = 'history'
    _MEMBERS = 'members/'
    _OPTIME = 'optime'
    _LEADER_OPTIME = _OPTIME + '/' + _LEADER
    _SYNC = 'sync'

    def __init__(self, config):
        """
        :param config: dict, reference to config section of selected DCS.
            i.e.: `zookeeper` for zookeeper, `etcd` for etcd, etc...
        """
        self._name = config['name']
        self._base_path = os.path.join('/', config.get('namespace', '/service/').strip('/'), config['scope'])
        self._set_loop_wait(config.get('loop_wait', 10))

        self._ctl = bool(config.get('patronictl', False))
        self._cluster = None
        self._cluster_thread_lock = Lock()
        self._last_leader_operation = ''
        self.event = Event()

    def client_path(self, path):
        return '/'.join([self._base_path, path.lstrip('/')])

    @property
    def initialize_path(self):
        return self.client_path(self._INITIALIZE)

    @property
    def config_path(self):
        return self.client_path(self._CONFIG)

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
    def history_path(self):
        return self.client_path(self._HISTORY)

    @property
    def leader_optime_path(self):
        return self.client_path(self._LEADER_OPTIME)

    @property
    def sync_path(self):
        return self.client_path(self._SYNC)

    @abc.abstractmethod
    def set_ttl(self, ttl):
        """Set the new ttl value for leader key"""

    @abc.abstractmethod
    def set_retry_timeout(self, retry_timeout):
        """Set the new value for retry_timeout"""

    def _set_loop_wait(self, loop_wait):
        self._loop_wait = loop_wait

    def reload_config(self, config):
        self._set_loop_wait(config['loop_wait'])
        self.set_ttl(config['ttl'])
        self.set_retry_timeout(config['retry_timeout'])

    @property
    def loop_wait(self):
        return self._loop_wait

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
            except Exception:
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
    def _write_leader_optime(self, last_operation):
        """write current xlog location into `/optime/leader` key in DCS
        :param last_operation: absolute xlog location in bytes
        :returns: `!True` on success."""

    def write_leader_optime(self, last_operation):
        if self._last_leader_operation != last_operation and self._write_leader_optime(last_operation):
            self._last_leader_operation = last_operation

    @abc.abstractmethod
    def _update_leader(self):
        """Update leader key (or session) ttl

        :returns: `!True` if leader key (or session) has been updated successfully.
            If not, `!False` must be returned and current instance would be demoted.

        You have to use CAS (Compare And Swap) operation in order to update leader key,
        for example for etcd `prevValue` parameter must be used."""

    def update_leader(self, last_operation):
        """Update leader key (or session) ttl and optime/leader

        :param last_operation: absolute xlog location in bytes
        :returns: `!True` if leader key (or session) has been updated successfully.
            If not, `!False` must be returned and current instance would be demoted."""

        ret = self._update_leader()
        if ret and last_operation:
            self.write_leader_optime(last_operation)
        return ret

    @abc.abstractmethod
    def attempt_to_acquire_leader(self, permanent=False):
        """Attempt to acquire leader lock
        This method should create `/leader` key with value=`~self._name`
        :param permanent: if set to `!True`, the leader key will never expire.
         Used in patronictl for the external master
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
        return self.set_failover_value(json.dumps(failover_value, separators=(',', ':')), index)

    @abc.abstractmethod
    def set_config_value(self, value, index=None):
        """Create or update `/config` key"""

    @abc.abstractmethod
    def touch_member(self, data, ttl=None, permanent=False):
        """Update member key in DCS.
        This method should create or update key with the name = '/members/' + `~self._name`
        and value = data in a given DCS.

        :param data: information about instance (including connection strings)
        :param ttl: ttl for member key, optional parameter. If it is None `~self.member_ttl will be used`
        :param permanent: if set to `!True`, the member key will never expire.
         Used in patronictl for the external master.
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

    @staticmethod
    def sync_state(leader, sync_standby):
        """Build sync_state dict"""
        return {'leader': leader, 'sync_standby': sync_standby}

    def write_sync_state(self, leader, sync_standby, index=None):
        sync_value = self.sync_state(leader, sync_standby)
        return self.set_sync_state_value(json.dumps(sync_value, separators=(',', ':')), index)

    @abc.abstractmethod
    def set_history_value(self, value):
        """"""

    @abc.abstractmethod
    def set_sync_state_value(self, value, index=None):
        """"""

    @abc.abstractmethod
    def delete_sync_state(self, index=None):
        """"""

    def watch(self, leader_index, timeout):
        """If the current node is a master it should just sleep.
        Any other node should watch for changes of leader key with a given timeout

        :param leader_index: index of a leader key
        :param timeout: timeout in seconds
        :returns: `!True` if you would like to reschedule the next run of ha cycle"""

        self.event.wait(timeout)
        return self.event.isSet()
