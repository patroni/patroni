import abc
import dateutil
import importlib
import inspect
import json
import logging
import os
import pkgutil
import re
import six
import sys
import time

from collections import defaultdict, namedtuple
from copy import deepcopy
from patroni.exceptions import PatroniException
from patroni.utils import parse_bool, uri
from random import randint
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl
from threading import Event, Lock

slot_name_re = re.compile('^[a-z0-9_]{1,63}$')
logger = logging.getLogger(__name__)


def slot_name_from_member_name(member_name):
    """Translate member name to valid PostgreSQL slot name.

    PostgreSQL replication slot names must be valid PostgreSQL names. This function maps the wider space of
    member names to valid PostgreSQL names. Names are lowercased, dashes and periods common in hostnames
    are replaced with underscores, other characters are encoded as their unicode codepoint. Name is truncated
    to 64 characters. Multiple different member names may map to a single slot name."""

    def replace_char(match):
        c = match.group(0)
        return '_' if c in '-.' else "u{:04d}".format(ord(c))

    slot_name = re.sub('[^a-z0-9_]', replace_char, member_name.lower())
    return slot_name[0:63]


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
        toc = set()
        for importer in pkgutil.iter_importers(dcs_dirname):
            if hasattr(importer, 'toc'):
                toc |= importer.toc
        return [module for module in toc if module.startswith(module_prefix) and module.count('.') == 2]
    else:
        return [module_prefix + name for _, name, is_pkg in pkgutil.iter_modules([dcs_dirname]) if not is_pkg]


def get_dcs(config):
    modules = dcs_modules()

    for module_name in modules:
        name = module_name.split('.')[-1]
        if name in config:  # we will try to import only modules which have configuration section in the config file
            try:
                module = importlib.import_module(module_name)
                for key, item in module.__dict__.items():  # iterate through the module content
                    # try to find implementation of AbstractDCS interface, class name must match with module_name
                    if key.lower() == name and inspect.isclass(item) and issubclass(item, AbstractDCS):
                        # propagate some parameters
                        config[name].update({p: config[p] for p in ('namespace', 'name', 'scope', 'loop_wait',
                                             'patronictl', 'ttl', 'retry_timeout') if p in config})
                        return item(config[name])
            except ImportError:
                logger.debug('Failed to import %s', module_name)

    available_implementations = []
    for module_name in modules:
        name = module_name.split('.')[-1]
        try:
            module = importlib.import_module(module_name)
            available_implementations.extend(name for key, item in module.__dict__.items() if key.lower() == name
                                             and inspect.isclass(item) and issubclass(item, AbstractDCS))
        except ImportError:
            logger.info('Failed to import %s', module_name)
    raise PatroniException("""Can not find suitable configuration of distributed configuration store
Available implementations: """ + ', '.join(sorted(set(available_implementations))))


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
        conn_url = self.data.get('conn_url')
        conn_kwargs = self.data.get('conn_kwargs')
        if conn_url:
            return conn_url

        if conn_kwargs:
            conn_url = uri('postgresql', (conn_kwargs.get('host'), conn_kwargs.get('port', 5432)))
            self.data['conn_url'] = conn_url
            return conn_url

    def conn_kwargs(self, auth=None):
        defaults = {
            "host": "",
            "port": "",
            "database": ""
        }
        ret = self.data.get('conn_kwargs')
        if ret:
            defaults.update(ret)
            ret = defaults
        else:
            r = urlparse(self.conn_url)
            ret = {
                'host': r.hostname,
                'port': r.port or 5432,
                'database': r.path[1:]
            }
            self.data['conn_kwargs'] = ret.copy()

        # apply any remaining authentication parameters
        if auth and isinstance(auth, dict):
            ret.update({k: v for k, v in auth.items() if v is not None})
            if 'username' in auth:
                ret['user'] = ret.pop('username')
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


class RemoteMember(Member):
    """ Represents a remote master for a standby cluster
    """
    def __new__(cls, name, data):
        return super(RemoteMember, cls).__new__(cls, None, name, None, data)

    @staticmethod
    def allowed_keys():
        return ('primary_slot_name',
                'create_replica_methods',
                'restore_command',
                'archive_cleanup_command',
                'recovery_min_apply_delay',
                'no_replication_slot')

    def __getattr__(self, name):
        if name in RemoteMember.allowed_keys():
            return self.data.get(name)


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
    def data(self):
        return self.member.data

    @property
    def timeline(self):
        return self.data.get('timeline')

    @property
    def checkpoint_after_promote(self):
        """
        >>> Leader(1, '', Member.from_node(1, '', '', '{"version":"z"}')).checkpoint_after_promote
        """
        version = self.data.get('version')
        if version:
            try:
                # 1.5.6 is the last version which doesn't expose checkpoint_after_promote: false
                if tuple(map(int, version.split('.'))) > (1, 5, 6):
                    return self.data['role'] == 'master' and 'checkpoint_after_promote' not in self.data
            except Exception:
                logger.debug('Failed to parse Patroni version %s', version)


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
        False
        """

        try:
            data = json.loads(data)
        except (TypeError, ValueError):
            data = None
            modify_index = 0
        if not isinstance(data, dict):
            data = {}
        return ClusterConfig(index, data, index if modify_index is None else modify_index)

    @property
    def permanent_slots(self):
        return isinstance(self.data, dict) and (
                self.data.get('permanent_replication_slots') or
                self.data.get('permanent_slots') or self.data.get('slots')
        ) or {}


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


class TimelineHistory(namedtuple('TimelineHistory', 'index,value,lines')):
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
        return TimelineHistory(index, value, lines)


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

    def check_mode(self, mode):
        return bool(self.config and parse_bool(self.config.data.get(mode)))

    def is_paused(self):
        return self.check_mode('pause')

    def is_synchronous_mode(self):
        return self.check_mode('synchronous_mode')

    def get_replication_slots(self, my_name, role):
        # if the replicatefrom tag is set on the member - we should not create the replication slot for it on
        # the current master, because that member would replicate from elsewhere. We still create the slot if
        # the replicatefrom destination member is currently not a member of the cluster (fallback to the
        # master), or if replicatefrom destination member happens to be the current master
        use_slots = self.config and self.config.data.get('postgresql', {}).get('use_slots', True)
        if role in ('master', 'standby_leader'):
            slot_members = [m.name for m in self.members if use_slots and m.name != my_name and
                            (m.replicatefrom is None or m.replicatefrom == my_name or
                             not self.has_member(m.replicatefrom))]
            permanent_slots = (self.config and self.config.permanent_slots or {}).copy()
        else:
            # only manage slots for replicas that replicate from this one, except for the leader among them
            slot_members = [m.name for m in self.members if use_slots and
                            m.replicatefrom == my_name and m.name != self.leader.name]
            permanent_slots = {}

        slots = {slot_name_from_member_name(name): {'type': 'physical'} for name in slot_members}

        if len(slots) < len(slot_members):
            # Find which names are conflicting for a nicer error message
            slot_conflicts = defaultdict(list)
            for name in slot_members:
                slot_conflicts[slot_name_from_member_name(name)].append(name)
            logger.error("Following cluster members share a replication slot name: %s",
                         "; ".join("{} map to {}".format(", ".join(v), k)
                                   for k, v in slot_conflicts.items() if len(v) > 1))

        # "merge" replication slots for members with permanent_replication_slots
        for name, value in permanent_slots.items():
            if not slot_name_re.match(name):
                logger.error("Invalid permanent replication slot name '%s'", name)
                logger.error("Slot name may only contain lower case letters, numbers, and the underscore chars")
                continue

            value = deepcopy(value) if value else {'type': 'physical'}
            if isinstance(value, dict):
                if 'type' not in value:
                    value['type'] = 'logical' if value.get('database') and value.get('plugin') else 'physical'

                if value['type'] == 'physical':
                    if name != my_name:  # Don't try to create permanent physical replication slot for yourself
                        slots[name] = value
                    continue
                elif value['type'] == 'logical' and value.get('database') and value.get('plugin'):
                    if name in slots:
                        logger.error("Permanent logical replication slot {'%s': %s} is conflicting with" +
                                     " physical replication slot for cluster member", name, value)
                    else:
                        slots[name] = value
                    continue

            logger.error("Bad value for slot '%s' in permanent_slots: %s", name, permanent_slots[name])

        return slots

    def has_permanent_logical_slots(self, name):
        slots = self.get_replication_slots(name, 'master').values()
        return any(v for v in slots if v.get("type") == "logical")

    @property
    def timeline(self):
        """
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, 0).timeline
        0
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, TimelineHistory.from_node(1, '[]')).timeline
        1
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, TimelineHistory.from_node(1, '[["a"]]')).timeline
        0
        """
        if self.history:
            if self.history.lines:
                try:
                    return int(self.history.lines[-1][0]) + 1
                except Exception:
                    logger.error('Failed to parse cluster history from DCS: %s', self.history.lines)
            elif self.history.value == '[]':
                return 1
        return 0


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
        self._base_path = re.sub('/+', '/', '/'.join(['', config.get('namespace', 'service'), config['scope']]))
        self._set_loop_wait(config.get('loop_wait', 10))

        self._ctl = bool(config.get('patronictl', False))
        self._cluster = None
        self._cluster_valid_till = 0
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
    def ttl(self):
        """Get new ttl value"""

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
        try:
            cluster = self._load_cluster()
        except Exception:
            self.reset_cluster()
            raise

        with self._cluster_thread_lock:
            self._cluster = cluster
            self._cluster_valid_till = time.time() + self.ttl
            return cluster

    @property
    def cluster(self):
        with self._cluster_thread_lock:
            return self._cluster if self._cluster_valid_till > time.time() else None

    def reset_cluster(self):
        with self._cluster_thread_lock:
            self._cluster = None
            self._cluster_valid_till = 0

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

    def update_leader(self, last_operation, access_is_restricted=False):
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
    def touch_member(self, data, permanent=False):
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
