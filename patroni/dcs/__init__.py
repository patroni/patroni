import abc
import dateutil.parser
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
from random import randint
from six.moves.urllib_parse import urlparse, urlunparse, parse_qsl
from threading import Event, Lock

from ..exceptions import PatroniFatalException
from ..utils import deep_compare, parse_bool, uri

CITUS_COORDINATOR_GROUP_ID = 0
citus_group_re = re.compile('^(0|[1-9][0-9]*)$')
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
        # dcs_dirname may contain a dot, which causes pkgutil.iter_importers()
        # to misinterpret the path as a package name. This can be avoided
        # altogether by not passing a path at all, because PyInstaller's
        # FrozenImporter is a singleton and registered as top-level finder.
        for importer in pkgutil.iter_importers():
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
                        # From citus section we only need "group" parameter, but will propagate everything just in case.
                        if isinstance(config.get('citus'), dict):
                            config[name].update(config['citus'])
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
    raise PatroniFatalException("""Can not find suitable configuration of distributed configuration store
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
                if not isinstance(data, dict):
                    data = {}
            except (TypeError, ValueError):
                data = {}
        return Member(index, name, session, data)

    @property
    def conn_url(self):
        conn_url = self.data.get('conn_url')
        if conn_url:
            return conn_url

        conn_kwargs = self.data.get('conn_kwargs')
        if conn_kwargs:
            conn_url = uri('postgresql', (conn_kwargs.get('host'), conn_kwargs.get('port', 5432)))
            self.data['conn_url'] = conn_url
            return conn_url

    def conn_kwargs(self, auth=None):
        defaults = {
            "host": None,
            "port": None,
            "dbname": None
        }
        ret = self.data.get('conn_kwargs')
        if ret:
            defaults.update(ret)
            ret = defaults
        else:
            conn_url = self.conn_url
            if not conn_url:
                return {}  # due to the invalid conn_url we don't care about authentication parameters
            r = urlparse(conn_url)
            ret = {
                'host': r.hostname,
                'port': r.port or 5432,
                'dbname': r.path[1:]
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

    @property
    def version(self):
        version = self.data.get('version')
        if version:
            try:
                return tuple(map(int, version.split('.')))
            except Exception:
                logger.debug('Failed to parse Patroni version %s', version)


class RemoteMember(Member):
    """Represents a remote member (typically a primary) for a standby cluster"""
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
        version = self.member.version
        # 1.5.6 is the last version which doesn't expose checkpoint_after_promote: false
        if version and version > (1, 5, 6):
            return self.data.get('role') in ('master', 'primary') and 'checkpoint_after_promote' not in self.data


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

    @property
    def ignore_slots_matchers(self):
        return isinstance(self.data, dict) and self.data.get('ignore_slots') or []

    @property
    def max_timelines_history(self):
        return self.data.get('max_timelines_history', 0)


class SyncState(namedtuple('SyncState', 'index,leader,sync_standby')):
    """Immutable object (namedtuple) which represents last observed synhcronous replication state

    :param index: modification index of a synchronization key in a Configuration Store
    :param leader: reference to member that was leader
    :param sync_standby: synchronous standby list (comma delimited) which are last synchronized to leader
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

    @property
    def members(self):
        """ Returns sync_standby in list """
        return self.sync_standby and self.sync_standby.split(',') or []

    def matches(self, name):
        """
        Returns if a node name matches one of the nodes in the sync state

        >>> s = SyncState(1, 'foo', 'bar,zoo')
        >>> s.matches('foo')
        True
        >>> s.matches('bar')
        True
        >>> s.matches('zoo')
        True
        >>> s.matches('baz')
        False
        >>> s.matches(None)
        False
        >>> SyncState(1, None, None).matches('foo')
        False
        """
        return name is not None and name in [self.leader] + self.members


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


class Cluster(namedtuple('Cluster', 'initialize,config,leader,last_lsn,members,'
                                    'failover,sync,history,slots,failsafe,workers')):

    """Immutable object (namedtuple) which represents PostgreSQL cluster.
    Consists of the following fields:
    :param initialize: shows whether this cluster has initialization key stored in DC or not.
    :param config: global dynamic configuration, reference to `ClusterConfig` object
    :param leader: `Leader` object which represents current leader of the cluster
    :param last_lsn: int or long object containing position of last known leader LSN.
        This value is stored in the `/status` key or `/optime/leader` (legacy) key
    :param members: list of Member object, all PostgreSQL cluster members including leader
    :param failover: reference to `Failover` object
    :param sync: reference to `SyncState` object, last observed synchronous replication state.
    :param history: reference to `TimelineHistory` object
    :param slots: state of permanent logical replication slots on the primary in the format: {"slot_name": int}
    :param failsafe: failsafe topology. Node is allowed to become the leader only if its name is found in this list.
    :param workers: workers of the Citus cluster, optional. Format: {int(group): Cluster()}"""

    def __new__(cls, *args):
        # Make workers argument optional
        if len(cls._fields) == len(args) + 1:
            args = args + ({},)
        return super(Cluster, cls).__new__(cls, *args)

    @property
    def leader_name(self):
        return self.leader and self.leader.name

    def is_unlocked(self):
        return not self.leader_name

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

    @property
    def __permanent_slots(self):
        return self.config and self.config.permanent_slots or {}

    @property
    def __permanent_physical_slots(self):
        return {name: value for name, value in self.__permanent_slots.items()
                if not value or isinstance(value, dict) and value.get('type', 'physical') == 'physical'}

    @property
    def __permanent_logical_slots(self):
        return {name: value for name, value in self.__permanent_slots.items() if isinstance(value, dict)
                and value.get('type', 'logical') == 'logical' and value.get('database') and value.get('plugin')}

    @property
    def use_slots(self):
        return self.config and (self.config.data.get('postgresql') or {}).get('use_slots', True)

    def get_replication_slots(self, my_name, role, nofailover, major_version, show_error=False):
        # if the replicatefrom tag is set on the member - we should not create the replication slot for it on
        # the current primary, because that member would replicate from elsewhere. We still create the slot if
        # the replicatefrom destination member is currently not a member of the cluster (fallback to the
        # primary), or if replicatefrom destination member happens to be the current primary
        use_slots = self.use_slots
        if role in ('master', 'primary', 'standby_leader'):
            slot_members = [m.name for m in self.members if use_slots and m.name != my_name and
                            (m.replicatefrom is None or m.replicatefrom == my_name or
                             not self.has_member(m.replicatefrom))]
            permanent_slots = self.__permanent_slots if use_slots and \
                role in ('master', 'primary') else self.__permanent_physical_slots
        else:
            # only manage slots for replicas that replicate from this one, except for the leader among them
            slot_members = [m.name for m in self.members if use_slots and
                            m.replicatefrom == my_name and m.name != self.leader_name]
            permanent_slots = self.__permanent_logical_slots if use_slots and not nofailover else {}

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
        disabled_permanent_logical_slots = []
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
                    # Don't try to create permanent physical replication slot for yourself
                    if name != slot_name_from_member_name(my_name):
                        slots[name] = value
                    continue
                elif value['type'] == 'logical' and value.get('database') and value.get('plugin'):
                    if major_version < 110000:
                        disabled_permanent_logical_slots.append(name)
                    elif name in slots:
                        logger.error("Permanent logical replication slot {'%s': %s} is conflicting with" +
                                     " physical replication slot for cluster member", name, value)
                    else:
                        slots[name] = value
                    continue

            logger.error("Bad value for slot '%s' in permanent_slots: %s", name, permanent_slots[name])

        if disabled_permanent_logical_slots and show_error:
            logger.error("Permanent logical replication slots supported by Patroni only starting from PostgreSQL 11. "
                         "Following slots will not be created: %s.", disabled_permanent_logical_slots)

        return slots

    def has_permanent_logical_slots(self, my_name, nofailover, major_version=110000):
        if major_version < 110000:
            return False
        slots = self.get_replication_slots(my_name, 'replica', nofailover, major_version).values()
        return any(v for v in slots if v.get("type") == "logical")

    def should_enforce_hot_standby_feedback(self, my_name, nofailover, major_version):
        """
        The hot_standby_feedback must be enabled if the current replica has logical slots
        or it is working as a cascading replica for the other node that has logical slots.
        """

        if major_version < 110000:
            return False

        if self.has_permanent_logical_slots(my_name, nofailover, major_version):
            return True

        if self.use_slots:
            members = [m for m in self.members if m.replicatefrom == my_name and m.name != self.leader_name]
            return any(self.should_enforce_hot_standby_feedback(m.name, m.nofailover, major_version) for m in members)
        return False

    def get_my_slot_name_on_primary(self, my_name, replicatefrom):
        """
        P <-- I <-- L
        In case of cascading replication we have to check not our physical slot,
        but slot of the replica that connects us to the primary.
        """

        m = self.get_member(replicatefrom, False) if replicatefrom else None
        return self.get_my_slot_name_on_primary(m.name, m.replicatefrom) if m else slot_name_from_member_name(my_name)

    @property
    def timeline(self):
        """
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, 0, 0, None).timeline
        0
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, TimelineHistory.from_node(1, '[]'), 0, None).timeline
        1
        >>> Cluster(0, 0, 0, 0, 0, 0, 0, TimelineHistory.from_node(1, '[["a"]]'), 0, None).timeline
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

    @property
    def min_version(self):
        return next(iter(sorted(filter(lambda v: v, [m.version for m in self.members])) + [None]))


class ReturnFalseException(Exception):
    pass


def catch_return_false_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ReturnFalseException:
            return False

    return wrapper


@six.add_metaclass(abc.ABCMeta)
class AbstractDCS(object):

    _INITIALIZE = 'initialize'
    _CONFIG = 'config'
    _LEADER = 'leader'
    _FAILOVER = 'failover'
    _HISTORY = 'history'
    _MEMBERS = 'members/'
    _OPTIME = 'optime'
    _STATUS = 'status'  # JSON, contains "leader_lsn" and confirmed_flush_lsn of logical "slots" on the leader
    _LEADER_OPTIME = _OPTIME + '/' + _LEADER  # legacy
    _SYNC = 'sync'
    _FAILSAFE = 'failsafe'

    def __init__(self, config):
        """
        :param config: dict, reference to config section of selected DCS.
            i.e.: `zookeeper` for zookeeper, `etcd` for etcd, etc...
        """
        self._name = config['name']
        self._base_path = re.sub('/+', '/', '/'.join(['', config.get('namespace', 'service'), config['scope']]))
        self._citus_group = str(config['group']) if isinstance(config.get('group'), six.integer_types) else None
        self._set_loop_wait(config.get('loop_wait', 10))

        self._ctl = bool(config.get('patronictl', False))
        self._cluster = None
        self._cluster_valid_till = 0
        self._cluster_thread_lock = Lock()
        self._last_lsn = ''
        self._last_seen = 0
        self._last_status = {}
        self._last_failsafe = {}
        self.event = Event()

    def client_path(self, path):
        components = [self._base_path]
        if self._citus_group:
            components.append(self._citus_group)
        components.append(path.lstrip('/'))
        return '/'.join(components)

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
    def status_path(self):
        return self.client_path(self._STATUS)

    @property
    def leader_optime_path(self):
        return self.client_path(self._LEADER_OPTIME)

    @property
    def sync_path(self):
        return self.client_path(self._SYNC)

    @property
    def failsafe_path(self):
        return self.client_path(self._FAILSAFE)

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

    @property
    def last_seen(self):
        return self._last_seen

    @abc.abstractmethod
    def _cluster_loader(self, path):
        """Load and build the `Cluster` object from DCS, which
        represents a single Patroni cluster.

        :param path: the path in DCS where to load Cluster(s) from.
        :returns: `Cluster`"""

    def _citus_cluster_loader(self, path):
        """Load and build `Cluster` onjects from DCS that represent all
        Patroni clusters from a single Citus cluster.

        :param path: the path in DCS where to load Cluster(s) from.
        :returns: all Citus groups as `dict`, with group ids as keys"""

    @abc.abstractmethod
    def _load_cluster(self, path, loader):
        """Internally this method should call the `loader` method that
        will build `Cluster` object which represents current state and
        topology of the cluster in DCS. This method supposed to be
        called only by `get_cluster` method.

        :param path: the path in DCS where to load Cluster(s) from.
        :param loader: one of `_cluster_loader` or `_citus_cluster_loader`
        :raise: `~DCSError` in case of communication problems with DCS.
        If the current node was running as a primary and exception
        raised, instance would be demoted."""

    def _bypass_caches(self):
        """Used only in zookeeper"""

    def is_citus_coordinator(self):
        return self._citus_group == str(CITUS_COORDINATOR_GROUP_ID)

    def get_citus_coordinator(self):
        try:
            path = '{0}/{1}/'.format(self._base_path, CITUS_COORDINATOR_GROUP_ID)
            return self._load_cluster(path, self._cluster_loader)
        except Exception as e:
            logger.error('Failed to load Citus coordinator cluster from %s: %r', self.__class__.__name__, e)

    def _get_citus_cluster(self):
        groups = self._load_cluster(self._base_path + '/', self._citus_cluster_loader)
        if isinstance(groups, Cluster):  # Zookeeper could return a cached version
            cluster = groups
        else:
            cluster = groups.pop(CITUS_COORDINATOR_GROUP_ID,
                                 Cluster(None, None, None, None, [], None, None, None, None, None))
            cluster.workers.update(groups)
        return cluster

    def get_cluster(self, force=False):
        if force:
            self._bypass_caches()
        try:
            cluster = self._get_citus_cluster() if self.is_citus_coordinator()\
                else self._load_cluster(self.client_path(''), self._cluster_loader)
        except Exception:
            self.reset_cluster()
            raise

        self._last_seen = int(time.time())
        self._last_status = {self._OPTIME: cluster.last_lsn, 'slots': cluster.slots}
        self._last_failsafe = cluster.failsafe

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
    def _write_leader_optime(self, last_lsn):
        """write current WAL LSN into `/optime/leader` key in DCS

        :param last_lsn: absolute WAL LSN in bytes
        :returns: `!True` on success."""

    def write_leader_optime(self, last_lsn):
        self.write_status({self._OPTIME: last_lsn})

    @abc.abstractmethod
    def _write_status(self, value):
        """write current WAL LSN and confirmed_flush_lsn of permanent slots into the `/status` key in DCS

        :param value: status serialized in JSON forman
        :returns: `!True` on success."""

    def write_status(self, value):
        if not deep_compare(self._last_status, value) and self._write_status(json.dumps(value, separators=(',', ':'))):
            self._last_status = value
        cluster = self.cluster
        min_version = cluster and cluster.min_version
        if min_version and min_version < (2, 1, 0) and self._last_lsn != value[self._OPTIME]:
            self._last_lsn = value[self._OPTIME]
            self._write_leader_optime(str(value[self._OPTIME]))

    @abc.abstractmethod
    def _write_failsafe(self, value):
        """Write current cluster topology to DCS that will be used by failsafe mechanism (if enabled).

        :param value: failsafe topology serialized in JSON format
        :returns: `!True` on success."""

    def write_failsafe(self, value):
        if not (isinstance(self._last_failsafe, dict) and deep_compare(self._last_failsafe, value))\
                and self._write_failsafe(json.dumps(value, separators=(',', ':'))):
            self._last_failsafe = value

    @property
    def failsafe(self):
        return self._last_failsafe

    @abc.abstractmethod
    def _update_leader(self):
        """Update leader key (or session) ttl

        :returns: `!True` if leader key (or session) has been updated successfully.

        You have to use CAS (Compare And Swap) operation in order to update leader key,
        for example for etcd `prevValue` parameter must be used.
        If update fails due to DCS not being accessible or because it is not able to
        process requests (hopefuly temporary), the ~DCSError exception should be raised."""

    def update_leader(self, last_lsn, slots=None, failsafe=None):
        """Update leader key (or session) ttl and optime/leader

        :param last_lsn: absolute WAL LSN in bytes
        :param slots: dict with permanent slots confirmed_flush_lsn
        :returns: `!True` if leader key (or session) has been updated successfully."""

        ret = self._update_leader()
        if ret and last_lsn:
            status = {self._OPTIME: last_lsn}
            if slots:
                status['slots'] = slots
            self.write_status(status)

        if ret and failsafe is not None:
            self.write_failsafe(failsafe)

        return ret

    @abc.abstractmethod
    def attempt_to_acquire_leader(self):
        """Attempt to acquire leader lock
        This method should create `/leader` key with value=`~self._name`
        :returns: `!True` if key has been created successfully.

        Key must be created atomically. In case if key already exists it should not be
        overwritten and `!False` must be returned.

        If key creation fails due to DCS not being accessible or because it is not able to
        process requests (hopefuly temporary), the ~DCSError exception should be raised"""

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
    def touch_member(self, data):
        """Update member key in DCS.
        This method should create or update key with the name = '/members/' + `~self._name`
        and value = data in a given DCS.

        :param data: information about instance (including connection strings)
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
    def _delete_leader(self):
        """Remove leader key from DCS.
        This method should remove leader key if current instance is the leader"""

    def delete_leader(self, last_lsn=None):
        """Update optime/leader and voluntarily remove leader key from DCS.
        This method should remove leader key if current instance is the leader.
        :param last_lsn: latest checkpoint location in bytes"""

        if last_lsn:
            self.write_status({self._OPTIME: last_lsn})
        return self._delete_leader()

    @abc.abstractmethod
    def cancel_initialization(self):
        """ Removes the initialize key for a cluster """

    @abc.abstractmethod
    def delete_cluster(self):
        """Delete cluster from DCS"""

    @staticmethod
    def sync_state(leader, sync_standby):
        """Build sync_state dict
           sync_standby dictionary key being kept for backward compatibility
        """
        return {'leader': leader, 'sync_standby': sync_standby and ','.join(sorted(sync_standby)) or None}

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
        """If the current node is a leader it should just sleep.
        Any other node should watch for changes of leader key with a given timeout

        :param leader_index: index of a leader key
        :param timeout: timeout in seconds
        :returns: `!True` if you would like to reschedule the next run of ha cycle"""

        self.event.wait(timeout)
        return self.event.is_set()
