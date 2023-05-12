import abc
import dateutil.parser
import datetime
import importlib
import inspect
import json
import logging
import os
import pkgutil
import re
import sys
import time

from collections import defaultdict
from copy import deepcopy
from random import randint
from threading import Event, Lock
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Optional, Set, Tuple, Union, TYPE_CHECKING
from urllib.parse import urlparse, urlunparse, parse_qsl

from ..exceptions import PatroniFatalException
from ..utils import deep_compare, uri
if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

CITUS_COORDINATOR_GROUP_ID = 0
citus_group_re = re.compile('^(0|[1-9][0-9]*)$')
slot_name_re = re.compile('^[a-z0-9_]{1,63}$')
logger = logging.getLogger(__name__)


def slot_name_from_member_name(member_name: str) -> str:
    """Translate member name to valid PostgreSQL slot name.

    PostgreSQL replication slot names must be valid PostgreSQL names. This function maps the wider space of
    member names to valid PostgreSQL names. Names are lowercased, dashes and periods common in hostnames
    are replaced with underscores, other characters are encoded as their unicode codepoint. Name is truncated
    to 64 characters. Multiple different member names may map to a single slot name."""

    def replace_char(match: Any) -> str:
        c = match.group(0)
        return '_' if c in '-.' else "u{:04d}".format(ord(c))

    slot_name = re.sub('[^a-z0-9_]', replace_char, member_name.lower())
    return slot_name[0:63]


def parse_connection_string(value: str) -> Tuple[str, Union[str, None]]:
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


def dcs_modules() -> List[str]:
    """Get names of DCS modules, depending on execution environment. If being packaged with PyInstaller,
    modules aren't discoverable dynamically by scanning source directory because `FrozenImporter` doesn't
    implement `iter_modules` method. But it is still possible to find all potential DCS modules by
    iterating through `toc`, which contains list of all "frozen" resources."""

    dcs_dirname = os.path.dirname(__file__)
    module_prefix = __package__ + '.'

    if getattr(sys, 'frozen', False):
        toc: Set[str] = set()
        # dcs_dirname may contain a dot, which causes pkgutil.iter_importers()
        # to misinterpret the path as a package name. This can be avoided
        # altogether by not passing a path at all, because PyInstaller's
        # FrozenImporter is a singleton and registered as top-level finder.
        for importer in pkgutil.iter_importers():
            if hasattr(importer, 'toc'):
                toc |= getattr(importer, 'toc')
        return [module for module in toc if module.startswith(module_prefix) and module.count('.') == 2]
    else:
        return [module_prefix + name for _, name, is_pkg in pkgutil.iter_modules([dcs_dirname]) if not is_pkg]


def get_dcs(config: Union['Config', Dict[str, Any]]) -> 'AbstractDCS':
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

    available_implementations: List[str] = []
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


_Version = Union[int, str]
_Session = Union[int, float, str, None]


class Member(NamedTuple):
    """Immutable object (namedtuple) which represents single member of PostgreSQL cluster.
    Consists of the following fields:
    :param version: modification version of a given member key in a Configuration Store
    :param name: name of PostgreSQL cluster member
    :param session: either session id or just ttl in seconds
    :param data: arbitrary data i.e. conn_url, api_url, xlog location, state, role, tags, etc...

    There are two mandatory keys in a data:
    conn_url: connection string containing host, user and password which could be used to access this member.
    api_url: REST API url of patroni instance
    """
    version: _Version
    name: str
    session: _Session
    data: Dict[str, Any]

    @staticmethod
    def from_node(version: _Version, name: str, session: _Session, value: str) -> 'Member':
        """
        >>> Member.from_node(-1, '', '', '{"conn_url": "postgres://foo@bar/postgres"}') is not None
        True
        >>> Member.from_node(-1, '', '', '{')
        Member(version=-1, name='', session='', data={})
        """
        if value.startswith('postgres'):
            conn_url, api_url = parse_connection_string(value)
            data = {'conn_url': conn_url, 'api_url': api_url}
        else:
            try:
                data = json.loads(value)
                assert isinstance(data, dict)
            except (AssertionError, TypeError, ValueError):
                data: Dict[str, Any] = {}
        return Member(version, name, session, data)

    @property
    def conn_url(self) -> Optional[str]:
        conn_url = self.data.get('conn_url')
        if conn_url:
            return conn_url

        conn_kwargs = self.data.get('conn_kwargs')
        if conn_kwargs:
            conn_url = uri('postgresql', (conn_kwargs.get('host'), conn_kwargs.get('port', 5432)))
            self.data['conn_url'] = conn_url
            return conn_url

    def conn_kwargs(self, auth: Union[Any, Dict[str, Any], None] = None) -> Dict[str, Any]:
        defaults = {
            "host": None,
            "port": None,
            "dbname": None
        }
        ret: Optional[Dict[str, Any]] = self.data.get('conn_kwargs')
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
    def api_url(self) -> Optional[str]:
        return self.data.get('api_url')

    @property
    def tags(self) -> Dict[str, Any]:
        return self.data.get('tags', {})

    @property
    def nofailover(self) -> bool:
        return self.tags.get('nofailover', False)

    @property
    def replicatefrom(self) -> Optional[str]:
        return self.tags.get('replicatefrom')

    @property
    def clonefrom(self) -> bool:
        return self.tags.get('clonefrom', False) and bool(self.conn_url)

    @property
    def state(self) -> str:
        return self.data.get('state', 'unknown')

    @property
    def is_running(self) -> bool:
        return self.state == 'running'

    @property
    def patroni_version(self) -> Optional[Tuple[int, ...]]:
        version = self.data.get('version')
        if version:
            try:
                return tuple(map(int, version.split('.')))
            except Exception:
                logger.debug('Failed to parse Patroni version %s', version)


class RemoteMember(Member):
    """Represents a remote member (typically a primary) for a standby cluster"""

    @classmethod
    def from_name_and_data(cls, name: str, data: Dict[str, Any]) -> 'RemoteMember':
        return super(RemoteMember, cls).__new__(cls, -1, name, None, data)

    @staticmethod
    def allowed_keys() -> Tuple[str, ...]:
        return ('primary_slot_name',
                'create_replica_methods',
                'restore_command',
                'archive_cleanup_command',
                'recovery_min_apply_delay',
                'no_replication_slot')

    def __getattr__(self, name: str) -> Any:
        if name in RemoteMember.allowed_keys():
            return self.data.get(name)


class Leader(NamedTuple):
    """Immutable object (namedtuple) which represents leader key.

    Consists of the following fields:
    :param version: modification version of a leader key in a Configuration Store
    :param session: either session id or just ttl in seconds
    :param member: reference to a `Member` object which represents current leader (see `Cluster.members`)
    """
    version: _Version
    session: _Session
    member: Member

    @property
    def name(self) -> str:
        return self.member.name

    def conn_kwargs(self, auth: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        return self.member.conn_kwargs(auth)

    @property
    def conn_url(self) -> Optional[str]:
        return self.member.conn_url

    @property
    def data(self) -> Dict[str, Any]:
        return self.member.data

    @property
    def timeline(self) -> Optional[int]:
        return self.data.get('timeline')

    @property
    def checkpoint_after_promote(self) -> Optional[bool]:
        """
        >>> Leader(1, '', Member.from_node(1, '', '', '{"version":"z"}')).checkpoint_after_promote

        """
        version = self.member.patroni_version
        # 1.5.6 is the last version which doesn't expose checkpoint_after_promote: false
        if version and version > (1, 5, 6):
            return self.data.get('role') in ('master', 'primary') and 'checkpoint_after_promote' not in self.data


class Failover(NamedTuple):

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
    version: _Version
    leader: Optional[str]
    candidate: Optional[str]
    scheduled_at: Optional[datetime.datetime]

    @staticmethod
    def from_node(version: _Version, value: Union[str, Dict[str, str]]) -> 'Failover':
        if isinstance(value, dict):
            data: Dict[str, Any] = value
        elif value:
            try:
                data = json.loads(value)
                assert isinstance(data, dict)
            except AssertionError:
                data = {}
            except ValueError:
                t = [a.strip() for a in value.split(':')]
                leader = t[0]
                candidate = t[1] if len(t) > 1 else None
                return Failover(version, leader, candidate, None)
        else:
            data = {}

        if data.get('scheduled_at'):
            data['scheduled_at'] = dateutil.parser.parse(data['scheduled_at'])

        return Failover(version, data.get('leader'), data.get('member'), data.get('scheduled_at'))

    def __len__(self) -> int:
        return int(bool(self.leader)) + int(bool(self.candidate))


class ClusterConfig(NamedTuple):
    version: _Version
    data: Dict[str, Any]
    modify_version: _Version

    @staticmethod
    def from_node(version: _Version, value: str, modify_version: Optional[_Version] = None) -> 'ClusterConfig':
        """
        >>> ClusterConfig.from_node(1, '{') is None
        False
        """

        try:
            data = json.loads(value)
            assert isinstance(data, dict)
        except (AssertionError, TypeError, ValueError):
            data: Dict[str, Any] = {}
            modify_version = 0
        return ClusterConfig(version, data, version if modify_version is None else modify_version)

    @property
    def permanent_slots(self) -> Dict[str, Any]:
        return self.data.get('permanent_replication_slots')\
            or self.data.get('permanent_slots') or self.data.get('slots') or {}

    @property
    def ignore_slots_matchers(self) -> List[Dict[str, Any]]:
        return self.data.get('ignore_slots') or []

    @property
    def max_timelines_history(self) -> int:
        return self.data.get('max_timelines_history', 0)


class SyncState(NamedTuple):
    """Immutable object (namedtuple) which represents last observed synhcronous replication state

    :param version: modification version of a synchronization key in a Configuration Store
    :param leader: reference to member that was leader
    :param sync_standby: synchronous standby list (comma delimited) which are last synchronized to leader
    """
    version: Optional[_Version]
    leader: Optional[str]
    sync_standby: Optional[str]

    @staticmethod
    def from_node(version: Optional[_Version], value: Union[str, Dict[str, Any], None]) -> 'SyncState':
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
        try:
            if value and isinstance(value, str):
                value = json.loads(value)
            assert isinstance(value, dict)
            return SyncState(version, value.get('leader'), value.get('sync_standby'))
        except (AssertionError, TypeError, ValueError):
            return SyncState.empty(version)

    @staticmethod
    def empty(version: Optional[_Version] = None) -> 'SyncState':
        return SyncState(version, None, None)

    @property
    def is_empty(self) -> bool:
        """:returns: True if /sync key is not valid (doesn't have a leader)."""
        return not self.leader

    @staticmethod
    def _str_to_list(value: str) -> List[str]:
        """Splits a string by comma and returns list of strings.

        :param value: a comma separated string
        :returns: list of non-empty strings after splitting an input value by comma
        """
        return list(filter(lambda a: a, [s.strip() for s in value.split(',')]))

    @property
    def members(self) -> List[str]:
        """:returns: sync_standby as list."""
        return self._str_to_list(self.sync_standby) if not self.is_empty and self.sync_standby else []

    def matches(self, name: Optional[str], check_leader: bool = False) -> bool:
        """Checks if node is presented in the /sync state.

        Since PostgreSQL does case-insensitive checks for synchronous_standby_name we do it also.
        :param name: name of the node
        :param check_leader: by default the name is searched in members, check_leader=True will include leader to list
        :returns: `True` if the /sync key not :func:`is_empty` and a given name is among presented in the sync state
        >>> s = SyncState(1, 'foo', 'bar,zoo')
        >>> s.matches('foo')
        False
        >>> s.matches('fOo', True)
        True
        >>> s.matches('Bar')
        True
        >>> s.matches('zoO')
        True
        >>> s.matches('baz')
        False
        >>> s.matches(None)
        False
        >>> SyncState.empty(1).matches('foo')
        False
        """
        ret = False
        if name and not self.is_empty:
            search_str = (self.sync_standby or '') + (',' + (self.leader or '') if check_leader else '')
            ret = name.lower() in self._str_to_list(search_str.lower())
        return ret

    def leader_matches(self, name: Optional[str]) -> bool:
        """:returns: `True` if name is matching the `SyncState.leader` value."""
        return bool(name and not self.is_empty and name.lower() == (self.leader or '').lower())


_HistoryTuple = Union[Tuple[int, int, str], Tuple[int, int, str, str], Tuple[int, int, str, str, str]]


class TimelineHistory(NamedTuple):
    """Object representing timeline history file"""
    version: _Version
    value: Any
    lines: List[_HistoryTuple]

    @staticmethod
    def from_node(version: _Version, value: str) -> 'TimelineHistory':
        """
        >>> h = TimelineHistory.from_node(1, 2)
        >>> h.lines
        []
        """
        try:
            lines = json.loads(value)
            assert isinstance(lines, list)
        except (AssertionError, TypeError, ValueError):
            lines: List[_HistoryTuple] = []
        return TimelineHistory(version, value, lines)


class Cluster(NamedTuple):
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
    :param workers: workers of the Citus cluster, optional. Format: {int(group): Cluster()}
    """
    initialize: Optional[str]
    config: Optional[ClusterConfig]
    leader: Optional[Leader]
    last_lsn: int
    members: List[Member]
    failover: Optional[Failover]
    sync: SyncState
    history: Optional[TimelineHistory]
    slots: Optional[Dict[str, int]]
    failsafe: Optional[Dict[str, str]]
    workers: Dict[int, 'Cluster'] = {}

    @staticmethod
    def empty() -> 'Cluster':
        return Cluster(None, None, None, 0, [], None, SyncState.empty(), None, None, None)

    def is_empty(self):
        return self.initialize is None and self.config is None and self.leader is None and self.last_lsn == 0\
            and self.members == [] and self.failover is None and self.sync.version is None\
            and self.history is None and self.slots is None and self.failsafe is None and self.workers == {}

    def __len__(self) -> int:
        return int(not self.is_empty())

    @property
    def leader_name(self) -> Optional[str]:
        return self.leader and self.leader.name

    def is_unlocked(self) -> bool:
        return not self.leader_name

    def has_member(self, member_name: str) -> bool:
        return any(m for m in self.members if m.name == member_name)

    def get_member(self, member_name: str, fallback_to_leader: bool = True) -> Union[Member, Leader, None]:
        return ([m for m in self.members if m.name == member_name] or [self.leader if fallback_to_leader else None])[0]

    def get_clone_member(self, exclude_name: str) -> Union[Member, Leader, None]:
        exclude = [exclude_name] + ([self.leader.name] if self.leader else [])
        candidates = [m for m in self.members if m.clonefrom and m.is_running and m.name not in exclude]
        return candidates[randint(0, len(candidates) - 1)] if candidates else self.leader

    @property
    def __permanent_slots(self) -> Dict[str, Union[Dict[str, Any], Any]]:
        return self.config and self.config.permanent_slots or {}

    @property
    def __permanent_physical_slots(self) -> Dict[str, Any]:
        return {name: value for name, value in self.__permanent_slots.items()
                if not value or isinstance(value, dict) and value.get('type', 'physical') == 'physical'}

    @property
    def __permanent_logical_slots(self) -> Dict[str, Any]:
        return {name: value for name, value in self.__permanent_slots.items() if isinstance(value, dict)
                and value.get('type', 'logical') == 'logical' and value.get('database') and value.get('plugin')}

    @property
    def use_slots(self) -> bool:
        return bool(self.config and (self.config.data.get('postgresql') or {}).get('use_slots', True))

    def get_replication_slots(self, my_name: str, role: str, nofailover: bool,
                              major_version: int, show_error: bool = False) -> Dict[str, Dict[str, Any]]:
        # if the replicatefrom tag is set on the member - we should not create the replication slot for it on
        # the current primary, because that member would replicate from elsewhere. We still create the slot if
        # the replicatefrom destination member is currently not a member of the cluster (fallback to the
        # primary), or if replicatefrom destination member happens to be the current primary
        use_slots = self.use_slots
        if role in ('master', 'primary', 'standby_leader'):
            slot_members = [m.name for m in self.members if use_slots and m.name != my_name
                            and (m.replicatefrom is None or m.replicatefrom == my_name
                                 or not self.has_member(m.replicatefrom))]
            permanent_slots = self.__permanent_slots if use_slots and \
                role in ('master', 'primary') else self.__permanent_physical_slots
        else:
            # only manage slots for replicas that replicate from this one, except for the leader among them
            slot_members = [m.name for m in self.members if use_slots
                            and m.replicatefrom == my_name and m.name != self.leader_name]
            permanent_slots = self.__permanent_logical_slots if use_slots and not nofailover else {}

        slots = {slot_name_from_member_name(name): {'type': 'physical'} for name in slot_members}

        if len(slots) < len(slot_members):
            # Find which names are conflicting for a nicer error message
            slot_conflicts: Dict[str, List[str]] = defaultdict(list)
            for name in slot_members:
                slot_conflicts[slot_name_from_member_name(name)].append(name)
            logger.error("Following cluster members share a replication slot name: %s",
                         "; ".join("{} map to {}".format(", ".join(v), k)
                                   for k, v in slot_conflicts.items() if len(v) > 1))

        # "merge" replication slots for members with permanent_replication_slots
        disabled_permanent_logical_slots: List[str] = []
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
                        logger.error("Permanent logical replication slot {'%s': %s} is conflicting with"
                                     " physical replication slot for cluster member", name, value)
                    else:
                        slots[name] = value
                    continue

            logger.error("Bad value for slot '%s' in permanent_slots: %s", name, permanent_slots[name])

        if disabled_permanent_logical_slots and show_error:
            logger.error("Permanent logical replication slots supported by Patroni only starting from PostgreSQL 11. "
                         "Following slots will not be created: %s.", disabled_permanent_logical_slots)

        return slots

    def has_permanent_logical_slots(self, my_name: str, nofailover: bool, major_version: int = 110000) -> bool:
        if major_version < 110000:
            return False
        slots = self.get_replication_slots(my_name, 'replica', nofailover, major_version).values()
        return any(v for v in slots if v.get("type") == "logical")

    def should_enforce_hot_standby_feedback(self, my_name: str, nofailover: bool, major_version: int) -> bool:
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

    def get_my_slot_name_on_primary(self, my_name: str, replicatefrom: Optional[str]) -> str:
        """
        P <-- I <-- L
        In case of cascading replication we have to check not our physical slot,
        but slot of the replica that connects us to the primary.
        """

        m = self.get_member(replicatefrom, False) if replicatefrom else None
        return self.get_my_slot_name_on_primary(m.name, m.replicatefrom)\
            if isinstance(m, Member) else slot_name_from_member_name(my_name)

    @property
    def timeline(self) -> int:
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
    def min_version(self) -> Optional[Tuple[int, ...]]:
        return next(iter(sorted(m.patroni_version for m in self.members if m.patroni_version)), None)


class ReturnFalseException(Exception):
    pass


def catch_return_false_exception(func: Callable[..., Any]) -> Any:
    def wrapper(*args: Any, **kwargs: Any):
        try:
            return func(*args, **kwargs)
        except ReturnFalseException:
            return False

    return wrapper


class AbstractDCS(abc.ABC):

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

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        :param config: dict, reference to config section of selected DCS.
            i.e.: `zookeeper` for zookeeper, `etcd` for etcd, etc...
        """
        self._name = config['name']
        self._base_path = re.sub('/+', '/', '/'.join(['', config.get('namespace', 'service'), config['scope']]))
        self._citus_group = str(config['group']) if isinstance(config.get('group'), int) else None
        self._set_loop_wait(config.get('loop_wait', 10))

        self._ctl = bool(config.get('patronictl', False))
        self._cluster: Optional[Cluster] = None
        self._cluster_valid_till: float = 0
        self._cluster_thread_lock = Lock()
        self._last_lsn: int = 0
        self._last_seen: int = 0
        self._last_status: Dict[str, Any] = {}
        self._last_failsafe: Optional[Dict[str, str]] = {}
        self.event = Event()

    def client_path(self, path: str) -> str:
        components = [self._base_path]
        if self._citus_group:
            components.append(self._citus_group)
        components.append(path.lstrip('/'))
        return '/'.join(components)

    @property
    def initialize_path(self) -> str:
        return self.client_path(self._INITIALIZE)

    @property
    def config_path(self) -> str:
        return self.client_path(self._CONFIG)

    @property
    def members_path(self) -> str:
        return self.client_path(self._MEMBERS)

    @property
    def member_path(self) -> str:
        return self.client_path(self._MEMBERS + self._name)

    @property
    def leader_path(self) -> str:
        return self.client_path(self._LEADER)

    @property
    def failover_path(self) -> str:
        return self.client_path(self._FAILOVER)

    @property
    def history_path(self) -> str:
        return self.client_path(self._HISTORY)

    @property
    def status_path(self) -> str:
        return self.client_path(self._STATUS)

    @property
    def leader_optime_path(self) -> str:
        return self.client_path(self._LEADER_OPTIME)

    @property
    def sync_path(self) -> str:
        return self.client_path(self._SYNC)

    @property
    def failsafe_path(self) -> str:
        return self.client_path(self._FAILSAFE)

    @abc.abstractmethod
    def set_ttl(self, ttl: int) -> Optional[bool]:
        """Set the new ttl value for leader key"""

    @property
    @abc.abstractmethod
    def ttl(self) -> int:
        """Get new ttl value"""

    @abc.abstractmethod
    def set_retry_timeout(self, retry_timeout: int) -> None:
        """Set the new value for retry_timeout"""

    def _set_loop_wait(self, loop_wait: int) -> None:
        self._loop_wait = loop_wait

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        self._set_loop_wait(config['loop_wait'])
        self.set_ttl(config['ttl'])
        self.set_retry_timeout(config['retry_timeout'])

    @property
    def loop_wait(self) -> int:
        return self._loop_wait

    @property
    def last_seen(self) -> int:
        return self._last_seen

    @abc.abstractmethod
    def _cluster_loader(self, path: Any) -> Cluster:
        """Load and build the `Cluster` object from DCS, which
        represents a single Patroni cluster.

        :param path: the path in DCS where to load Cluster(s) from.
        :returns: `Cluster`"""

    @abc.abstractmethod
    def _citus_cluster_loader(self, path: Any) -> Union[Cluster, Dict[int, Cluster]]:
        """Load and build `Cluster` onjects from DCS that represent all
        Patroni clusters from a single Citus cluster.

        :param path: the path in DCS where to load Cluster(s) from.
        :returns: all Citus groups as `dict`, with group ids as keys"""

    @abc.abstractmethod
    def _load_cluster(
            self, path: str, loader: Callable[[Any], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        """Internally this method should call the `loader` method that
        will build `Cluster` object which represents current state and
        topology of the cluster in DCS. This method supposed to be
        called only by `get_cluster` method.

        :param path: the path in DCS where to load Cluster(s) from.
        :param loader: one of `_cluster_loader` or `_citus_cluster_loader`
        :raise: `~DCSError` in case of communication problems with DCS.
        If the current node was running as a primary and exception
        raised, instance would be demoted."""

    def _bypass_caches(self) -> None:
        """Used only in zookeeper"""

    def __get_patroni_cluster(self, path: Optional[str] = None) -> Cluster:
        if path is None:
            path = self.client_path('')
        cluster = self._load_cluster(path, self._cluster_loader)
        assert isinstance(cluster, Cluster)
        return cluster

    def is_citus_coordinator(self) -> bool:
        return self._citus_group == str(CITUS_COORDINATOR_GROUP_ID)

    def get_citus_coordinator(self) -> Optional[Cluster]:
        try:
            return self.__get_patroni_cluster('{0}/{1}/'.format(self._base_path, CITUS_COORDINATOR_GROUP_ID))
        except Exception as e:
            logger.error('Failed to load Citus coordinator cluster from %s: %r', self.__class__.__name__, e)

    def _get_citus_cluster(self) -> Cluster:
        groups = self._load_cluster(self._base_path + '/', self._citus_cluster_loader)
        if isinstance(groups, Cluster):  # Zookeeper could return a cached version
            cluster = groups
        else:
            assert isinstance(groups, dict)
            cluster = groups.pop(CITUS_COORDINATOR_GROUP_ID, Cluster.empty())
            cluster.workers.update(groups)
        return cluster

    def get_cluster(self, force: bool = False) -> Cluster:
        if force:
            self._bypass_caches()
        try:
            cluster = self._get_citus_cluster() if self.is_citus_coordinator() else self.__get_patroni_cluster()
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
    def cluster(self) -> Optional[Cluster]:
        with self._cluster_thread_lock:
            return self._cluster if self._cluster_valid_till > time.time() else None

    def reset_cluster(self) -> None:
        with self._cluster_thread_lock:
            self._cluster = None
            self._cluster_valid_till = 0

    @abc.abstractmethod
    def _write_leader_optime(self, last_lsn: str) -> bool:
        """write current WAL LSN into `/optime/leader` key in DCS

        :param last_lsn: absolute WAL LSN in bytes
        :returns: `!True` on success."""

    def write_leader_optime(self, last_lsn: int) -> None:
        self.write_status({self._OPTIME: last_lsn})

    @abc.abstractmethod
    def _write_status(self, value: str) -> bool:
        """write current WAL LSN and confirmed_flush_lsn of permanent slots into the `/status` key in DCS

        :param value: status serialized in JSON forman
        :returns: `!True` on success."""

    def write_status(self, value: Dict[str, Any]) -> None:
        if not deep_compare(self._last_status, value) and self._write_status(json.dumps(value, separators=(',', ':'))):
            self._last_status = value
        cluster = self.cluster
        min_version = cluster and cluster.min_version
        if min_version and min_version < (2, 1, 0) and self._last_lsn != value[self._OPTIME]:
            self._last_lsn = value[self._OPTIME]
            self._write_leader_optime(str(value[self._OPTIME]))

    @abc.abstractmethod
    def _write_failsafe(self, value: str) -> bool:
        """Write current cluster topology to DCS that will be used by failsafe mechanism (if enabled).

        :param value: failsafe topology serialized in JSON format
        :returns: `!True` on success."""

    def write_failsafe(self, value: Dict[str, str]) -> None:
        if not (isinstance(self._last_failsafe, dict) and deep_compare(self._last_failsafe, value))\
                and self._write_failsafe(json.dumps(value, separators=(',', ':'))):
            self._last_failsafe = value

    @property
    def failsafe(self) -> Optional[Dict[str, str]]:
        return self._last_failsafe

    @abc.abstractmethod
    def _update_leader(self) -> bool:
        """Update leader key (or session) ttl

        :returns: `!True` if leader key (or session) has been updated successfully.

        You have to use CAS (Compare And Swap) operation in order to update leader key,
        for example for etcd `prevValue` parameter must be used.
        If update fails due to DCS not being accessible or because it is not able to
        process requests (hopefuly temporary), the ~DCSError exception should be raised."""

    def update_leader(self, last_lsn: Optional[int], slots: Optional[Dict[str, int]] = None,
                      failsafe: Optional[Dict[str, str]] = None) -> bool:
        """Update leader key (or session) ttl and optime/leader

        :param last_lsn: absolute WAL LSN in bytes
        :param slots: dict with permanent slots confirmed_flush_lsn
        :returns: `!True` if leader key (or session) has been updated successfully."""

        ret = self._update_leader()
        if ret and last_lsn:
            status: Dict[str, Any] = {self._OPTIME: last_lsn}
            if slots:
                status['slots'] = slots
            self.write_status(status)

        if ret and failsafe is not None:
            self.write_failsafe(failsafe)

        return ret

    @abc.abstractmethod
    def attempt_to_acquire_leader(self) -> bool:
        """Attempt to acquire leader lock
        This method should create `/leader` key with value=`~self._name`
        :returns: `!True` if key has been created successfully.

        Key must be created atomically. In case if key already exists it should not be
        overwritten and `!False` must be returned.

        If key creation fails due to DCS not being accessible or because it is not able to
        process requests (hopefuly temporary), the ~DCSError exception should be raised"""

    @abc.abstractmethod
    def set_failover_value(self, value: str, version: Optional[Any] = None) -> bool:
        """Create or update `/failover` key"""

    def manual_failover(self, leader: Optional[str], candidate: Optional[str],
                        scheduled_at: Optional[datetime.datetime] = None, version: Optional[Any] = None) -> bool:
        failover_value = {}
        if leader:
            failover_value['leader'] = leader

        if candidate:
            failover_value['member'] = candidate

        if scheduled_at:
            failover_value['scheduled_at'] = scheduled_at.isoformat()
        return self.set_failover_value(json.dumps(failover_value, separators=(',', ':')), version)

    @abc.abstractmethod
    def set_config_value(self, value: str, version: Optional[Any] = None) -> bool:
        """Create or update `/config` key"""

    @abc.abstractmethod
    def touch_member(self, data: Dict[str, Any]) -> bool:
        """Update member key in DCS.
        This method should create or update key with the name = '/members/' + `~self._name`
        and value = data in a given DCS.

        :param data: information about instance (including connection strings)
        :param ttl: ttl for member key, optional parameter. If it is None `~self.member_ttl will be used`
        :returns: `!True` on success otherwise `!False`
        """

    @abc.abstractmethod
    def take_leader(self) -> bool:
        """This method should create leader key with value = `~self._name` and ttl=`~self.ttl`
        Since it could be called only on initial cluster bootstrap it could create this key regardless,
        overwriting the key if necessary."""

    @abc.abstractmethod
    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        """Race for cluster initialization.

        :param create_new: False if the key should already exist (in the case we are setting the system_id)
        :param sysid: PostgreSQL cluster system identifier, if specified, is written to the key
        :returns: `!True` if key has been created successfully.

        this method should create atomically initialize key and return `!True`
        otherwise it should return `!False`"""

    @abc.abstractmethod
    def _delete_leader(self) -> bool:
        """Remove leader key from DCS.
        This method should remove leader key if current instance is the leader"""

    def delete_leader(self, last_lsn: Optional[int] = None) -> bool:
        """Update optime/leader and voluntarily remove leader key from DCS.
        This method should remove leader key if current instance is the leader.
        :param last_lsn: latest checkpoint location in bytes"""

        if last_lsn:
            self.write_status({self._OPTIME: last_lsn})
        return self._delete_leader()

    @abc.abstractmethod
    def cancel_initialization(self) -> bool:
        """ Removes the initialize key for a cluster """

    @abc.abstractmethod
    def delete_cluster(self) -> bool:
        """Delete cluster from DCS"""

    @staticmethod
    def sync_state(leader: Optional[str], sync_standby: Optional[Collection[str]]) -> Dict[str, Any]:
        """Build sync_state dict.
        The sync_standby key being kept for backward compatibility.
        :param leader: name of the leader node that manages /sync key
        :param sync_standby: collection of currently known synchronous standby node names
        :returns: dictionary that later could be serialized to JSON or saved directly to DCS
        """
        return {'leader': leader, 'sync_standby': ','.join(sorted(sync_standby)) if sync_standby else None}

    def write_sync_state(self, leader: Optional[str], sync_standby: Optional[Collection[str]],
                         version: Optional[Any] = None) -> Optional[SyncState]:
        """Write the new synchronous state to DCS.
        Calls :func:`sync_state` method to build a dict and than calls DCS specific :func:`set_sync_state_value` method.
        :param leader: name of the leader node that manages /sync key
        :param sync_standby: collection of currently known synchronous standby node names
        :param version: for conditional update of the key/object
        :returns: the new :class:`SyncState` object or None
        """
        sync_value = self.sync_state(leader, sync_standby)
        ret = self.set_sync_state_value(json.dumps(sync_value, separators=(',', ':')), version)
        if not isinstance(ret, bool):
            return SyncState.from_node(ret, sync_value)

    @abc.abstractmethod
    def set_history_value(self, value: str) -> bool:
        """"""

    @abc.abstractmethod
    def set_sync_state_value(self, value: str, version: Optional[Any] = None) -> Union[Any, bool]:
        """Set synchronous state in DCS, should be implemented in the child class.

        :param value: the new value of /sync key
        :param version: for conditional update of the key/object
        :returns: version of the new object or `False` in case of error
        """

    @abc.abstractmethod
    def delete_sync_state(self, version: Optional[Any] = None) -> bool:
        """"""

    def watch(self, leader_version: Optional[Any], timeout: float) -> bool:
        """If the current node is a leader it should just sleep.
        Any other node should watch for changes of leader key with a given timeout

        :param leader_version: version of a leader key
        :param timeout: timeout in seconds
        :returns: `!True` if you would like to reschedule the next run of ha cycle"""

        self.event.wait(timeout)
        return self.event.is_set()
