"""Abstract classes for Distributed Configuration Store."""
import abc
import datetime
import json
import logging
import re
import time
from collections import defaultdict
from copy import deepcopy
from random import randint
from threading import Event, Lock
from typing import Any, Callable, Collection, Dict, Iterator, List, \
    NamedTuple, Optional, Tuple, Type, TYPE_CHECKING, Union
from urllib.parse import urlparse, urlunparse, parse_qsl

import dateutil.parser

from .. import global_config
from ..dynamic_loader import iter_classes, iter_modules
from ..exceptions import PatroniFatalException
from ..utils import deep_compare, uri
from ..tags import Tags
from ..utils import parse_int

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config
    from ..postgresql import Postgresql
    from ..postgresql.mpp import AbstractMPP

SLOT_ADVANCE_AVAILABLE_VERSION = 110000
slot_name_re = re.compile('^[a-z0-9_]{1,63}$')
logger = logging.getLogger(__name__)


def slot_name_from_member_name(member_name: str) -> str:
    """Translate member name to valid PostgreSQL slot name.

    .. note::
        PostgreSQL's replication slot names must be valid PostgreSQL names. This function maps the wider space of
        member names to valid PostgreSQL names. Names have their case lowered, dashes and periods common in hostnames
        are replaced with underscores, other characters are encoded as their unicode codepoint. Name is truncated
        to 64 characters. Multiple different member names may map to a single slot name.

    :param member_name: The string to convert to a slot name.

    :returns: The string converted using the rules described above.
    """

    def replace_char(match: Any) -> str:
        c = match.group(0)
        return '_' if c in '-.' else f"u{ord(c):04d}"

    slot_name = re.sub('[^a-z0-9_]', replace_char, member_name.lower())
    return slot_name[0:63]


def parse_connection_string(value: str) -> Tuple[str, Union[str, None]]:
    """Split and rejoin a URL string into a connection URL and an API URL.

    .. note::
        Original Governor stores connection strings for each cluster members in a following format:

            postgres://{username}:{password}@{connect_address}/postgres

        Since each of our patroni instances provides their own REST API endpoint, it's good to store this information
        in DCS along with PostgreSQL connection string. In order to not introduce new keys and be compatible with
        original Governor we decided to extend original connection string in a following way:

            postgres://{username}:{password}@{connect_address}/postgres?application_name={api_url}

        This way original Governor could use such connection string as it is, because of feature of ``libpq`` library.

    :param value: The URL string to split.

    :returns: the connection string stored in DCS split into two parts, ``conn_url`` and ``api_url``.
    """
    scheme, netloc, path, params, query, fragment = urlparse(value)
    conn_url = urlunparse((scheme, netloc, path, params, '', fragment))
    api_url = ([v for n, v in parse_qsl(query) if n == 'application_name'] or [None])[0]
    return conn_url, api_url


def dcs_modules() -> List[str]:
    """Get names of DCS modules, depending on execution environment.

    :returns: list of known module names with absolute python module path namespace, e.g. ``patroni.dcs.etcd``.
    """
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(__package__, str)
    return iter_modules(__package__)


def iter_dcs_classes(
        config: Optional[Union['Config', Dict[str, Any]]] = None
) -> Iterator[Tuple[str, Type['AbstractDCS']]]:
    """Attempt to import DCS modules that are present in the given configuration.

    .. note::
            If a module successfully imports we can assume that all its requirements are installed.

    :param config: configuration information with possible DCS names as keys. If given, only attempt to import DCS
                   modules defined in the configuration. Else, if ``None``, attempt to import any supported DCS module.

    :returns: an iterator of tuples, each containing the module ``name`` and the imported DCS class object.
    """
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(__package__, str)
    return iter_classes(__package__, AbstractDCS, config)


def get_dcs(config: Union['Config', Dict[str, Any]]) -> 'AbstractDCS':
    """Attempt to load a Distributed Configuration Store from known available implementations.

    .. note::
        Using the list of available DCS classes returned by :func:`iter_classes` attempt to dynamically
        instantiate the class that implements a DCS using the abstract class :class:`AbstractDCS`.

        Basic top-level configuration parameters retrieved from *config* are propagated to the DCS specific config
        before being passed to the module DCS class.

        If no module is found to satisfy configuration then report and log an error. This will cause Patroni to exit.

    :raises :exc:`PatroniFatalException`: if a load of all available DCS modules have been tried and none succeeded.

    :param config: object or dictionary with Patroni configuration. This is normally a representation of the main
                   Patroni

    :returns: The first successfully loaded DCS module which is an implementation of :class:`AbstractDCS`.
    """
    for name, dcs_class in iter_dcs_classes(config):
        # Propagate some parameters from top level of config if defined to the DCS specific config section.
        config[name].update({
            p: config[p] for p in ('namespace', 'name', 'scope', 'loop_wait',
                                   'patronictl', 'ttl', 'retry_timeout')
            if p in config})

        from patroni.postgresql.mpp import get_mpp
        return dcs_class(config[name], get_mpp(config))

    available_implementations = ', '.join(sorted([n for n, _ in iter_dcs_classes()]))
    raise PatroniFatalException("Can not find suitable configuration of distributed configuration store\n"
                                f"Available implementations: {available_implementations}")


_Version = Union[int, str]
_Session = Union[int, float, str, None]


class Member(Tags, NamedTuple('Member',
                              [('version', _Version),
                               ('name', str),
                               ('session', _Session),
                               ('data', Dict[str, Any])])):
    """Immutable object (namedtuple) which represents single member of PostgreSQL cluster.

    .. note::
        We are using an old-style attribute declaration here because otherwise it is not possible to override
        ``__new__`` method in the :class:`RemoteMember` class.

    .. note::
        These two keys in data are always written to the DCS, but care is taken to maintain consistency and resilience
        from data that is read:

        ``conn_url``: connection string containing host, user and password which could be used to access this member.
        ``api_url``: REST API url of patroni instance

    Consists of the following fields:

    :ivar version: modification version of a given member key in a Configuration Store.
    :ivar name: name of PostgreSQL cluster member.
    :ivar session: either session id or just ttl in seconds.
    :ivar data: dictionary containing arbitrary data i.e. ``conn_url``, ``api_url``, ``xlog_location``, ``state``,
                ``role``, ``tags``, etc...
    """

    @staticmethod
    def from_node(version: _Version, name: str, session: _Session, value: str) -> 'Member':
        """Factory method for instantiating :class:`Member` from a JSON serialised string or object.

        :param version: modification version of a given member key in a Configuration Store.
        :param name: name of PostgreSQL cluster member.
        :param session: either session id or just ttl in seconds.
        :param value: JSON encoded string containing arbitrary data i.e. ``conn_url``, ``api_url``,
                      ``xlog_location``, ``state``, ``role``, ``tags``, etc. OR a connection URL
                      starting with ``postgres://``.

        :returns: an :class:`Member` instance built with the given arguments.

        :Example:

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
        """The ``conn_url`` value from :attr:`~Member.data` if defined or constructed from ``conn_kwargs``."""
        conn_url = self.data.get('conn_url')
        if conn_url:
            return conn_url

        conn_kwargs = self.data.get('conn_kwargs')
        if conn_kwargs:
            conn_url = uri('postgresql', (conn_kwargs.get('host'), conn_kwargs.get('port', 5432)))
            self.data['conn_url'] = conn_url
            return conn_url

        return None

    def conn_kwargs(self, auth: Union[Any, Dict[str, Any], None] = None) -> Dict[str, Any]:
        """Give keyword arguments used for PostgreSQL connection settings.

        :param auth: Authentication properties - can be defined as anything supported by the ``psycopg2`` or
                     ``psycopg`` modules.
                     Converts a key of ``username`` to ``user`` if supplied.

        :returns: A dictionary containing a merge of default parameter keys ``host``, ``port`` and ``dbname``, with
                 the contents of :attr:`~Member.data` ``conn_kwargs`` key. If those are not defined will
                 parse and reform connection parameters from :attr:`~Member.conn_url`. One of these two attributes
                 needs to have data defined to construct the output dictionary. Finally, *auth* parameters are merged
                 with the dictionary before returned.
        """
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
        """The ``api_url`` value from :attr:`~Member.data` if defined."""
        return self.data.get('api_url')

    @property
    def tags(self) -> Dict[str, Any]:
        """The ``tags`` value from :attr:`~Member.data` if defined, otherwise an empty dictionary."""
        return self.data.get('tags', {})

    @property
    def clonefrom(self) -> bool:
        """``True`` if both ``clonefrom`` tag is ``True`` and a connection URL is defined."""
        return super().clonefrom and bool(self.conn_url)

    @property
    def state(self) -> str:
        """The ``state`` value of :attr:`~Member.data`."""
        return self.data.get('state', 'unknown')

    @property
    def is_running(self) -> bool:
        """``True`` if the member :attr:`~Member.state` is ``running``."""
        return self.state == 'running'

    @property
    def patroni_version(self) -> Optional[Tuple[int, ...]]:
        """The ``version`` string value from :attr:`~Member.data` converted to tuple.

        :Example:

            >>> Member.from_node(1, '', '', '{"version":"1.2.3"}').patroni_version
            (1, 2, 3)

        """
        version = self.data.get('version')
        if version:
            try:
                return tuple(map(int, version.split('.')))
            except Exception:
                logger.debug('Failed to parse Patroni version %s', version)
        return None

    @property
    def lsn(self) -> Optional[int]:
        """Current LSN (receive/flush/replay)."""
        return parse_int(self.data.get('xlog_location'))


class RemoteMember(Member):
    """Represents a remote member (typically a primary) for a standby cluster.

    :cvar ALLOWED_KEYS: Controls access to relevant key names that could be in stored :attr:`~RemoteMember.data`.
    """

    ALLOWED_KEYS: Tuple[str, ...] = (
        'primary_slot_name',
        'create_replica_methods',
        'restore_command',
        'archive_cleanup_command',
        'recovery_min_apply_delay',
        'no_replication_slot'
    )

    def __new__(cls, name: str, data: Dict[str, Any]) -> 'RemoteMember':
        """Factory method to construct instance from given *name* and *data*.

        :param name: name of the remote member.
        :param data: dictionary of member information, which can contain keys from :const:`~RemoteMember.ALLOWED_KEYS`
                     but also member connection information ``api_url`` and ``conn_kwargs``, and slot information.

        :returns: constructed instance using supplied parameters.
        """
        return super(RemoteMember, cls).__new__(cls, -1, name, None, data)

    def __getattr__(self, name: str) -> Any:
        """Dictionary style key lookup.

        :param name: key to lookup.

        :returns: value of *name* key in :attr:`~RemoteMember.data` if key *name* is in
                  :cvar:`~RemoteMember.ALLOWED_KEYS`, else ``None``.
        """
        return self.data.get(name) if name in RemoteMember.ALLOWED_KEYS else None


class Leader(NamedTuple):
    """Immutable object (namedtuple) which represents leader key.

    Consists of the following fields:

    :ivar version: modification version of a leader key in a Configuration Store
    :ivar session: either session id or just ttl in seconds
    :ivar member: reference to a :class:`Member` object which represents current leader (see :attr:`Cluster.members`)
    """

    version: _Version
    session: _Session
    member: Member

    @property
    def name(self) -> str:
        """The leader "member" name."""
        return self.member.name

    def conn_kwargs(self, auth: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Connection keyword arguments.

        :param auth: an optional dictionary containing authentication information.

        :returns: the result of the called :meth:`Member.conn_kwargs` method.
        """
        return self.member.conn_kwargs(auth)

    @property
    def conn_url(self) -> Optional[str]:
        """Connection URL value of the :class:`Member` instance."""
        return self.member.conn_url

    @property
    def data(self) -> Dict[str, Any]:
        """Data value of the :class:`Member` instance."""
        return self.member.data

    @property
    def timeline(self) -> Optional[int]:
        """Timeline value of :attr:`~Member.data`."""
        return self.data.get('timeline')

    @property
    def checkpoint_after_promote(self) -> Optional[bool]:
        """Determine whether a checkpoint has occurred for this leader after promotion.

        :returns: ``True`` if the role is ``master`` or ``primary`` and ``checkpoint_after_promote`` is not set,
                 ``False`` if not a ``master`` or ``primary`` or if the checkpoint hasn't occurred.
                 If the version of Patroni is older than 1.5.6, return ``None``.

        :Example:

            >>> Leader(1, '', Member.from_node(1, '', '', '{"version":"z"}')).checkpoint_after_promote
        """
        version = self.member.patroni_version
        # 1.5.6 is the last version which doesn't expose checkpoint_after_promote: false
        if version and version > (1, 5, 6):
            return self.data.get('role') in ('master', 'primary') and 'checkpoint_after_promote' not in self.data
        return None


class Failover(NamedTuple):
    """Immutable object (namedtuple) representing configuration information required for failover/switchover capability.

    :ivar version: version of the object.
    :ivar leader: name of the leader. If value isn't empty we treat it as a switchover from the specified node.
    :ivar candidate: the name of the member node to be considered as a failover candidate.
    :ivar scheduled_at: in the case of a switchover the :class:`~datetime.datetime` object to perform the scheduled
        switchover.

    :Example:

        >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader"}'))
        True

        >>> 'Failover' in str(Failover.from_node(1, {"leader": "cluster_leader"}))
        True

        >>> 'Failover' in str(Failover.from_node(1, '{"leader": "cluster_leader", "member": "cluster_candidate"}'))
        True

        >>> Failover.from_node(1, 'null') is None
        False

        >>> n = '''{"leader": "cluster_leader", "member": "cluster_candidate",
        ...         "scheduled_at": "2016-01-14T10:09:57.1394Z"}'''

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
        """Factory method to parse *value* as failover configuration.

        :param version: version number for the object.
        :param value: JSON serialized data or a dictionary of configuration.
                      Can also be a colon ``:`` delimited list of leader, followed by candidate name (legacy format).
                      If ``scheduled_at`` key is defined the value will be parsed by :func:`dateutil.parser.parse`.

        :returns: constructed :class:`Failover` information object
        """
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
        """Implement ``len`` function capability.

        .. note::
           This magic method aids in the evaluation of "emptiness" of a :class:`Failover` instance. For example:

           >>> failover = Failover.from_node(1, None)
           >>> len(failover)
           0
           >>> assert bool(failover) is False

           >>> failover = Failover.from_node(1, {"leader": "cluster_leader"})
           >>> len(failover)
           1
           >>> assert bool(failover) is True

           This makes it easier to write ``if cluster.failover`` rather than the longer statement.

        """
        return int(bool(self.leader)) + int(bool(self.candidate))


class ClusterConfig(NamedTuple):
    """Immutable object (namedtuple) which represents cluster configuration.

    :ivar version: version number for the object.
    :ivar data: dictionary of configuration information.
    :ivar modify_version: modified version number.
    """

    version: _Version
    data: Dict[str, Any]
    modify_version: _Version

    @staticmethod
    def from_node(version: _Version, value: str, modify_version: Optional[_Version] = None) -> 'ClusterConfig':
        """Factory method to parse *value* as configuration information.

        :param version: version number for object.
        :param value: raw JSON serialized data, if not parsable replaced with an empty dictionary.
        :param modify_version: optional modify version number, use *version* if not provided.

        :returns: constructed :class:`ClusterConfig` instance.

        :Example:

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


class SyncState(NamedTuple):
    """Immutable object (namedtuple) which represents last observed synchronous replication state.

    :ivar version: modification version of a synchronization key in a Configuration Store.
    :ivar leader: reference to member that was leader.
    :ivar sync_standby: synchronous standby list (comma delimited) which are last synchronized to leader.
    """

    version: Optional[_Version]
    leader: Optional[str]
    sync_standby: Optional[str]

    @staticmethod
    def from_node(version: Optional[_Version], value: Union[str, Dict[str, Any], None]) -> 'SyncState':
        """Factory method to parse *value* as synchronisation state information.

        :param version: optional *version* number for the object.
        :param value: (optionally JSON serialised) synchronisation state information

        :returns: constructed :class:`SyncState` object.

        :Example:

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
        """Construct an empty :class:`SyncState` instance.

        :param version: optional version number.

        :returns: empty synchronisation state object.
        """
        return SyncState(version, None, None)

    @property
    def is_empty(self) -> bool:
        """``True`` if ``/sync`` key is not valid (doesn't have a leader)."""
        return not self.leader

    @staticmethod
    def _str_to_list(value: str) -> List[str]:
        """Splits a string by comma and returns list of strings.

        :param value: a comma separated string.

        :returns: list of non-empty strings after splitting an input value by comma.
        """
        return list(filter(lambda a: a, [s.strip() for s in value.split(',')]))

    @property
    def members(self) -> List[str]:
        """:attr:`~SyncState.sync_standby` as list or an empty list if undefined or object considered ``empty``."""
        return self._str_to_list(self.sync_standby) if not self.is_empty and self.sync_standby else []

    def matches(self, name: Optional[str], check_leader: bool = False) -> bool:
        """Checks if node is presented in the /sync state.

        Since PostgreSQL does case-insensitive checks for synchronous_standby_name we do it also.

        :param name: name of the node.
        :param check_leader: by default the *name* is searched for only in members, a value of ``True`` will include the
                             leader to list.

        :returns: ``True`` if the ``/sync`` key not :func:`is_empty` and the given *name* is among those presented in
                  the sync state.

        :Example:
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
        """Compare the given *name* to stored leader value.

        :returns: ``True`` if *name* is matching the :attr:`~SyncState.leader` value.
        """
        return bool(name and not self.is_empty and name.lower() == (self.leader or '').lower())


_HistoryTuple = Union[Tuple[int, int, str], Tuple[int, int, str, str], Tuple[int, int, str, str, str]]


class TimelineHistory(NamedTuple):
    """Object representing timeline history file.

    .. note::
        The content held in *lines* deserialized from *value* are lines parsed from PostgreSQL timeline history files,
        consisting of the timeline number, the LSN where the timeline split and any other string held in the file.
        The files are parsed by :func:`~patroni.postgresql.misc.parse_history`.

    :ivar version: version number of the file.
    :ivar value: raw JSON serialised data consisting of parsed lines from history files.
    :ivar lines: ``List`` of ``Tuple`` parsed lines from history files.
    """

    version: _Version
    value: Any
    lines: List[_HistoryTuple]

    @staticmethod
    def from_node(version: _Version, value: str) -> 'TimelineHistory':
        """Parse the given JSON serialized string as a list of timeline history lines.

        :param version: version number
        :param value: JSON serialized string, consisting of parsed lines of PostgreSQL timeline history files,
                      see :class:`TimelineHistory`.

        :returns: composed timeline history object using parsed lines.

        :Example:

            If the passed *value* argument is not parsed an empty list of lines is returned:

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


class Status(NamedTuple):
    """Immutable object (namedtuple) which represents `/status` key.

    Consists of the following fields:

    :ivar last_lsn: :class:`int` object containing position of last known leader LSN.
    :ivar slots: state of permanent replication slots on the primary in the format: ``{"slot_name": int}``.
    """
    last_lsn: int
    slots: Optional[Dict[str, int]]

    @staticmethod
    def empty() -> 'Status':
        """Construct an empty :class:`Status` instance.

        :returns: empty :class:`Status` object.
        """
        return Status(0, None)

    @staticmethod
    def from_node(value: Union[str, Dict[str, Any], None]) -> 'Status':
        """Factory method to parse *value* as :class:`Status` object.

        :param value: JSON serialized string

        :returns: constructed :class:`Status` object.
        """
        try:
            if isinstance(value, str):
                value = json.loads(value)
        except Exception:
            return Status.empty()

        if isinstance(value, int):  # legacy
            return Status(value, None)

        if not isinstance(value, dict):
            return Status.empty()

        try:
            last_lsn = int(value.get('optime', ''))
        except Exception:
            last_lsn = 0

        slots: Union[str, Dict[str, int], None] = value.get('slots')
        if isinstance(slots, str):
            try:
                slots = json.loads(slots)
            except Exception:
                slots = None
        if not isinstance(slots, dict):
            slots = None

        return Status(last_lsn, slots)


class Cluster(NamedTuple('Cluster',
                         [('initialize', Optional[str]),
                          ('config', Optional[ClusterConfig]),
                          ('leader', Optional[Leader]),
                          ('status', Status),
                          ('members', List[Member]),
                          ('failover', Optional[Failover]),
                          ('sync', SyncState),
                          ('history', Optional[TimelineHistory]),
                          ('failsafe', Optional[Dict[str, str]]),
                          ('workers', Dict[int, 'Cluster'])])):
    """Immutable object (namedtuple) which represents PostgreSQL or MPP cluster.

    .. note::
        We are using an old-style attribute declaration here because otherwise it is not possible to override `__new__`
        method. Without it the *workers* by default gets always the same :class:`dict` object that could be mutated.

    Consists of the following fields:

    :ivar initialize: shows whether this cluster has initialization key stored in DC or not.
    :ivar config: global dynamic configuration, reference to `ClusterConfig` object.
    :ivar leader: :class:`Leader` object which represents current leader of the cluster.
    :ivar status: :class:`Status` object which represents the `/status` key.
    :ivar members: list of:class:` Member` objects, all PostgreSQL cluster members including leader
    :ivar failover: reference to :class:`Failover` object.
    :ivar sync: reference to :class:`SyncState` object, last observed synchronous replication state.
    :ivar history: reference to `TimelineHistory` object.
    :ivar failsafe: failsafe topology. Node is allowed to become the leader only if its name is found in this list.
    :ivar workers: dictionary of workers of the MPP cluster, optional. Each key representing the group and the
                   corresponding value is a :class:`Cluster` instance.
    """

    def __new__(cls, *args: Any, **kwargs: Any):
        """Make workers argument optional and set it to an empty dict object."""
        if len(args) < len(cls._fields) and 'workers' not in kwargs:
            kwargs['workers'] = {}
        return super(Cluster, cls).__new__(cls, *args, **kwargs)

    @property
    def last_lsn(self) -> int:
        """Last known leader LSN."""
        return self.status.last_lsn

    @property
    def slots(self) -> Optional[Dict[str, int]]:
        """State of permanent replication slots on the primary in the format: ``{"slot_name": int}``."""
        return self.status.slots

    @staticmethod
    def empty() -> 'Cluster':
        """Produce an empty :class:`Cluster` instance."""
        return Cluster(None, None, None, Status.empty(), [], None, SyncState.empty(), None, None, {})

    def is_empty(self):
        """Validate definition of all attributes of this :class:`Cluster` instance.

        :returns: ``True`` if all attributes of the current :class:`Cluster` are unpopulated.
        """
        return all((self.initialize is None, self.config is None, self.leader is None, self.last_lsn == 0,
                    self.members == [], self.failover is None, self.sync.version is None,
                    self.history is None, self.slots is None, self.failsafe is None, self.workers == {}))

    def __len__(self) -> int:
        """Implement ``len`` function capability.

        .. note::
           This magic method aids in the evaluation of "emptiness" of a ``Cluster`` instance. For example:

           >>> cluster = Cluster.empty()
           >>> len(cluster)
           0

           >>> assert bool(cluster) is False

           >>> cluster = Cluster(None, None, None, Status(0, None), [1, 2, 3], None, SyncState.empty(), None, None, {})
           >>> len(cluster)
           1

           >>> assert bool(cluster) is True

           This makes it easier to write ``if cluster`` rather than the longer statement.

        """
        return int(not self.is_empty())

    @property
    def leader_name(self) -> Optional[str]:
        """The name of the leader if defined otherwise ``None``."""
        return self.leader and self.leader.name

    def is_unlocked(self) -> bool:
        """Check if the cluster does not have the leader.

        :returns: ``True`` if a leader name is not defined.
        """
        return not self.leader_name

    def has_member(self, member_name: str) -> bool:
        """Check if the given member name is present in the cluster.

        :param member_name: name to look up in the :attr:`~Cluster.members`.

        :returns: ``True`` if the member name is found.
        """
        return any(m for m in self.members if m.name == member_name)

    def get_member(self, member_name: str, fallback_to_leader: bool = True) -> Union[Member, Leader, None]:
        """Get :class:`Member` object by name or the :class:`Leader`.

        :param member_name: name of the member to retrieve.
        :param fallback_to_leader: if ``True`` return the :class:`Leader` instead if the member cannot be found.

        :returns: the :class:`Member` if found or :class:`Leader` object.
        """
        return next((m for m in self.members if m.name == member_name),
                    self.leader if fallback_to_leader else None)

    def get_clone_member(self, exclude_name: str) -> Union[Member, Leader, None]:
        """Get member or leader object to use as clone source.

        :param exclude_name: name of a member name to exclude.

        :returns: a randomly selected candidate member from available running members that are configured to as viable
                 sources for cloning (has tag ``clonefrom`` in configuration). If no member is appropriate the current
                 leader is used.
        """
        exclude = [exclude_name] + ([self.leader.name] if self.leader else [])
        candidates = [m for m in self.members if m.clonefrom and m.is_running and m.name not in exclude]
        return candidates[randint(0, len(candidates) - 1)] if candidates else self.leader

    @staticmethod
    def is_physical_slot(value: Union[Any, Dict[str, Any]]) -> bool:
        """Check whether provided configuration is for permanent physical replication slot.

        :param value: configuration of the permanent replication slot.

        :returns: ``True`` if *value* is a physical replication slot, otherwise ``False``.
        """
        return not value or isinstance(value, dict) and value.get('type', 'physical') == 'physical'

    @staticmethod
    def is_logical_slot(value: Union[Any, Dict[str, Any]]) -> bool:
        """Check whether provided configuration is for permanent logical replication slot.

        :param value: configuration of the permanent replication slot.

        :returns: ``True`` if *value* is a logical replication slot, otherwise ``False``.
        """
        return isinstance(value, dict) \
            and value.get('type', 'logical') == 'logical' \
            and bool(value.get('database') and value.get('plugin'))

    @property
    def __permanent_slots(self) -> Dict[str, Union[Dict[str, Any], Any]]:
        """Dictionary of permanent replication slots with their known LSN."""
        ret: Dict[str, Union[Dict[str, Any], Any]] = global_config.permanent_slots

        members: Dict[str, int] = {slot_name_from_member_name(m.name): m.lsn or 0 for m in self.members}
        slots: Dict[str, int] = {k: parse_int(v) or 0 for k, v in (self.slots or {}).items()}
        for name, value in list(ret.items()):
            if not value:
                value = ret[name] = {}
            if isinstance(value, dict):
                # for permanent physical slots we want to get MAX LSN from the `Cluster.slots` and from the
                # member with the matching name. It is necessary because we may have the replication slot on
                # the primary that is streaming from the other standby node using the `replicatefrom` tag.
                lsn = max(members.get(name, 0) if self.is_physical_slot(value) else 0, slots.get(name, 0))
                if lsn:
                    value['lsn'] = lsn
                else:
                    # Don't let anyone set 'lsn' in the global configuration :)
                    value.pop('lsn', None)
        return ret

    @property
    def __permanent_physical_slots(self) -> Dict[str, Any]:
        """Dictionary of permanent ``physical`` replication slots."""
        return {name: value for name, value in self.__permanent_slots.items() if self.is_physical_slot(value)}

    @property
    def __permanent_logical_slots(self) -> Dict[str, Any]:
        """Dictionary of permanent ``logical`` replication slots."""
        return {name: value for name, value in self.__permanent_slots.items() if self.is_logical_slot(value)}

    def get_replication_slots(self, postgresql: 'Postgresql', member: Tags, *,
                              role: Optional[str] = None, show_error: bool = False) -> Dict[str, Dict[str, Any]]:
        """Lookup configured slot names in the DCS, report issues found and merge with permanent slots.

        Will log an error if:

            * Any logical slots are disabled, due to version compatibility, and *show_error* is ``True``.

        :param postgresql: reference to :class:`Postgresql` object.
        :param member: reference to an object implementing :class:`Tags` interface.
        :param role: role of the node, if not set will be taken from *postgresql*.
        :param show_error: if ``True`` report error if any disabled logical slots or conflicting slot names are found.

        :returns: final dictionary of slot names, after merging with permanent slots and performing sanity checks.
        """
        name = member.name if isinstance(member, Member) else postgresql.name
        role = role or postgresql.role

        slots: Dict[str, Dict[str, str]] = self._get_members_slots(name, role)
        permanent_slots: Dict[str, Any] = self._get_permanent_slots(postgresql, member, role)

        disabled_permanent_logical_slots: List[str] = self._merge_permanent_slots(
            slots, permanent_slots, name, postgresql.major_version)

        if disabled_permanent_logical_slots and show_error:
            logger.error("Permanent logical replication slots supported by Patroni only starting from PostgreSQL 11. "
                         "Following slots will not be created: %s.", disabled_permanent_logical_slots)

        return slots

    def _merge_permanent_slots(self, slots: Dict[str, Dict[str, str]], permanent_slots: Dict[str, Any], name: str,
                               major_version: int) -> List[str]:
        """Merge replication *slots* for members with *permanent_slots*.

        Perform validation of configured permanent slot name, skipping invalid names.

        Will update *slots* in-line based on ``type`` of slot, ``physical`` or ``logical``, and name of node.
        Type is assumed to be ``physical`` if there are no attributes stored as the slot value.

        :param slots: Slot names with existing attributes if known.
        :param name: name of this node.
        :param permanent_slots: dictionary containing slot name key and slot information values.
        :param major_version: postgresql major version.

        :returns: List of disabled permanent, logical slot names, if postgresql version < 11.
        """
        disabled_permanent_logical_slots: List[str] = []

        for slot_name, value in permanent_slots.items():
            if not slot_name_re.match(slot_name):
                logger.error("Invalid permanent replication slot name '%s'", slot_name)
                logger.error("Slot name may only contain lower case letters, numbers, and the underscore chars")
                continue

            value = deepcopy(value) if value else {'type': 'physical'}
            if isinstance(value, dict):
                if 'type' not in value:
                    value['type'] = 'logical' if value.get('database') and value.get('plugin') else 'physical'

                if value['type'] == 'physical':
                    # Don't try to create permanent physical replication slot for yourself
                    if slot_name != slot_name_from_member_name(name):
                        slots[slot_name] = value
                    continue

                if self.is_logical_slot(value):
                    if major_version < SLOT_ADVANCE_AVAILABLE_VERSION:
                        disabled_permanent_logical_slots.append(slot_name)
                    elif slot_name in slots:
                        logger.error("Permanent logical replication slot {'%s': %s} is conflicting with"
                                     " physical replication slot for cluster member", slot_name, value)
                    else:
                        slots[slot_name] = value
                    continue

            logger.error("Bad value for slot '%s' in permanent_slots: %s", slot_name, permanent_slots[slot_name])
        return disabled_permanent_logical_slots

    def _get_permanent_slots(self, postgresql: 'Postgresql', tags: Tags, role: str) -> Dict[str, Any]:
        """Get configured permanent replication slots.

        .. note::
            Permanent replication slots are only considered if ``use_slots`` configuration is enabled.
            A node that is not supposed to become a leader (*nofailover*) will not have permanent replication slots.
            Also node with disabled streaming (*nostream*) and its cascading followers must not have permanent
            logical slots due to lack of feedback from node to primary, which makes them unsafe to use.

            In a standby cluster we only support physical replication slots.

            The returned dictionary for a non-standby cluster always contains permanent logical replication slots in
            order to show a warning if they are not supported by PostgreSQL before v11.

        :param postgresql: reference to :class:`Postgresql` object.
        :param tags: reference to an object implementing :class:`Tags` interface.
        :param role: role of the node -- ``primary``, ``standby_leader`` or ``replica``.

        :returns: dictionary of permanent slot names mapped to attributes.
        """
        if not global_config.use_slots or tags.nofailover:
            return {}

        if global_config.is_standby_cluster or self.get_slot_name_on_primary(postgresql.name, tags) is None:
            return self.__permanent_physical_slots \
                if postgresql.major_version >= SLOT_ADVANCE_AVAILABLE_VERSION or role == 'standby_leader' else {}

        return self.__permanent_slots if postgresql.major_version >= SLOT_ADVANCE_AVAILABLE_VERSION\
            or role in ('master', 'primary') else self.__permanent_logical_slots

    def _get_members_slots(self, name: str, role: str) -> Dict[str, Dict[str, str]]:
        """Get physical replication slots configuration for members that sourcing from this node.

        If the ``replicatefrom`` tag is set on the member - we should not create the replication slot for it on
        the current primary, because that member would replicate from elsewhere. We still create the slot if
        the ``replicatefrom`` destination member is currently not a member of the cluster (fallback to the
        primary), or if ``replicatefrom`` destination member happens to be the current primary.

        If the ``nostream`` tag is set on the member - we should not create the replication slot for it on
        the current primary or any other member even if ``replicatefrom`` is set, because ``nostream`` disables
        WAL streaming.

        Will log an error if:

            * Conflicting slot names between members are found

        :param name: name of this node.
        :param role: role of this node, if this is a ``primary`` or ``standby_leader`` return list of members
                     replicating from this node. If not then return a list of members replicating as cascaded
                     replicas from this node.

        :returns: dictionary of physical replication slots that should exist on a given node.
        """
        if not global_config.use_slots:
            return {}

        # we always want to exclude the member with our name from the list,
        # also exlude members with disabled WAL streaming
        members = filter(lambda m: m.name != name and not m.nostream, self.members)

        if role in ('master', 'primary', 'standby_leader'):
            members = [m for m in members if m.replicatefrom is None
                       or m.replicatefrom == name or not self.has_member(m.replicatefrom)]
        else:
            # only manage slots for replicas that replicate from this one, except for the leader among them
            members = [m for m in members if m.replicatefrom == name and m.name != self.leader_name]

        slots = {slot_name_from_member_name(m.name): {'type': 'physical'} for m in members}
        if len(slots) < len(members):
            # Find which names are conflicting for a nicer error message
            slot_conflicts: Dict[str, List[str]] = defaultdict(list)
            for member in members:
                slot_conflicts[slot_name_from_member_name(member.name)].append(member.name)
            logger.error("Following cluster members share a replication slot name: %s",
                         "; ".join(f"{', '.join(v)} map to {k}"
                                   for k, v in slot_conflicts.items() if len(v) > 1))
        return slots

    def has_permanent_slots(self, postgresql: 'Postgresql', member: Tags) -> bool:
        """Check if our node has permanent replication slots configured.

        :param postgresql: reference to :class:`Postgresql` object.
        :param member: reference to an object implementing :class:`Tags` interface for
                       the node that we are checking permanent logical replication slots for.

        :returns: ``True`` if there are permanent replication slots configured, otherwise ``False``.
        """
        role = 'replica'
        members_slots: Dict[str, Dict[str, str]] = self._get_members_slots(postgresql.name, role)
        permanent_slots: Dict[str, Any] = self._get_permanent_slots(postgresql, member, role)
        slots = deepcopy(members_slots)
        self._merge_permanent_slots(slots, permanent_slots, postgresql.name, postgresql.major_version)
        return len(slots) > len(members_slots) or any(self.is_physical_slot(v) for v in permanent_slots.values())

    def filter_permanent_slots(self, postgresql: 'Postgresql', slots: Dict[str, int]) -> Dict[str, int]:
        """Filter out all non-permanent slots from provided *slots* dict.

        :param postgresql: reference to :class:`Postgresql` object.
        :param slots: slot names with LSN values.

        :returns: a :class:`dict` object that contains only slots that are known to be permanent.
        """
        if postgresql.major_version < SLOT_ADVANCE_AVAILABLE_VERSION:
            return {}  # for legacy PostgreSQL we don't support permanent slots on standby nodes

        permanent_slots: Dict[str, Any] = self._get_permanent_slots(postgresql, RemoteMember('', {}), 'replica')
        members_slots = {slot_name_from_member_name(m.name) for m in self.members}

        return {name: value for name, value in slots.items() if name in permanent_slots
                and (self.is_physical_slot(permanent_slots[name])
                     or self.is_logical_slot(permanent_slots[name]) and name not in members_slots)}

    def _has_permanent_logical_slots(self, postgresql: 'Postgresql', member: Tags) -> bool:
        """Check if the given member node has permanent ``logical`` replication slots configured.

        :param postgresql: reference to a :class:`Postgresql` object.
        :param member: reference to an object implementing :class:`Tags` interface for
                       the node that we are checking permanent logical replication slots for.

        :returns: ``True`` if any detected replications slots are ``logical``, otherwise ``False``.
        """
        slots = self.get_replication_slots(postgresql, member, role='replica').values()
        return any(v for v in slots if v.get("type") == "logical")

    def should_enforce_hot_standby_feedback(self, postgresql: 'Postgresql', member: Tags) -> bool:
        """Determine whether ``hot_standby_feedback`` should be enabled for the given member.

        The ``hot_standby_feedback`` must be enabled if the current replica has ``logical`` slots,
        or it is working as a cascading replica for the other node that has ``logical`` slots.

        :param postgresql: reference to a :class:`Postgresql` object.
        :param member: reference to an object implementing :class:`Tags` interface for
                       the node that we are checking permanent logical replication slots for.

        :returns: ``True`` if this node or any member replicating from this node has
                  permanent logical slots, otherwise ``False``.
        """
        if self._has_permanent_logical_slots(postgresql, member):
            return True

        if global_config.use_slots:
            name = member.name if isinstance(member, Member) else postgresql.name
            members = [m for m in self.members if m.replicatefrom == name and m.name != self.leader_name]
            return any(self.should_enforce_hot_standby_feedback(postgresql, m) for m in members)
        return False

    def get_slot_name_on_primary(self, name: str, tags: Tags) -> Optional[str]:
        """Get the name of physical replication slot for this node on the primary.

        .. note::
            P <-- I <-- L

            In case of cascading replication we have to check not our physical slot, but slot of the replica that
            connects us to the primary.

        :param name: name of the member node to check.
        :param tags: reference to an object implementing :class:`Tags` interface.

        :returns: the slot name on the primary that is in use for physical replication on this node.
        """
        if tags.nostream:
            return None
        replicatefrom = self.get_member(tags.replicatefrom, False) if tags.replicatefrom else None
        return self.get_slot_name_on_primary(replicatefrom.name, replicatefrom) \
            if isinstance(replicatefrom, Member) else slot_name_from_member_name(name)

    @property
    def timeline(self) -> int:
        """Get the cluster history index from the :attr:`~Cluster.history`.

        :returns: If the recorded history is empty assume timeline is ``1``, if it is not defined or the stored history
                  is not formatted as expected ``0`` is returned and an error will be logged.
                  Otherwise, the last number stored incremented by 1 is returned.

        :Example:

            No history provided:
            >>> Cluster(0, 0, 0, Status.empty(), 0, 0, 0, 0,  None, {}).timeline
            0

            Empty history assume timeline is ``1``:
            >>> Cluster(0, 0, 0, Status.empty(), 0, 0, 0, TimelineHistory.from_node(1, '[]'),  None, {}).timeline
            1

            Invalid history format, a string of ``a``, returns ``0``:
            >>> Cluster(0, 0, 0, Status.empty(), 0, 0, 0, TimelineHistory.from_node(1, '[["a"]]'), None, {}).timeline
            0

            History as a list of strings:
            >>> history = TimelineHistory.from_node(1, '[["3", "2", "1"]]')
            >>> Cluster(0, 0, 0, Status.empty(), 0, 0, 0, history, None, {}).timeline
            4
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
        """Lowest Patroni software version found in known members of the cluster."""
        return next(iter(sorted(m.patroni_version for m in self.members if m.patroni_version)), None)


class ReturnFalseException(Exception):
    """Exception to be caught by the :func:`catch_return_false_exception` decorator."""


def catch_return_false_exception(func: Callable[..., Any]) -> Any:
    """Decorator function for catching functions raising :exc:`ReturnFalseException`.

    :param func: function to be wrapped.

    :returns: wrapped function.
    """

    def wrapper(*args: Any, **kwargs: Any):
        try:
            return func(*args, **kwargs)
        except ReturnFalseException:
            return False

    return wrapper


class AbstractDCS(abc.ABC):
    """Abstract representation of DCS modules.

    Implementations of a concrete DCS class, using appropriate backend client interfaces, must include the following
    methods and properties.

    Functional methods that are critical in their timing, required to complete within ``retry_timeout`` period in order
    to prevent the DCS considered inaccessible, each perform construction of complex data objects:

        * :meth:`~AbstractDCS._postgresql_cluster_loader`:
            method which processes the structure of data stored in the DCS used to build the :class:`Cluster` object
            with all relevant associated data.
        * :meth:`~AbstractDCS._mpp_cluster_loader`:
            Similar to above but specifically representing MPP group and workers information.
        * :meth:`~AbstractDCS._load_cluster`:
            main method for calling specific ``loader`` method to build the :class:`Cluster` object representing the
            state and topology of the cluster.

    Functional methods that are critical in their timing and must be written with ACID transaction properties in mind:

        * :meth:`~AbstractDCS.attempt_to_acquire_leader`:
            method used in the leader race to attempt to acquire the leader lock by creating the leader key in the DCS,
            if it does not exist.
        * :meth:`~AbstractDCS._update_leader`:
            method to update ``leader`` key in DCS. Relies on Compare-And-Set to ensure the Primary lock key is updated.
            If this fails to update within the ``retry_timeout`` window the Primary will be demoted.

    Functional method that relies on Compare-And-Create to ensure only one member creates the relevant key:

        * :meth:`~AbstractDCS.initialize`:
            method used in the race for cluster initialization which creates the ``initialize`` key in the DCS.

    DCS backend getter and setter methods and properties:

        * :meth:`~AbstractDCS.take_leader`: method to create a new leader key in the DCS.
        * :meth:`~AbstractDCS.set_ttl`: method for setting TTL value in DCS.
        * :meth:`~AbstractDCS.ttl`: property which returns the current TTL.
        * :meth:`~AbstractDCS.set_retry_timeout`: method for setting ``retry_timeout`` in DCS backend.
        * :meth:`~AbstractDCS._write_leader_optime`: compatibility method to write WAL LSN to DCS.
        * :meth:`~AbstractDCS._write_status`: method to write WAL LSN for slots to the DCS.
        * :meth:`~AbstractDCS._write_failsafe`: method to write cluster topology to the DCS, used by failsafe mechanism.
        * :meth:`~AbstractDCS.touch_member`: method to update individual member key in the DCS.
        * :meth:`~AbstractDCS.set_history_value`: method to set the ``history`` key in the DCS.

    DCS setter methods using Compare-And-Set which although important are less critical if they fail, attempts can be
    retried or may result in warning log messages:

        * :meth:`~AbstractDCS.set_failover_value`: method to create and/or update the ``failover`` key in the DCS.
        * :meth:`~AbstractDCS.set_config_value`: method to create and/or update the ``failover`` key in the DCS.
        * :meth:`~AbstractDCS.set_sync_state_value`: method to set the synchronous state ``sync`` key in the DCS.

    DCS data and key removal methods:

        * :meth:`~AbstractDCS.delete_sync_state`:
            likewise, a method to remove synchronous state ``sync`` key from the DCS.
        * :meth:`~AbstractDCS.delete_cluster`:
            method which will remove cluster information from the DCS. Used only from `patronictl`.
        * :meth:`~AbstractDCS._delete_leader`:
            method relies on CAS, used by a member that is the current leader, to remove the ``leader`` key in the DCS.
        * :meth:`~AbstractDCS.cancel_initialization`:
            method to remove the ``initialize`` key for the cluster from the DCS.

    If either of the `sync_state` set or delete methods fail, although not critical, this may result in
    ``Synchronous replication key updated by someone else`` messages being logged.

    Care should be taken to consult each abstract method for any additional information and requirements such as
    expected exceptions that should be raised in certain conditions and the object types for arguments and return from
    methods and properties.
    """

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

    def __init__(self, config: Dict[str, Any], mpp: 'AbstractMPP') -> None:
        """Prepare DCS paths, MPP object, initial values for state information and processing dependencies.

        :ivar config: :class:`dict`, reference to config section of selected DCS.
                      i.e.: ``zookeeper`` for zookeeper, ``etcd`` for etcd, etc...
        """
        self._mpp = mpp
        self._name = config['name']
        self._base_path = re.sub('/+', '/', '/'.join(['', config.get('namespace', 'service'), config['scope']]))
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

    @property
    def mpp(self) -> 'AbstractMPP':
        """Get the effective underlying MPP, if any has been configured."""
        return self._mpp

    def client_path(self, path: str) -> str:
        """Construct the absolute key name from appropriate parts for the DCS type.

        :param path: The key name within the current Patroni cluster.

        :returns: absolute key name for the current Patroni cluster.
        """
        components = [self._base_path]
        if self._mpp.is_enabled():
            components.append(str(self._mpp.group))
        components.append(path.lstrip('/'))
        return '/'.join(components)

    @property
    def initialize_path(self) -> str:
        """Get the client path for ``initialize``."""
        return self.client_path(self._INITIALIZE)

    @property
    def config_path(self) -> str:
        """Get the client path for ``config``."""
        return self.client_path(self._CONFIG)

    @property
    def members_path(self) -> str:
        """Get the client path for ``members``."""
        return self.client_path(self._MEMBERS)

    @property
    def member_path(self) -> str:
        """Get the client path for ``member`` representing this node."""
        return self.client_path(self._MEMBERS + self._name)

    @property
    def leader_path(self) -> str:
        """Get the client path for ``leader``."""
        return self.client_path(self._LEADER)

    @property
    def failover_path(self) -> str:
        """Get the client path for ``failover``."""
        return self.client_path(self._FAILOVER)

    @property
    def history_path(self) -> str:
        """Get the client path for ``history``."""
        return self.client_path(self._HISTORY)

    @property
    def status_path(self) -> str:
        """Get the client path for ``status``."""
        return self.client_path(self._STATUS)

    @property
    def leader_optime_path(self) -> str:
        """Get the client path for ``optime/leader`` (legacy key, superseded by ``status``)."""
        return self.client_path(self._LEADER_OPTIME)

    @property
    def sync_path(self) -> str:
        """Get the client path for ``sync``."""
        return self.client_path(self._SYNC)

    @property
    def failsafe_path(self) -> str:
        """Get the client path for ``failsafe``."""
        return self.client_path(self._FAILSAFE)

    @abc.abstractmethod
    def set_ttl(self, ttl: int) -> Optional[bool]:
        """Set the new *ttl* value for DCS keys."""

    @property
    @abc.abstractmethod
    def ttl(self) -> int:
        """Get current ``ttl`` value."""

    @abc.abstractmethod
    def set_retry_timeout(self, retry_timeout: int) -> None:
        """Set the new value for *retry_timeout*."""

    def _set_loop_wait(self, loop_wait: int) -> None:
        """Set new *loop_wait* value.

        :param loop_wait: value to set.
        """
        self._loop_wait = loop_wait

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        """Load and set relevant values from configuration.

        Sets ``loop_wait``, ``ttl`` and ``retry_timeout`` properties.

        :param config: Loaded configuration information object or dictionary of key value pairs.
        """
        self._set_loop_wait(config['loop_wait'])
        self.set_ttl(config['ttl'])
        self.set_retry_timeout(config['retry_timeout'])

    @property
    def loop_wait(self) -> int:
        """The recorded value for cluster HA loop wait time in seconds."""
        return self._loop_wait

    @property
    def last_seen(self) -> int:
        """The time recorded when the DCS was last reachable."""
        return self._last_seen

    @abc.abstractmethod
    def _postgresql_cluster_loader(self, path: Any) -> Cluster:
        """Load and build the :class:`Cluster` object from DCS, which represents a single PostgreSQL cluster.

        :param path: the path in DCS where to load :class:`Cluster` from.

        :returns: :class:`Cluster` instance.
        """

    @abc.abstractmethod
    def _mpp_cluster_loader(self, path: Any) -> Dict[int, Cluster]:
        """Load and build all PostgreSQL clusters from a single MPP cluster.

        :param path: the path in DCS where to load Cluster(s) from.

        :returns: all MPP groups as :class:`dict`, with group IDs as keys and :class:`Cluster` objects as values.
        """

    @abc.abstractmethod
    def _load_cluster(
            self, path: str, loader: Callable[[Any], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        """Main abstract method that implements the loading of :class:`Cluster` instance.

        .. note::
            Internally this method should call the *loader* method that will build :class:`Cluster` object which
            represents current state and topology of the cluster in DCS. This method supposed to be called only by
            the :meth:`~AbstractDCS.get_cluster` method.

        :param path: the path in DCS where to load Cluster(s) from.
        :param loader: one of :meth:`~AbstractDCS._postgresql_cluster_loader` or
                       :meth:`~AbstractDCS._mpp_cluster_loader`.

        :raise: :exc:`~DCSError` in case of communication problems with DCS. If the current node was running as a
                primary and exception raised, instance would be demoted.
        """

    def __get_postgresql_cluster(self, path: Optional[str] = None) -> Cluster:
        """Low level method to load a :class:`Cluster` object from DCS.

        :param path: optional client path in DCS backend to load from.

        :returns: a loaded :class:`Cluster` instance.
        """
        if path is None:
            path = self.client_path('')
        cluster = self._load_cluster(path, self._postgresql_cluster_loader)
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(cluster, Cluster)
        return cluster

    def is_mpp_coordinator(self) -> bool:
        """:class:`Cluster` instance has a Coordinator group ID.

        :returns: ``True`` if the given node is running as the MPP Coordinator.
        """
        return self._mpp.is_coordinator()

    def get_mpp_coordinator(self) -> Optional[Cluster]:
        """Load the PostgreSQL cluster for the MPP Coordinator.

        .. note::
            This method is only executed on the worker nodes to find the coordinator.

        :returns: Select :class:`Cluster` instance associated with the MPP Coordinator group ID.
        """
        try:
            return self.__get_postgresql_cluster(f'{self._base_path}/{self._mpp.coordinator_group_id}/')
        except Exception as e:
            logger.error('Failed to load %s coordinator cluster from %s: %r',
                         self._mpp.type, self.__class__.__name__, e)
        return None

    def _get_mpp_cluster(self) -> Cluster:
        """Load MPP cluster from DCS.

        :returns: A MPP :class:`Cluster` instance for the coordinator with workers clusters in the `Cluster.workers`
                  dict.
        """
        groups = self._load_cluster(self._base_path + '/', self._mpp_cluster_loader)
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(groups, dict)
        cluster = groups.pop(self._mpp.coordinator_group_id, Cluster.empty())
        cluster.workers.update(groups)
        return cluster

    def get_cluster(self) -> Cluster:
        """Retrieve a fresh view of DCS.

        .. note::
            Stores copy of time, status and failsafe values for comparison in DCS update decisions.
            Caching is required to avoid overhead placed upon the REST API.

            Returns either a PostgreSQL or MPP implementation of :class:`Cluster` depending on availability.

        :returns:
        """
        try:
            cluster = self._get_mpp_cluster() if self.is_mpp_coordinator() else self.__get_postgresql_cluster()
        except Exception:
            self.reset_cluster()
            raise

        self._last_seen = int(time.time())
        self._last_status = {self._OPTIME: cluster.last_lsn}
        if cluster.slots:
            self._last_status['slots'] = cluster.slots
        self._last_failsafe = cluster.failsafe

        with self._cluster_thread_lock:
            self._cluster = cluster
            self._cluster_valid_till = time.time() + self.ttl
            return cluster

    @property
    def cluster(self) -> Optional[Cluster]:
        """Cached DCS cluster information that has not yet expired."""
        with self._cluster_thread_lock:
            return self._cluster if self._cluster_valid_till > time.time() else None

    def reset_cluster(self) -> None:
        """Clear cached state of DCS."""
        with self._cluster_thread_lock:
            self._cluster = None
            self._cluster_valid_till = 0

    @abc.abstractmethod
    def _write_leader_optime(self, last_lsn: str) -> bool:
        """Write current WAL LSN into ``/optime/leader`` key in DCS.

        :param last_lsn: absolute WAL LSN in bytes.

        :returns: ``True`` if successfully committed to DCS.
        """

    def write_leader_optime(self, last_lsn: int) -> None:
        """Write value for WAL LSN to ``optime/leader`` key in DCS.

        .. note::
            This method abstracts away the required data structure of :meth:`~Cluster.write_status`, so it
            is not needed in the caller. However, the ``optime/leader`` is only written in
            :meth:`~Cluster.write_status` when the cluster has members with a Patroni version that
            is old enough to require it (i.e. the old Patroni version doesn't understand the new format).

        :param last_lsn: absolute WAL LSN in bytes.
        """
        self.write_status({self._OPTIME: last_lsn})

    @abc.abstractmethod
    def _write_status(self, value: str) -> bool:
        """Write current WAL LSN and ``confirmed_flush_lsn`` of permanent slots into the ``/status`` key in DCS.

        :param value: status serialized in JSON format.

        :returns: ``True`` if successfully committed to DCS.
        """

    def write_status(self, value: Dict[str, Any]) -> None:
        """Write cluster status to DCS if changed.

        .. note::
            The DCS key ``/status`` was introduced in Patroni version 2.1.0. Previous to this the position of last known
            leader LSN was stored in ``optime/leader``. This method has detection for backwards compatibility of members
            with a version older than this.

        :param value: JSON serializable dictionary with current WAL LSN and ``confirmed_flush_lsn`` of permanent slots.
        """
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

        :param value: failsafe topology serialized in JSON format.

        :returns: ``True`` if successfully committed to DCS.
        """

    def write_failsafe(self, value: Dict[str, str]) -> None:
        """Write the ``/failsafe`` key in DCS.

        :param value: dictionary value to set, consisting of the ``name`` and ``api_url`` of members.
        """
        if not (isinstance(self._last_failsafe, dict) and deep_compare(self._last_failsafe, value)) \
                and self._write_failsafe(json.dumps(value, separators=(',', ':'))):
            self._last_failsafe = value

    @property
    def failsafe(self) -> Optional[Dict[str, str]]:
        """Stored value of :attr:`~AbstractDCS._last_failsafe`."""
        return self._last_failsafe

    @abc.abstractmethod
    def _update_leader(self, leader: Leader) -> bool:
        """Update ``leader`` key (or session) ttl.

        .. note::
            You have to use CAS (Compare And Swap) operation in order to update leader key, for example for etcd
            ``prevValue`` parameter must be used.

            If update fails due to DCS not being accessible or because it is not able to process requests (hopefully
            temporary), the :exc:`DCSError` exception should be raised.

        :param leader: a reference to a current :class:`leader` object.

        :returns: ``True`` if ``leader`` key (or session) has been updated successfully.
        """

    def update_leader(self,
                      leader: Leader,
                      last_lsn: Optional[int],
                      slots: Optional[Dict[str, int]] = None,
                      failsafe: Optional[Dict[str, str]] = None) -> bool:
        """Update ``leader`` key (or session) ttl and optime/leader.

        :param leader: :class:`Leader` object with information about the leader.
        :param last_lsn: absolute WAL LSN in bytes.
        :param slots: dictionary with permanent slots ``confirmed_flush_lsn``.
        :param failsafe: if defined dictionary passed to :meth:`~AbstractDCS.write_failsafe`.

        :returns: ``True`` if ``leader`` key (or session) has been updated successfully.
        """
        ret = self._update_leader(leader)
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
        """Attempt to acquire leader lock.

        .. note::
            This method should create ``/leader`` key with the value :attr:`~AbstractDCS._name`.

            The key must be created atomically. In case the key already exists it should not be
            overwritten and ``False`` must be returned.

            If key creation fails due to DCS not being accessible or because it is not able to
            process requests (hopefully temporary), the :exc:`DCSError` exception should be raised.

        :returns: ``True`` if key has been created successfully.
        """

    @abc.abstractmethod
    def set_failover_value(self, value: str, version: Optional[Any] = None) -> bool:
        """Create or update ``/failover`` key.

        :param value: value to set.
        :param version: for conditional update of the key/object.

        :returns: ``True`` if successfully committed to DCS.
        """

    def manual_failover(self, leader: Optional[str], candidate: Optional[str],
                        scheduled_at: Optional[datetime.datetime] = None, version: Optional[Any] = None) -> bool:
        """Prepare dictionary with given values and set ``/failover`` key in DCS.

        :param leader: value to set for ``leader``.
        :param candidate: value to set for ``member``.
        :param scheduled_at: value converted to ISO date format for ``scheduled_at``.
        :param version: for conditional update of the key/object.

        :returns: ``True`` if successfully committed to DCS.
        """
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
        """Create or update ``/config`` key in DCS.

        :param value: new value to set in the ``config`` key.
        :param version: for conditional update of the key/object.

        :returns: ``True`` if successfully committed to DCS.
        """

    @abc.abstractmethod
    def touch_member(self, data: Dict[str, Any]) -> bool:
        """Update member key in DCS.

        .. note::
            This method should create or update key with the name with ``/members/`` + :attr:`~AbstractDCS._name`
            and the value of *data* in a given DCS.

        :param data: information about an instance (including connection strings).

        :returns: ``True`` if successfully committed to DCS.
        """

    @abc.abstractmethod
    def take_leader(self) -> bool:
        """Establish a new leader in DCS.

        .. note::
            This method should create leader key with value of :attr:`~AbstractDCS._name` and ``ttl`` of
            :attr:`~AbstractDCS.ttl`.

            Since it could be called only on initial cluster bootstrap it could create this key regardless,
            overwriting the key if necessary.

        :returns: ``True`` if successfully committed to DCS.
        """

    @abc.abstractmethod
    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        """Race for cluster initialization.

        This method should atomically create ``initialize`` key and return ``True``,
        otherwise it should return ``False``.

        :param create_new: ``False`` if the key should already exist (in the case we are setting the system_id).
        :param sysid: PostgreSQL cluster system identifier, if specified, is written to the key.

        :returns: ``True`` if key has been created successfully.
        """

    @abc.abstractmethod
    def _delete_leader(self, leader: Leader) -> bool:
        """Remove leader key from DCS.

        This method should remove leader key if current instance is the leader.

        :param leader: :class:`Leader` object with information about the leader.

        :returns: ``True`` if successfully committed to DCS.
        """

    def delete_leader(self, leader: Optional[Leader], last_lsn: Optional[int] = None) -> bool:
        """Update ``optime/leader`` and voluntarily remove leader key from DCS.

        This method should remove leader key if current instance is the leader.

        :param leader: :class:`Leader` object with information about the leader.
        :param last_lsn: latest checkpoint location in bytes.

        :returns: boolean result of called abstract :meth:`~AbstractDCS._delete_leader`.
        """
        if last_lsn:
            self.write_status({self._OPTIME: last_lsn})
        return bool(leader) and self._delete_leader(leader)

    @abc.abstractmethod
    def cancel_initialization(self) -> bool:
        """Removes the ``initialize`` key for a cluster.

        :returns: ``True`` if successfully committed to DCS.
        """

    @abc.abstractmethod
    def delete_cluster(self) -> bool:
        """Delete cluster from DCS.

        :returns: ``True`` if successfully committed to DCS.
        """

    @staticmethod
    def sync_state(leader: Optional[str], sync_standby: Optional[Collection[str]]) -> Dict[str, Any]:
        """Build ``sync_state`` dictionary.

        :param leader: name of the leader node that manages ``/sync`` key.
        :param sync_standby: collection of currently known synchronous standby node names.

        :returns: dictionary that later could be serialized to JSON or saved directly to DCS.
        """
        return {'leader': leader, 'sync_standby': ','.join(sorted(sync_standby)) if sync_standby else None}

    def write_sync_state(self, leader: Optional[str], sync_standby: Optional[Collection[str]],
                         version: Optional[Any] = None) -> Optional[SyncState]:
        """Write the new synchronous state to DCS.

        Calls :meth:`~AbstractDCS.sync_state` to build a dictionary and then calls DCS specific
        :meth:`~AbstractDCS.set_sync_state_value`.

        :param leader: name of the leader node that manages ``/sync`` key.
        :param sync_standby: collection of currently known synchronous standby node names.
        :param version: for conditional update of the key/object.

        :returns: the new :class:`SyncState` object or ``None``.
        """
        sync_value = self.sync_state(leader, sync_standby)
        ret = self.set_sync_state_value(json.dumps(sync_value, separators=(',', ':')), version)
        if not isinstance(ret, bool):
            return SyncState.from_node(ret, sync_value)
        return None

    @abc.abstractmethod
    def set_history_value(self, value: str) -> bool:
        """Set value for ``history`` in DCS.

        :param value: new value of ``history`` key/object.

        :returns: ``True`` if successfully committed to DCS.
        """

    @abc.abstractmethod
    def set_sync_state_value(self, value: str, version: Optional[Any] = None) -> Union[Any, bool]:
        """Set synchronous state in DCS.

        :param value: the new value of ``/sync`` key.
        :param version: for conditional update of the key/object.

        :returns: *version* of the new object or ``False`` in case of error.
        """

    @abc.abstractmethod
    def delete_sync_state(self, version: Optional[Any] = None) -> bool:
        """Delete the synchronous state from DCS.

        :param version: for conditional deletion of the key/object.

        :returns: ``True`` if delete successful.
        """

    def watch(self, leader_version: Optional[Any], timeout: float) -> bool:
        """Sleep if the current node is a leader, otherwise, watch for changes of leader key with a given *timeout*.

        :param leader_version: version of a leader key.
        :param timeout: timeout in seconds.

        :returns: if ``True`` this will reschedule the next run of the HA cycle.
        """
        _ = leader_version
        self.event.wait(timeout)
        return self.event.is_set()
