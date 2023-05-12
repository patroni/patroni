import logging
import re
import time

from copy import deepcopy
from typing import Collection, List, NamedTuple, Tuple, TYPE_CHECKING

from ..collections import CaseInsensitiveDict, CaseInsensitiveSet
from ..dcs import Cluster
from ..psycopg import quote_ident as _quote_ident
if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql

logger = logging.getLogger(__name__)

SYNC_STANDBY_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z_0-9\$]*$')
SYNC_REP_PARSER_RE = re.compile(r"""
           (?P<first> [fF][iI][rR][sS][tT] )
         | (?P<any> [aA][nN][yY] )
         | (?P<space> \s+ )
         | (?P<ident> [A-Za-z_][A-Za-z_0-9\$]* )
         | (?P<dquot> " (?: [^"]+ | "" )* " )
         | (?P<star> [*] )
         | (?P<num> \d+ )
         | (?P<comma> , )
         | (?P<parenstart> \( )
         | (?P<parenend> \) )
         | (?P<JUNK> . )
        """, re.X)


def quote_ident(value: str) -> str:
    """Very simplified version of `psycopg` :func:`quote_ident` function."""
    return value if SYNC_STANDBY_NAME_RE.match(value) else _quote_ident(value)


class _SSN(NamedTuple):
    """class representing "synchronous_standby_names" value after parsing.

    :ivar sync_type: possible values: 'off', 'priority', 'quorum'
    :ivar has_star: is set to `True` if "synchronous_standby_names" contains '*'
    :ivar num: how many nodes are required to be synchronous
    :ivar members: collection of standby names listed in "synchronous_standby_names"
    """
    sync_type: str
    has_star: bool
    num: int
    members: CaseInsensitiveSet


_EMPTY_SSN = _SSN('off', False, 0, CaseInsensitiveSet())


def parse_sync_standby_names(value: str) -> _SSN:
    """Parse postgresql synchronous_standby_names to constituent parts.

    :param value: the value of `synchronous_standby_names`
    :returns: :class:`_SSN` object
    :raises `ValueError`: if the configuration value can not be parsed

    >>> parse_sync_standby_names('').sync_type
    'off'

    >>> parse_sync_standby_names('FiRsT').sync_type
    'priority'

    >>> 'first' in parse_sync_standby_names('FiRsT').members
    True

    >>> set(parse_sync_standby_names('"1"').members)
    {'1'}

    >>> parse_sync_standby_names(' a , b ').members == {'a', 'b'}
    True

    >>> parse_sync_standby_names(' a , b ').num
    1

    >>> parse_sync_standby_names('ANY 4("a",*,b)').has_star
    True

    >>> parse_sync_standby_names('ANY 4("a",*,b)').num
    4

    >>> parse_sync_standby_names('1')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unparseable synchronous_standby_names value

    >>> parse_sync_standby_names('a,')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unparseable synchronous_standby_names value

    >>> parse_sync_standby_names('ANY 4("a" b,"c c")')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unparseable synchronous_standby_names value

    >>> parse_sync_standby_names('FIRST 4("a",)')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unparseable synchronous_standby_names value

    >>> parse_sync_standby_names('2 (,)')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Unparseable synchronous_standby_names value
    """
    tokens = [(m.lastgroup, m.group(0), m.start())
              for m in SYNC_REP_PARSER_RE.finditer(value)
              if m.lastgroup != 'space']
    if not tokens:
        return deepcopy(_EMPTY_SSN)

    if [t[0] for t in tokens[0:3]] == ['any', 'num', 'parenstart'] and tokens[-1][0] == 'parenend':
        sync_type = 'quorum'
        num = int(tokens[1][1])
        synclist = tokens[3:-1]
    elif [t[0] for t in tokens[0:3]] == ['first', 'num', 'parenstart'] and tokens[-1][0] == 'parenend':
        sync_type = 'priority'
        num = int(tokens[1][1])
        synclist = tokens[3:-1]
    elif [t[0] for t in tokens[0:2]] == ['num', 'parenstart'] and tokens[-1][0] == 'parenend':
        sync_type = 'priority'
        num = int(tokens[0][1])
        synclist = tokens[2:-1]
    else:
        sync_type = 'priority'
        num = 1
        synclist = tokens

    has_star = False
    members = CaseInsensitiveSet()
    for i, (a_type, a_value, a_pos) in enumerate(synclist):
        if i % 2 == 1:  # odd elements are supposed to be commas
            if len(synclist) == i + 1:  # except the last token
                raise ValueError("Unparseable synchronous_standby_names value %r: Unexpected token %s %r at %d" %
                                 (value, a_type, a_value, a_pos))
            elif a_type != 'comma':
                raise ValueError("Unparseable synchronous_standby_names value %r: ""Got token %s %r while"
                                 " expecting comma at %d" % (value, a_type, a_value, a_pos))
        elif a_type in {'ident', 'first', 'any'}:
            members.add(a_value)
        elif a_type == 'star':
            members.add(a_value)
            has_star = True
        elif a_type == 'dquot':
            members.add(a_value[1:-1].replace('""', '"'))
        else:
            raise ValueError("Unparseable synchronous_standby_names value %r: Unexpected token %s %r at %d" %
                             (value, a_type, a_value, a_pos))
    return _SSN(sync_type, has_star, num, members)


class SyncHandler(object):
    """Class responsible for working with the `synchronous_standby_names`.

    Sync standbys are chosen based on their state in `pg_stat_replication`.
    When `synchronous_standby_names` is changed we memorize the `_primary_flush_lsn`
    and the `current_state()` method will count newly added names as "sync" only when
    they reached memorized LSN and also reported as "sync" by `pg_stat_replication`"""

    def __init__(self, postgresql: 'Postgresql') -> None:
        self._postgresql = postgresql
        self._synchronous_standby_names = ''  # last known value of synchronous_standby_names
        self._ssn_data = deepcopy(_EMPTY_SSN)
        self._primary_flush_lsn = 0
        # "sync" replication connections, that were verified to reach self._primary_flush_lsn at some point
        self._ready_replicas = CaseInsensitiveDict({})  # keys: member names, values: connection pids

    def _handle_synchronous_standby_names_change(self) -> None:
        """Handles changes of "synchronous_standby_names" GUC.

        If "synchronous_standby_names" was changed, we need to check that newly added replicas have
        reached `self._primary_flush_lsn`. Only after that they could be counted as synchronous.
        """
        synchronous_standby_names = self._postgresql.synchronous_standby_names()
        if synchronous_standby_names == self._synchronous_standby_names:
            return

        self._synchronous_standby_names = synchronous_standby_names
        try:
            self._ssn_data = parse_sync_standby_names(synchronous_standby_names)
        except ValueError as e:
            logger.warning('%s', e)
            self._ssn_data = deepcopy(_EMPTY_SSN)

        # Invalidate cache of "sync" connections
        for app_name in list(self._ready_replicas.keys()):
            if app_name not in self._ssn_data.members:
                del self._ready_replicas[app_name]

        # Newly connected replicas will be counted as sync only when reached self._primary_flush_lsn
        self._primary_flush_lsn = self._postgresql.last_operation()
        self._postgresql.query('SELECT pg_catalog.txid_current()')  # Ensure some WAL traffic to move replication
        self._postgresql.reset_cluster_info_state(None)  # Reset internal cache to query fresh values

    def current_state(self, cluster: Cluster) -> Tuple[CaseInsensitiveSet, CaseInsensitiveSet]:
        """Finds best candidates to be the synchronous standbys.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.

        Standbys are selected based on values from the global configuration:
        - `maximum_lag_on_syncnode`: would help swapping unhealthy sync replica in case if it stops
          responding (or hung). Please set the value high enough so it won't unncessarily swap sync
          standbys during high loads. Any value less or equal of 0 keeps the behavior backward compatible.
          Please note that it will not also swap sync standbys in case where all replicas are hung.
        - `synchronous_node_count`: controlls how many nodes should be set as synchronous.

        :returns: tuple of candidates :class:`CaseInsensitiveSet` and synchronous standbys :class:`CaseInsensitiveSet`.
        """
        self._handle_synchronous_standby_names_change()

        # Pick candidates based on who has higher replay/remote_write/flush lsn.
        sort_col = {
            'remote_apply': 'replay',
            'remote_write': 'write'
        }.get(self._postgresql.synchronous_commit(), 'flush') + '_lsn'

        pg_stat_replication = [(r['pid'], r['application_name'], r['sync_state'], r[sort_col])
                               for r in self._postgresql.pg_stat_replication()
                               if r[sort_col] is not None]

        members = CaseInsensitiveDict({m.name: m for m in cluster.members})
        replica_list: List[Tuple[int, str, str, int, bool]] = []
        # pg_stat_replication.sync_state has 4 possible states - async, potential, quorum, sync.
        # That is, alphabetically they are in the reversed order of priority.
        # Since we are doing reversed sort on (sync_state, lsn) tuples, it helps to keep the result
        # consistent in case if a synchronous standby member is slowed down OR async node receiving
        # changes faster than the sync member (very rare but possible).
        # Such cases would trigger sync standby member swapping, but only if lag on a sync node exceeding a threshold.
        for pid, app_name, sync_state, replica_lsn in sorted(pg_stat_replication, key=lambda r: r[2:4], reverse=True):
            member = members.get(app_name)
            if member and member.is_running and not member.tags.get('nosync', False):
                replica_list.append((pid, member.name, sync_state, replica_lsn, bool(member.nofailover)))

        max_lsn = max(replica_list, key=lambda x: x[3])[3]\
            if len(replica_list) > 1 else self._postgresql.last_operation()

        if TYPE_CHECKING:  # pragma: no cover
            assert self._postgresql.global_config is not None
        sync_node_count = self._postgresql.global_config.synchronous_node_count\
            if self._postgresql.supports_multiple_sync else 1
        sync_node_maxlag = self._postgresql.global_config.maximum_lag_on_syncnode

        candidates = CaseInsensitiveSet()
        sync_nodes = CaseInsensitiveSet()
        # Prefer members without nofailover tag. We are relying on the fact that sorts are guaranteed to be stable.
        for pid, app_name, sync_state, replica_lsn, _ in sorted(replica_list, key=lambda x: x[4]):
            # if standby name is listed in the /sync key we can count it as synchronous, otherwice
            # it becomes really synchronous when sync_state = 'sync' and it is known that it managed to catch up
            if app_name not in self._ready_replicas and app_name in self._ssn_data.members and\
                    (cluster.sync.matches(app_name) or sync_state == 'sync' and replica_lsn >= self._primary_flush_lsn):
                self._ready_replicas[app_name] = pid

            if sync_node_maxlag <= 0 or max_lsn - replica_lsn <= sync_node_maxlag:
                candidates.add(app_name)
                if sync_state == 'sync' and app_name in self._ready_replicas:
                    sync_nodes.add(app_name)
            if len(candidates) >= sync_node_count:
                break

        return candidates, sync_nodes

    def set_synchronous_standby_names(self, sync: Collection[str]) -> None:
        """Constructs and sets "synchronous_standby_names" GUC value.

        :param sync: set of nodes to sync to
        """
        has_asterisk = '*' in sync
        if has_asterisk:
            sync = ['*']
        else:
            sync = [quote_ident(x) for x in sync]

        if self._postgresql.supports_multiple_sync and len(sync) > 1:
            sync_param = '{0} ({1})'.format(len(sync), ','.join(sync))
        else:
            sync_param = next(iter(sync), None)

        if not (self._postgresql.config.set_synchronous_standby_names(sync_param)
                and self._postgresql.state == 'running' and self._postgresql.is_leader()) or has_asterisk:
            return

        time.sleep(0.1)  # Usualy it takes 1ms to reload postgresql.conf, but we will give it 100ms

        # Reset internal cache to query fresh values
        self._postgresql.reset_cluster_info_state(None)

        # timeline == 0 -- indicates that this is the replica, shoudn't ever happen
        if self._postgresql.get_primary_timeline() > 0:
            self._handle_synchronous_standby_names_change()
