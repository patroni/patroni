import logging
import re
import time

from copy import deepcopy
from typing import Collection, Iterator, List, NamedTuple, Optional, Tuple, TYPE_CHECKING

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
            if a_type != 'comma':
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


class _SyncState(NamedTuple):
    """Class representing the current synchronous state.

    :ivar sync_type: possible values: 'off', 'priority', 'quorum'
    :ivar numsync: how many nodes are required to be synchronous (according to ``synchronous_standby_names``).
                   Is ``0`` in case if synchronous_standby_names value is invalid or has ``*``.
    :ivar numsync_confirmed: how many nodes are known to be synchronous according to the ``pg_stat_replication``
                             view. Only nodes that caught up with the ``SyncHandler._primary_flush_lsn` are counted.
    :ivar sync: collection of synchronous node names. In case of quorum commit all nodes listed
                in ``synchronous_standby_names`` or nodes that are confirmed to be synchronous according
                to the `pg_stat_replication` view.
    :ivar active: collection of node names that are streaming and have no restrictions to become synchronous.
    """
    sync_type: str
    numsync: int
    numsync_confirmed: int
    sync: CaseInsensitiveSet
    active: CaseInsensitiveSet


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
        # Ensure some WAL traffic to move replication
        self._postgresql.query("""DO $$
BEGIN
    SET local synchronous_commit = 'off';
    PERFORM * FROM pg_catalog.txid_current();
END;$$""")
        self._postgresql.reset_cluster_info_state(None)  # Reset internal cache to query fresh values

    def _get_replica_list(self, cluster: Cluster) -> Iterator[Tuple[int, str, str, int, bool]]:
        """Yields candidates based on higher replay/remote_write/flush lsn.

        .. note::
            Tuples are reverse ordered by sync_state and LSN fields so nodes that already synchronous or having
            higher LSN values are preferred.

        :param cluster: current cluster topology from DCS.

        :yields: tuples composed of:

            * pid - a PID of walsender process
            * member name - matches with the application_name
            * sync_state - one of ("async", "potential", "quorum", "sync")
            * LSN - write_lsn, flush_lsn, or replica_lsn, depending on the value of ``synchronous_commit`` GUC
            * nofailover - whether the member has ``nofailover`` tag set
        """

        # What column from pg_stat_replication we want to sort on? Choose based on ``synchronous_commit`` value.
        sort_col = {
            'remote_apply': 'replay',
            'remote_write': 'write'
        }.get(self._postgresql.synchronous_commit(), 'flush') + '_lsn'
        pg_stat_replication = [(r['pid'], r['application_name'], r['sync_state'], r[sort_col])
                               for r in self._postgresql.pg_stat_replication()
                               if r[sort_col] is not None]

        members = CaseInsensitiveDict({m.name: m for m in cluster.members})

        # pg_stat_replication.sync_state has 4 possible states - async, potential, quorum, sync.
        # That is, alphabetically they are in the reversed order of priority.
        # Since we are doing reversed sort on (sync_state, lsn) tuples, it helps to keep the result
        # consistent in case if a synchronous standby member is slowed down OR async node receiving
        # changes faster than the sync member (very rare but possible).
        # Such cases would trigger sync standby member swapping, but only if lag on a sync node exceeding a threshold.
        for pid, app_name, sync_state, replica_lsn in sorted(pg_stat_replication, key=lambda r: r[2:4], reverse=True):
            member = members.get(app_name)
            if member and member.is_running and not member.tags.get('nosync', False):
                yield (pid, member.name, sync_state, replica_lsn, bool(member.nofailover))

    def _process_replica_readiness(self, cluster: Cluster, replica_list: List[Tuple[int, str, str, int, bool]]) -> None:
        """Flags replicas as truely "synchronous" when they caught up with "_primary_flush_lsn"."""
        if TYPE_CHECKING:  # pragma: no cover
            assert self._postgresql.global_config is not None
        for pid, app_name, sync_state, replica_lsn, _ in replica_list:
            if app_name not in self._ready_replicas and app_name in self._ssn_data.members:
                if self._postgresql.global_config.is_quorum_commit_mode:
                    # When quorum commit is enabled we can't check against cluster.sync because nodes
                    # are written there when at least one of them caught up with _primary_flush_lsn.
                    if replica_lsn >= self._primary_flush_lsn\
                            and (sync_state == 'quorum' or (not self._postgresql.supports_quorum_commit
                                                            and sync_state in ('sync', 'potential'))):
                        self._ready_replicas[app_name] = pid
                elif cluster.sync.matches(app_name) or sync_state == 'sync' and replica_lsn >= self._primary_flush_lsn:
                    # if standby name is listed in the /sync key we can count it as synchronous, otherwise it becomes
                    # "really" synchronous when sync_state = 'sync' and we known that it managed to catch up
                    self._ready_replicas[app_name] = pid

    def current_state(self, cluster: Cluster) -> _SyncState:
        """Finds best candidates to be the synchronous standbys.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.

        Standbys are selected based on values from the global configuration:

            - `maximum_lag_on_syncnode`: would help swapping unhealthy sync replica in case if it stops
              responding (or hung). Please set the value high enough so it won't unncessarily swap sync
              standbys during high loads. Any value less or equal of 0 keeps the behavior backward compatible.
              Please note that it will not also swap sync standbys in case where all replicas are hung.

            - `synchronous_node_count`: controlls how many nodes should be set as synchronous.

        :param cluster: current cluster topology from DCS

        :returns: current synchronous replication state as a :class:`_SyncState` object
        """
        self._handle_synchronous_standby_names_change()

        replica_list = list(self._get_replica_list(cluster))
        self._process_replica_readiness(cluster, replica_list)

        active = CaseInsensitiveSet()
        sync_nodes = CaseInsensitiveSet()
        numsync_confirmed = 0

        if TYPE_CHECKING:  # pragma: no cover
            assert self._postgresql.global_config is not None
        sync_node_count = self._postgresql.global_config.synchronous_node_count\
            if self._postgresql.supports_multiple_sync else 1
        sync_node_maxlag = self._postgresql.global_config.maximum_lag_on_syncnode

        # When checking *maximum_lag_on_syncnode* we want to compare with the most
        # up-to-date replica or with cluster LSN if there is only one replica.
        max_lsn = max(replica_list, key=lambda x: x[3])[3]\
            if len(replica_list) > 1 else self._postgresql.last_operation()

        # Prefer members without nofailover tag. We are relying on the fact that sorts are guaranteed to be stable.
        for _, app_name, sync_state, replica_lsn, nofailover in sorted(replica_list, key=lambda x: x[4]):
            if sync_node_maxlag <= 0 or max_lsn - replica_lsn <= sync_node_maxlag:
                if self._postgresql.global_config.is_quorum_commit_mode:
                    # add nodes with nofailover tag only to get enough "active" nodes
                    if not nofailover or len(active) < sync_node_count:
                        if app_name in self._ready_replicas:
                            numsync_confirmed += 1
                        active.add(app_name)
                else:
                    active.add(app_name)
                    if sync_state == 'sync' and app_name in self._ready_replicas:
                        sync_nodes.add(app_name)
                        numsync_confirmed += 1
                    if len(active) >= sync_node_count:
                        break

        if self._postgresql.global_config.is_quorum_commit_mode:
            sync_nodes = CaseInsensitiveSet() if self._ssn_data.has_star else self._ssn_data.members

        return _SyncState(
            self._ssn_data.sync_type,
            0 if self._ssn_data.has_star else self._ssn_data.num,
            numsync_confirmed,
            sync_nodes,
            active)

    def set_synchronous_standby_names(self, sync: Collection[str], num: Optional[int] = None) -> None:
        """Constructs and sets "synchronous_standby_names" GUC value.

        :param sync: set of nodes to sync to
        :param num: specifies number of nodes to sync to. The *num* is set only in case if quorum commit is enabled
        """
        # Special case. If sync nodes set is empty but requested num of sync nodes >= 1
        # we want to set synchronous_standby_names to '*'
        has_asterisk = '*' in sync or num and num >= 1 and not sync
        if has_asterisk:
            sync = ['*']
        else:
            sync = [quote_ident(x) for x in sorted(sync)]

        if self._postgresql.supports_multiple_sync and len(sync) > 1:
            if num is None:
                num = len(sync)
            sync_param = ','.join(sync)
        else:
            sync_param = next(iter(sync), None)

        if TYPE_CHECKING:  # pragma: no cover
            assert self._postgresql.global_config is not None

        if self._postgresql.global_config.is_quorum_commit_mode and sync or\
                self._postgresql.supports_multiple_sync and len(sync) > 1:
            prefix = 'ANY ' if self._postgresql.global_config.is_quorum_commit_mode\
                and self._postgresql.supports_quorum_commit else ''
            sync_param = '{0}{1} ({2})'.format(prefix, num, sync_param)

        if not (self._postgresql.config.set_synchronous_standby_names(sync_param)
                and self._postgresql.state == 'running' and self._postgresql.is_leader()) or has_asterisk:
            return

        time.sleep(0.1)  # Usualy it takes 1ms to reload postgresql.conf, but we will give it 100ms

        # Reset internal cache to query fresh values
        self._postgresql.reset_cluster_info_state(None)

        # timeline == 0 -- indicates that this is the replica
        if self._postgresql.get_primary_timeline() > 0:
            self._handle_synchronous_standby_names_change()
