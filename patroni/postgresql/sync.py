import logging
import re

from .validator import CaseInsensitiveDict
from ..psycopg import quote_ident as _quote_ident

logger = logging.getLogger(__name__)

SYNC_STANDBY_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z_0-9\$]*$')


def quote_ident(value):
    """Very simplified version of quote_ident"""
    return value if SYNC_STANDBY_NAME_RE.match(value) else _quote_ident(value)


class SyncHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql

    def current_state(self, cluster, sync_node_count=1, sync_node_maxlag=-1):
        """Finds the best candidate to be the synchronous standby.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.
        Parameter sync_node_maxlag(maximum_lag_on_syncnode) would help swapping unhealthy sync replica in case
        if it stops responding (or hung). Please set the value high enough so it won't unncessarily swap sync
        standbys during high loads. Any less or equal of 0 value keep the behavior backward compatible and
        will not swap. Please note that it will not also swap sync standbys in case where all replicas are hung.

        :returns tuple of candidates list and synchronous standby list. """

        # Pick candidates based on who has higher replay/remote_write/flush lsn.
        sort_col = {
            'remote_apply': 'replay',
            'remote_write': 'write'
        }.get(self._postgresql.synchronous_commit(), 'flush') + '_lsn'
        pg_stat_replication = [(r['application_name'], r['sync_state'], r[sort_col])
                               for r in self._postgresql.pg_stat_replication() or []
                               if r[sort_col] is not None]

        members = CaseInsensitiveDict({m.name: m for m in cluster.members})
        replica_list = []
        # pg_stat_replication.sync_state has 4 possible states - async, potential, quorum, sync.
        # That is, alphabetically they are in the reversed order of priority.
        # Since we are doing reversed sort on (sync_state, lsn) tuples, it helps to keep the result
        # consistent in case if a synchronous standby member is slowed down OR async node receiving
        # changes faster than the sync member (very rare but possible).
        # Such cases would trigger sync standby member swapping, but only if lag on a sync node exceeding a threshold.
        for app_name, sync_state, replica_lsn in sorted(pg_stat_replication, key=lambda r: (r[1], r[2]), reverse=True):
            member = members.get(app_name)
            if member and member.is_running and not member.tags.get('nosync', False):
                replica_list.append((member.name, sync_state, replica_lsn, bool(member.nofailover)))

        max_lsn = max(replica_list, key=lambda x: x[2])[2]\
            if len(replica_list) > 1 else self._postgresql.last_operation()

        if self._postgresql.major_version < 90600:
            sync_node_count = 1

        candidates = []
        sync_nodes = []
        # Prefer members without nofailover tag. We are relying on the fact that sorts are guaranteed to be stable.
        for app_name, sync_state, replica_lsn, _ in sorted(replica_list, key=lambda x: x[3]):
            if sync_node_maxlag <= 0 or max_lsn - replica_lsn <= sync_node_maxlag:
                candidates.append(app_name)
                if sync_state == 'sync':
                    sync_nodes.append(app_name)
            if len(candidates) >= sync_node_count:
                break

        return candidates, sync_nodes

    def set_synchronous_standby_names(self, value):
        if value and value != ['*']:
            value = [quote_ident(x) for x in value]

        if self._postgresql.major_version >= 90600 and len(value) > 1:
            sync_param = '{0} ({1})'.format(len(value), ','.join(value))
        else:
            sync_param = next(iter(value), None)

        if not (self._postgresql.config.set_synchronous_standby_names(sync_param) and
                self._postgresql.state == 'running' and self._postgresql.is_leader()) or value == ['*']:
            return

        # Reset internal cache to query fresh values
        self._postgresql.reset_cluster_info_state(None)
