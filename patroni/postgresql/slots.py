"""Replication slot handling.

Provides classes for the creation, monitoring, management and synchronisation of PostgreSQL replication slots.
"""

import logging
import os
import shutil

from collections import defaultdict
from contextlib import contextmanager
from threading import Condition, Thread
from typing import Any, Collection, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

from .. import global_config
from ..dcs import Cluster, Leader
from ..file_perm import pg_perm
from ..psycopg import OperationalError
from ..tags import Tags
from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir, PostgresqlRole

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor

    from . import Postgresql

logger = logging.getLogger(__name__)


def compare_slots(s1: Dict[str, Any], s2: Dict[str, Any], dbid: str = 'database') -> bool:
    """Compare 2 replication slot objects for equality.

    ..note ::
        If the first argument is a ``physical`` replication slot then only the `type` of the second slot is compared.
        If the first argument is another ``type`` (e.g. ``logical``) then *dbid* and ``plugin`` are compared.

    :param s1: First slot dictionary to be compared.
    :param s2: Second slot dictionary to be compared.
    :param dbid: Optional attribute to be compared when comparing ``logical`` replication slots.

    :return: ``True`` if the slot ``type`` of *s1* and *s2* is matches, and the ``type`` of *s1* is ``physical``,
             OR the ``types`` match AND the *dbid* and ``plugin`` attributes are equal.

    """
    return (s1['type'] == s2['type']
            and (s1['type'] == 'physical'
                 or s1.get(dbid) == s2.get(dbid)
                 and s1['plugin'] == s2['plugin']))


class SlotsAdvanceThread(Thread):
    """Daemon process :class:``Thread`` object for advancing logical replication slots on replicas.

    This ensures that slot advancing queries sent to postgres do not block the main loop.
    """

    def __init__(self, slots_handler: 'SlotsHandler') -> None:
        """Create and start a new thread for handling slot advance queries.

        :param slots_handler: The calling class instance for reference to slot information attributes.
        """
        super().__init__()
        self.daemon = True
        self._slots_handler = slots_handler

        # _copy_slots and _failed are used to asynchronously give some feedback to the main thread
        self._copy_slots: List[str] = []
        self._failed = False
        # {'dbname1': {'slot1': 100, 'slot2': 100}, 'dbname2': {'slot3': 100}}
        self._scheduled: Dict[str, Dict[str, int]] = defaultdict(dict)
        self._condition = Condition()  # protect self._scheduled from concurrent access and to wakeup the run() method

        self.start()

    def sync_slot(self, cur: Union['cursor', 'Cursor[Any]'], database: str, slot: str, lsn: int) -> None:
        """Execute a ``pg_replication_slot_advance`` query and store success for scheduled synchronisation task.

        :param cur: database connection cursor.
        :param database: name of the database associated with the slot.
        :param slot: name of the slot to be synchronised.
        :param lsn: last known LSN position
        """
        failed = copy = False
        try:
            cur.execute("SELECT pg_catalog.pg_replication_slot_advance(%s, %s)", (slot, format_lsn(lsn)))
        except Exception as e:
            logger.error("Failed to advance logical replication slot '%s': %r", slot, e)
            failed = True
            # WAL file is gone or slot is invalidated
            copy = isinstance(e, OperationalError) and e.diag.sqlstate in ('58P01', '55000')
        with self._condition:
            if self._scheduled and failed:
                if copy and slot not in self._copy_slots:
                    self._copy_slots.append(slot)
                self._failed = True

            new_lsn = self._scheduled.get(database, {}).get(slot, 0)
            # remove slot from the self._scheduled structure if it is to be copied or if it wasn't changed
            if copy or (new_lsn == lsn and database in self._scheduled):
                self._scheduled[database].pop(slot)
                if not self._scheduled[database]:
                    self._scheduled.pop(database)

    def sync_slots_in_database(self, database: str, slots: List[str]) -> None:
        """Synchronise slots for a single database.

        :param database: name of the database.
        :param slots: list of slot names to synchronise.
        """
        with self._slots_handler.get_local_connection_cursor(dbname=database, options='-c statement_timeout=0') as cur:
            for slot in slots:
                with self._condition:
                    lsn = self._scheduled.get(database, {}).get(slot, 0)
                if lsn:
                    self.sync_slot(cur, database, slot, lsn)

    def sync_slots(self) -> None:
        """Synchronise slots for all scheduled databases."""
        with self._condition:
            databases = list(self._scheduled.keys())
        for database in databases:
            with self._condition:
                slots = list(self._scheduled.get(database, {}).keys())
            if slots:
                try:
                    self.sync_slots_in_database(database, slots)
                except Exception as e:
                    logger.error('Failed to advance replication slots in database %s: %r', database, e)

    def run(self) -> None:
        """Thread main loop entrypoint.

        .. note::
            Thread will wait until a sync is scheduled from outside, normally triggered during the HA loop or a wakeup
            call.
        """
        while True:
            with self._condition:
                if not self._scheduled:
                    self._condition.wait()

            self.sync_slots()

    def schedule(self, advance_slots: Dict[str, Dict[str, int]]) -> Tuple[bool, List[str]]:
        """Trigger a synchronisation of slots.

        This is the main entrypoint for Patroni HA loop wakeup call.

        :param advance_slots: dictionary containing slots that need to be advanced

        :return: tuple of failure status and a list of slots to be copied
        """
        with self._condition:
            for database, values in advance_slots.items():
                for name, value in values.items():
                    # Don't schedule sync for slots that just failed to be advanced and scheduled to be copied
                    if name not in self._copy_slots:
                        self._scheduled[database][name] = value
            ret = (self._failed, self._copy_slots)
            self._copy_slots = []
            self._failed = False
            self._condition.notify()

        return ret

    def clean(self) -> None:
        """Reset state of the daemon."""
        with self._condition:
            self._scheduled.clear()
            self._failed = False
            self._copy_slots = []


class SlotsHandler:
    """Handler for managing and storing information on replication slots in PostgreSQL.

    :ivar pg_replslot_dir: system location path of the PostgreSQL replication slots.
    :ivar _logical_slots_processing_queue: yet to be processed logical replication slots on the primary
    """

    def __init__(self, postgresql: 'Postgresql') -> None:
        """Create an instance with storage attributes for replication slots and schedule the first synchronisation.

        :param postgresql: Calling class instance providing interface to PostgreSQL.
        """
        self._force_readiness_check = False
        self._schedule_load_slots = False
        self._postgresql = postgresql
        self._advance = None
        self._replication_slots: Dict[str, Dict[str, Any]] = {}  # already existing replication slots
        self._logical_slots_processing_queue: Dict[str, Optional[int]] = {}
        self.pg_replslot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot')
        self.schedule()

    def _query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Helper method for :meth:`Postgresql.query`.

        :param sql: SQL statement to execute.
        :param params: parameters to pass through to :meth:`Postgresql.query`.

        :returns: query response.
        """
        return self._postgresql.query(sql, *params, retry=False)

    @staticmethod
    def _copy_items(src: Dict[str, Any], dst: Dict[str, Any], keys: Optional[Collection[str]] = None) -> None:
        """Select values from *src* dictionary to update in *dst* dictionary for optional supplied *keys*.

        :param src: source dictionary that *keys* will be looked up from.
        :param dst: destination dictionary to be updated.
        :param keys: optional list of keys to be looked up in the source dictionary.
        """
        dst.update({key: src[key] for key in keys or ('datoid', 'catalog_xmin', 'confirmed_flush_lsn')})

    def process_permanent_slots(self, slots: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process replication slot information from the host and prepare information used in subsequent cluster tasks.

        .. note::
            This methods solves three problems.

            The ``cluster_info_query`` from :class:``Postgresql`` is executed every HA loop and returns information
            about all replication slots that exists on the current host.

            Based on this information perform the following actions:

            1. For the primary we want to expose to DCS permanent logical slots, therefore build (and return) a dict
               that maps permanent logical slot names to ``confirmed_flush_lsn``.
            2. detect if one of the previously known permanent slots is missing and schedule resync.
            3. Update the local cache with the fresh ``catalog_xmin`` and ``confirmed_flush_lsn`` for every known slot.

           This info is used when performing the check of logical slot readiness on standbys.

        :param slots: replication slot information that exists on the current host.

        :return: dictionary of logical slot names to ``confirmed_flush_lsn``.
        """
        ret: Dict[str, int] = {}

        slots_dict: Dict[str, Dict[str, Any]] = {slot['slot_name']: slot for slot in slots or []}
        for name, value in slots_dict.items():
            if name in self._replication_slots:
                if compare_slots(value, self._replication_slots[name], 'datoid'):
                    if value['type'] == 'logical':
                        ret[name] = value['confirmed_flush_lsn']
                        self._copy_items(value, self._replication_slots[name])
                    else:
                        ret[name] = value['restart_lsn']
                        self._copy_items(value, self._replication_slots[name], ('restart_lsn', 'xmin'))
                else:
                    self._schedule_load_slots = True

        # It could happen that the slot was deleted in the background, we want to detect this case
        if any(name not in slots_dict for name in self._replication_slots.keys()):
            self._schedule_load_slots = True

        return ret

    def load_replication_slots(self) -> None:
        """Query replication slot information from the database and store it for processing by other tasks.

        .. note::
            Only supported from PostgreSQL version 9.4 onwards.

        Store replication slot ``name``, ``type``, ``plugin``, ``database`` and ``datoid``.
        If PostgreSQL version is 10 or newer also store ``catalog_xmin`` and ``confirmed_flush_lsn``.

        When using logical slots, store information separately for slot synchronisation  on replica nodes.
        """
        if self._postgresql.major_version >= 90400 and self._schedule_load_slots:
            replication_slots: Dict[str, Dict[str, Any]] = {}
            pg_wal_lsn_diff = f"pg_catalog.pg_{self._postgresql.wal_name}_{self._postgresql.lsn_name}_diff"
            extra = f", catalog_xmin, {pg_wal_lsn_diff}(confirmed_flush_lsn, '0/0')::bigint" \
                if self._postgresql.major_version >= 100000 else ""
            filter_columns = tuple(fltr for fltr, major in (('temporary', 100000), ('failover', 170000))
                                   if self._postgresql.major_version >= major)
            where_filter = ' AND '.join(map(lambda col: f'NOT {col}', filter_columns))
            where_condition = f' WHERE {where_filter}' if where_filter else ''
            for r in self._query("SELECT slot_name, slot_type, xmin, "
                                 f"{pg_wal_lsn_diff}(restart_lsn, '0/0')::bigint, plugin, database, datoid{extra}"
                                 f" FROM pg_catalog.pg_replication_slots{where_condition}"):
                value = {'type': r[1]}
                if r[1] == 'logical':
                    value.update(plugin=r[4], database=r[5], datoid=r[6])
                    if self._postgresql.major_version >= 100000:
                        value.update(catalog_xmin=r[7], confirmed_flush_lsn=r[8])
                else:
                    value.update(xmin=r[2], restart_lsn=r[3])
                replication_slots[r[0]] = value
            self._replication_slots = replication_slots
            self._schedule_load_slots = False
            if self._force_readiness_check:
                self._logical_slots_processing_queue = {n: None for n, v in replication_slots.items()
                                                        if v['type'] == 'logical'}
                self._force_readiness_check = False

    def ignore_replication_slot(self, cluster: Cluster, name: str) -> bool:
        """Check if slot *name* should not be managed by Patroni.

        :param cluster: cluster state information object.
        :param name: name of the slot to ignore

        :returns: ``True`` if slot *name* matches any slot specified in ``ignore_slots`` configuration,
                 otherwise will pass through and return result of :meth:`AbstractMPPHandler.ignore_replication_slot`.
        """
        slot = self._replication_slots[name]
        if cluster.config:
            for matcher in global_config.ignore_slots_matchers:
                if (
                        (matcher.get("name") is None or matcher["name"] == name)
                        and all(not matcher.get(a) or matcher[a] == slot.get(a)
                                for a in ('database', 'plugin', 'type'))
                ):
                    return True
        return self._postgresql.mpp_handler.ignore_replication_slot(slot)

    def drop_replication_slot(self, name: str) -> Tuple[bool, bool]:
        """Drop a named slot from Postgres.

        :param name: name of the slot to be dropped.

        :returns: a tuple of ``active`` and ``dropped``. ``active`` is ``True`` if the slot is active,
                  ``dropped`` is ``True`` if the slot was successfully dropped. If the slot was not found return
                  ``False`` for both.
        """
        rows = self._query(('WITH slots AS (SELECT slot_name, active'
                            ' FROM pg_catalog.pg_replication_slots WHERE slot_name = %s),'
                            ' dropped AS (SELECT pg_catalog.pg_drop_replication_slot(slot_name),'
                            ' true AS dropped FROM slots WHERE not active) '
                            'SELECT active, COALESCE(dropped, false) FROM slots'
                            ' FULL OUTER JOIN dropped ON true'), name)
        return (rows[0][0], rows[0][1]) if rows else (False, False)

    def _drop_replication_slot(self, name: str) -> None:
        """Drop replication slot by name.

        .. note::
            If not able to drop the slot, it will log a message and set the flag to reload slots.

        :param name: name of the slot to be dropped.
        """
        active, dropped = self.drop_replication_slot(name)
        if dropped:
            logger.info("Dropped replication slot '%s'", name)
            if name in self._replication_slots:
                del self._replication_slots[name]
        else:
            self._schedule_load_slots = True
            if active:
                logger.warning("Unable to drop replication slot '%s', slot is active", name)
            else:
                logger.error("Failed to drop replication slot '%s'", name)

    def _drop_incorrect_slots(self, cluster: Cluster, slots: Dict[str, Any]) -> None:
        """Compare required slots and configured as permanent slots with those found, dropping extraneous ones.

        .. note::
            Slots that are not contained in *slots* will be dropped.
            Slots can be filtered out with ``ignore_slots`` configuration.

            Slots that have matching names but do not match attributes in *slots* will also be dropped.

        :param cluster: cluster state information object.
        :param slots: dictionary of desired slot names as keys with slot attributes as a dictionary value, if known.
        """
        # drop old replication slots which are not presented in desired slots.
        for name in set(self._replication_slots) - set(slots):
            if not global_config.is_paused and not self.ignore_replication_slot(cluster, name):
                logger.info("Trying to drop unknown replication slot '%s'", name)
                self._drop_replication_slot(name)

        # drop slots with matching names but attributes that do not match, e.g. `plugin` or `database`.
        for name, value in slots.items():
            if name in self._replication_slots and not compare_slots(value, self._replication_slots[name]):
                logger.info("Trying to drop replication slot '%s' because value is changing from %s to %s",
                            name, self._replication_slots[name], value)
                if self.drop_replication_slot(name) == (False, True):
                    self._replication_slots.pop(name)
                else:
                    logger.error("Failed to drop replication slot '%s'", name)
                    self._schedule_load_slots = True

    def _ensure_physical_slots(self, slots: Dict[str, Any], clean_inactive_physical_slots: bool) -> None:
        """Create or advance physical replication *slots*.

        Any failures are logged and do not interrupt creation of all *slots*.

        :param slots: A dictionary mapping slot name to slot attributes. This method only considers a slot
                      if the value is a dictionary with the key ``type`` and a value of ``physical``.
        :param clean_inactive_physical_slots: whether replication slots with ``xmin`` and not expected
                                              to be active should be dropped.
        """
        immediately_reserve = ', true' if self._postgresql.major_version >= 90600 else ''
        for name, value in slots.items():
            if value['type'] != 'physical':
                continue
            # First we want to detect physical replication slots that are not
            # expected to be active but have NOT NULL xmin value and drop them.
            # As the slot is not expected to be active, nothing would be consuming this
            # slot, consequently no hot-standby feedback messages would be received
            # by Postgres regarding this slot. In that case, the `xmin` value would never
            # change, which would prevent Postgres from advancing the xmin horizon.
            if self._postgresql.can_advance_slots and name in self._replication_slots and\
                    self._replication_slots[name]['type'] == 'physical':
                self._copy_items(self._replication_slots[name], value, ('restart_lsn', 'xmin'))
                if clean_inactive_physical_slots and value.get('expected_active') is False and value['xmin']:
                    logger.warning('Dropping physical replication slot %s because of its xmin value %s',
                                   name, value['xmin'])
                    self._drop_replication_slot(name)

            # Now we will create physical replication slots that are missing.
            if name not in self._replication_slots:
                try:
                    self._query(f"SELECT pg_catalog.pg_create_physical_replication_slot(%s{immediately_reserve})"
                                f" WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots"
                                f" WHERE slot_type = 'physical' AND slot_name = %s)",
                                name, name)
                except Exception:
                    logger.exception("Failed to create physical replication slot '%s'", name)
                self._schedule_load_slots = True
            # And advance restart_lsn on physical replication slots that are not expected to be active.
            elif self._postgresql.can_advance_slots and self._replication_slots[name]['type'] == 'physical' and\
                    value.get('expected_active') is not True and not value['xmin']:
                restart_lsn = value.get('restart_lsn')
                if not restart_lsn:
                    # This slot either belongs to a member or was configured as a permanent slot. However, for some
                    # reason the slot was created by an external agent instead of by Patroni, and it was created without
                    # reserving the LSN. We drop the slot here, as we cannot advance it, and let Patroni recreate and
                    # manage it in the next cycle.
                    logger.warning('Dropping physical replication slot %s because it has no restart_lsn. '
                                   'This slot was probably not created by Patroni, but by an external agent.', name)
                    self._drop_replication_slot(name)
                    continue
                lsn = value.get('lsn')
                if lsn and lsn > restart_lsn:  # The slot has feedback in DCS and needs to be advanced
                    try:
                        lsn = format_lsn(lsn)
                        self._query("SELECT pg_catalog.pg_replication_slot_advance(%s, %s)", name, lsn)
                    except Exception as exc:
                        logger.error("Error while advancing replication slot %s to position '%s': %r", name, lsn, exc)

    @contextmanager
    def get_local_connection_cursor(self, **kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Create a new database connection to local server.

        Create a non-blocking connection cursor to avoid the situation where an execution of the query of
        ``pg_replication_slot_advance`` takes longer than the timeout on a HA loop, which could cause a false
        failure state.

        :param kwargs: Any keyword arguments to pass to :func:`psycopg.connect`.

        :yields: connection cursor object, note implementation varies depending on version of :mod:`psycopg`.
        """
        conn_kwargs = {**self._postgresql.connection_pool.conn_kwargs, **kwargs}
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def _ensure_logical_slots_primary(self, slots: Dict[str, Any]) -> None:
        """Create any missing logical replication *slots* on the primary.

        If the logical slot already exists, copy state information into the replication slots structure stored in the
        class instance.

        :param slots: Slots that should exist are supplied in a dictionary, mapping slot name to any attributes.
                      The method will only consider slots that have a value that is a dictionary with a key ``type``
                      with a value that is ``logical``.

        """
        # Group logical slots to be created by database name
        logical_slots: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for name, value in slots.items():
            if value['type'] == 'logical':
                if self._replication_slots.get(name, {}).get('datoid'):
                    self._copy_items(self._replication_slots[name], value)
                else:
                    logical_slots[value['database']][name] = value

        # Create new logical slots
        for database, values in logical_slots.items():
            with self.get_local_connection_cursor(dbname=database) as cur:
                for name, value in values.items():
                    try:
                        cur.execute("SELECT pg_catalog.pg_create_logical_replication_slot(%s, %s)"
                                    " WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots"
                                    " WHERE slot_type = 'logical' AND slot_name = %s)",
                                    (name, value['plugin'], name))
                    except Exception as e:
                        logger.error("Failed to create logical replication slot '%s' plugin='%s': %r",
                                     name, value['plugin'], e)
                        slots.pop(name)
                    self._schedule_load_slots = True

    def schedule_advance_slots(self, slots: Dict[str, Dict[str, int]]) -> Tuple[bool, List[str]]:
        """Wrapper to ensure slots advance daemon thread is started if not already.

        :param slots: dictionary containing slot information.

        :return: tuple with the result of the scheduling of slot advancement: ``failed`` and list of slots to copy.
        """
        if not self._advance:
            self._advance = SlotsAdvanceThread(self)
        return self._advance.schedule(slots)

    def _ensure_logical_slots_replica(self, slots: Dict[str, Any]) -> List[str]:
        """Update logical *slots* on replicas.

        If the logical slot already exists, copy state information into the replication slots structure stored in the
        class instance. Slots that exist are also advanced if their ``confirmed_flush_lsn`` is smaller than the state
        of the slot stored in DCS or at least the ``replay_lsn`` of this replica.

        As logical slots can only be created when the primary is available, pass the list of slots that need to be
        copied back to the caller. They will be created on replicas with :meth:`SlotsHandler.copy_logical_slots`.

        :param slots: A dictionary mapping slot name to slot attributes. This method only considers a slot
                      if the value is a dictionary with the key ``type`` and a value of ``logical``.

        :returns: list of slots to be copied from the primary.
        """
        # Group logical slots to be advanced by database name
        advance_slots: Dict[str, Dict[str, int]] = defaultdict(dict)
        create_slots: List[str] = []  # Collect logical slots to be created on the replica

        for name, value in slots.items():
            if value['type'] != 'logical':
                continue

            # If the logical already exists, copy some information about it into the original structure
            if name in self._replication_slots and compare_slots(value, self._replication_slots[name]):
                self._copy_items(self._replication_slots[name], value)

                # The slot has feedback in DCS
                if 'lsn' in value:
                    # we can not advance past replay_lsn
                    advance_value = min(value['lsn'], self._postgresql.replay_lsn())
                    # Skip slots that don't need to be advanced
                    if value['confirmed_flush_lsn'] < advance_value:
                        advance_slots[value['database']][name] = advance_value
            elif name not in self._replication_slots and 'lsn' in value:
                # We want to copy only slots with feedback in a DCS
                create_slots.append(name)

        # Slots to be copied from the primary should be removed from the *slots* structure,
        # otherwise Patroni falsely assumes that they already exist.
        for name in create_slots:
            slots.pop(name)

        error, copy_slots = self.schedule_advance_slots(advance_slots)
        if error:
            self._schedule_load_slots = True
        return create_slots + copy_slots

    def sync_replication_slots(self, cluster: Cluster, tags: Tags) -> List[str]:
        """During the HA loop read, check and alter replication slots found in the cluster.

        Read physical and logical slots from ``pg_replication_slots``, then compare to those configured in the DCS.
        Drop any slots that do not match those required by configuration and are not configured as permanent.
        Create any missing physical slots, or advance their position according to feedback stored in DCS.
        If we are the primary then create logical slots, otherwise if logical slots are known and active create
        them on replica nodes by copying slot files from the primary.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.

        :returns: list of logical replication slots names that should be copied from the primary.
        """
        ret = []
        if self._postgresql.major_version >= 90400 and cluster.config:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql, tags, show_error=True)

                self._drop_incorrect_slots(cluster, slots)

                # We don't want to clean physical replication slots with xmin feedback if:
                # - cluster has no leader
                # - current node is a leader, but still running as a standby
                clean_inactive_physical_slots = not cluster.is_unlocked() and \
                    (cluster.leader and cluster.leader.name != self._postgresql.name or self._postgresql.is_primary())
                self._ensure_physical_slots(slots, clean_inactive_physical_slots)

                if self._postgresql.is_primary():
                    self._logical_slots_processing_queue.clear()
                    self._ensure_logical_slots_primary(slots)
                else:
                    self.check_logical_slots_readiness(cluster, tags)
                    ret = self._ensure_logical_slots_replica(slots)

                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True
        return ret

    @contextmanager
    def _get_leader_connection_cursor(self, leader: Leader) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Create a new database connection to the leader.

        .. note::
            Uses rewind user credentials because it has enough permissions to read files from PGDATA.
            Sets the options ``connect_timeout`` to ``3`` and ``statement_timeout`` to ``2000``.

        :param leader: object with information on the leader

        :yields: connection cursor object, note implementation varies depending on version of ``psycopg``.
        """
        conn_kwargs = leader.conn_kwargs(self._postgresql.config.rewind_credentials)
        conn_kwargs['dbname'] = self._postgresql.database
        with get_connection_cursor(connect_timeout=3, options="-c statement_timeout=2000", **conn_kwargs) as cur:
            yield cur

    def check_logical_slots_readiness(self, cluster: Cluster, tags: Tags) -> bool:
        """Determine whether all known logical slots are synchronised from the leader.

        1) Retrieve the current ``catalog_xmin`` value for the physical slot from the cluster leader, and
        2) using previously stored list of "unready" logical slots, those which have yet to be checked hence have no
           stored slot attributes,
        3) store logical slot ``catalog_xmin`` when the physical slot ``catalog_xmin`` becomes valid.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.

        :returns: ``False`` if any issue while checking logical slots readiness, ``True`` otherwise.
        """
        catalog_xmin = None
        if self._logical_slots_processing_queue and cluster.leader:
            slot_name = cluster.get_slot_name_on_primary(self._postgresql.name, tags)
            try:
                with self._get_leader_connection_cursor(cluster.leader) as cur:
                    cur.execute("SELECT slot_name, catalog_xmin FROM pg_catalog.pg_get_replication_slots()"
                                " WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = ANY(%s)",
                                ([n for n, v in self._logical_slots_processing_queue.items()
                                  if v is None] + [slot_name],))
                    slots = {row[0]: row[1] for row in cur}
                    if slot_name not in slots:
                        logger.warning('Physical slot %s does not exist on the primary', slot_name)
                        return False
                    catalog_xmin = slots.pop(slot_name)
            except Exception as e:
                logger.error("Failed to check %s physical slot on the primary: %r", slot_name, e)
                return False

            if not self._update_pending_logical_slot_primary(slots, catalog_xmin):
                return False  # since `catalog_xmin` isn't valid further checks don't make any sense

        self._ready_logical_slots(catalog_xmin)
        return True

    def _update_pending_logical_slot_primary(self, slots: Dict[str, Any], catalog_xmin: Optional[int] = None) -> bool:
        """Store pending logical slot information for ``catalog_xmin`` on the primary.

        Remember ``catalog_xmin`` of logical slots on the primary when ``catalog_xmin`` of the physical slot became
        valid. Logical slots on replica will be safe to use after promote when ``catalog_xmin`` of the physical slot
        overtakes these values.

        :param slots: dictionary of slot information from the primary
        :param catalog_xmin: ``catalog_xmin`` of the physical slot used by this replica to stream changes from primary.

        :returns: ``False`` if any issue was faced while processing, ``True`` otherwise.
        """
        if catalog_xmin is not None:
            for name, value in slots.items():
                self._logical_slots_processing_queue[name] = value
            return True

        # Replica isn't streaming or the hot_standby_feedback isn't enabled
        try:
            if not self._query("SELECT pg_catalog.current_setting('hot_standby_feedback')::boolean")[0][0]:
                logger.error('Logical slot failover requires "hot_standby_feedback". Please check postgresql.auto.conf')
        except Exception as e:
            logger.error('Failed to check the hot_standby_feedback setting: %r', e)
        return False

    def _ready_logical_slots(self, primary_physical_catalog_xmin: Optional[int] = None) -> None:
        """Ready logical slots by comparing primary physical slot ``catalog_xmin`` to logical ``catalog_xmin``.

        The logical slot on a replica is safe to use when the physical replica slot on the primary:

            1. has a nonzero/non-null ``catalog_xmin`` represented by ``primary_physical_xmin``.
            2. has a ``catalog_xmin`` that is not newer (greater) than the ``catalog_xmin`` of any slot on the standby
            3. overtook the ``catalog_xmin`` of remembered values of logical slots on the primary.

        :param primary_physical_catalog_xmin: is the value retrieved from ``pg_catalog.pg_get_replication_slots()`` for
                                              the physical replication slot on the primary.
        """
        # Make a copy of processing queue keys as a list as the queue dictionary is modified inside the loop.
        for name in list(self._logical_slots_processing_queue):
            primary_logical_catalog_xmin = self._logical_slots_processing_queue[name]
            standby_logical_slot = self._replication_slots.get(name, {})
            standby_logical_catalog_xmin = standby_logical_slot.get('catalog_xmin', 0)
            if TYPE_CHECKING:  # pragma: no cover
                assert primary_logical_catalog_xmin is not None

            if (
                    not standby_logical_slot
                    or primary_physical_catalog_xmin is not None
                    and primary_logical_catalog_xmin <= primary_physical_catalog_xmin <= standby_logical_catalog_xmin
            ):

                del self._logical_slots_processing_queue[name]

                if standby_logical_slot:
                    logger.info('Logical slot %s is safe to be used after a failover', name)

    def copy_logical_slots(self, cluster: Cluster, tags: Tags, create_slots: List[str]) -> None:
        """Create logical replication slots on standby nodes.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.
        :param create_slots: list of slot names to copy from the primary.
        """
        leader = cluster.leader
        if not leader:
            return
        slots = cluster.get_replication_slots(self._postgresql, tags, role=PostgresqlRole.REPLICA)
        copy_slots: Dict[str, Dict[str, Any]] = {}
        with self._get_leader_connection_cursor(leader) as cur:
            try:
                filter_failover = ' NOT failover AND' if self._postgresql.major_version >= 170000 else ''
                cur.execute("SELECT slot_name, slot_type, datname, plugin, catalog_xmin, "
                            "pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, "
                            "pg_catalog.pg_read_binary_file('pg_replslot/' || slot_name || '/state')"
                            " FROM pg_catalog.pg_get_replication_slots() JOIN pg_catalog.pg_database ON datoid = oid"
                            f" WHERE{filter_failover} NOT pg_catalog.pg_is_in_recovery()"
                            " AND slot_name = ANY(%s)", (create_slots,))

                for r in cur:
                    if r[0] in slots:  # slot_name is defined in the global configuration
                        slot = {'type': r[1], 'database': r[2], 'plugin': r[3],
                                'catalog_xmin': r[4], 'confirmed_flush_lsn': r[5], 'data': r[6]}
                        if compare_slots(slot, slots[r[0]]):
                            copy_slots[r[0]] = slot
                        else:
                            logger.warning('Will not copy the logical slot "%s" due to the configuration mismatch: '
                                           'configuration=%s, slot on the primary=%s', r[0], slots[r[0]], slot)
            except Exception as e:
                logger.error("Failed to copy logical slots from the %s via postgresql connection: %r", leader.name, e)

        if copy_slots and self._postgresql.stop():
            if self._advance:
                self._advance.clean()
            pg_perm.set_permissions_from_data_directory(self._postgresql.data_dir)
            for name, value in copy_slots.items():
                slot_dir = os.path.join(self.pg_replslot_dir, name)
                slot_tmp_dir = slot_dir + '.tmp'
                if os.path.exists(slot_tmp_dir):
                    shutil.rmtree(slot_tmp_dir)
                os.makedirs(slot_tmp_dir)
                os.chmod(slot_tmp_dir, pg_perm.dir_create_mode)
                fsync_dir(slot_tmp_dir)
                slot_filename = os.path.join(slot_tmp_dir, 'state')
                with open(slot_filename, 'wb') as f:
                    os.chmod(slot_filename, pg_perm.file_create_mode)
                    f.write(value['data'])
                    f.flush()
                    os.fsync(f.fileno())
                if os.path.exists(slot_dir):
                    shutil.rmtree(slot_dir)
                os.rename(slot_tmp_dir, slot_dir)
                os.chmod(slot_dir, pg_perm.dir_create_mode)
                fsync_dir(slot_dir)
                self._logical_slots_processing_queue[name] = None
            fsync_dir(self.pg_replslot_dir)
            self._postgresql.start()

    def schedule(self, value: Optional[bool] = None) -> None:
        """Schedule the loading of slot information from the database.

        :param value: the optional value can be used to unschedule if set to ``False`` or force it to be ``True``.
                      If it is omitted the value will be ``True`` if this PostgreSQL node supports slot replication.
        """
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = self._force_readiness_check = value

    def on_promote(self) -> None:
        """Entry point from HA cycle used when a standby node is to be promoted to primary.

        .. note::
            If logical replication slot synchronisation is enabled then slot advancement will be triggered.
            If any logical slots that were copied are yet to be confirmed as ready a warning message will be logged.

        """
        if self._advance:
            self._advance.clean()

        if self._logical_slots_processing_queue:
            logger.warning('Logical replication slots that might be unsafe to use after promote: %s',
                           set(self._logical_slots_processing_queue))
