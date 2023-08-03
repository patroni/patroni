import logging
import os
import shutil

from collections import defaultdict
from contextlib import contextmanager
from threading import Condition, Thread
from typing import Any, Dict, Iterator, List, Optional, Union, Tuple, TYPE_CHECKING

from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir
from ..dcs import Cluster, Leader
from ..file_perm import pg_perm
from ..psycopg import OperationalError

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

    def __init__(self, slots_handler: 'SlotsHandler') -> None:
        super(SlotsAdvanceThread, self).__init__()
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
        failed = copy = False
        try:
            cur.execute("SELECT pg_catalog.pg_replication_slot_advance(%s, %s)", (slot, format_lsn(lsn)))
        except Exception as e:
            logger.error("Failed to advance logical replication slot '%s': %r", slot, e)
            failed = True
            copy = isinstance(e, OperationalError) and e.diag.sqlstate == '58P01'  # WAL file is gone
        with self._condition:
            if self._scheduled and failed:
                if copy and slot not in self._copy_slots:
                    self._copy_slots.append(slot)
                self._failed = True

            new_lsn = self._scheduled.get(database, {}).get(slot, 0)
            # remove slot from the self._scheduled structure only if it wasn't changed
            if new_lsn == lsn and database in self._scheduled:
                self._scheduled[database].pop(slot)
                if not self._scheduled[database]:
                    self._scheduled.pop(database)

    def sync_slots_in_database(self, database: str, slots: List[str]) -> None:
        with self._slots_handler.get_local_connection_cursor(dbname=database, options='-c statement_timeout=0') as cur:
            for slot in slots:
                with self._condition:
                    lsn = self._scheduled.get(database, {}).get(slot, 0)
                if lsn:
                    self.sync_slot(cur, database, slot, lsn)

    def sync_slots(self) -> None:
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
        while True:
            with self._condition:
                if not self._scheduled:
                    self._condition.wait()

            self.sync_slots()

    def schedule(self, advance_slots: Dict[str, Dict[str, int]]) -> Tuple[bool, List[str]]:
        with self._condition:
            for database, values in advance_slots.items():
                self._scheduled[database].update(values)
            ret = (self._failed, self._copy_slots)
            self._copy_slots = []
            self._failed = False
            self._condition.notify()

        return ret

    def on_promote(self) -> None:
        with self._condition:
            self._scheduled.clear()
            self._failed = False
            self._copy_slots = []


class SlotsHandler(object):

    def __init__(self, postgresql: 'Postgresql') -> None:
        self._postgresql = postgresql
        self._advance = None
        self._replication_slots: Dict[str, Dict[str, Any]] = {}  # already existing replication slots
        self._logical_slots_processing_queue: Dict[str, Optional[int]] = {}
        self.pg_replslot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot')
        self.schedule()

    def _query(self, sql: str, *params: Any) -> Union['cursor', 'Cursor[Any]']:
        return self._postgresql.query(sql, *params, retry=False)

    @staticmethod
    def _copy_items(src: Dict[str, Any], dst: Dict[str, Any], keys: Optional[List[str]] = None) -> None:
        dst.update({key: src[key] for key in keys or ('datoid', 'catalog_xmin', 'confirmed_flush_lsn')})

    def process_permanent_slots(self, slots: List[Dict[str, Any]]) -> Dict[str, int]:
        """This methods solves three problems at once (I know, it is weird).

        The cluster_info_query from `Postgresql` is executed every HA loop and returns
        information about all replication slots that exists on the current host.
        Based on this information we perform the following actions:
        1. For the primary we want to expose to DCS permanent logical slots, therefore the method
           builds (and returns) a dict, that maps permanent logical slot names and confirmed_flush_lsns.
        2. This method also detects if one of the previously known permanent slots got missing and schedules resync.
        3. Updates the local cache with the fresh catalog_xmin and confirmed_flush_lsn for every known slot.
           This info is used when performing the check of logical slot readiness on standbys.
        """
        ret: Dict[str, int] = {}

        slots_dict: Dict[str, Dict[str, Any]] = {slot['slot_name']: slot for slot in slots or []}
        if slots_dict:
            for name, value in slots_dict.items():
                if name in self._replication_slots:
                    if compare_slots(value, self._replication_slots[name], 'datoid'):
                        if value['type'] == 'logical':
                            ret[name] = value['confirmed_flush_lsn']
                            self._copy_items(value, self._replication_slots[name])
                    else:
                        self._schedule_load_slots = True

        # It could happen that the slot was deleted in the background, we want to detect this case
        if any(name not in slots_dict for name in self._replication_slots.keys()):
            self._schedule_load_slots = True

        return ret

    def load_replication_slots(self) -> None:
        if self._postgresql.major_version >= 90400 and self._schedule_load_slots:
            replication_slots: Dict[str, Dict[str, Any]] = {}
            extra = ", catalog_xmin, pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint"\
                if self._postgresql.major_version >= 100000 else ""
            skip_temp_slots = ' WHERE NOT temporary' if self._postgresql.major_version >= 100000 else ''
            cursor = self._query('SELECT slot_name, slot_type, plugin, database, datoid'
                                 '{0} FROM pg_catalog.pg_replication_slots{1}'.format(extra, skip_temp_slots))
            for r in cursor:
                value = {'type': r[1]}
                if r[1] == 'logical':
                    value.update(plugin=r[2], database=r[3], datoid=r[4])
                    if self._postgresql.major_version >= 100000:
                        value.update(catalog_xmin=r[5], confirmed_flush_lsn=r[6])
                replication_slots[r[0]] = value
            self._replication_slots = replication_slots
            self._schedule_load_slots = False
            if self._force_readiness_check:
                self._logical_slots_processing_queue = {n: None for n, v in replication_slots.items()
                                                        if v['type'] == 'logical'}
                self._force_readiness_check = False

    def ignore_replication_slot(self, cluster: Cluster, name: str) -> bool:
        slot = self._replication_slots[name]
        if cluster.config:
            for matcher in cluster.config.ignore_slots_matchers:
                if ((matcher.get("name") is None or matcher["name"] == name)
                   and all(not matcher.get(a) or matcher[a] == slot.get(a) for a in ('database', 'plugin', 'type'))):
                    return True
        return self._postgresql.citus_handler.ignore_replication_slot(slot)

    def drop_replication_slot(self, name: str) -> Tuple[bool, bool]:
        """Returns a tuple(active, dropped)"""
        cursor = self._query(('WITH slots AS (SELECT slot_name, active'
                              ' FROM pg_catalog.pg_replication_slots WHERE slot_name = %s),'
                              ' dropped AS (SELECT pg_catalog.pg_drop_replication_slot(slot_name),'
                              ' true AS dropped FROM slots WHERE not active) '
                              'SELECT active, COALESCE(dropped, false) FROM slots'
                              ' FULL OUTER JOIN dropped ON true'), name)
        row = cursor.fetchone()
        if not row:
            row = (False, False)
        return row

    def _drop_incorrect_slots(self, cluster: Cluster, slots: Dict[str, Any], paused: bool) -> None:
        # drop old replication slots which are not presented in desired slots
        for name in set(self._replication_slots) - set(slots):
            if not paused and not self.ignore_replication_slot(cluster, name):
                active, dropped = self.drop_replication_slot(name)
                if dropped:
                    logger.info("Dropped unknown replication slot '%s'", name)
                else:
                    self._schedule_load_slots = True
                    if active:
                        logger.debug("Unable to drop unknown replication slot '%s', slot is still active", name)
                    else:
                        logger.error("Failed to drop replication slot '%s'", name)
        for name, value in slots.items():
            if name in self._replication_slots and not compare_slots(value, self._replication_slots[name]):
                logger.info("Trying to drop replication slot '%s' because value is changing from %s to %s",
                            name, self._replication_slots[name], value)
                if self.drop_replication_slot(name) == (False, True):
                    self._replication_slots.pop(name)
                else:
                    logger.error("Failed to drop replication slot '%s'", name)
                    self._schedule_load_slots = True

    def _ensure_physical_slots(self, slots: Dict[str, Any]) -> None:
        immediately_reserve = ', true' if self._postgresql.major_version >= 90600 else ''
        for name, value in slots.items():
            if name not in self._replication_slots and value['type'] == 'physical':
                try:
                    self._query(("SELECT pg_catalog.pg_create_physical_replication_slot(%s{0})"
                                 " WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots"
                                 " WHERE slot_type = 'physical' AND slot_name = %s)").format(
                                     immediately_reserve), name, name)
                except Exception:
                    logger.exception("Failed to create physical replication slot '%s'", name)
                self._schedule_load_slots = True

    @contextmanager
    def get_local_connection_cursor(self, **kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        conn_kwargs = self._postgresql.config.local_connect_kwargs
        conn_kwargs.update(kwargs)
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def _ensure_logical_slots_primary(self, slots: Dict[str, Any]) -> None:
        # Group logical slots to be created by database name
        logical_slots: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for name, value in slots.items():
            if value['type'] == 'logical':
                # If the logical already exists, copy some information about it into the original structure
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
        if not self._advance:
            self._advance = SlotsAdvanceThread(self)
        return self._advance.schedule(slots)

    def _ensure_logical_slots_replica(self, cluster: Cluster, slots: Dict[str, Any]) -> List[str]:
        # Group logical slots to be advanced by database name
        advance_slots: Dict[str, Dict[str, int]] = defaultdict(dict)
        create_slots: List[str] = []  # And collect logical slots to be created on the replica
        for name, value in slots.items():
            if value['type'] == 'logical':
                # If the logical already exists, copy some information about it into the original structure
                if self._replication_slots.get(name, {}).get('datoid'):
                    self._copy_items(self._replication_slots[name], value)
                    if cluster.slots and name in cluster.slots:
                        try:  # Skip slots that doesn't need to be advanced
                            if value['confirmed_flush_lsn'] < int(cluster.slots[name]):
                                advance_slots[value['database']][name] = int(cluster.slots[name])
                        except Exception as e:
                            logger.error('Failed to parse "%s": %r', cluster.slots[name], e)
                elif cluster.slots and name in cluster.slots:  # We want to copy only slots with feedback in a DCS
                    create_slots.append(name)

        error, copy_slots = self.schedule_advance_slots(advance_slots)
        if error:
            self._schedule_load_slots = True
        return create_slots + copy_slots

    def sync_replication_slots(self, cluster: Cluster, nofailover: bool,
                               replicatefrom: Optional[str] = None, paused: bool = False) -> List[str]:
        ret = []
        if self._postgresql.major_version >= 90400 and cluster.config:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql.name, self._postgresql.role,
                                                      nofailover, self._postgresql.major_version, True)

                self._drop_incorrect_slots(cluster, slots, paused)

                self._ensure_physical_slots(slots)

                if self._postgresql.is_leader():
                    self._logical_slots_processing_queue.clear()
                    self._ensure_logical_slots_primary(slots)
                elif cluster.slots and slots:
                    self.check_logical_slots_readiness(cluster, replicatefrom)

                    ret = self._ensure_logical_slots_replica(cluster, slots)

                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True
        return ret

    @contextmanager
    def _get_leader_connection_cursor(self, leader: Leader) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        conn_kwargs = leader.conn_kwargs(self._postgresql.config.rewind_credentials)
        conn_kwargs['dbname'] = self._postgresql.database
        with get_connection_cursor(connect_timeout=3, options="-c statement_timeout=2000", **conn_kwargs) as cur:
            yield cur

    def check_logical_slots_readiness(self, cluster: Cluster, replicatefrom: Optional[str]) -> bool:
        """Determine whether all known logical slots are synchronised from the leader.

         1) Retrieve the current ``catalog_xmin`` value for the physical slot from the cluster leader, and
         2) using previously stored list of "unready" logical slots, those which have yet to be checked hence have no
            stored slot attributes,
         3) store logical slot ``catalog_xmin`` when the physical slot ``catalog_xmin`` becomes valid.

         :param cluster: object containing stateful information for the cluster.
         :param replicatefrom: name of the member that should be used to replicate from.

         :returns: ``False`` if any issue while checking logical slots readiness, ``True`` otherwise.
         """
        catalog_xmin = None
        if self._logical_slots_processing_queue and cluster.leader:
            slot_name = cluster.get_my_slot_name_on_primary(self._postgresql.name, replicatefrom)
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
            cur = self._query("SELECT pg_catalog.current_setting('hot_standby_feedback')::boolean")
            row = cur.fetchone()
            if row and not row[0]:
                logger.error('Logical slot failover requires "hot_standby_feedback".'
                             ' Please check postgresql.auto.conf')
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

    def copy_logical_slots(self, cluster: Cluster, create_slots: List[str]) -> None:
        leader = cluster.leader
        if not leader:
            return
        slots = cluster.get_replication_slots(self._postgresql.name, 'replica', False, self._postgresql.major_version)
        copy_slots: Dict[str, Dict[str, Any]] = {}
        with self._get_leader_connection_cursor(leader) as cur:
            try:
                cur.execute("SELECT slot_name, slot_type, datname, plugin, catalog_xmin, "
                            "pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, "
                            "pg_catalog.pg_read_binary_file('pg_replslot/' || slot_name || '/state')"
                            " FROM pg_catalog.pg_get_replication_slots() JOIN pg_catalog.pg_database ON datoid = oid"
                            " WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = ANY(%s)", (create_slots,))

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
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = self._force_readiness_check = value

    def on_promote(self) -> None:
        if self._advance:
            self._advance.on_promote()

        if self._logical_slots_processing_queue:
            logger.warning('Logical replication slots that might be unsafe to use after promote: %s',
                           set(self._logical_slots_processing_queue))
