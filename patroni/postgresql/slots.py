import logging
import os
import shutil

from collections import defaultdict
from contextlib import contextmanager
from threading import Condition, Thread

from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir
from ..psycopg import OperationalError

logger = logging.getLogger(__name__)


def compare_slots(s1, s2, dbid='database'):
    return s1['type'] == s2['type'] and (s1['type'] == 'physical' or
                                         s1.get(dbid) == s2.get(dbid) and s1['plugin'] == s2['plugin'])


class SlotsAdvanceThread(Thread):

    def __init__(self, slots_handler):
        super(SlotsAdvanceThread, self).__init__()
        self.daemon = True
        self._slots_handler = slots_handler

        # _copy_slots and _failed are used to asynchronously give some feedback to the main thread
        self._copy_slots = []
        self._failed = False

        self._scheduled = defaultdict(dict)  # {'dbname1': {'slot1': 100, 'slot2': 100}, 'dbname2': {'slot3': 100}}
        self._condition = Condition()  # protect self._scheduled from concurrent access and to wakeup the run() method

        self.start()

    def sync_slot(self, cur, database, slot, lsn):
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

    def sync_slots_in_database(self, database, slots):
        with self._slots_handler.get_local_connection_cursor(dbname=database, options='-c statement_timeout=0') as cur:
            for slot in slots:
                with self._condition:
                    lsn = self._scheduled.get(database, {}).get(slot, 0)
                if lsn:
                    self.sync_slot(cur, database, slot, lsn)

    def sync_slots(self):
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

    def run(self):
        while True:
            with self._condition:
                if not self._scheduled:
                    self._condition.wait()

            self.sync_slots()

    def schedule(self, advance_slots):
        with self._condition:
            for database, values in advance_slots.items():
                self._scheduled[database].update(values)
            ret = (self._failed, self._copy_slots)
            self._copy_slots = []
            self._failed = False
            self._condition.notify()

        return ret

    def on_promote(self):
        with self._condition:
            self._scheduled.clear()
            self._failed = False
            self._copy_slots = []


class SlotsHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._advance = None
        self._replication_slots = {}  # already existing replication slots
        self._unready_logical_slots = {}
        self.pg_replslot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot')
        self.schedule()

    def _query(self, sql, *params):
        return self._postgresql.query(sql, *params, retry=False)

    @staticmethod
    def _copy_items(src, dst, keys=None):
        dst.update({key: src[key] for key in keys or ('datoid', 'catalog_xmin', 'confirmed_flush_lsn')})

    def process_permanent_slots(self, slots):
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
        ret = {}

        slots = {slot['slot_name']: slot for slot in slots or []}
        if slots:
            for name, value in slots.items():
                if name in self._replication_slots:
                    if compare_slots(value, self._replication_slots[name], 'datoid'):
                        if value['type'] == 'logical':
                            ret[name] = value['confirmed_flush_lsn']
                            self._copy_items(value, self._replication_slots[name])
                    else:
                        self._schedule_load_slots = True

        # It could happen that the slots was deleted in the background, we want to detect this case
        if any(name not in slots for name in self._replication_slots.keys()):
            self._schedule_load_slots = True

        return ret

    def load_replication_slots(self):
        if self._postgresql.major_version >= 90400 and self._schedule_load_slots:
            replication_slots = {}
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
                self._unready_logical_slots = {n: None for n, v in replication_slots.items() if v['type'] == 'logical'}
                self._force_readiness_check = False

    def ignore_replication_slot(self, cluster, name):
        slot = self._replication_slots[name]
        for matcher in cluster.config.ignore_slots_matchers:
            if ((matcher.get("name") is None or matcher["name"] == name)
               and all(not matcher.get(a) or matcher[a] == slot.get(a) for a in ('database', 'plugin', 'type'))):
                return True
        return self._postgresql.citus_handler.ignore_replication_slot(slot)

    def drop_replication_slot(self, name):
        """Returns a tuple(active, dropped)"""
        cursor = self._query(('WITH slots AS (SELECT slot_name, active' +
                              ' FROM pg_catalog.pg_replication_slots WHERE slot_name = %s),' +
                              ' dropped AS (SELECT pg_catalog.pg_drop_replication_slot(slot_name),' +
                              ' true AS dropped FROM slots WHERE not active) ' +
                              'SELECT active, COALESCE(dropped, false) FROM slots' +
                              ' FULL OUTER JOIN dropped ON true'), name)
        return cursor.fetchone() if cursor.rowcount == 1 else (False, False)

    def _drop_incorrect_slots(self, cluster, slots, paused):
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

    def _ensure_physical_slots(self, slots):
        immediately_reserve = ', true' if self._postgresql.major_version >= 90600 else ''
        for name, value in slots.items():
            if name not in self._replication_slots and value['type'] == 'physical':
                try:
                    self._query(("SELECT pg_catalog.pg_create_physical_replication_slot(%s{0})" +
                                 " WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots" +
                                 " WHERE slot_type = 'physical' AND slot_name = %s)").format(
                                     immediately_reserve), name, name)
                except Exception:
                    logger.exception("Failed to create physical replication slot '%s'", name)
                self._schedule_load_slots = True

    @contextmanager
    def get_local_connection_cursor(self, **kwargs):
        conn_kwargs = self._postgresql.config.local_connect_kwargs
        conn_kwargs.update(kwargs)
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def _ensure_logical_slots_primary(self, slots):
        # Group logical slots to be created by database name
        logical_slots = defaultdict(dict)
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
                        cur.execute("SELECT pg_catalog.pg_create_logical_replication_slot(%s, %s)" +
                                    " WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots" +
                                    " WHERE slot_type = 'logical' AND slot_name = %s)",
                                    (name, value['plugin'], name))
                    except Exception as e:
                        logger.error("Failed to create logical replication slot '%s' plugin='%s': %r",
                                     name, value['plugin'], e)
                        slots.pop(name)
                    self._schedule_load_slots = True

    def schedule_advance_slots(self, slots):
        if not self._advance:
            self._advance = SlotsAdvanceThread(self)
        return self._advance.schedule(slots)

    def _ensure_logical_slots_replica(self, cluster, slots):
        advance_slots = defaultdict(dict)  # Group logical slots to be advanced by database name
        create_slots = []  # And collect logical slots to be created on the replica
        for name, value in slots.items():
            if value['type'] == 'logical':
                # If the logical already exists, copy some information about it into the original structure
                if self._replication_slots.get(name, {}).get('datoid'):
                    self._copy_items(self._replication_slots[name], value)
                    if name in cluster.slots:
                        try:  # Skip slots that doesn't need to be advanced
                            if value['confirmed_flush_lsn'] < int(cluster.slots[name]):
                                advance_slots[value['database']][name] = int(cluster.slots[name])
                        except Exception as e:
                            logger.error('Failed to parse "%s": %r', cluster.slots[name], e)
                elif name in cluster.slots:  # We want to copy only slots with feedback in a DCS
                    create_slots.append(name)

        error, copy_slots = self.schedule_advance_slots(advance_slots)
        if error:
            self._schedule_load_slots = True
        return create_slots + copy_slots

    def sync_replication_slots(self, cluster, nofailover, replicatefrom=None, paused=False):
        ret = None
        if self._postgresql.major_version >= 90400 and cluster.config:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql.name, self._postgresql.role,
                                                      nofailover, self._postgresql.major_version, True)

                self._drop_incorrect_slots(cluster, slots, paused)

                self._ensure_physical_slots(slots)

                if self._postgresql.is_leader():
                    self._unready_logical_slots.clear()
                    self._ensure_logical_slots_primary(slots)
                elif cluster.slots and slots:
                    self.check_logical_slots_readiness(cluster, nofailover, replicatefrom)

                    ret = self._ensure_logical_slots_replica(cluster, slots)

                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True
        return ret

    @contextmanager
    def _get_leader_connection_cursor(self, leader):
        conn_kwargs = leader.conn_kwargs(self._postgresql.config.rewind_credentials)
        conn_kwargs['dbname'] = self._postgresql.database
        with get_connection_cursor(connect_timeout=3, options="-c statement_timeout=2000", **conn_kwargs) as cur:
            yield cur

    def check_logical_slots_readiness(self, cluster, nofailover, replicatefrom):
        if self._unready_logical_slots:
            slot_name = cluster.get_my_slot_name_on_primary(self._postgresql.name, replicatefrom)
            try:
                with self._get_leader_connection_cursor(cluster.leader) as cur:
                    cur.execute("SELECT slot_name, catalog_xmin FROM pg_catalog.pg_get_replication_slots()"
                                " WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = ANY(%s)",
                                ([n for n, v in self._unready_logical_slots.items() if v is None] + [slot_name],))
                    slots = {row[0]: row[1] for row in cur}
                    if slot_name not in slots:
                        return logger.warning('Physical slot %s does not exist on the primary', slot_name)
                    catalog_xmin = slots.pop(slot_name)
            except Exception as e:
                return logger.error("Failed to check %s physical slot on the primary: %r", slot_name, e)
            # Remember catalog_xmin of logical slots on the primary when catalog_xmin of
            # the physical slot became valid. Logical slots on replica will be safe to use after
            # promote when catalog_xmin of the physical slot overtakes these values.
            if catalog_xmin:
                for name, value in slots.items():
                    self._unready_logical_slots[name] = value
            else:  # Replica isn't streaming or the hot_standby_feedback isn't enabled
                try:
                    cur = self._query("SELECT pg_catalog.current_setting('hot_standby_feedback')::boolean")
                    if not cur.fetchone()[0]:
                        logger.error('Logical slot failover requires "hot_standby_feedback".'
                                     ' Please check postgresql.auto.conf')
                except Exception as e:
                    logger.error('Failed to check the hot_standby_feedback setting: %r', e)
                return  # since `catalog_xmin` isn't valid further checks don't make any sense

        for name in list(self._unready_logical_slots):
            value = self._replication_slots.get(name)
            # The logical slot on a replica is safe to use when the physical replica slot on the primary:
            # 1. has a nonzero/non-null catalog_xmin
            # 2. has a catalog_xmin that is not newer (greater) than the catalog_xmin of any slot on the standby
            # 3. overtook the catalog_xmin of remembered values of logical slots on the primary.
            if not value or self._unready_logical_slots[name] <= catalog_xmin <= value['catalog_xmin']:
                del self._unready_logical_slots[name]
                if value:
                    logger.info('Logical slot %s is safe to be used after a failover', name)

    def copy_logical_slots(self, cluster, create_slots):
        leader = cluster.leader
        slots = cluster.get_replication_slots(self._postgresql.name, 'replica', False, self._postgresql.major_version)
        with self._get_leader_connection_cursor(leader) as cur:
            try:
                cur.execute("SELECT slot_name, slot_type, datname, plugin, catalog_xmin, "
                            "pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, "
                            "pg_catalog.pg_read_binary_file('pg_replslot/' || slot_name || '/state')"
                            " FROM pg_catalog.pg_get_replication_slots() JOIN pg_catalog.pg_database ON datoid = oid"
                            " WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = ANY(%s)", (create_slots,))

                create_slots = {}
                for r in cur:
                    if r[0] in slots:  # slot_name is defined in the global configuration
                        slot = {'type': r[1], 'database': r[2], 'plugin': r[3],
                                'catalog_xmin': r[4], 'confirmed_flush_lsn': r[5], 'data': r[6]}
                        if compare_slots(slot, slots[r[0]]):
                            create_slots[r[0]] = slot
                        else:
                            logger.warning('Will not copy the logical slot "%s" due to the configuration mismatch: ' +
                                           'configuration=%s, slot on the primary=%s', r[0], slots[r[0]], slot)
            except Exception as e:
                logger.error("Failed to copy logical slots from the %s via postgresql connection: %r", leader.name, e)

        if isinstance(create_slots, dict) and create_slots and self._postgresql.stop():
            for name, value in create_slots.items():
                slot_dir = os.path.join(self._postgresql.slots_handler.pg_replslot_dir, name)
                slot_tmp_dir = slot_dir + '.tmp'
                if os.path.exists(slot_tmp_dir):
                    shutil.rmtree(slot_tmp_dir)
                os.makedirs(slot_tmp_dir)
                fsync_dir(slot_tmp_dir)
                with open(os.path.join(slot_tmp_dir, 'state'), 'wb') as f:
                    f.write(value['data'])
                    f.flush()
                    os.fsync(f.fileno())
                if os.path.exists(slot_dir):
                    shutil.rmtree(slot_dir)
                os.rename(slot_tmp_dir, slot_dir)
                fsync_dir(slot_dir)
                self._unready_logical_slots[name] = None
            fsync_dir(self._postgresql.slots_handler.pg_replslot_dir)
            self._postgresql.start()

    def schedule(self, value=None):
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = self._force_readiness_check = value

    def on_promote(self):
        if self._advance:
            self._advance.on_promote()

        if self._unready_logical_slots:
            logger.warning('Logical replication slots that might be unsafe to use after promote: %s',
                           set(self._unready_logical_slots))
