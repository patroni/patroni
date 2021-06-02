import errno
import logging
import os
import shutil

from collections import defaultdict
from contextlib import contextmanager
from psycopg2.errors import UndefinedFile

from .connection import get_connection_cursor
from .misc import format_lsn

logger = logging.getLogger(__name__)


def compare_slots(s1, s2, dbid='database'):
    return s1['type'] == s2['type'] and (s1['type'] == 'physical' or
                                         s1.get(dbid) == s2.get(dbid) and s1['plugin'] == s2['plugin'])


def fsync_dir(path):
    if os.name != 'nt':
        fd = os.open(path, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        except OSError as e:
            # Some filesystems don't like fsyncing directories and raise EINVAL. Ignoring it is usually safe.
            if e.errno != errno.EINVAL:
                raise
        finally:
            os.close(fd)


class SlotsHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._replication_slots = {}  # already existing replication slots
        self._unready_logical_slots = set()
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
            cursor = self._query('SELECT slot_name, slot_type, plugin, database, datoid'
                                 '{0} FROM pg_catalog.pg_replication_slots'.format(extra))
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
                self._unready_logical_slots = set(n for n, v in replication_slots.items() if v['type'] == 'logical')
                self._force_readiness_check = False

    def ignore_replication_slot(self, cluster, name):
        slot = self._replication_slots[name]
        for matcher in cluster.config.ignore_slots_matchers:
            if ((matcher.get("name") is None or matcher["name"] == name)
               and all(not matcher.get(a) or matcher[a] == slot.get(a) for a in ('database', 'plugin', 'type'))):
                return True
        return False

    def drop_replication_slot(self, name):
        cursor = self._query(('SELECT pg_catalog.pg_drop_replication_slot(%s) WHERE EXISTS (SELECT 1 ' +
                              'FROM pg_catalog.pg_replication_slots WHERE slot_name = %s AND NOT active)'), name, name)
        # In normal situation rowcount should be 1, otherwise either slot doesn't exists or it is still active
        return cursor.rowcount == 1

    def _drop_incorrect_slots(self, cluster, slots):
        # drop old replication slots which are not presented in desired slots
        for name in set(self._replication_slots) - set(slots):
            if not self.ignore_replication_slot(cluster, name) and not self.drop_replication_slot(name):
                logger.error("Failed to drop replication slot '%s'", name)
                self._schedule_load_slots = True

        for name, value in slots.items():
            if name in self._replication_slots and not compare_slots(value, self._replication_slots[name]):
                logger.info("Trying to drop replication slot '%s' because value is changing from %s to %s",
                            name, self._replication_slots[name], value)
                if self.drop_replication_slot(name):
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
    def _get_local_connection_cursor(self, database):
        conn_kwargs = self._postgresql.config.local_connect_kwargs
        conn_kwargs['database'] = database
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
            with self._get_local_connection_cursor(database) as cur:
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
                                advance_slots[value['database']][name] = value
                        except Exception as e:
                            logger.error('Failed to parse "%s": %r', cluster.slots[name], e)
                elif name in cluster.slots:  # We want to copy only slots with feedback in a DCS
                    create_slots.append(name)

        # Advance logical slots
        for database, values in advance_slots.items():
            with self._get_local_connection_cursor(database) as cur:
                for name, value in values.items():
                    try:
                        cur.execute("SELECT pg_catalog.pg_replication_slot_advance(%s, %s)",
                                    (name, format_lsn(int(cluster.slots[name]))))
                    except Exception as e:
                        logger.error("Failed to advance logical replication slot '%s': %r", name, e)
                        if isinstance(e, UndefinedFile):
                            create_slots.append(name)
                        self._schedule_load_slots = True
        return create_slots

    def sync_replication_slots(self, cluster, nofailover, replicatefrom=None):
        ret = None
        if self._postgresql.major_version >= 90400 and cluster.config:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql.name, self._postgresql.role,
                                                      nofailover, self._postgresql.major_version, True)

                self._drop_incorrect_slots(cluster, slots)

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
        conn_kwargs['database'] = self._postgresql.database
        with get_connection_cursor(connect_timeout=3, options="-c statement_timeout=2000", **conn_kwargs) as cur:
            yield cur

    def check_logical_slots_readiness(self, cluster, nofailover, replicatefrom):
        if self._unready_logical_slots:
            slot_name = cluster.get_my_slot_name_on_primary(self._postgresql.name, replicatefrom)
            try:
                with self._get_leader_connection_cursor(cluster.leader) as cur:
                    cur.execute("SELECT catalog_xmin FROM pg_catalog.pg_get_replication_slots()"
                                " WHERE NOT pg_catalog.pg_is_in_recovery() AND slot_name = %s", (slot_name,))
                    if cur.rowcount < 1:
                        return logger.warning('Physical slot %s does not exist on the primary', slot_name)
                    catalog_xmin = cur.fetchone()[0]
            except Exception as e:
                return logger.error("Failed to check %s physical slot on the primary: %r", slot_name, e)
        for name in list(self._unready_logical_slots):
            value = self._replication_slots.get(name)
            if not value or catalog_xmin <= value['catalog_xmin']:
                self._unready_logical_slots.remove(name)
                if value:
                    logger.info('Logical slot %s is safe to be used after a failover', name)

    def copy_logical_slots(self, leader, slots):
        with self._get_leader_connection_cursor(leader) as cur:
            try:
                cur.execute("SELECT slot_name, catalog_xmin, "
                            "pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint, "
                            "pg_catalog.pg_read_binary_file('pg_replslot/' || slot_name || '/state')"
                            " FROM pg_catalog.pg_get_replication_slots() WHERE NOT pg_catalog.pg_is_in_recovery()"
                            " AND slot_name = ANY(%s)", (slots,))
                slots = {r[0]: {'catalog_xmin': r[1], 'confirmed_flush_lsn': r[2], 'data': r[3]} for r in cur}
            except Exception as e:
                logger.error("Failed to copy logical slots from the %s via postgresql connection: %r", leader.name, e)

        if isinstance(slots, dict) and self._postgresql.stop():
            pg_replslot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot')
            for name, value in slots.items():
                slot_dir = os.path.join(pg_replslot_dir, name)
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
                self._unready_logical_slots.add(name)
            fsync_dir(pg_replslot_dir)
            self._postgresql.start()

    def schedule(self, value=None):
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = self._force_readiness_check = value

    def on_promote(self):
        if self._unready_logical_slots:
            logger.warning('Logical replication slots that might be unsafe to use after promote: %s',
                           self._unready_logical_slots)
