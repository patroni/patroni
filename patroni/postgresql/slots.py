import logging
import os

from collections import defaultdict

from .connection import get_connection_cursor
from .misc import format_lsn

logger = logging.getLogger(__name__)


def compare_slots(s1, s2, dbid='database'):
    return s1['type'] == s2['type'] and (s1['type'] == 'physical' or
                                         s1.get(dbid) == s2.get(dbid) and s1['plugin'] == s2['plugin'])


class SlotsHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._replication_slots = {}  # already existing replication slots
        self.schedule()

    def _query(self, sql, *params):
        return self._postgresql.query(sql, *params, retry=False)

    def process_permanent_slots(self, slots):
        """
        We want to expose information only about permanent slots that are configured in DCS.
        This function performs such filtering and in addition to that it checks for
        discrepancies and might schedule the resync.
        """

        ret = {}

        slots = {slot['slot_name']: slot for slot in slots or []}
#        logger.error('%s %s', slots, self._replication_slots)
        if slots:
            for name, value in slots.items():
                if name in self._replication_slots:
                    if compare_slots(value, self._replication_slots[name], 'datoid'):
                        if value['type'] == 'logical':
                            ret[name] = value['confirmed_flush_lsn']
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

    def _ensure_logical_slots_primary(self, slots):
        # Group logical slots to be created by database name
        logical_slots = defaultdict(dict)
        for name, value in slots.items():
            if value['type'] == 'logical':
                # If the logical already exists, copy some information about it into the original structure
                if self._replication_slots.get(name, {}).get('datoid'):
                    value['datoid'] = self._replication_slots[name]['datoid']
                    value['catalog_xmin'] = self._replication_slots[name]['catalog_xmin']
                    value['confirmed_flush_lsn'] = self._replication_slots[name]['confirmed_flush_lsn']
                else:
                    logical_slots[value['database']][name] = value

        # Create new logical slots
        for database, values in logical_slots.items():
            conn_kwargs = self._postgresql.config.local_connect_kwargs
            conn_kwargs['database'] = database
            with get_connection_cursor(**conn_kwargs) as cur:
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
                    value['datoid'] = self._replication_slots[name]['datoid']
                    value['catalog_xmin'] = self._replication_slots[name]['catalog_xmin']
                    value['confirmed_flush_lsn'] = self._replication_slots[name]['confirmed_flush_lsn']
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
            conn_kwargs = self._postgresql.config.local_connect_kwargs
            conn_kwargs['database'] = database
            with get_connection_cursor(**conn_kwargs) as cur:
                for name, value in values.items():
                    try:
                        cur.execute("SELECT catalog_xmin, pg_catalog.pg_wal_lsn_diff("
                                    "(pg_catalog.pg_replication_slot_advance(slot_name, %s)).end_lsn, '0/0')"
                                    " FROM pg_get_replication_slots() WHERE slot_name = %s",
                                    (format_lsn(int(cluster.slots[name])), name))
                        catalog_xmin, end_lsn = cur.fetchone()
                        slots[name].update(confirmed_flush_lsn=end_lsn, catalog_xmin=catalog_xmin)
                    except Exception as e:
                        logger.exception("Failed to advance logical replication slot '%s': %r", name, e)
                        self._schedule_load_slots = True
        return create_slots

    def sync_replication_slots(self, cluster, nofailover=None):
        ret = None
        if self._postgresql.major_version >= 90400 and cluster.config:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql.name, self._postgresql.role,
                                                      nofailover, self._postgresql.major_version, True)

                self._drop_incorrect_slots(cluster, slots)

                self._ensure_physical_slots(slots)

                if self._postgresql.is_leader():
                    self._ensure_logical_slots_primary(slots)
                elif cluster.slots and slots:
                    ret = self._ensure_logical_slots_replica(cluster, slots)

                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True
        return ret

    def copy_logical_slots(self, leader, slots):
        conn_kwargs = leader.conn_kwargs(self._postgresql.config.rewind_credentials)
        conn_kwargs['database'] = self._postgresql.database
        with get_connection_cursor(connect_timeout=3, options="-c statement_timeout=2000", **conn_kwargs) as cur:
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
            for name, value in slots.items():
                slot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot', name)
                os.makedirs(slot_dir)
                with open(os.path.join(slot_dir, 'state'), 'wb') as f:
                    f.write(value['data'])
            self._postgresql.start()

    def schedule(self, value=None):
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = value
