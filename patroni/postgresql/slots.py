import logging

from patroni.postgresql.connection import get_connection_cursor
from collections import defaultdict

logger = logging.getLogger(__name__)


def compare_slots(s1, s2):
    return s1['type'] == s2['type'] and (s1['type'] == 'physical' or
                                         s1['database'] == s2['database'] and s1['plugin'] == s2['plugin'])


class SlotsHandler(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._replication_slots = {}  # already existing replication slots
        self.schedule()

    def _query(self, sql, *params):
        return self._postgresql.query(sql, *params, retry=False)

    def load_replication_slots(self):
        if self._postgresql.major_version >= 90400 and self._schedule_load_slots:
            replication_slots = {}
            cursor = self._query('SELECT slot_name, slot_type, plugin, database FROM pg_catalog.pg_replication_slots')
            for r in cursor:
                value = {'type': r[1]}
                if r[1] == 'logical':
                    value.update({'plugin': r[2], 'database': r[3]})
                replication_slots[r[0]] = value
            self._replication_slots = replication_slots
            self._schedule_load_slots = False

    def drop_replication_slot(self, name):
        cursor = self._query(('SELECT pg_catalog.pg_drop_replication_slot(%s) WHERE EXISTS (SELECT 1 ' +
                              'FROM pg_catalog.pg_replication_slots WHERE slot_name = %s AND NOT active)'), name, name)
        # In normal situation rowcount should be 1, otherwise either slot doesn't exists or it is still active
        return cursor.rowcount == 1

    def sync_replication_slots(self, cluster):
        if self._postgresql.major_version >= 90400:
            try:
                self.load_replication_slots()

                slots = cluster.get_replication_slots(self._postgresql.name, self._postgresql.role)

                # drop old replication slots which are not presented in desired slots
                for name in set(self._replication_slots) - set(slots):
                    if not self.drop_replication_slot(name):
                        logger.error("Failed to drop replication slot '%s'", name)
                        self._schedule_load_slots = True

                immediately_reserve = ', true' if self._postgresql.major_version >= 90600 else ''

                logical_slots = defaultdict(dict)
                for name, value in slots.items():
                    if name in self._replication_slots and not compare_slots(value, self._replication_slots[name]):
                        logger.info("Trying to drop replication slot '%s' because value is changing from %s to %s",
                                    name, self._replication_slots[name], value)
                        if not self.drop_replication_slot(name):
                            logger.error("Failed to drop replication slot '%s'", name)
                            self._schedule_load_slots = True
                            continue
                        self._replication_slots.pop(name)
                    if name not in self._replication_slots:
                        if value['type'] == 'physical':
                            try:
                                self._query(("SELECT pg_catalog.pg_create_physical_replication_slot(%s{0})" +
                                             " WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots" +
                                             " WHERE slot_type = 'physical' AND slot_name = %s)").format(
                                                 immediately_reserve), name, name)
                            except Exception:
                                logger.exception("Failed to create physical replication slot '%s'", name)
                                self._schedule_load_slots = True
                        elif value['type'] == 'logical' and name not in self._replication_slots:
                            logical_slots[value['database']][name] = value

                # create new logical slots
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
                            except Exception:
                                logger.exception("Failed to create logical replication slot '%s' plugin='%s'",
                                                 name, value['plugin'])
                                self._schedule_load_slots = True
                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True

    def schedule(self, value=None):
        if value is None:
            value = self._postgresql.major_version >= 90400
        self._schedule_load_slots = value
