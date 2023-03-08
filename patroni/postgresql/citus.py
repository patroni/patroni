import logging
import re
import time

from six.moves.urllib_parse import urlparse
from threading import Condition, Event, Thread

from .connection import Connection
from ..dcs import CITUS_COORDINATOR_GROUP_ID
from ..psycopg import connect, quote_ident

CITUS_SLOT_NAME_RE = re.compile(r'^citus_shard_(move|split)_slot(_[1-9][0-9]*){2,3}$')
logger = logging.getLogger(__name__)


class PgDistNode(object):
    """Represents a single row in the `pg_dist_node` table"""

    def __init__(self, group, host, port, event, nodeid=None, timeout=None, cooldown=None):
        self.group = group
        # A weird way of pausing client connections by adding the `-demoted` suffix to the hostname
        self.host = host + ('-demoted' if event == 'before_demote' else '')
        self.port = port
        # Event that is trying to change or changed the given row.
        # Possible values: before_demote, before_promote, after_promote.
        self.event = event
        self.nodeid = nodeid

        # If transaction was started, we need to COMMIT/ROLLBACK before the deadline
        self.timeout = timeout
        self.cooldown = cooldown or 10000  # 10s by default
        self.deadline = 0

        # All changes in the pg_dist_node are serialized on the Patroni
        # side by performing them from a thread. The thread, that is
        # requested a change, sometimes needs to wait for a result.
        # For example, we want to pause client connections before demoting
        # the worker, and once it is done notify the calling thread.
        self._event = Event()

    def wait(self):
        self._event.wait()

    def wakeup(self):
        self._event.set()

    def __eq__(self, other):
        return isinstance(other, PgDistNode) and self.event == other.event\
            and self.host == other.host and self.port == other.port

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return ('PgDistNode(nodeid={0},group={1},host={2},port={3},event={4})'
                .format(self.nodeid, self.group, self.host, self.port, self.event))

    def __repr__(self):
        return str(self)


class CitusHandler(Thread):

    def __init__(self, postgresql, config):
        super(CitusHandler, self).__init__()
        self.daemon = True
        self._postgresql = postgresql
        self._config = config
        self._connection = Connection()
        self._pg_dist_node = {}  # Cache of pg_dist_node: {groupid: PgDistNode()}
        self._tasks = []  # Requests to change pg_dist_node, every task is a `PgDistNode`
        self._condition = Condition()  # protects _pg_dist_node, _tasks, and _schedule_load_pg_dist_node
        self._in_flight = None  # Reference to the `PgDistNode` if there is a transaction in progress changing it
        self.schedule_cache_rebuild()

    def is_enabled(self):
        return isinstance(self._config, dict)

    def group(self):
        return self._config['group']

    def is_coordinator(self):
        return self.is_enabled() and self.group() == CITUS_COORDINATOR_GROUP_ID

    def is_worker(self):
        return self.is_enabled() and not self.is_coordinator()

    def set_conn_kwargs(self, kwargs):
        if self.is_enabled():
            kwargs.update({'dbname': self._config['database'],
                           'options': '-c statement_timeout=0 -c idle_in_transaction_session_timeout=0'})
            self._connection.set_conn_kwargs(kwargs)

    def schedule_cache_rebuild(self):
        with self._condition:
            self._schedule_load_pg_dist_node = True

    def on_demote(self):
        with self._condition:
            self._pg_dist_node.clear()
            self._tasks[:] = []
            self._in_flight = None

    def query(self, sql, *params):
        try:
            logger.debug('query(%s, %s)', sql, params)
            cursor = self._connection.cursor()
            cursor.execute(sql, params or None)
            return cursor
        except Exception as e:
            logger.error('Exception when executing query "%s", (%s): %r', sql, params, e)
            self._connection.close()
            self._in_flight = None
            self.schedule_cache_rebuild()
            raise e

    def load_pg_dist_node(self):
        """Read from the `pg_dist_node` table and put it into the local cache"""

        with self._condition:
            if not self._schedule_load_pg_dist_node:
                return True
            self._schedule_load_pg_dist_node = False

        try:
            cursor = self.query("SELECT nodeid, groupid, nodename, nodeport, noderole"
                                " FROM pg_catalog.pg_dist_node WHERE noderole = 'primary'")
        except Exception:
            return False

        with self._condition:
            self._pg_dist_node = {r[1]: PgDistNode(r[1], r[2], r[3], 'after_promote', r[0]) for r in cursor}
        return True

    def sync_pg_dist_node(self, cluster):
        """Maintain the `pg_dist_node` from the coordinator leader every heartbeat loop.

        We can't always rely on REST API calls from worker nodes in order
        to maintain `pg_dist_node`, therefore at least once per heartbeat
        loop we make sure that workes registered in `self._pg_dist_node`
        cache are matching the cluster view from DCS by creating tasks
        the same way as it is done from the REST API."""

        if not self.is_coordinator():
            return

        with self._condition:
            if not self.is_alive():
                self.start()

        self.add_task('after_promote', CITUS_COORDINATOR_GROUP_ID, self._postgresql.connection_string)

        for group, worker in cluster.workers.items():
            leader = worker.leader
            if leader and leader.conn_url\
                    and leader.data.get('role') in ('master', 'primary') and leader.data.get('state') == 'running':
                self.add_task('after_promote', group, leader.conn_url)

    def find_task_by_group(self, group):
        for i, task in enumerate(self._tasks):
            if task.group == group:
                return i

    def pick_task(self):
        """Returns the tuple(i, task), where `i` - is the task index in the self._tasks list

        Tasks are picked by following priorities:
        1. If there is already a transaction in progress, pick a task
           that that will change already affected worker primary.
        2. If the coordinator address should be changed - pick a task
           with group=0 (coordinators are always in group 0).
        3. Pick a task that is the oldest (first from the self._tasks)"""

        with self._condition:
            if self._in_flight:
                i = self.find_task_by_group(self._in_flight.group)
            else:
                while True:
                    i = self.find_task_by_group(CITUS_COORDINATOR_GROUP_ID)  # set_coordinator
                    if i is None and self._tasks:
                        i = 0
                    if i is None:
                        break
                    task = self._tasks[i]
                    if task == self._pg_dist_node.get(task.group):
                        self._tasks.pop(i)  # nothing to do because cached version of pg_dist_node already matches
                    else:
                        break
            task = self._tasks[i] if i is not None else None

            # When tasks are added it could happen that self._pg_dist_node
            # wasn't ready (self._schedule_load_pg_dist_node is False)
            # and hence the nodeid wasn't filled.
            if task and task.group in self._pg_dist_node:
                task.nodeid = self._pg_dist_node[task.group].nodeid
            return i, task

    def update_node(self, task):
        if task.group == CITUS_COORDINATOR_GROUP_ID:
            return self.query("SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default')",
                              task.host, task.port)

        if task.nodeid is None and task.event != 'before_demote':
            task.nodeid = self.query("SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default')",
                                     task.host, task.port, task.group).fetchone()[0]
        elif task.nodeid is not None:
            # XXX: statement_timeout?
            self.query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, %s)',
                       task.nodeid, task.host, task.port, task.cooldown)

    def process_task(self, task):
        """Updates a single row in `pg_dist_node` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node
        or before promoting the other worker if there is not transaction
        in progress. And, the transaction it is committed when the
        switchover/failover completed.

        This method returns `True` if node was updated (optionally,
        transaction was committed) as an indicator that
        the `self._pg_dist_node` cache should be updated.

        The maximum lifetime of the transaction in progress
        is controlled outside of this method."""

        if task.event == 'after_promote':
            # The after_promote may happen without previous before_demote and/or
            # before_promore.  In this case we just call self.update_node() method.
            # If there is a transaction in progress, it could be that it already did
            # required changes and we can simply COMMIT.
            if not self._in_flight or self._in_flight.host != task.host or self._in_flight.port != task.port:
                self.update_node(task)
            if self._in_flight:
                self.query('COMMIT')
                self._in_flight = None
            return True
        else:  # before_demote, before_promote
            if task.timeout:
                task.deadline = time.time() + task.timeout
            if not self._in_flight:
                self.query('BEGIN')
            self.update_node(task)
            self._in_flight = task
        return False

    def process_tasks(self):
        while True:
            if not self._in_flight and not self.load_pg_dist_node():
                break

            i, task = self.pick_task()
            if not task:
                break
            try:
                update_cache = self.process_task(task)
            except Exception as e:
                logger.error('Exception when working with pg_dist_node: %r', e)
                update_cache = False
            with self._condition:
                if self._tasks:
                    if update_cache:
                        self._pg_dist_node[task.group] = task
                    if id(self._tasks[i]) == id(task):
                        self._tasks.pop(i)
            task.wakeup()

    def run(self):
        while True:
            try:
                with self._condition:
                    if self._schedule_load_pg_dist_node:
                        timeout = -1
                    elif self._in_flight:
                        timeout = self._in_flight.deadline - time.time() if self._tasks else None
                    else:
                        timeout = -1 if self._tasks else None

                    if timeout is None or timeout > 0:
                        self._condition.wait(timeout)
                    elif self._in_flight:
                        logger.warning('Rolling back transaction. Last known status: %s', self._in_flight)
                        self.query('ROLLBACK')
                        self._in_flight = None
                self.process_tasks()
            except Exception:
                logger.exception('run')

    def _add_task(self, task):
        with self._condition:
            i = self.find_task_by_group(task.group)

            # task.timeout is None is an indicator that it was scheduled
            # from the sync_pg_dist_node() and we don't want to override
            # already existing task created from REST API.
            if task.timeout is None and (i is not None or self._in_flight and self._in_flight.group == task.group):
                return False

            # Override already existing task for the same worker group
            if i is not None:
                if task != self._tasks[i]:
                    logger.debug('Overriding existing task: %s != %s', self._tasks[i], task)
                    self._tasks[i] = task
                    self._condition.notify()
                    return True
            # Add the task to the list if Worker node state is different from the cached `pg_dist_node`
            elif self._schedule_load_pg_dist_node or task != self._pg_dist_node.get(task.group)\
                    or self._in_flight and task.group == self._in_flight.group:
                logger.debug('Adding the new task: %s', task)
                self._tasks.append(task)
                self._condition.notify()
                return True
        return False

    def add_task(self, event, group, conn_url, timeout=None, cooldown=None):
        try:
            r = urlparse(conn_url)
        except Exception as e:
            return logger.error('Failed to parse connection url %s: %r', conn_url, e)
        host = r.hostname
        port = r.port or 5432
        task = PgDistNode(group, host, port, event, timeout=timeout, cooldown=cooldown)
        return task if self._add_task(task) else None

    def handle_event(self, cluster, event):
        if not self.is_alive():
            return

        cluster = cluster.workers.get(event['group'])
        if not (cluster and cluster.leader and cluster.leader.name == event['leader'] and cluster.leader.conn_url):
            return

        task = self.add_task(event['type'], event['group'],
                             cluster.leader.conn_url,
                             event['timeout'], event['cooldown']*1000)
        if task and event['type'] == 'before_demote':
            task.wait()

    def bootstrap(self):
        if not self.is_enabled():
            return

        conn_kwargs = self._postgresql.config.local_connect_kwargs
        conn_kwargs['options'] = '-c synchronous_commit=local -c statement_timeout=0'
        if self._config['database'] != self._postgresql.database:
            conn = connect(**conn_kwargs)
            try:
                with conn.cursor() as cur:
                    cur.execute('CREATE DATABASE {0}'.format(quote_ident(self._config['database'], conn)))
            finally:
                conn.close()

        conn_kwargs['dbname'] = self._config['database']
        conn = connect(**conn_kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute('CREATE EXTENSION citus')

                superuser = self._postgresql.config.superuser
                params = {k: superuser[k] for k in ('password', 'sslcert', 'sslkey') if k in superuser}
                if params:
                    cur.execute("INSERT INTO pg_catalog.pg_dist_authinfo VALUES"
                                "(0, pg_catalog.current_user(), %s)",
                                (self._postgresql.config.format_dsn(params),))
        finally:
            conn.close()

    def adjust_postgres_gucs(self, parameters):
        if not self.is_enabled():
            return

        # citus extension must be on the first place in shared_preload_libraries
        shared_preload_libraries = list(filter(
            lambda el: el and el != 'citus',
            [p.strip() for p in parameters.get('shared_preload_libraries', '').split(',')]))
        parameters['shared_preload_libraries'] = ','.join(['citus'] + shared_preload_libraries)

        # if not explicitly set Citus overrides max_prepared_transactions to max_connections*2
        if parameters.get('max_prepared_transactions') == 0:
            parameters['max_prepared_transactions'] = parameters['max_connections'] * 2

        # Resharding in Citus implemented using logical replication
        parameters['wal_level'] = 'logical'

    def ignore_replication_slot(self, slot):
        if self.is_enabled() and self._postgresql.is_leader() and\
                slot['type'] == 'logical' and slot['database'] == self._config['database']:
            m = CITUS_SLOT_NAME_RE.match(slot['name'])
            return m and {'move': 'pgoutput', 'split': 'citus'}.get(m.group(1)) == slot['plugin']
        return False
