import logging
import re
import time

from threading import Condition, Event, Thread
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Union, Tuple, TYPE_CHECKING

from . import AbstractMPP, AbstractMPPHandler
from ...dcs import Cluster
from ...psycopg import connect, quote_ident, ProgrammingError
from ...utils import parse_int

if TYPE_CHECKING:  # pragma: no cover
    from .. import Postgresql

CITUS_COORDINATOR_GROUP_ID = 0
CITUS_SLOT_NAME_RE = re.compile(r'^citus_shard_(move|split)_slot(_[1-9][0-9]*){2,3}$')
logger = logging.getLogger(__name__)


class PgDistNode(object):
    """Represents a single row in the `pg_dist_node` table"""

    def __init__(self, group: int, host: str, port: int, event: str, nodeid: Optional[int] = None,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> None:
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
        self.deadline: float = 0

        # All changes in the pg_dist_node are serialized on the Patroni
        # side by performing them from a thread. The thread, that is
        # requested a change, sometimes needs to wait for a result.
        # For example, we want to pause client connections before demoting
        # the worker, and once it is done notify the calling thread.
        self._event = Event()

    def wait(self) -> None:
        self._event.wait()

    def wakeup(self) -> None:
        self._event.set()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PgDistNode) and self.event == other.event\
            and self.host == other.host and self.port == other.port

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __str__(self) -> str:
        return ('PgDistNode(nodeid={0},group={1},host={2},port={3},event={4})'
                .format(self.nodeid, self.group, self.host, self.port, self.event))

    def __repr__(self) -> str:
        return str(self)


class Citus(AbstractMPP):

    group_re = re.compile('^(0|[1-9][0-9]*)$')

    @staticmethod
    def validate_config(config: Union[Any, Dict[str, Union[str, int]]]) -> bool:
        """Check whether provided config is good for a given MPP.

        :param config: configuration of ``citus`` MPP section.

        :returns: ``True`` is config passes validation, otherwise ``False``.
        """
        return isinstance(config, dict) \
            and isinstance(config.get('database'), str) \
            and parse_int(config.get('group')) is not None

    @property
    def group(self) -> int:
        """The group of this Citus node."""
        return int(self._config['group'])

    @property
    def coordinator_group_id(self) -> int:
        """The group id of the Citus coordinator PostgreSQL cluster."""
        return CITUS_COORDINATOR_GROUP_ID


class CitusHandler(Citus, AbstractMPPHandler, Thread):
    """Define the interfaces for handling an underlying Citus cluster."""

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        """"Initialize a new instance of :class:`CitusHandler`.

        :param postgresql: the Postgres node.
        :param config: the ``citus`` MPP config section.
        """
        Thread.__init__(self)
        AbstractMPPHandler.__init__(self, postgresql, config)
        self.daemon = True
        if config:
            self._connection = postgresql.connection_pool.get(
                'citus', {'dbname': config['database'],
                          'options': '-c statement_timeout=0 -c idle_in_transaction_session_timeout=0'})
        self._pg_dist_node: Dict[int, PgDistNode] = {}  # Cache of pg_dist_node: {groupid: PgDistNode()}
        self._tasks: List[PgDistNode] = []  # Requests to change pg_dist_node, every task is a `PgDistNode`
        self._in_flight: Optional[PgDistNode] = None  # Reference to the `PgDistNode` being changed in a transaction
        self._schedule_load_pg_dist_node = True  # Flag that "pg_dist_node" should be queried from the database
        self._condition = Condition()  # protects _pg_dist_node, _tasks, _in_flight, and _schedule_load_pg_dist_node
        self.schedule_cache_rebuild()

    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """
        with self._condition:
            self._schedule_load_pg_dist_node = True

    def on_demote(self) -> None:
        with self._condition:
            self._pg_dist_node.clear()
            empty_tasks: List[PgDistNode] = []
            self._tasks[:] = empty_tasks
            self._in_flight = None

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        try:
            logger.debug('query(%s, %s)', sql, params)
            return self._connection.query(sql, *params)
        except Exception as e:
            logger.error('Exception when executing query "%s", (%s): %r', sql, params, e)
            self._connection.close()
            with self._condition:
                self._in_flight = None
            self.schedule_cache_rebuild()
            raise e

    def load_pg_dist_node(self) -> bool:
        """Read from the `pg_dist_node` table and put it into the local cache"""

        with self._condition:
            if not self._schedule_load_pg_dist_node:
                return True
            self._schedule_load_pg_dist_node = False

        try:
            rows = self.query("SELECT nodeid, groupid, nodename, nodeport, noderole"
                              " FROM pg_catalog.pg_dist_node WHERE noderole = 'primary'")
        except Exception:
            return False

        with self._condition:
            self._pg_dist_node = {r[1]: PgDistNode(r[1], r[2], r[3], 'after_promote', r[0]) for r in rows}
        return True

    def sync_meta_data(self, cluster: Cluster) -> None:
        """Maintain the ``pg_dist_node`` from the coordinator leader every heartbeat loop.

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

    def find_task_by_group(self, group: int) -> Optional[int]:
        for i, task in enumerate(self._tasks):
            if task.group == group:
                return i

    def pick_task(self) -> Tuple[Optional[int], Optional[PgDistNode]]:
        """Returns the tuple(i, task), where `i` - is the task index in the self._tasks list

        Tasks are picked by following priorities:

        1. If there is already a transaction in progress, pick a task
           that that will change already affected worker primary.
        2. If the coordinator address should be changed - pick a task
           with group=0 (coordinators are always in group 0).
        3. Pick a task that is the oldest (first from the self._tasks)
        """

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

    def update_node(self, task: PgDistNode) -> None:
        if task.nodeid is not None:
            self.query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, %s)',
                       task.nodeid, task.host, task.port, task.cooldown)
        elif task.event != 'before_demote':
            task.nodeid = self.query("SELECT pg_catalog.citus_add_node(%s, %s, %s, 'primary', 'default')",
                                     task.host, task.port, task.group)[0][0]

    def process_task(self, task: PgDistNode) -> bool:
        """Updates a single row in `pg_dist_node` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node or before promoting the other worker if
        there is no transaction in progress. And, the transaction is committed when the switchover/failover completed.

        .. note:
            The maximum lifetime of the transaction in progress is controlled outside of this method.

        .. note:
            Read access to `self._in_flight` isn't protected because we know it can't be changed outside of our thread.

        :param task: reference to a :class:`PgDistNode` object that represents a row to be updated/created.
        :returns: `True` if the row was succesfully created/updated or transaction in progress
            was committed as an indicator that the `self._pg_dist_node` cache should be updated,
            or, if the new transaction was opened, this method returns `False`.
        """

        if task.event == 'after_promote':
            # The after_promote may happen without previous before_demote and/or
            # before_promore.  In this case we just call self.update_node() method.
            # If there is a transaction in progress, it could be that it already did
            # required changes and we can simply COMMIT.
            if not self._in_flight or self._in_flight.host != task.host or self._in_flight.port != task.port:
                self.update_node(task)
            if self._in_flight:
                self.query('COMMIT')
            return True
        else:  # before_demote, before_promote
            if task.timeout:
                task.deadline = time.time() + task.timeout
            if not self._in_flight:
                self.query('BEGIN')
            self.update_node(task)
        return False

    def process_tasks(self) -> None:
        while True:
            # Read access to `_in_flight` isn't protected because we know it can't be changed outside of our thread.
            if not self._in_flight and not self.load_pg_dist_node():
                break

            i, task = self.pick_task()
            if not task or i is None:
                break
            try:
                update_cache = self.process_task(task)
            except Exception as e:
                logger.error('Exception when working with pg_dist_node: %r', e)
                update_cache = None
            with self._condition:
                if self._tasks:
                    if update_cache:
                        self._pg_dist_node[task.group] = task

                    if update_cache is False:  # an indicator that process_tasks has started a transaction
                        self._in_flight = task
                    else:
                        self._in_flight = None

                    if id(self._tasks[i]) == id(task):
                        self._tasks.pop(i)
            task.wakeup()

    def run(self) -> None:
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

    def _add_task(self, task: PgDistNode) -> bool:
        with self._condition:
            i = self.find_task_by_group(task.group)

            # The `PgDistNode.timeout` == None is an indicator that it was scheduled from the sync_meta_data().
            if task.timeout is None:
                # We don't want to override the already existing task created from REST API.
                if i is not None and self._tasks[i].timeout is not None:
                    return False

                # There is a little race condition with tasks created from REST API - the call made "before" the member
                # key is updated in DCS. Therefore it is possible that :func:`sync_meta_data` will try to create a task
                # based on the outdated values of "state"/"role". To solve it we introduce an artificial timeout.
                # Only when the timeout is reached new tasks could be scheduled from sync_meta_data()
                if self._in_flight and self._in_flight.group == task.group and self._in_flight.timeout is not None\
                        and self._in_flight.deadline > time.time():
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

    def add_task(self, event: str, group: int, conn_url: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> Optional[PgDistNode]:
        try:
            r = urlparse(conn_url)
        except Exception as e:
            return logger.error('Failed to parse connection url %s: %r', conn_url, e)
        host = r.hostname
        if host:
            port = r.port or 5432
            task = PgDistNode(group, host, port, event, timeout=timeout, cooldown=cooldown)
            return task if self._add_task(task) else None

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        if not self.is_alive():
            return

        worker = cluster.workers.get(event['group'])
        if not (worker and worker.leader and worker.leader.name == event['leader'] and worker.leader.conn_url):
            return

        task = self.add_task(event['type'], event['group'],
                             worker.leader.conn_url,
                             event['timeout'], event['cooldown'] * 1000)
        if task and event['type'] == 'before_demote':
            task.wait()

    def bootstrap(self) -> None:
        """Bootstrap handler.

        Is called when the new cluster is initialized (through ``initdb`` or a custom bootstrap method).
        """
        conn_kwargs = {**self._postgresql.connection_pool.conn_kwargs,
                       'options': '-c synchronous_commit=local -c statement_timeout=0'}
        if self._config['database'] != self._postgresql.database:
            conn = connect(**conn_kwargs)
            try:
                with conn.cursor() as cur:
                    cur.execute('CREATE DATABASE {0}'.format(
                        quote_ident(self._config['database'], conn)).encode('utf-8'))
            except ProgrammingError as exc:
                if exc.diag.sqlstate == '42P04':  # DuplicateDatabase
                    logger.debug('Exception when creating database: %r', exc)
                else:
                    raise exc
            finally:
                conn.close()

        conn_kwargs['dbname'] = self._config['database']
        conn = connect(**conn_kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute('CREATE EXTENSION IF NOT EXISTS citus')

                superuser = self._postgresql.config.superuser
                params = {k: superuser[k] for k in ('password', 'sslcert', 'sslkey') if k in superuser}
                if params:
                    cur.execute("INSERT INTO pg_catalog.pg_dist_authinfo VALUES"
                                "(0, pg_catalog.current_user(), %s)",
                                (self._postgresql.config.format_dsn(params),))

                if self.is_coordinator():
                    r = urlparse(self._postgresql.connection_string)
                    cur.execute("SELECT pg_catalog.citus_set_coordinator_host(%s, %s, 'primary', 'default')",
                                (r.hostname, r.port or 5432))
        finally:
            conn.close()

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        :param parameters: dictionary of GUCs, with key as GUC name and the corresponding value as current GUC value.
        """
        # citus extension must be on the first place in shared_preload_libraries
        shared_preload_libraries = list(filter(
            lambda el: el and el != 'citus',
            [p.strip() for p in parameters.get('shared_preload_libraries', '').split(',')]))
        parameters['shared_preload_libraries'] = ','.join(['citus'] + shared_preload_libraries)

        # if not explicitly set Citus overrides max_prepared_transactions to max_connections*2
        if parameters['max_prepared_transactions'] == 0:
            parameters['max_prepared_transactions'] = parameters['max_connections'] * 2

        # Resharding in Citus implemented using logical replication
        parameters['wal_level'] = 'logical'

        # Sometimes Citus needs to connect to the local postgres. We will do it the same way as Patroni does.
        parameters['citus.local_hostname'] = self._postgresql.connection_pool.conn_kwargs.get('host', 'localhost')

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """Check whether provided replication *slot* existing in the database should not be removed.

        .. note::
            MPP database may create replication slots for its own use, for example to migrate data between workers
            using logical replication, and we don't want to suddenly drop them.

        :param slot: dictionary containing the replication slot settings, like ``name``, ``database``, ``type``, and
                     ``plugin``.

        :returns: ``True`` if the replication slots should not be removed, otherwise ``False``.
        """
        if self._postgresql.is_primary() and slot['type'] == 'logical' and slot['database'] == self._config['database']:
            m = CITUS_SLOT_NAME_RE.match(slot['name'])
            return bool(m and {'move': 'pgoutput', 'split': 'citus'}.get(m.group(1)) == slot['plugin'])
        return False
