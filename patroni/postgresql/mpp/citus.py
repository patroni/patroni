import logging
import re
import time

from threading import Condition, Event, Thread
from typing import Any, cast, Collection, Dict, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING, Union
from urllib.parse import urlparse

from ...dcs import Cluster
from ...psycopg import connect, ProgrammingError, quote_ident
from ...utils import parse_int
from ..misc import PostgresqlRole, PostgresqlState
from . import AbstractMPP, AbstractMPPHandler

if TYPE_CHECKING:  # pragma: no cover
    from .. import Postgresql

CITUS_COORDINATOR_GROUP_ID = 0
CITUS_SLOT_NAME_RE = re.compile(r'^citus_shard_(move|split)_slot(_[1-9][0-9]*){2,3}$')
logger = logging.getLogger(__name__)


class PgDistNode:
    """Represents a single row in "pg_dist_node" table.

    .. note::

        Unlike "noderole" possible values of ``role`` are ``primary``, ``secondary``, and ``demoted``.
        The last one is used to pause client connections on the coordinator to the worker by
        appending ``-demoted`` suffix to the "nodename". The actual "noderole" in DB remains ``primary``.

    :ivar host: "nodename" value
    :ivar port: "nodeport" value
    :ivar role: "noderole" value
    :ivar nodeid: "nodeid" value
    """

    def __init__(self, host: str, port: int, role: str, nodeid: Optional[int] = None) -> None:
        """Create a :class:`PgDistNode` object based on given arguments.

        :param host: "nodename" of the Citus coordinator or worker.
        :param port: "nodeport" of the Citus coordinator or worker.
        :param role: "noderole" value.
        :param nodeid: id of the row in the "pg_dist_node".
        """
        self.host = host
        self.port = port
        self.role = role
        self.nodeid = nodeid

    def __hash__(self) -> int:
        """Defines a hash function to put :class:`PgDistNode` objects to :class:`PgDistGroup` set-like object.

        .. note::
            We use (:attr:`host`, :attr:`port`) tuple here because it is one of the UNIQUE constraints on the
            "pg_dist_node" table. The :attr:`role` value is irrelevant here because nodes may change their roles.
        """
        return hash((self.host, self.port))

    def __eq__(self, other: Any) -> bool:
        """Defines a comparison function.

        :returns: ``True`` if :attr:`host` and :attr:`port` between two instances are the same.
        """
        return isinstance(other, PgDistNode) and self.host == other.host and self.port == other.port

    def __str__(self) -> str:
        return ('PgDistNode(nodeid={0},host={1},port={2},role={3})'
                .format(self.nodeid, self.host, self.port, self.role))

    def __repr__(self) -> str:
        return str(self)

    def is_primary(self) -> bool:
        """Checks whether this object represents "primary" in a corresponding group.

        :returns: ``True`` if this object represents the ``primary``.
        """
        return self.role in (PostgresqlRole.PRIMARY, PostgresqlRole.DEMOTED)

    def as_tuple(self, include_nodeid: bool = False) -> Tuple[str, int, str, Optional[int]]:
        """Helper method to compare two :class:`PgDistGroup` objects.

        .. note::

            *include_nodeid* is set to ``True`` only in unit-tests.

        :param include_nodeid: whether :attr:`nodeid` should be taken into account when comparison is performed.

        :returns: :class:`tuple` object with :attr:`host`, :attr:`port`, :attr:`role`, and optionally :attr:`nodeid`.
        """
        return self.host, self.port, self.role, (self.nodeid if include_nodeid else None)


class PgDistGroup(Set[PgDistNode]):
    """A :class:`set`-like object that represents a Citus group in "pg_dist_node" table.

    This class implements a set of methods to compare topology and if it is necessary
    to transition from the old to the new topology in a "safe" manner:

        * register new primary/secondaries
        * replace gone secondaries with added secondaries
        * failover and switchover

    Typically there will be at least one :class:`PgDistNode` object registered (``primary``).
    In addition to that there could be one or more ``secondary`` nodes.

    :ivar failover: whether the ``primary`` row should be updated as a result of :func:`transition` method call.
    :ivar groupid: the "groupid" from "pg_dist_node".
    """

    def __init__(self, groupid: int, nodes: Optional[Collection[PgDistNode]] = None) -> None:
        """Creates a :class:`PgDistGroup` object based on given arguments.

        :param groupid: the groupid from "pg_dist_node".
        :param nodes: a collection of :class:`PgDistNode` objects that belong to a *groupid*.
        """
        self.failover = False
        self.groupid = groupid

        if nodes:
            self.update(nodes)

    def equals(self, other: 'PgDistGroup', check_nodeid: bool = False) -> bool:
        """Compares two :class:`PgDistGroup` objects.

        :param other: what we want to compare with.
        :param check_nodeid: whether :attr:`PgDistNode.nodeid` should be compared in addition to
                             :attr:`PgDistNode.host`, :attr:`PgDistNode.port`, and :attr:`PgDistNode.role`.

        :returns: ``True`` if two :class:`PgDistGroup` objects are fully identical.
        """
        return self.groupid == other.groupid\
            and set(v.as_tuple(check_nodeid) for v in self) == set(v.as_tuple(check_nodeid) for v in other)

    def primary(self) -> Optional[PgDistNode]:
        """Finds and returns :class:`PgDistNode` object that represents the "primary".

        :returns: :class:`PgDistNode` object which represents the "primary" or ``None`` if not found.
        """
        return next(iter(v for v in self if v.is_primary()), None)

    def get(self, value: PgDistNode) -> Optional[PgDistNode]:
        """Performs a lookup of the actual value in a set.

        .. note::
            It is necessary because :func:`__hash__` and :func:`__eq__` methods in :class:`PgDistNode` are
            redefined and effectively they check only :attr:`PgDistNode.host` and :attr:`PgDistNode.port` attributes.

        :param value: the key we search for.
        :returns: the actual :class:`PgDistNode` value from this :class:`PgDistGroup` object or ``None`` if not found.
        """
        return next(iter(v for v in self if v == value), None)

    def transition(self, old: 'PgDistGroup') -> Iterator[PgDistNode]:
        """Compares this topology with the old one and yields transitions that transform the old to the new one.

        .. note::
            The actual yielded object is :class:`PgDistNode` that will be passed to
            the :meth:`CitusHandler.update_node` to execute all transitions in a transaction.

            In addition to the yielding transactions this method fills up :attr:`PgDistNode.nodeid`
            attribute for nodes that are presented in the old and in the new topology.

            There are a few simple rules/constraints that are imposed by Citus and must be followed:
            - adding/removing nodes is only possible when metadata is synced to all registered "priorities".

            - the "primary" row in "pg_dist_node" always keeps the nodeid (unless it is
              removed, but it is not supported by Patroni).

            - "nodename", "nodeport" must be unique across all rows in the "pg_dist_node". This means that
              every time we want to change the nodeid of an existing node (i.e. to change it from secondary
              to primary), we should first write some other "nodename"/"nodeport" to the row it's currently in.

            - updating "broken" nodes always works and metadata is synced asynchnonously after the commit.

        Following these rules below is an example of the switchover between node1 (primary, nodeid=4)
        and node2 (secondary, nodeid=5).

        .. code-block:: SQL

            BEGIN;
                SELECT citus_update_node(4, 'node1-demoted', 5432);
                SELECT citus_update_node(5, 'node1', 5432);
                SELECT citus_update_node(4, 'node2', 5432);
            COMMIT;

        :param old: the last known topology registered in "pg_dist_node" for a given :attr:`groupid`.

        :yields: :class:`PgDistNode` objects that must be updated/added/removed in "pg_dist_node".
        """
        self.failover = old.failover

        new_primary = self.primary()
        assert new_primary is not None
        old_primary = old.primary()

        gone_nodes = old - self - {old_primary}
        added_nodes = self - old - {new_primary}

        if not old_primary:
            # We did not have any nodes in the group yet and we're adding one now
            yield new_primary
        elif old_primary == new_primary:
            new_primary.nodeid = old_primary.nodeid
            # Controlled switchover with pausing client connections.
            # Achieved by updating the primary row and putting hostname = '${host}-demoted' in a transaction.
            if old_primary.role != new_primary.role:
                self.failover = True
                yield new_primary
        elif old_primary != new_primary:
            self.failover = True

            new_primary_old_node = old.get(new_primary)
            old_primary_new_node = self.get(old_primary)

            # The new primary was registered as a secondary before failover
            if new_primary_old_node:
                new_node = None
                # Old primary is gone and some new secondaries were added.
                # We can use the row of promoted secondary to add the new secondary.
                if not old_primary_new_node and added_nodes:
                    new_node = added_nodes.pop()
                    new_node.nodeid = new_primary_old_node.nodeid
                    yield new_node

                    # notify _maybe_register_old_primary_as_secondary that the old primary should not be re-registered
                    old_primary.role = 'secondary'
                # In opposite case we need to change the primary record to '${host}-demoted:${port}'
                # before we can put its host:port to the row of promoted secondary.
                elif old_primary.role == 'primary':
                    old_primary.role = 'demoted'
                    yield old_primary

                # The old primary is gone and the promoted secondary row wasn't yet used.
                if not old_primary_new_node and not new_node:
                    # We have to "add" the gone primary to the row of promoted secondary because
                    # nodes could not be removed while the metadata isn't synced.
                    old_primary_new_node = PgDistNode(old_primary.host, old_primary.port, new_primary_old_node.role)
                    self.add(old_primary_new_node)

                # put the old primary instead of promoted secondary
                if old_primary_new_node:
                    old_primary_new_node.nodeid = new_primary_old_node.nodeid
                    yield old_primary_new_node

            # update the primary record with the new information
            new_primary.nodeid = old_primary.nodeid
            yield new_primary

            # The new primary was never registered as a standby and there are secondaries that have gone away. Since
            # nodes can't be removed while metadata isn't synced we have to temporarily "add" the old primary back.
            if not new_primary_old_node and gone_nodes:
                # We were in the middle of controlled switchover while the primary disappeared.
                # If there are any gone nodes that can't be reused for new secondaries we will
                # use one of them to temporarily "add" the old primary back as a secondary.
                if not old_primary_new_node and old_primary.role == 'demoted' and len(gone_nodes) > len(added_nodes):
                    old_primary_new_node = PgDistNode(old_primary.host, old_primary.port, 'secondary')
                    self.add(old_primary_new_node)

                # Use one of the gone secondaries to put host:port of the old primary there.
                if old_primary_new_node:
                    old_primary_new_node.nodeid = gone_nodes.pop().nodeid
                    yield old_primary_new_node

        # Fill nodeid for standbys in the new topology from the old ones
        old_replicas = {v: v for v in old if not v.is_primary()}
        for n in self:
            if not n.is_primary() and not n.nodeid and n in old_replicas:
                n.nodeid = old_replicas[n].nodeid

        # Reuse nodeid's of gone standbys to "add" new standbys
        while gone_nodes and added_nodes:
            a = added_nodes.pop()
            a.nodeid = gone_nodes.pop().nodeid
            yield a

        # Adding or removing nodes operations are executed on primaries in all Citus groups in 2PC.
        # If we know that the primary was updated (self.failover is True) that automatically means that
        # adding/removing nodes calls will fail and the whole transaction will be aborted. Therefore
        # we discard operations that add/remove secondaries if we know that the primary was just updated.
        # The inconsistency will be automatically resolved on the next Patroni heartbeat loop.

        # Remove remaining nodes that are gone, but only in case if metadata is in sync (self.failover is False).
        for g in gone_nodes:
            if not self.failover:
                # Remove the node if we expect metadata to be in sync
                yield PgDistNode(g.host, g.port, '')
            else:
                # Otherwise add these nodes to the new topology
                self.add(g)

        # Add new nodes to the metadata, but only in case if metadata is in sync (self.failover is False).
        for a in added_nodes:
            if not self.failover:
                # Add the node if we expect metadata to be in sync
                yield a
            else:
                # Otherwise remove them from the new topology
                self.discard(a)


class PgDistTask(PgDistGroup):
    """A "task" that represents the current or desired state of "pg_dist_node" for a provided *groupid*.

    :ivar group: the "groupid" in "pg_dist_node".
    :ivar event: an "event" that resulted in creating this task.
                 possible values: "before_demote", "before_promote", "after_promote".
    :ivar timeout: a transaction timeout if the task resulted in starting a transaction.
    :ivar cooldown: the cooldown value for ``citus_update_node()`` UDF call.
    :ivar deadline: the time in unix seconds when the transaction is allowed to be rolled back.
    """

    def __init__(self, groupid: int, nodes: Optional[Collection[PgDistNode]], event: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> None:
        """Create a :class:`PgDistTask` object based on given arguments.

        :param groupid: the groupid from "pg_dist_node".
        :param nodes: a collection of :class:`PgDistNode` objects that belong to a *groupid*.
        :param event: an "event" that resulted in creating this task.
        :param timeout: a transaction timeout if the task resulted in starting a transaction.
        :param cooldown: the cooldown value for ``citus_update_node()`` UDF call.
        """
        super(PgDistTask, self).__init__(groupid, nodes)

        # Event that is trying to change or changed the given row.
        # Possible values: before_demote, before_promote, after_promote.
        self.event = event

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
        """Wait until this task is processed by a dedicated thread."""
        self._event.wait()

    def wakeup(self) -> None:
        """Notify a thread that created a task that it was processed."""
        self._event.set()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PgDistTask) and self.event == other.event\
            and super(PgDistTask, self).equals(other)

    def __ne__(self, other: Any) -> bool:
        return not self == other


class Citus(AbstractMPP):

    group_re = re.compile('^(0|[1-9][0-9]*)$')

    @staticmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for a given MPP.

        :param config: configuration of ``citus`` MPP section.

        :returns: ``True`` is config passes validation, otherwise ``False``.
        """
        return isinstance(config, dict) \
            and isinstance(cast(Dict[str, Any], config).get('database'), str) \
            and parse_int(cast(Dict[str, Any], config).get('group')) is not None

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
        self._pg_dist_group: Dict[int, PgDistTask] = {}  # Cache of pg_dist_node: {groupid: PgDistTask()}
        self._tasks: List[PgDistTask] = []  # Requests to change pg_dist_group, every task is a `PgDistTask`
        self._in_flight: Optional[PgDistTask] = None  # Reference to the `PgDistTask` being changed in a transaction
        self._schedule_load_pg_dist_group = True  # Flag that "pg_dist_group" should be queried from the database
        self._condition = Condition()  # protects _pg_dist_group, _tasks, _in_flight, and _schedule_load_pg_dist_group
        self.schedule_cache_rebuild()

    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """
        with self._condition:
            self._schedule_load_pg_dist_group = True

    def on_demote(self) -> None:
        with self._condition:
            self._pg_dist_group.clear()
            empty_tasks: List[PgDistTask] = []
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

    def load_pg_dist_group(self) -> bool:
        """Read from the `pg_dist_node` table and put it into the local cache"""

        with self._condition:
            if not self._schedule_load_pg_dist_group:
                return True
            self._schedule_load_pg_dist_group = False

        try:
            rows = self.query('SELECT groupid, nodename, nodeport, noderole, nodeid FROM pg_catalog.pg_dist_node')
        except Exception:
            return False

        pg_dist_group: Dict[int, PgDistTask] = {}

        for row in rows:
            if row[0] not in pg_dist_group:
                pg_dist_group[row[0]] = PgDistTask(row[0], nodes=set(), event='after_promote')
            pg_dist_group[row[0]].add(PgDistNode(*row[1:]))

        with self._condition:
            self._pg_dist_group = pg_dist_group
        return True

    def sync_meta_data(self, cluster: Cluster) -> None:
        """Maintain the ``pg_dist_node`` from the coordinator leader every heartbeat loop.

        We can't always rely on REST API calls from worker nodes in order
        to maintain `pg_dist_node`, therefore at least once per heartbeat
        loop we make sure that works registered in `self._pg_dist_group`
        cache are matching the cluster view from DCS by creating tasks
        the same way as it is done from the REST API."""

        if not self.is_coordinator():
            return

        with self._condition:
            if not self.is_alive():
                self.start()

        self.add_task('after_promote', CITUS_COORDINATOR_GROUP_ID, cluster,
                      self._postgresql.name, self._postgresql.connection_string)

        for groupid, worker in cluster.workers.items():
            leader = worker.leader
            if leader and leader.conn_url\
                    and leader.data.get('role') in (PostgresqlRole.MASTER, PostgresqlRole.PRIMARY)\
                    and leader.data.get('state') == PostgresqlState.RUNNING:
                self.add_task('after_promote', groupid, worker, leader.name, leader.conn_url)

    def find_task_by_groupid(self, groupid: int) -> Optional[int]:
        for i, task in enumerate(self._tasks):
            if task.groupid == groupid:
                return i

    def pick_task(self) -> Tuple[Optional[int], Optional[PgDistTask]]:
        """Returns the tuple(i, task), where `i` - is the task index in the self._tasks list

        Tasks are picked by following priorities:

        1. If there is already a transaction in progress, pick a task
           that that will change already affected worker primary.
        2. If the coordinator address should be changed - pick a task
           with groupid=0 (coordinators are always in groupid 0).
        3. Pick a task that is the oldest (first from the self._tasks)
        """

        with self._condition:
            if self._in_flight:
                i = self.find_task_by_groupid(self._in_flight.groupid)
            else:
                while True:
                    i = self.find_task_by_groupid(CITUS_COORDINATOR_GROUP_ID)  # set_coordinator
                    if i is None and self._tasks:
                        i = 0
                    if i is None:
                        break
                    task = self._tasks[i]
                    if task == self._pg_dist_group.get(task.groupid):
                        self._tasks.pop(i)  # nothing to do because cached version of pg_dist_group already matches
                    else:
                        break
            task = self._tasks[i] if i is not None else None

            return i, task

    def update_node(self, groupid: int, node: PgDistNode, cooldown: float = 10000) -> None:
        if node.role not in ('primary', 'secondary', 'demoted'):
            self.query('SELECT pg_catalog.citus_remove_node(%s, %s)', node.host, node.port)
        elif node.nodeid is not None:
            host = node.host + ('-demoted' if node.role == 'demoted' else '')
            self.query('SELECT pg_catalog.citus_update_node(%s, %s, %s, true, %s)',
                       node.nodeid, host, node.port, cooldown)
        elif node.role != 'demoted':
            node.nodeid = self.query("SELECT pg_catalog.citus_add_node(%s, %s, %s, %s, 'default')",
                                     node.host, node.port, groupid, node.role)[0][0]

    def update_group(self, task: PgDistTask, transaction: bool) -> None:
        current_state = self._in_flight\
            or self._pg_dist_group.get(task.groupid)\
            or PgDistTask(task.groupid, set(), 'after_promote')
        transitions = list(task.transition(current_state))
        if transitions:
            if not transaction and len(transitions) > 1:
                self.query('BEGIN')
            for node in transitions:
                self.update_node(task.groupid, node, task.cooldown)
            if not transaction and len(transitions) > 1:
                task.failover = False
                self.query('COMMIT')

    def process_task(self, task: PgDistTask) -> bool:
        """Updates a single row in `pg_dist_group` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node or before promoting the other worker if
        there is no transaction in progress. And, the transaction is committed when the switchover/failover completed.

        .. note:
            The maximum lifetime of the transaction in progress is controlled outside of this method.

        .. note:
            Read access to `self._in_flight` isn't protected because we know it can't be changed outside of our thread.

        :param task: reference to a :class:`PgDistTask` object that represents a row to be updated/created.
        :returns: ``True`` if the row was successfully created/updated or transaction in progress
            was committed as an indicator that the `self._pg_dist_group` cache should be updated,
            or, if the new transaction was opened, this method returns `False`.
        """

        if task.event == 'after_promote':
            self.update_group(task, self._in_flight is not None)
            if self._in_flight:
                self.query('COMMIT')
            task.failover = False
            return True
        else:  # before_demote, before_promote
            if task.timeout:
                task.deadline = time.time() + task.timeout
            if not self._in_flight:
                self.query('BEGIN')
            self.update_group(task, True)
        return False

    def process_tasks(self) -> None:
        while True:
            # Read access to `_in_flight` isn't protected because we know it can't be changed outside of our thread.
            if not self._in_flight and not self.load_pg_dist_group():
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
                        self._pg_dist_group[task.groupid] = task

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
                    if self._schedule_load_pg_dist_group:
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

    def _add_task(self, task: PgDistTask) -> bool:
        with self._condition:
            i = self.find_task_by_groupid(task.groupid)

            # The `PgDistNode.timeout` == None is an indicator that it was scheduled from the sync_meta_data().
            if task.timeout is None:
                # We don't want to override the already existing task created from REST API.
                if i is not None and self._tasks[i].timeout is not None:
                    return False

                # There is a little race condition with tasks created from REST API - the call made "before" the member
                # key is updated in DCS. Therefore it is possible that :func:`sync_meta_data` will try to create a task
                # based on the outdated values of "state"/"role". To solve it we introduce an artificial timeout.
                # Only when the timeout is reached new tasks could be scheduled from sync_meta_data()
                if self._in_flight and self._in_flight.groupid == task.groupid and self._in_flight.timeout is not None\
                        and self._in_flight.deadline > time.time():
                    return False

            # Override already existing task for the same worker groupid
            if i is not None:
                if task != self._tasks[i]:
                    logger.debug('Overriding existing task: %s != %s', self._tasks[i], task)
                    self._tasks[i] = task
                    self._condition.notify()
                    return True
            # Add the task to the list if Worker node state is different from the cached `pg_dist_group`
            elif self._schedule_load_pg_dist_group or task != self._pg_dist_group.get(task.groupid)\
                    or self._in_flight and task.groupid == self._in_flight.groupid:
                logger.debug('Adding the new task: %s', task)
                self._tasks.append(task)
                self._condition.notify()
                return True
        return False

    @staticmethod
    def _pg_dist_node(role: str, conn_url: str) -> Optional[PgDistNode]:
        try:
            r = urlparse(conn_url)
            if r.hostname:
                return PgDistNode(r.hostname, r.port or 5432, role)
        except Exception as e:
            logger.error('Failed to parse connection url %s: %r', conn_url, e)

    def add_task(self, event: str, groupid: int, cluster: Cluster, leader_name: str, leader_url: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> Optional[PgDistTask]:
        primary = self._pg_dist_node('demoted' if event == 'before_demote' else 'primary', leader_url)
        if not primary:
            return

        task = PgDistTask(groupid, {primary}, event=event, timeout=timeout, cooldown=cooldown)
        for member in cluster.members:
            secondary = self._pg_dist_node('secondary', member.conn_url)\
                if member.name != leader_name and not member.noloadbalance and member.is_running and member.conn_url\
                else None
            if secondary:
                task.add(secondary)
        return task if self._add_task(task) else None

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        if not self.is_alive():
            return

        worker = cluster.workers.get(event['group'])
        if not (worker and worker.leader and worker.leader.name == event['leader'] and worker.leader.conn_url):
            return logger.info('Discarding event %s', event)

        task = self.add_task(event['type'], event['group'], worker,
                             worker.leader.name, worker.leader.conn_url,
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
            map(str.strip, parameters.get('shared_preload_libraries', '').split(',')))
        )  # pyright: ignore [reportUnknownArgumentType]
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
