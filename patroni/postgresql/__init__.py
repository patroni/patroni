import logging
import os
import re
import shlex
import shutil
import subprocess
import time

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from enum import IntEnum
from threading import current_thread, Lock
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

from dateutil import tz
from psutil import TimeoutExpired

from .. import global_config, psycopg
from ..async_executor import CriticalTask
from ..collections import CaseInsensitiveDict, CaseInsensitiveSet, EMPTY_DICT
from ..dcs import Cluster, Leader, Member, slot_name_from_member_name
from ..exceptions import PostgresConnectionException
from ..tags import Tags
from ..utils import data_directory_is_empty, parse_int, polling_loop, Retry, RetryFailedError
from .bootstrap import Bootstrap
from .callback_executor import CallbackAction, CallbackExecutor
from .cancellable import CancellableSubprocess
from .config import ConfigHandler, mtime
from .connection import ConnectionPool, get_connection_cursor
from .misc import parse_history, parse_lsn, postgres_major_version_to_int, PostgresqlRole, PostgresqlState
from .mpp import AbstractMPP
from .postmaster import PostmasterProcess
from .slots import SlotsHandler
from .sync import SyncHandler

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection as Connection3, Cursor
    from psycopg2 import connection as connection3, cursor

logger = logging.getLogger(__name__)

STOP_POLLING_INTERVAL = 1


class PgIsReadyStatus(IntEnum):
    """Possible PostgreSQL connection status ``pg_isready`` utility can report.

    :cvar RUNNING: return code 0. PostgreSQL is accepting connections normally.
    :cvar REJECT: return code 1. PostgreSQL is rejecting connections.
    :cvar NO_RESPONSE: return code 2. There was no response to the connection attempt.
    :cvar UNKNOWN: Return code 3. No connection attempt was made, something went wrong.
    """

    RUNNING = 0
    REJECT = 1
    NO_RESPONSE = 2
    UNKNOWN = 3


@contextmanager
def null_context():
    yield


class Postgresql(object):

    POSTMASTER_START_TIME = "pg_catalog.pg_postmaster_start_time()"
    TL_LSN = ("CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 "
              "ELSE ('x' || pg_catalog.substr(pg_catalog.pg_{0}file_name("
              "pg_catalog.pg_current_{0}_{1}()), 1, 8))::bit(32)::int END, "  # primary timeline
              "CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 ELSE "
              "pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_current_{0}{2}_{1}(), '0/0')::bigint END, "  # wal(_flush)?_lsn
              "pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_last_{0}_replay_{1}(), '0/0')::bigint, "
              "pg_catalog.pg_{0}_{1}_diff(COALESCE(pg_catalog.pg_last_{0}_receive_{1}(), '0/0'), '0/0')::bigint, "
              "pg_catalog.pg_is_in_recovery() AND pg_catalog.pg_is_{0}_replay_paused()")

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        self.name: str = config['name']
        self.scope: str = config['scope']
        self._data_dir: str = config['data_dir']
        self._database = config.get('database', 'postgres')
        self._version_file = os.path.join(self._data_dir, 'PG_VERSION')
        self._pg_control = os.path.join(self._data_dir, 'global', 'pg_control')
        self.connection_string: str
        self.proxy_url: Optional[str]
        self._major_version = self.get_major_version()

        self._state_lock = Lock()
        self.set_state(PostgresqlState.STOPPED)

        self._pending_restart_reason = CaseInsensitiveDict()
        self.connection_pool = ConnectionPool()
        self._connection = self.connection_pool.get('heartbeat')
        self.mpp_handler = mpp.get_handler_impl(self)
        self._bin_dir = config.get('bin_dir') or ''
        self._role_lock = Lock()
        self.set_role(PostgresqlRole.UNINITIALIZED)
        self.config = ConfigHandler(self, config)
        self.config.check_directories()

        self.bootstrap = Bootstrap(self)
        self.bootstrapping = False
        self.__thread_ident = current_thread().ident

        self.slots_handler = SlotsHandler(self)
        self.sync_handler = SyncHandler(self)

        self._callback_executor = CallbackExecutor()
        self.__cb_called = False
        self.__cb_pending = None

        self.cancellable = CancellableSubprocess()

        self._sysid = ''
        self.retry = Retry(max_tries=-1, deadline=config['retry_timeout'] / 2.0, max_delay=1,
                           retry_exceptions=PostgresConnectionException)

        # Retry 'pg_is_in_recovery()' only once
        self._is_leader_retry = Retry(max_tries=1, deadline=config['retry_timeout'] / 2.0, max_delay=1,
                                      retry_exceptions=PostgresConnectionException)

        self.set_role(self.get_postgres_role_from_data_directory())
        self._state_entry_timestamp = 0

        self._cluster_info_state = {}
        self._should_query_slots = True
        self._enforce_hot_standby_feedback = False
        self._cached_replica_timeline = None

        # Last known running process
        self._postmaster_proc = None

        self._available_gucs = None

        if self.is_running():
            # If we found postmaster process we need to figure out whether postgres is accepting connections
            self.set_state(PostgresqlState.STARTING)
            self.check_startup_state_changed()

        if self.state == PostgresqlState.RUNNING:  # we are "joining" already running postgres
            # we know that PostgreSQL is accepting connections and can read some GUC's from pg_settings
            self.config.load_current_server_parameters()

            self.set_role(PostgresqlRole.PRIMARY if self.is_primary() else PostgresqlRole.REPLICA)

            hba_saved = self.config.replace_pg_hba()
            ident_saved = self.config.replace_pg_ident()

            if self.major_version < 120000 or self.role == PostgresqlRole.PRIMARY:
                # If PostgreSQL is running as a primary or we run PostgreSQL that is older than 12 we can
                # call reload_config() once again (the first call happened in the ConfigHandler constructor),
                # so that it can figure out if config files should be updated and pg_ctl reload executed.
                self.config.reload_config(config, sighup=bool(hba_saved or ident_saved))
            elif hba_saved or ident_saved:
                self.reload()
        elif not self.is_running() and self.role == PostgresqlRole.PRIMARY:
            self.set_role(PostgresqlRole.DEMOTED)

    @property
    def create_replica_methods(self) -> List[str]:
        return self.config.get('create_replica_methods', []) or self.config.get('create_replica_method', []) or []

    @property
    def major_version(self) -> int:
        return self._major_version

    @property
    def database(self) -> str:
        return self._database

    @property
    def data_dir(self) -> str:
        return self._data_dir

    @property
    def callback(self) -> Dict[str, str]:
        return self.config.get('callbacks', {}) or {}

    @property
    def wal_dir(self) -> str:
        return os.path.join(self._data_dir, 'pg_' + self.wal_name)

    @property
    def wal_name(self) -> str:
        return 'wal' if self._major_version >= 100000 else 'xlog'

    @property
    def wal_flush(self) -> str:
        """For PostgreSQL 9.6 onwards we want to use pg_current_wal_flush_lsn()/pg_current_xlog_flush_location()."""
        return '_flush' if self._major_version >= 90600 else ''

    @property
    def lsn_name(self) -> str:
        return 'lsn' if self._major_version >= 100000 else 'location'

    @property
    def supports_quorum_commit(self) -> bool:
        """``True`` if quorum commit is supported by Postgres."""
        return self._major_version >= 100000

    @property
    def supports_multiple_sync(self) -> bool:
        """:returns: `True` if Postgres version supports more than one synchronous node."""
        return self._major_version >= 90600

    @property
    def can_advance_slots(self) -> bool:
        """``True`` if :attr:``major_version`` is greater than 110000."""
        return self.major_version >= 110000

    @property
    def cluster_info_query(self) -> str:
        """Returns the monitoring query with a fixed number of fields.

        The query text is constructed based on current state in DCS and PostgreSQL version:

        1. function names depend on version. wal/lsn for v10+ and xlog/location for pre v10.
        2. for primary we query timeline_id (extracted from pg_walfile_name()) and pg_current_wal_lsn()
        3. for replicas we query pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), and  pg_is_wal_replay_paused()
        4. for v9.6+ we query primary_slot_name and primary_conninfo from pg_stat_get_wal_receiver()
        5. for v11+ with permanent logical slots we query from pg_replication_slots and aggregate the result
        6. for standby_leader node running v9.6+ we also query pg_control_checkpoint to fetch timeline_id
        7. if sync replication is enabled we query pg_stat_replication and aggregate the result.
           In addition to that we get current values of synchronous_commit and synchronous_standby_names GUCs.

        If some conditions are not satisfied we simply put static values instead. E.g., NULL, 0, '', and so on.
        """

        extra = ", " + (("pg_catalog.current_setting('synchronous_commit'), "
                         "pg_catalog.current_setting('synchronous_standby_names'), "
                         "(SELECT pg_catalog.json_agg(r.*) FROM (SELECT w.pid as pid, application_name, sync_state,"
                         " pg_catalog.pg_{0}_{1}_diff(write_{1}, '0/0')::bigint AS write_lsn,"
                         " pg_catalog.pg_{0}_{1}_diff(flush_{1}, '0/0')::bigint AS flush_lsn,"
                         " pg_catalog.pg_{0}_{1}_diff(replay_{1}, '0/0')::bigint AS replay_lsn "
                         "FROM pg_catalog.pg_stat_get_wal_senders() w,"
                         " pg_catalog.pg_stat_get_activity(w.pid)"
                         " WHERE w.state = 'streaming') r)").format(self.wal_name, self.lsn_name)
                        if global_config.is_synchronous_mode
                        and self.role in (PostgresqlRole.PRIMARY, PostgresqlRole.PROMOTED) else "'on', '', NULL")

        if self._major_version >= 90600:
            filter_failover = ' WHERE NOT failover' if self._major_version >= 170000 else ''
            extra = ("pg_catalog.current_setting('restore_command')" if self._major_version >= 120000 else "NULL") +\
                ", " + ("(SELECT pg_catalog.json_agg(s.*) FROM (SELECT slot_name, slot_type as type, datoid::bigint, "
                        "plugin, catalog_xmin, pg_catalog.pg_wal_lsn_diff(confirmed_flush_lsn, '0/0')::bigint"
                        " AS confirmed_flush_lsn, pg_catalog.pg_wal_lsn_diff(restart_lsn, '0/0')::bigint"
                        f" AS restart_lsn, xmin FROM pg_catalog.pg_get_replication_slots(){filter_failover}) AS s)"
                        if self._should_query_slots and self.can_advance_slots else "NULL") + extra

            written_lsn = ("pg_catalog.pg_wal_lsn_diff(written_lsn, '0/0')::bigint"
                           if self._major_version >= 130000 else "NULL")
            extra = (", CASE WHEN latest_end_lsn IS NULL THEN NULL ELSE received_tli END, {0}, slot_name, "
                     "conninfo, status, {1} FROM pg_catalog.pg_stat_get_wal_receiver()").format(written_lsn, extra)
            if self.role == PostgresqlRole.STANDBY_LEADER:
                extra = "timeline_id" + extra + ", pg_catalog.pg_control_checkpoint()"
            else:
                extra = "0" + extra
        else:
            extra = "0, NULL, NULL, NULL, NULL, NULL, NULL, NULL" + extra

        return ("SELECT " + self.TL_LSN + ", {3}").format(self.wal_name, self.lsn_name, self.wal_flush, extra)

    @property
    def available_gucs(self) -> CaseInsensitiveSet:
        """GUCs available in this Postgres server."""
        if not self._available_gucs:
            self._available_gucs = self._get_gucs()
        return self._available_gucs

    def _version_file_exists(self) -> bool:
        return not self.data_directory_empty() and os.path.isfile(self._version_file)

    def get_major_version(self) -> int:
        """Reads major version from PG_VERSION file

        :returns: major PostgreSQL version in integer format or 0 in case of missing file or errors"""
        if self._version_file_exists():
            try:
                with open(self._version_file) as f:
                    return postgres_major_version_to_int(f.read().strip())
            except Exception:
                logger.exception('Failed to read PG_VERSION from %s', self._data_dir)
        return 0

    def pgcommand(self, cmd: str) -> str:
        """Return path to the specified PostgreSQL command.

        .. note::
            If ``postgresql.bin_name.*cmd*`` was configured by the user then that binary name is used, otherwise the
            default binary name *cmd* is used.

        :param cmd: the Postgres binary name to get path to.

        :returns: path to Postgres binary named *cmd*.
        """
        return os.path.join(self._bin_dir, (self.config.get('bin_name', {}) or EMPTY_DICT).get(cmd, cmd))

    def pg_ctl(self, cmd: str, *args: str, **kwargs: Any) -> bool:
        """Builds and executes pg_ctl command

        :returns: `!True` when return_code == 0, otherwise `!False`"""

        pg_ctl = [self.pgcommand('pg_ctl'), cmd]
        return subprocess.call(pg_ctl + ['-D', self._data_dir] + list(args), **kwargs) == 0

    def initdb(self, *args: str, **kwargs: Any) -> bool:
        """Builds and executes the initdb command.

        :param args: List of arguments to be joined into the initdb command.
        :param kwargs: Keyword arguments to pass to ``subprocess.call``.

        :returns: ``True`` if the result of ``subprocess.call`, the exit code, is ``0``.
        """
        initdb = [self.pgcommand('initdb')] + list(args) + [self.data_dir]
        return subprocess.call(initdb, **kwargs) == 0

    def pg_isready(self) -> PgIsReadyStatus:
        """Runs pg_isready to see if PostgreSQL is accepting connections.

        :returns: one of :class:`PgIsReadyStatus` values."""

        r = self.connection_pool.conn_kwargs
        cmd = [self.pgcommand('pg_isready'), '-p', r['port'], '-d', self._database]

        # Host is not set if we are connecting via default unix socket
        if 'host' in r:
            cmd.extend(['-h', r['host']])

        # We only need the username because pg_isready does not try to authenticate
        if 'user' in r:
            cmd.extend(['-U', r['user']])

        ret = subprocess.call(cmd)
        try:
            return PgIsReadyStatus(ret)
        except ValueError:
            return PgIsReadyStatus.UNKNOWN

    def reload_config(self, config: Dict[str, Any], sighup: bool = False) -> None:
        self.config.reload_config(config, sighup)
        self._is_leader_retry.deadline = self.retry.deadline = config['retry_timeout'] / 2.0

    @property
    def pending_restart_reason(self) -> CaseInsensitiveDict:
        """Get :attr:`_pending_restart_reason` value.

        :attr:`_pending_restart_reason` is a :class:`CaseInsensitiveDict` object of the PG parameters that are
        causing pending restart state. Every key is a parameter name, value - a dictionary containing the old
        and the new value (see :func:`~patroni.postgresql.config.get_param_diff`).
        """
        return self._pending_restart_reason

    def set_pending_restart_reason(self, diff_dict: CaseInsensitiveDict) -> None:
        """Set new or update current :attr:`_pending_restart_reason`.

        :param diff_dict: :class:``CaseInsensitiveDict`` object with the parameters that are causing pending restart
            state with the diff of their values. Used to reset/update the :attr:`_pending_restart_reason`.
        """
        self._pending_restart_reason = diff_dict

    @property
    def sysid(self) -> str:
        if not self._sysid and not self.bootstrapping:
            data = self.controldata()
            self._sysid = data.get('Database system identifier', '')
        return self._sysid

    def get_postgres_role_from_data_directory(self) -> PostgresqlRole:
        if self.data_directory_empty() or not self.controldata():
            return PostgresqlRole.UNINITIALIZED
        elif self.config.recovery_conf_exists():
            return PostgresqlRole.REPLICA
        else:
            return PostgresqlRole.PRIMARY

    @property
    def server_version(self) -> int:
        return self._connection.server_version

    def connection(self) -> Union['connection3', 'Connection3[Any]']:
        return self._connection.get()

    def _query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        :param sql: SQL statement to execute.
        :param params: parameters to pass.

        :returns: a query response as a list of tuples if there is any.
        :raises:
            :exc:`~psycopg.Error` if had issues while executing *sql*.

            :exc:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.

            :exc:`~patroni.utils.RetryFailedError`: if it was detected that connection/query failed due to PostgreSQL
            restart.
        """
        try:
            return self._connection.query(sql, *params)
        except PostgresConnectionException as exc:
            if self.state == PostgresqlState.RESTARTING:
                raise RetryFailedError('cluster is being restarted') from exc
            raise

    def query(self, sql: str, *params: Any, retry: bool = True) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        :param sql: SQL statement to execute.
        :param params: parameters to pass.
        :param retry: whether the query should be retried upon failure or given up immediately.

        :returns: a query response as a list of tuples if there is any.
        :raises:
            :exc:`~psycopg.Error` if had issues while executing *sql*.

            :exc:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.

            :exc:`~patroni.utils.RetryFailedError`: if it was detected that connection/query failed due to PostgreSQL
            restart or if retry deadline was exceeded.
        """
        if not retry:
            return self._query(sql, *params)
        try:
            return self.retry(self._query, sql, *params)
        except RetryFailedError as exc:
            raise PostgresConnectionException(str(exc)) from exc

    def pg_control_exists(self) -> bool:
        return os.path.isfile(self._pg_control)

    def data_directory_empty(self) -> bool:
        if self.pg_control_exists():
            return False
        return data_directory_is_empty(self._data_dir)

    def replica_method_options(self, method: str) -> Dict[str, Any]:
        return deepcopy(self.config.get(method, {}) or EMPTY_DICT.copy())

    def replica_method_can_work_without_replication_connection(self, method: str) -> bool:
        return method != 'basebackup' and bool(self.replica_method_options(method).get('no_leader'))

    def can_create_replica_without_replication_connection(self, replica_methods: Optional[List[str]]) -> bool:
        """ go through the replication methods to see if there are ones
            that does not require a working replication connection.
        """
        if replica_methods is None:
            replica_methods = self.create_replica_methods
        return any(self.replica_method_can_work_without_replication_connection(m) for m in replica_methods)

    @property
    def enforce_hot_standby_feedback(self) -> bool:
        return self._enforce_hot_standby_feedback

    def set_enforce_hot_standby_feedback(self, value: bool) -> None:
        # If we enable or disable the hot_standby_feedback we need to update postgresql.conf and reload
        if self._enforce_hot_standby_feedback != value:
            self._enforce_hot_standby_feedback = value
            if self.is_running():
                self.config.write_postgresql_conf()
                self.reload()

    def reset_cluster_info_state(self, cluster: Optional[Cluster], tags: Optional[Tags] = None) -> None:
        """Reset monitoring query cache.

        .. note::
            It happens in the beginning of heart-beat loop and on change of `synchronous_standby_names`.

        :param cluster: currently known cluster state from DCS
        :param tags: reference to an object implementing :class:`Tags` interface.
        """
        self._cluster_info_state = {}

        if not tags:
            return

        if global_config.is_standby_cluster:
            # Standby cluster can't have logical replication slots, and we don't need to enforce hot_standby_feedback
            self.set_enforce_hot_standby_feedback(False)

        if cluster and cluster.config and cluster.config.modify_version:
            # We want to enable hot_standby_feedback if the replica is supposed
            # to have a logical slot or in case if it is the cascading replica.
            self.set_enforce_hot_standby_feedback(not global_config.is_standby_cluster and self.can_advance_slots
                                                  and cluster.should_enforce_hot_standby_feedback(self, tags))
            self._should_query_slots = global_config.member_slots_ttl > 0 or cluster.has_permanent_slots(self, tags)

    def _cluster_info_state_get(self, name: str) -> Optional[Any]:
        if not self._cluster_info_state:
            try:
                result = self._is_leader_retry(self._query, self.cluster_info_query)[0]
                cluster_info_state = dict(zip(['timeline', 'wal_position', 'replay_lsn',
                                               'receive_lsn', 'replay_paused', 'pg_control_timeline',
                                               'received_tli', 'write_location', 'slot_name', 'conninfo',
                                               'receiver_state', 'restore_command', 'slots', 'synchronous_commit',
                                               'synchronous_standby_names', 'pg_stat_replication'], result))
                if self._should_query_slots and self.can_advance_slots:
                    cluster_info_state['slots'] =\
                        self.slots_handler.process_permanent_slots(cluster_info_state['slots'])
                self._cluster_info_state = cluster_info_state
            except RetryFailedError as e:  # SELECT failed two times
                self._cluster_info_state = {'error': str(e)}
                if not self.is_starting() and self.pg_isready() == PgIsReadyStatus.REJECT:
                    self.set_state(PostgresqlState.STARTING)

        if 'error' in self._cluster_info_state:
            raise PostgresConnectionException(self._cluster_info_state['error'])

        return self._cluster_info_state.get(name)

    def replay_lsn(self) -> Optional[int]:
        return self._cluster_info_state_get('replay_lsn')

    def receive_lsn(self) -> Optional[int]:
        write = self._cluster_info_state_get('write_location')
        received = self._cluster_info_state_get('receive_lsn')
        return max(received, write) if received and write else write or received

    def slots(self) -> Dict[str, int]:
        """Get replication slots state.

        ..note::
            Since this methods is supposed to be used only by the leader and only to publish state of
            replication slots to DCS so that other nodes can advance LSN on respective replication slots,
            we are also adding our own name to the list. All slots that shouldn't be published to DCS
            later will be filtered out by :meth:`~Cluster.maybe_filter_permanent_slots` method.

        :returns: A :class:`dict` object with replication slot names and LSNs as absolute values.
        """
        return {**(self._cluster_info_state_get('slots') or {}),
                slot_name_from_member_name(self.name): self.last_operation()} \
            if self.can_advance_slots else {}

    def primary_slot_name(self) -> Optional[str]:
        return self._cluster_info_state_get('slot_name')

    def primary_conninfo(self) -> Optional[str]:
        return self._cluster_info_state_get('conninfo')

    def received_timeline(self) -> Optional[int]:
        return self._cluster_info_state_get('received_tli')

    def synchronous_commit(self) -> str:
        """:returns: "synchronous_commit" GUC value."""
        return self._cluster_info_state_get('synchronous_commit') or 'on'

    def synchronous_standby_names(self) -> str:
        """:returns: "synchronous_standby_names" GUC value."""
        return self._cluster_info_state_get('synchronous_standby_names') or ''

    def pg_stat_replication(self) -> List[Dict[str, Any]]:
        """:returns: a result set of 'SELECT * FROM pg_stat_replication'."""
        return self._cluster_info_state_get('pg_stat_replication') or []

    def replication_state_from_parameters(self, is_primary: bool, receiver_state: Optional[str],
                                          restore_command: Optional[str]) -> Optional[str]:
        """Figure out the replication state from input parameters.

        .. note::
            This method could be only called when Postgres is up, running and queries are successfully executed.

        :is_primary: `True` is postgres is not running in recovery
        :receiver_state: value from `pg_stat_get_wal_receiver.state` or None if Postgres is older than 9.6
        :restore_command: value of ``restore_command`` GUC for PostgreSQL 12+ or
                          `postgresql.recovery_conf.restore_command` if it is set in Patroni configuration

        :returns: - `None` for the primary and for Postgres older than 9.6;
                  - 'streaming' if replica is streaming according to the `pg_stat_wal_receiver` view;
                  - 'in archive recovery' if replica isn't streaming and there is a `restore_command`
        """
        if self._major_version >= 90600 and not is_primary:
            if receiver_state == 'streaming':
                return 'streaming'
            # For Postgres older than 12 we get `restore_command` from Patroni config, otherwise we check GUC
            if self._major_version < 120000 and self.config.restore_command() or restore_command:
                return 'in archive recovery'

    def replication_state(self) -> Optional[str]:
        """Checks replication state from `pg_stat_get_wal_receiver()`.

        .. note::
            Available only since 9.6

        :returns: ``streaming``, ``in archive recovery``, or ``None``
        """
        return self.replication_state_from_parameters(self.is_primary(),
                                                      self._cluster_info_state_get('receiver_state'),
                                                      self._cluster_info_state_get('restore_command'))

    def is_primary(self) -> bool:
        try:
            return bool(self._cluster_info_state_get('timeline'))
        except PostgresConnectionException:
            logger.warning('Failed to determine PostgreSQL state from the connection, falling back to cached role')
            return bool(self.is_running() and self.role == PostgresqlRole.PRIMARY)

    def replay_paused(self) -> bool:
        return self._cluster_info_state_get('replay_paused') or False

    def resume_wal_replay(self) -> None:
        self._query('SELECT pg_catalog.pg_{0}_replay_resume()'.format(self.wal_name))

    def handle_parameter_change(self) -> None:
        if self.major_version >= 140000 and not self.is_starting() and self.replay_paused():
            logger.info('Resuming paused WAL replay for PostgreSQL 14+')
            self.resume_wal_replay()

    def pg_control_timeline(self) -> Optional[int]:
        try:

            return int(self.controldata().get("Latest checkpoint's TimeLineID", ""))
        except (TypeError, ValueError):
            logger.exception('Failed to parse timeline from pg_controldata output')

    def parse_wal_record(self, timeline: str,
                         lsn: str) -> Union[Tuple[str, str, str, str], Tuple[None, None, None, None]]:
        out, err = self.waldump(timeline, lsn, 1)
        if out and not err:
            match = re.match(r'^rmgr:\s+(.+?)\s+len \(rec/tot\):\s+\d+/\s+\d+, tx:\s+\d+, '
                             r'lsn: ([0-9A-Fa-f]+/[0-9A-Fa-f]+), prev ([0-9A-Fa-f]+/[0-9A-Fa-f]+), '
                             r'.*?desc: (.+)', out.decode('utf-8'))
            if match:
                return match.group(1), match.group(2), match.group(3), match.group(4)
        return None, None, None, None

    def latest_checkpoint_locations(self, data: Optional[Dict[str, str]] = None) -> Tuple[Optional[int], Optional[int]]:
        """Get shutdown checkpoint location.

        :param data: :class:`dict` object with values returned by `pg_controldata` tool.

        :returns: a tuple of checkpoint LSN for the cleanly shut down primary, and LSN of prev wal record (SWITCH)
                  if we know that the checkpoint was written to the new WAL file due to the archive_mode=on.
        """
        if data is None:
            data = self.controldata()
        timeline = data.get("Latest checkpoint's TimeLineID")
        lsn = checkpoint_lsn = data.get('Latest checkpoint location')
        prev_lsn = None
        if data.get('Database cluster state') == 'shut down' and lsn and timeline and checkpoint_lsn:
            try:
                checkpoint_lsn = parse_lsn(checkpoint_lsn)
                rm_name, lsn, prev, desc = self.parse_wal_record(timeline, lsn)
                desc = str(desc).strip().lower()
                if rm_name == 'XLOG' and lsn and parse_lsn(lsn) == checkpoint_lsn and prev and\
                        desc.startswith('checkpoint') and desc.endswith('shutdown'):
                    _, lsn, _, desc = self.parse_wal_record(timeline, prev)
                    prev = parse_lsn(prev)
                    # If the cluster is shutdown with archive_mode=on, WAL is switched before writing the checkpoint.
                    # In this case we want to take the LSN of previous record (SWITCH) as the last known WAL location.
                    if lsn and parse_lsn(lsn) == prev and str(desc).strip() in ('xlog switch', 'SWITCH'):
                        prev_lsn = prev
            except Exception as e:
                logger.error('Exception when parsing WAL pg_%sdump output: %r', self.wal_name, e)
            if isinstance(checkpoint_lsn, int):
                return checkpoint_lsn, (prev_lsn or checkpoint_lsn)
        return None, None

    def is_running(self) -> Optional[PostmasterProcess]:
        """Returns PostmasterProcess if one is running on the data directory or None. If most recently seen process
        is running updates the cached process based on pid file."""
        if self._postmaster_proc:
            if self._postmaster_proc.is_running():
                return self._postmaster_proc
            self._postmaster_proc = None
            self._available_gucs = None

        # we noticed that postgres was restarted, force syncing of replication slots and check of logical slots
        self.slots_handler.schedule()

        self._postmaster_proc = PostmasterProcess.from_pidfile(self._data_dir)
        return self._postmaster_proc

    @property
    def cb_called(self) -> bool:
        return self.__cb_called

    def call_nowait(self, cb_type: CallbackAction) -> None:
        """pick a callback command and call it without waiting for it to finish """
        if self.bootstrapping:
            return
        if cb_type in (CallbackAction.ON_START, CallbackAction.ON_STOP,
                       CallbackAction.ON_RESTART, CallbackAction.ON_ROLE_CHANGE):
            self.__cb_called = True

        if self.callback and cb_type in self.callback:
            cmd = self.callback[cb_type]
            role = PostgresqlRole.PRIMARY if self.role == PostgresqlRole.PROMOTED else self.role
            try:
                cmd = shlex.split(self.callback[cb_type]) + [cb_type, role, self.scope]
                self._callback_executor.call(cmd)
            except Exception:
                logger.exception('callback %s %r %s %s failed', cmd, cb_type, role, self.scope)

    @property
    def role(self) -> PostgresqlRole:
        with self._role_lock:
            return self._role

    def set_role(self, value: PostgresqlRole) -> None:
        with self._role_lock:
            self._role = value

    @property
    def state(self) -> PostgresqlState:
        with self._state_lock:
            return self._state

    def set_state(self, value: PostgresqlState) -> None:
        with self._state_lock:
            self._state = value
            self._state_entry_timestamp = time.time()

    def time_in_state(self) -> float:
        return time.time() - self._state_entry_timestamp

    def is_starting(self) -> bool:
        return self.state in (PostgresqlState.STARTING, PostgresqlState.BOOTSTRAP_STARTING)

    def wait_for_port_open(self, postmaster: PostmasterProcess, timeout: float) -> bool:
        """Waits until PostgreSQL opens ports."""
        for _ in polling_loop(timeout):
            if self.cancellable.is_cancelled:
                return False

            if not postmaster.is_running():
                logger.error('postmaster is not running')
                self.set_state(PostgresqlState.START_FAILED)
                return False

            isready = self.pg_isready()
            if isready != PgIsReadyStatus.NO_RESPONSE:
                if isready not in [PgIsReadyStatus.REJECT, PgIsReadyStatus.RUNNING]:
                    logger.warning("Can't determine PostgreSQL startup status, assuming running")
                return True

        logger.warning("Timed out waiting for PostgreSQL to start")
        return False

    def start(self, timeout: Optional[float] = None, task: Optional[CriticalTask] = None,
              block_callbacks: bool = False, role: Optional[PostgresqlRole] = None,
              after_start: Optional[Callable[..., Any]] = None) -> Optional[bool]:
        """Start PostgreSQL

        Waits for postmaster to open ports or terminate so pg_isready can be used to check startup completion
        or failure.

        :returns: True if start was initiated and postmaster ports are open,
                  False if start failed, and None if postgres is still starting up"""
        # make sure we close all connections established against
        # the former node, otherwise, we might get a stalled one
        # after kill -9, which would report incorrect data to
        # patroni.
        self.connection_pool.close()

        state = (PostgresqlState.BOOTSTRAP_STARTING if self.bootstrap.running_custom_bootstrap
                 else PostgresqlState.STARTING)
        if self.is_running():
            logger.error('Cannot start PostgreSQL because one is already running.')
            self.set_state(state)
            return True

        if not block_callbacks:
            self.__cb_pending = CallbackAction.ON_START

        self.set_role(role or self.get_postgres_role_from_data_directory())

        self.set_state(state)
        self.set_pending_restart_reason(CaseInsensitiveDict())

        try:
            if not self.ensure_major_version_is_known():
                return None
            configuration = self.config.effective_configuration
        except Exception:
            return None

        self.config.check_directories()
        self.config.write_postgresql_conf(configuration)
        self.config.resolve_connection_addresses()
        self.config.replace_pg_hba()
        self.config.replace_pg_ident()

        options = ['--{0}={1}'.format(p, configuration[p]) for p in self.config.CMDLINE_OPTIONS
                   if p in configuration and p not in ('wal_keep_segments', 'wal_keep_size')]

        if self.cancellable.is_cancelled:
            return False

        with task or null_context():
            if task and task.is_cancelled:
                logger.info("PostgreSQL start cancelled.")
                return False

            self._postmaster_proc = PostmasterProcess.start(self.pgcommand('postgres'),
                                                            self._data_dir,
                                                            self.config.postgresql_conf,
                                                            options)

            if task:
                task.complete(self._postmaster_proc)

        start_timeout = timeout
        if not start_timeout:
            try:
                start_timeout = float(self.config.get('pg_ctl_timeout', 60) or 0)
            except ValueError:
                start_timeout = 60

        # We want postmaster to open ports before we continue
        if not self._postmaster_proc or not self.wait_for_port_open(self._postmaster_proc, start_timeout):
            return False

        ret = self.wait_for_startup(start_timeout)
        if ret is not None:
            if ret and after_start:
                after_start()
            return ret
        elif timeout is not None:
            return False
        else:
            return None

    def checkpoint(self, connect_kwargs: Optional[Dict[str, Any]] = None,
                   timeout: Optional[float] = None) -> Optional[str]:
        check_not_is_in_recovery = connect_kwargs is not None
        connect_kwargs = connect_kwargs or self.connection_pool.conn_kwargs
        for p in ['connect_timeout', 'options']:
            connect_kwargs.pop(p, None)
        if timeout:
            connect_kwargs['connect_timeout'] = timeout
        try:
            with get_connection_cursor(**connect_kwargs) as cur:
                cur.execute("SET statement_timeout = 0")
                if check_not_is_in_recovery:
                    cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                    row = cur.fetchone()
                    if not row or row[0]:
                        return 'is_in_recovery=true'
                cur.execute('CHECKPOINT')
        except psycopg.Error:
            logger.exception('Exception during CHECKPOINT')
            return 'not accessible or not healthy'

    def stop(self, mode: str = 'fast', block_callbacks: bool = False, checkpoint: Optional[bool] = None,
             on_safepoint: Optional[Callable[..., Any]] = None, on_shutdown: Optional[Callable[[int, int], Any]] = None,
             before_shutdown: Optional[Callable[..., Any]] = None, stop_timeout: Optional[int] = None) -> bool:
        """Stop PostgreSQL

        Supports a callback when a safepoint is reached. A safepoint is when no user backend can return a successful
        commit to users. Currently this means we wait for user backends to close. But in the future alternate mechanisms
        could be added.

        :param on_safepoint: This callback is called when no user backends are running.
        :param on_shutdown: is called when pg_controldata starts reporting `Database cluster state: shut down`
        :param before_shutdown: is called after running optional CHECKPOINT and before running pg_ctl stop
        """
        if checkpoint is None:
            checkpoint = False if mode == 'immediate' else True

        success, pg_signaled = self._do_stop(mode, block_callbacks, checkpoint, on_safepoint,
                                             on_shutdown, before_shutdown, stop_timeout)
        if success:
            # block_callbacks is used during restart to avoid
            # running start/stop callbacks in addition to restart ones
            if not block_callbacks:
                self.set_state(PostgresqlState.STOPPED)
                if pg_signaled:
                    self.call_nowait(CallbackAction.ON_STOP)
        else:
            logger.warning('pg_ctl stop failed')
            self.set_state(PostgresqlState.STOP_FAILED)
        return success

    def _do_stop(self, mode: str, block_callbacks: bool, checkpoint: bool,
                 on_safepoint: Optional[Callable[..., Any]], on_shutdown: Optional[Callable[[int, int], Any]],
                 before_shutdown: Optional[Callable[..., Any]], stop_timeout: Optional[int]) -> Tuple[bool, bool]:
        postmaster = self.is_running()
        if not postmaster:
            if on_safepoint:
                on_safepoint()
            return True, False

        if checkpoint and not self.is_starting():
            self.checkpoint(timeout=stop_timeout)

        if not block_callbacks:
            self.set_state(PostgresqlState.STOPPING)

        # invoke user-directed before stop script
        self._before_stop()

        if before_shutdown:
            before_shutdown()

        # Send signal to postmaster to stop
        success = postmaster.signal_stop(mode, self.pgcommand('pg_ctl'))
        if success is not None:
            if success and on_safepoint:
                on_safepoint()
            return success, True

        # We can skip safepoint detection if we don't have a callback
        if on_safepoint:
            # Wait for our connection to terminate so we can be sure that no new connections are being initiated
            self._wait_for_connection_close(postmaster)
            postmaster.wait_for_user_backends_to_close(stop_timeout)
            on_safepoint()

        if on_shutdown and mode in ('fast', 'smart'):
            i = 0
            # Wait for pg_controldata `Database cluster state:` to change to "shut down"
            while postmaster.is_running():
                data = self.controldata()
                if data.get('Database cluster state', '') == 'shut down':
                    checkpoint_lsn, prev_lsn = self.latest_checkpoint_locations(data)
                    if checkpoint_lsn is not None and prev_lsn is not None:
                        on_shutdown(checkpoint_lsn, prev_lsn)
                    break
                elif data.get('Database cluster state', '').startswith('shut down'):  # shut down in recovery
                    break
                elif stop_timeout and i >= stop_timeout:
                    stop_timeout = 0
                    break
                time.sleep(STOP_POLLING_INTERVAL)
                i += STOP_POLLING_INTERVAL

        try:
            postmaster.wait(timeout=stop_timeout)
        except TimeoutExpired:
            logger.warning("Timeout during postmaster stop, aborting Postgres.")
            if not self.terminate_postmaster(postmaster, mode, stop_timeout):
                postmaster.wait()

        return True, True

    def terminate_postmaster(self, postmaster: PostmasterProcess, mode: str,
                             stop_timeout: Optional[int]) -> Optional[bool]:
        if mode in ['fast', 'smart']:
            try:
                success = postmaster.signal_stop('immediate', self.pgcommand('pg_ctl'))
                if success:
                    return True
                postmaster.wait(timeout=stop_timeout)
                return True
            except TimeoutExpired:
                pass
        logger.warning("Sending SIGKILL to Postmaster and its children")
        return postmaster.signal_kill()

    def terminate_starting_postmaster(self, postmaster: PostmasterProcess) -> None:
        """Terminates a postmaster that has not yet opened ports or possibly even written a pid file. Blocks
        until the process goes away."""
        postmaster.signal_stop('immediate', self.pgcommand('pg_ctl'))
        postmaster.wait()

    def _wait_for_connection_close(self, postmaster: PostmasterProcess) -> None:
        try:
            while postmaster.is_running():  # Need a timeout here?
                self._connection.query("SELECT 1")
                time.sleep(STOP_POLLING_INTERVAL)
        except (psycopg.Error, PostgresConnectionException):
            pass

    def reload(self, block_callbacks: bool = False) -> bool:
        ret = self.pg_ctl('reload')
        if ret and not block_callbacks:
            self.call_nowait(CallbackAction.ON_RELOAD)
        return ret

    def check_for_startup(self) -> bool:
        """Checks PostgreSQL status and returns if PostgreSQL is in the middle of startup."""
        return self.is_starting() and not self.check_startup_state_changed()

    def check_startup_state_changed(self) -> bool:
        """Checks if PostgreSQL has completed starting up or failed or still starting.

        Should only be called when state == 'starting [after custom bootstrap]'

        :returns: True if state was changed from 'starting'
        """
        ready = self.pg_isready()

        if ready == PgIsReadyStatus.REJECT:
            return False
        elif ready == PgIsReadyStatus.NO_RESPONSE:
            ret = not self.is_running()
            if ret:
                self.set_state(PostgresqlState.START_FAILED)
                self.slots_handler.schedule(False)  # TODO: can remove this?
                self.config.save_configuration_files(True)  # TODO: maybe remove this?
            return ret
        else:
            if ready != PgIsReadyStatus.RUNNING:
                # Bad configuration or unexpected OS error. No idea of PostgreSQL status.
                # Let the main loop of run cycle clean up the mess.
                logger.warning("%s status returned from pg_isready",
                               "Unknown" if ready == PgIsReadyStatus.UNKNOWN else "Invalid")
            self.set_state(PostgresqlState.RUNNING)
            self.slots_handler.schedule()
            self.config.save_configuration_files(True)
            # TODO: __cb_pending can be None here after PostgreSQL restarts on its own. Do we want to call the callback?
            # Previously we didn't even notice.
            action = self.__cb_pending or CallbackAction.ON_START
            self.call_nowait(action)
            self.__cb_pending = None

            return True

    def wait_for_startup(self, timeout: float = 0) -> Optional[bool]:
        """Waits for PostgreSQL startup to complete or fail.

        :returns: True if start was successful, False otherwise"""
        if not self.is_starting():
            # Should not happen
            logger.warning("wait_for_startup() called when not in starting state")

        while not self.check_startup_state_changed():
            if self.cancellable.is_cancelled or timeout and self.time_in_state() > timeout:
                return None
            time.sleep(1)

        return self.state == PostgresqlState.RUNNING

    def restart(self, timeout: Optional[float] = None, task: Optional[CriticalTask] = None,
                block_callbacks: bool = False, role: Optional[PostgresqlRole] = None,
                before_shutdown: Optional[Callable[..., Any]] = None,
                after_start: Optional[Callable[..., Any]] = None) -> Optional[bool]:
        """Restarts PostgreSQL.

        When timeout parameter is set the call will block either until PostgreSQL has started, failed to start or
        timeout arrives.

        :returns: True when restart was successful and timeout did not expire when waiting.
        """
        self.set_state(PostgresqlState.RESTARTING)
        if not block_callbacks:
            self.__cb_pending = CallbackAction.ON_RESTART
        ret = self.stop(block_callbacks=True, before_shutdown=before_shutdown)\
            and self.start(timeout, task, True, role, after_start)
        if not ret and not self.is_starting():
            logger.warning('restart failed (%r)', self.state)
            self.set_state(PostgresqlState.RESTART_FAILED)
        return ret

    def is_healthy(self) -> bool:
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def get_guc_value(self, name: str) -> Optional[str]:
        cmd = [self.pgcommand('postgres'), '-D', self._data_dir, '-C', name,
               '--config-file={}'.format(self.config.postgresql_conf)]
        try:
            data = subprocess.check_output(cmd)
            if data:
                return data.decode('utf-8').strip()
        except Exception as e:
            logger.error('Failed to execute %s: %r', cmd, e)

    def controldata(self) -> Dict[str, str]:
        """ return the contents of pg_controldata, or non-True value if pg_controldata call failed """
        # Don't try to call pg_controldata during backup restore
        if self._version_file_exists() and self.state != PostgresqlState.CREATING_REPLICA:
            try:
                env = {**os.environ, 'LANG': 'C', 'LC_ALL': 'C'}
                data = subprocess.check_output([self.pgcommand('pg_controldata'), self._data_dir], env=env)
                if data:
                    data = filter(lambda e: ':' in e, data.decode('utf-8').splitlines())
                    # pg_controldata output depends on major version. Some of parameters are prefixed by 'Current '
                    return {k.replace('Current ', '', 1): v.strip() for k, v in map(lambda e: e.split(':', 1), data)}
            except Exception as e:
                logger.error("Error when calling pg_controldata: %r", e)
        return {}

    def waldump(self, timeline: Union[int, str], lsn: str, limit: int) -> Tuple[Optional[bytes], Optional[bytes]]:
        cmd = self.pgcommand('pg_{0}dump'.format(self.wal_name))
        env = {**os.environ, 'LANG': 'C', 'LC_ALL': 'C', 'PGDATA': self._data_dir}
        try:
            waldump = subprocess.Popen([cmd, '-t', str(timeline), '-s', lsn, '-n', str(limit)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            out, err = waldump.communicate()
            waldump.wait()
            return out, err
        except Exception as e:
            logger.error('Failed to execute `%s -t %s -s %s -n %s`: %r', cmd, timeline, lsn, limit, e)
            return None, None

    @contextmanager
    def get_replication_connection_cursor(self, host: Optional[str] = None, port: Union[int, str] = 5432,
                                          **kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        conn_kwargs = self.config.replication.copy()
        conn_kwargs.update(host=host, port=int(port) if port else None, user=conn_kwargs.pop('username'),
                           connect_timeout=3, replication=1, options='-c statement_timeout=2000')
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def get_replica_timeline(self) -> Optional[int]:
        try:
            with self.get_replication_connection_cursor(**self.config.local_replication_address) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                row = cur.fetchone()
                return row[1] if row else None
        except Exception:
            logger.exception('Can not fetch local timeline and lsn from replication connection')

    def replica_cached_timeline(self, primary_timeline: Optional[int]) -> Optional[int]:
        if not self._cached_replica_timeline or not primary_timeline\
                or self._cached_replica_timeline != primary_timeline:
            self._cached_replica_timeline = self.get_replica_timeline()
        return self._cached_replica_timeline

    def get_primary_timeline(self) -> int:
        """:returns: current timeline if postgres is running as a primary or 0."""
        return self._cluster_info_state_get('timeline') or 0

    def get_history(self, timeline: int) -> List[Union[Tuple[int, int, str], Tuple[int, int, str, str, str]]]:
        history_path = os.path.join(self.wal_dir, '{0:08X}.history'.format(timeline))
        history_mtime = mtime(history_path)
        history: List[Union[Tuple[int, int, str], Tuple[int, int, str, str, str]]] = []
        if history_mtime:
            try:
                with open(history_path, 'r') as f:
                    history_content = f.read()
                history = list(parse_history(history_content))
                if history[-1][0] == timeline - 1:
                    history_mtime = datetime.fromtimestamp(history_mtime).replace(tzinfo=tz.tzlocal())
                    history[-1] = history[-1][:3] + (history_mtime.isoformat(), self.name)
            except Exception:
                logger.exception('Failed to read and parse %s', (history_path,))
        return history

    def follow(self, member: Union[Leader, Member, None], role: PostgresqlRole = PostgresqlRole.REPLICA,
               timeout: Optional[float] = None, do_reload: bool = False) -> Optional[bool]:
        """Reconfigure postgres to follow a new member or use different recovery parameters.

        Method may call `on_role_change` callback if role is changing.

        :param member: The member to follow
        :param role: The desired role, one of :class:`~misc.PostgresqlRole` values, normally
            :class:`~misc.PostgresqlRole.REPLICA`, but could also be a :class:`~misc.PostgresqlRole.STANDBY_LEADER`
        :param timeout: start timeout, how long should the `start()` method wait for postgres accepting connections
        :param do_reload: indicates that after updating postgresql.conf we just need to do a reload instead of restart

        :returns: True - if restart/reload were successfully performed,
                  False - if restart/reload failed
                  None - if nothing was done or if Postgres is still in starting state after `timeout` seconds."""

        if not self.ensure_major_version_is_known():
            return None

        recovery_params = self.config.build_recovery_params(member)
        self.config.write_recovery_conf(recovery_params)

        # When we demoting the primary or standby_leader to replica or promoting replica to a standby_leader
        # and we know for sure that postgres was already running before, we will only execute on_role_change
        # callback and prevent execution of on_restart/on_start callback.
        # If the role remains the same (replica or standby_leader), we will execute on_start or on_restart
        change_role = self.cb_called and \
            (self.role in (PostgresqlRole.PRIMARY, PostgresqlRole.DEMOTED)
             or not {PostgresqlRole.STANDBY_LEADER, PostgresqlRole.REPLICA} - {self.role, role})
        if change_role:
            self.__cb_pending = CallbackAction.NOOP

        ret = True
        if self.is_running():
            if do_reload:
                self.config.write_postgresql_conf()
                ret = self.reload(block_callbacks=change_role)
                if ret and change_role:
                    self.set_role(role)
            else:
                ret = self.restart(block_callbacks=change_role, role=role)
        else:
            ret = self.start(timeout=timeout, block_callbacks=change_role, role=role) or None

        if change_role:
            # TODO: postpone this until start completes, or maybe do even earlier
            self.call_nowait(CallbackAction.ON_ROLE_CHANGE)
        return ret

    def _wait_promote(self, wait_seconds: int) -> Optional[bool]:
        for _ in polling_loop(wait_seconds):
            data = self.controldata()
            if data.get('Database cluster state') == 'in production':
                self.set_role(PostgresqlRole.PRIMARY)
                return True

    def _pre_promote(self) -> bool:
        """
        Runs a fencing script after the leader lock is acquired but before the replica is promoted.
        If the script exits with a non-zero code, promotion does not happen and the leader key is removed from DCS.
        """

        cmd = self.config.get('pre_promote')
        if not cmd:
            return True

        ret = self.cancellable.call(shlex.split(cmd))
        if ret is not None:
            logger.info('pre_promote script `%s` exited with %s', cmd, ret)
        return ret == 0

    def _before_stop(self) -> None:
        """Synchronously run a script prior to stopping postgres."""

        cmd = self.config.get('before_stop')
        if cmd:
            self._do_before_stop(cmd)

    def _do_before_stop(self, cmd: str) -> None:
        try:
            ret = self.cancellable.call(shlex.split(cmd))
            if ret is not None:
                logger.info('before_stop script `%s` exited with %s', cmd, ret)
        except Exception as e:
            logger.error('Exception when calling `%s`: %r', cmd, e)

    def promote(self, wait_seconds: int, task: CriticalTask,
                before_promote: Optional[Callable[..., Any]] = None) -> Optional[bool]:
        if self.role in (PostgresqlRole.PROMOTED, PostgresqlRole.PRIMARY):
            return True

        ret = self._pre_promote()
        with task:
            if task.is_cancelled:
                return False
            task.complete(ret)

        if ret is False:
            return False

        if self.cancellable.is_cancelled:
            logger.info("PostgreSQL promote cancelled.")
            return False

        if before_promote is not None:
            before_promote()

        self.slots_handler.on_promote()
        self.mpp_handler.schedule_cache_rebuild()

        ret = self.pg_ctl('promote', '-W')
        if ret:
            self.set_role(PostgresqlRole.PROMOTED)
            self.call_nowait(CallbackAction.ON_ROLE_CHANGE)
            ret = self._wait_promote(wait_seconds)
        return ret

    @staticmethod
    def _wal_position(is_primary: bool, wal_position: int,
                      receive_lsn: Optional[int], replay_lsn: Optional[int]) -> int:
        return wal_position if is_primary else max(receive_lsn or 0, replay_lsn or 0)

    def timeline_wal_position(self) -> Tuple[int, int, Optional[int], Optional[int], Optional[int]]:
        """Get timeline and various wal positions.

        :returns: a tuple composed of 5 integers representing timeline, ``pg_current_wal_lsn()`` position
            on primary/the biggest value among receive and replay LSN on replicas, ``pg_control_checkpoint()``
            value on a standby leader, receive and replay LSN on replicas (if available).
        """
        # This method could be called from different threads (simultaneously with some other `_query` calls).
        # If it is called not from main thread we will create a new cursor to execute statement.
        if current_thread().ident == self.__thread_ident:
            timeline = self._cluster_info_state_get('timeline') or 0
            wal_position = self._cluster_info_state_get('wal_position') or 0
            replay_lsn = self.replay_lsn()
            receive_lsn = self.receive_lsn()
            pg_control_timeline = self._cluster_info_state_get('pg_control_timeline')
        else:
            timeline, wal_position, replay_lsn, receive_lsn, _, pg_control_timeline, _, write_location = \
                self._query(self.cluster_info_query)[0][:8]
            receive_lsn = max(receive_lsn or 0, write_location or 0)

        wal_position = self._wal_position(bool(timeline), wal_position, receive_lsn, replay_lsn)
        return timeline, wal_position, pg_control_timeline, receive_lsn, replay_lsn

    def postmaster_start_time(self) -> Optional[str]:
        try:
            sql = "SELECT " + self.POSTMASTER_START_TIME
            return self.query(sql, retry=current_thread().ident == self.__thread_ident)[0][0].isoformat(sep=' ')
        except psycopg.Error:
            return None

    def last_operation(self) -> int:
        return self._wal_position(self.is_primary(), self._cluster_info_state_get('wal_position') or 0,
                                  self.receive_lsn(), self.replay_lsn())

    def configure_server_parameters(self) -> None:
        self._major_version = self.get_major_version()
        self.config.setup_server_parameters()

    def ensure_major_version_is_known(self) -> bool:
        """Calls configure_server_parameters() if `_major_version` is not known

        :returns: `True` if `_major_version` is set, otherwise `False`"""

        if not self._major_version:
            self.configure_server_parameters()
        return self._major_version > 0

    def pg_wal_realpath(self) -> Dict[str, str]:
        """Returns a dict containing the symlink (key) and target (value) for the wal directory"""
        links: Dict[str, str] = {}
        for pg_wal_dir in ('pg_xlog', 'pg_wal'):
            pg_wal_path = os.path.join(self._data_dir, pg_wal_dir)
            if os.path.exists(pg_wal_path) and os.path.islink(pg_wal_path):
                pg_wal_realpath = os.path.realpath(pg_wal_path)
                links[pg_wal_path] = pg_wal_realpath
        return links

    def pg_tblspc_realpaths(self) -> Dict[str, str]:
        """Returns a dict containing the symlink (key) and target (values) for the tablespaces"""
        links: Dict[str, str] = {}
        pg_tblsp_dir = os.path.join(self._data_dir, 'pg_tblspc')
        if os.path.exists(pg_tblsp_dir):
            for tsdn in os.listdir(pg_tblsp_dir):
                pg_tsp_path = os.path.join(pg_tblsp_dir, tsdn)
                if parse_int(tsdn) and os.path.islink(pg_tsp_path):
                    pg_tsp_rpath = os.path.realpath(pg_tsp_path)
                    links[pg_tsp_path] = pg_tsp_rpath
        return links

    def move_data_directory(self) -> None:
        if os.path.isdir(self._data_dir) and not self.is_running():
            try:
                postfix = 'failed'

                # let's see if the wal directory is a symlink, in this case we
                # should move the target
                for (source, pg_wal_realpath) in self.pg_wal_realpath().items():
                    logger.info('renaming WAL directory and updating symlink: %s', pg_wal_realpath)
                    new_name = '{0}.{1}'.format(pg_wal_realpath, postfix)
                    if os.path.exists(new_name):
                        shutil.rmtree(new_name)
                    os.rename(pg_wal_realpath, new_name)
                    os.unlink(source)
                    os.symlink(new_name, source)

                # Move user defined tablespace directory
                for (source, pg_tsp_rpath) in self.pg_tblspc_realpaths().items():
                    logger.info('renaming user defined tablespace directory and updating symlink: %s', pg_tsp_rpath)
                    new_name = '{0}.{1}'.format(pg_tsp_rpath, postfix)
                    if os.path.exists(new_name):
                        shutil.rmtree(new_name)
                    os.rename(pg_tsp_rpath, new_name)
                    os.unlink(source)
                    os.symlink(new_name, source)

                new_name = '{0}.{1}'.format(self._data_dir.rstrip(os.sep), postfix)
                logger.info('renaming data directory to %s', new_name)
                if os.path.exists(new_name):
                    shutil.rmtree(new_name)
                os.rename(self._data_dir, new_name)
            except OSError:
                logger.exception("Could not rename data directory %s", self._data_dir)

    def remove_data_directory(self) -> None:
        self.set_role(PostgresqlRole.UNINITIALIZED)
        logger.info('Removing data directory: %s', self._data_dir)
        try:
            if os.path.islink(self._data_dir):
                os.unlink(self._data_dir)
            elif not os.path.exists(self._data_dir):
                return
            elif os.path.isfile(self._data_dir):
                os.remove(self._data_dir)
            elif os.path.isdir(self._data_dir):

                # let's see if wal directory is a symlink, in this case we
                # should clean the target
                for pg_wal_realpath in self.pg_wal_realpath().values():
                    logger.info('Removing WAL directory: %s', pg_wal_realpath)
                    shutil.rmtree(pg_wal_realpath)

                # Remove user defined tablespace directories
                for pg_tsp_rpath in self.pg_tblspc_realpaths().values():
                    logger.info('Removing user defined tablespace directory: %s', pg_tsp_rpath)
                    shutil.rmtree(pg_tsp_rpath, ignore_errors=True)

                shutil.rmtree(self._data_dir)
        except (IOError, OSError):
            logger.exception('Could not remove data directory %s', self._data_dir)
            self.move_data_directory()

    def schedule_sanity_checks_after_pause(self) -> None:
        """
            After coming out of pause we have to:
            1. configure server parameters if necessary
            2. sync replication slots, because it might happen that slots were removed
            3. get new 'Database system identifier' to make sure that it wasn't changed
        """
        self.ensure_major_version_is_known()
        self.slots_handler.schedule()
        self.mpp_handler.schedule_cache_rebuild()
        self._sysid = ''

    def _get_gucs(self) -> CaseInsensitiveSet:
        """Get all available GUCs based on ``postgres --describe-config`` output.

        :returns: all available GUCs in the local Postgres server.
        """
        cmd = [self.pgcommand('postgres'), '--describe-config']
        return CaseInsensitiveSet({
            line.split('\t')[0] for line in subprocess.check_output(cmd).decode('utf-8').strip().split('\n')
        })
