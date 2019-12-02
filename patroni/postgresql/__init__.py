import logging
import os
import psycopg2
import shlex
import shutil
import subprocess
import time

from contextlib import contextmanager
from copy import deepcopy
from patroni.postgresql.callback_executor import CallbackExecutor
from patroni.postgresql.bootstrap import Bootstrap
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.connection import Connection, get_connection_cursor
from patroni.postgresql.misc import parse_history, postgres_major_version_to_int
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.postgresql.slots import SlotsHandler
from patroni.exceptions import PostgresConnectionException
from patroni.utils import Retry, RetryFailedError, polling_loop
from threading import current_thread, Lock


logger = logging.getLogger(__name__)

ACTION_ON_START = "on_start"
ACTION_ON_STOP = "on_stop"
ACTION_ON_RESTART = "on_restart"
ACTION_ON_RELOAD = "on_reload"
ACTION_ON_ROLE_CHANGE = "on_role_change"
ACTION_NOOP = "noop"

STATE_RUNNING = 'running'
STATE_REJECT = 'rejecting connections'
STATE_NO_RESPONSE = 'not responding'
STATE_UNKNOWN = 'unknown'

STOP_POLLING_INTERVAL = 1


@contextmanager
def null_context():
    yield


class Postgresql(object):

    def __init__(self, config):
        self.name = config['name']
        self.scope = config['scope']
        self._data_dir = config['data_dir']
        self._database = config.get('database', 'postgres')
        self._version_file = os.path.join(self._data_dir, 'PG_VERSION')
        self._pg_control = os.path.join(self._data_dir, 'global', 'pg_control')
        self._major_version = self.get_major_version()

        self._state_lock = Lock()
        self.set_state('stopped')

        self._pending_restart = False
        self._connection = Connection()
        self.config = ConfigHandler(self, config)
        self.config.check_directories()

        self._bin_dir = config.get('bin_dir') or ''
        self.bootstrap = Bootstrap(self)
        self.bootstrapping = False
        self.__thread_ident = current_thread().ident

        self.slots_handler = SlotsHandler(self)

        self._callback_executor = CallbackExecutor()
        self.__cb_called = False
        self.__cb_pending = None

        self.cancellable = CancellableSubprocess()

        self._sysid = None
        self.retry = Retry(max_tries=-1, deadline=config['retry_timeout']/2.0, max_delay=1,
                           retry_exceptions=PostgresConnectionException)

        # Retry 'pg_is_in_recovery()' only once
        self._is_leader_retry = Retry(max_tries=1, deadline=config['retry_timeout']/2.0, max_delay=1,
                                      retry_exceptions=PostgresConnectionException)

        self._role_lock = Lock()
        self.set_role(self.get_postgres_role_from_data_directory())
        self._state_entry_timestamp = None

        self._cluster_info_state = {}
        self._cached_replica_timeline = None

        # Last known running process
        self._postmaster_proc = None

        if self.is_running():
            self.set_state('running')
            self.set_role('master' if self.is_leader() else 'replica')
            self.config.write_postgresql_conf()  # we are "joining" already running postgres
            hba_saved = self.config.replace_pg_hba()
            ident_saved = self.config.replace_pg_ident()
            if hba_saved or ident_saved:
                self.reload()
        elif self.role == 'master':
            self.set_role('demoted')

    @property
    def create_replica_methods(self):
        return self.config.get('create_replica_methods', []) or self.config.get('create_replica_method', [])

    @property
    def major_version(self):
        return self._major_version

    @property
    def database(self):
        return self._database

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def callback(self):
        return self.config.get('callbacks') or {}

    @property
    def wal_name(self):
        return 'wal' if self._major_version >= 100000 else 'xlog'

    @property
    def lsn_name(self):
        return 'lsn' if self._major_version >= 100000 else 'location'

    @property
    def cluster_info_query(self):
        return ("SELECT CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 "
                "ELSE ('x' || pg_catalog.substr(pg_catalog.pg_{0}file_name("
                "pg_catalog.pg_current_{0}_{1}()), 1, 8))::bit(32)::int END, "
                "CASE WHEN pg_catalog.pg_is_in_recovery() THEN GREATEST("
                " pg_catalog.pg_{0}_{1}_diff(COALESCE("
                "pg_catalog.pg_last_{0}_receive_{1}(), '0/0'), '0/0')::bigint,"
                " pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_last_{0}_replay_{1}(), '0/0')::bigint)"
                "ELSE pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_current_{0}_{1}(), '0/0')::bigint "
                "END").format(self.wal_name, self.lsn_name)

    def _version_file_exists(self):
        return not self.data_directory_empty() and os.path.isfile(self._version_file)

    def get_major_version(self):
        if self._version_file_exists():
            try:
                with open(self._version_file) as f:
                    return postgres_major_version_to_int(f.read().strip())
            except Exception:
                logger.exception('Failed to read PG_VERSION from %s', self._data_dir)
        return 0

    def pgcommand(self, cmd):
        """Returns path to the specified PostgreSQL command"""
        return os.path.join(self._bin_dir, cmd)

    def pg_ctl(self, cmd, *args, **kwargs):
        """Builds and executes pg_ctl command

        :returns: `!True` when return_code == 0, otherwise `!False`"""

        pg_ctl = [self.pgcommand('pg_ctl'), cmd]
        return subprocess.call(pg_ctl + ['-D', self._data_dir] + list(args), **kwargs) == 0

    def pg_isready(self):
        """Runs pg_isready to see if PostgreSQL is accepting connections.

        :returns: 'ok' if PostgreSQL is up, 'reject' if starting up, 'no_resopnse' if not up."""

        r = self.config.local_connect_kwargs
        cmd = [self.pgcommand('pg_isready'), '-p', r['port'], '-d', self._database]

        # Host is not set if we are connecting via default unix socket
        if 'host' in r:
            cmd.extend(['-h', r['host']])

        # We only need the username because pg_isready does not try to authenticate
        if 'user' in r:
            cmd.extend(['-U', r['user']])

        ret = subprocess.call(cmd)
        return_codes = {0: STATE_RUNNING,
                        1: STATE_REJECT,
                        2: STATE_NO_RESPONSE,
                        3: STATE_UNKNOWN}
        return return_codes.get(ret, STATE_UNKNOWN)

    def reload_config(self, config, sighup=False):
        self.config.reload_config(config, sighup)
        self._is_leader_retry.deadline = self.retry.deadline = config['retry_timeout']/2.0

    @property
    def pending_restart(self):
        return self._pending_restart

    def set_pending_restart(self, value):
        self._pending_restart = value

    @property
    def sysid(self):
        if not self._sysid and not self.bootstrapping:
            data = self.controldata()
            self._sysid = data.get('Database system identifier', "")
        return self._sysid

    def get_postgres_role_from_data_directory(self):
        if self.data_directory_empty() or not self.controldata():
            return 'uninitialized'
        elif self.config.recovery_conf_exists():
            return 'replica'
        else:
            return 'master'

    @property
    def server_version(self):
        return self._connection.server_version

    def connection(self):
        return self._connection.get()

    def set_connection_kwargs(self, kwargs):
        self._connection.set_conn_kwargs(kwargs)

    def _query(self, sql, *params):
        """We are always using the same cursor, therefore this method is not thread-safe!!!
        You can call it from different threads only if you are holding explicit `AsyncExecutor` lock,
        because the main thread is always holding this lock when running HA cycle."""
        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql, params)
            return cursor
        except psycopg2.Error as e:
            if cursor and cursor.connection.closed == 0:
                # When connected via unix socket, psycopg2 can't recoginze 'connection lost'
                # and leaves `_cursor_holder.connection.closed == 0`, but psycopg2.OperationalError
                # is still raised (what is correct). It doesn't make sense to continiue with existing
                # connection and we will close it, to avoid its reuse by the `cursor` method.
                if isinstance(e, psycopg2.OperationalError):
                    self._connection.close()
                else:
                    raise e
            if self.state == 'restarting':
                raise RetryFailedError('cluster is being restarted')
            raise PostgresConnectionException('connection problems')

    def query(self, sql, *args, **kwargs):
        if not kwargs.get('retry', True):
            return self._query(sql, *args)
        try:
            return self.retry(self._query, sql, *args)
        except RetryFailedError as e:
            raise PostgresConnectionException(str(e))

    def pg_control_exists(self):
        return os.path.isfile(self._pg_control)

    def data_directory_empty(self):
        if self.pg_control_exists():
            return False
        if not os.path.exists(self._data_dir):
            return True
        return all(os.name != 'nt' and (n.startswith('.') or n == 'lost+found') for n in os.listdir(self._data_dir))

    def replica_method_options(self, method):
        return deepcopy(self.config.get(method, {}))

    def replica_method_can_work_without_replication_connection(self, method):
        return method != 'basebackup' and self.replica_method_options(method).get('no_master')

    def can_create_replica_without_replication_connection(self, replica_methods=None):
        """ go through the replication methods to see if there are ones
            that does not require a working replication connection.
        """
        if replica_methods is None:
            replica_methods = self.create_replica_methods
        return any(self.replica_method_can_work_without_replication_connection(m) for m in replica_methods)

    def reset_cluster_info_state(self):
        self._cluster_info_state = {}

    def _cluster_info_state_get(self, name):
        if not self._cluster_info_state:
            try:
                result = self._is_leader_retry(self._query, self.cluster_info_query).fetchone()
                self._cluster_info_state = dict(zip(['timeline', 'wal_position'], result))
            except RetryFailedError as e:  # SELECT failed two times
                self._cluster_info_state = {'error': str(e)}
                if not self.is_starting() and self.pg_isready() == STATE_REJECT:
                    self.set_state('starting')

        if 'error' in self._cluster_info_state:
            raise PostgresConnectionException(self._cluster_info_state['error'])

        return self._cluster_info_state.get(name)

    def is_leader(self):
        return bool(self._cluster_info_state_get('timeline'))

    def is_running(self):
        """Returns PostmasterProcess if one is running on the data directory or None. If most recently seen process
        is running updates the cached process based on pid file."""
        if self._postmaster_proc:
            if self._postmaster_proc.is_running():
                return self._postmaster_proc
            self._postmaster_proc = None

        # we noticed that postgres was restarted, force syncing of replication
        self.slots_handler.schedule()

        self._postmaster_proc = PostmasterProcess.from_pidfile(self._data_dir)
        return self._postmaster_proc

    @property
    def cb_called(self):
        return self.__cb_called

    def call_nowait(self, cb_name):
        """ pick a callback command and call it without waiting for it to finish """
        if self.bootstrapping:
            return
        if cb_name in (ACTION_ON_START, ACTION_ON_STOP, ACTION_ON_RESTART, ACTION_ON_ROLE_CHANGE):
            self.__cb_called = True

        if self.callback and cb_name in self.callback:
            cmd = self.callback[cb_name]
            try:
                cmd = shlex.split(self.callback[cb_name]) + [cb_name, self.role, self.scope]
                self._callback_executor.call(cmd)
            except Exception:
                logger.exception('callback %s %s %s %s failed', cmd, cb_name, self.role, self.scope)

    @property
    def role(self):
        with self._role_lock:
            return self._role

    def set_role(self, value):
        with self._role_lock:
            self._role = value

    @property
    def state(self):
        with self._state_lock:
            return self._state

    def set_state(self, value):
        with self._state_lock:
            self._state = value
            self._state_entry_timestamp = time.time()

    def time_in_state(self):
        return time.time() - self._state_entry_timestamp

    def is_starting(self):
        return self.state == 'starting'

    def wait_for_port_open(self, postmaster, timeout):
        """Waits until PostgreSQL opens ports."""
        for _ in polling_loop(timeout):
            if self.cancellable.is_cancelled:
                return False

            if not postmaster.is_running():
                logger.error('postmaster is not running')
                self.set_state('start failed')
                return False

            isready = self.pg_isready()
            if isready != STATE_NO_RESPONSE:
                if isready not in [STATE_REJECT, STATE_RUNNING]:
                    logger.warning("Can't determine PostgreSQL startup status, assuming running")
                return True

        logger.warning("Timed out waiting for PostgreSQL to start")
        return False

    def start(self, timeout=None, task=None, block_callbacks=False, role=None):
        """Start PostgreSQL

        Waits for postmaster to open ports or terminate so pg_isready can be used to check startup completion
        or failure.

        :returns: True if start was initiated and postmaster ports are open, False if start failed"""
        # make sure we close all connections established against
        # the former node, otherwise, we might get a stalled one
        # after kill -9, which would report incorrect data to
        # patroni.
        self._connection.close()

        if self.is_running():
            logger.error('Cannot start PostgreSQL because one is already running.')
            self.set_state('starting')
            return True

        if not block_callbacks:
            self.__cb_pending = ACTION_ON_START

        self.set_role(role or self.get_postgres_role_from_data_directory())

        self.set_state('starting')
        self._pending_restart = False

        configuration = self.config.effective_configuration
        self.config.check_directories()
        self.config.write_postgresql_conf(configuration)
        self.config.resolve_connection_addresses()
        self.config.replace_pg_hba()
        self.config.replace_pg_ident()

        options = ['--{0}={1}'.format(p, configuration[p]) for p in self.config.CMDLINE_OPTIONS
                   if p in configuration and p != 'wal_keep_segments']

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
                start_timeout = float(self.config.get('pg_ctl_timeout', 60))
            except ValueError:
                start_timeout = 60

        # We want postmaster to open ports before we continue
        if not self._postmaster_proc or not self.wait_for_port_open(self._postmaster_proc, start_timeout):
            return False

        ret = self.wait_for_startup(start_timeout)
        if ret is not None:
            return ret
        elif timeout is not None:
            return False
        else:
            return None

    def checkpoint(self, connect_kwargs=None):
        check_not_is_in_recovery = connect_kwargs is not None
        connect_kwargs = connect_kwargs or self.config.local_connect_kwargs
        for p in ['connect_timeout', 'options']:
            connect_kwargs.pop(p, None)
        try:
            with get_connection_cursor(**connect_kwargs) as cur:
                cur.execute("SET statement_timeout = 0")
                if check_not_is_in_recovery:
                    cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                    if cur.fetchone()[0]:
                        return 'is_in_recovery=true'
                return cur.execute('CHECKPOINT')
        except psycopg2.Error:
            logger.exception('Exception during CHECKPOINT')
            return 'not accessible or not healty'

    def stop(self, mode='fast', block_callbacks=False, checkpoint=None, on_safepoint=None):
        """Stop PostgreSQL

        Supports a callback when a safepoint is reached. A safepoint is when no user backend can return a successful
        commit to users. Currently this means we wait for user backends to close. But in the future alternate mechanisms
        could be added.

        :param on_safepoint: This callback is called when no user backends are running.
        """
        if checkpoint is None:
            checkpoint = False if mode == 'immediate' else True

        success, pg_signaled = self._do_stop(mode, block_callbacks, checkpoint, on_safepoint)
        if success:
            # block_callbacks is used during restart to avoid
            # running start/stop callbacks in addition to restart ones
            if not block_callbacks:
                self.set_state('stopped')
                if pg_signaled:
                    self.call_nowait(ACTION_ON_STOP)
        else:
            logger.warning('pg_ctl stop failed')
            self.set_state('stop failed')
        return success

    def _do_stop(self, mode, block_callbacks, checkpoint, on_safepoint):
        postmaster = self.is_running()
        if not postmaster:
            if on_safepoint:
                on_safepoint()
            return True, False

        if checkpoint and not self.is_starting():
            self.checkpoint()

        if not block_callbacks:
            self.set_state('stopping')

        # Send signal to postmaster to stop
        success = postmaster.signal_stop(mode)
        if success is not None:
            if success and on_safepoint:
                on_safepoint()
            return success, True

        # We can skip safepoint detection if we don't have a callback
        if on_safepoint:
            # Wait for our connection to terminate so we can be sure that no new connections are being initiated
            self._wait_for_connection_close(postmaster)
            postmaster.wait_for_user_backends_to_close()
            on_safepoint()

        postmaster.wait()

        return True, True

    @staticmethod
    def terminate_starting_postmaster(postmaster):
        """Terminates a postmaster that has not yet opened ports or possibly even written a pid file. Blocks
        until the process goes away."""
        postmaster.signal_stop('immediate')
        postmaster.wait()

    def _wait_for_connection_close(self, postmaster):
        try:
            with self.connection().cursor() as cur:
                while postmaster.is_running():  # Need a timeout here?
                    cur.execute("SELECT 1")
                    time.sleep(STOP_POLLING_INTERVAL)
        except psycopg2.Error:
            pass

    def reload(self):
        ret = self.pg_ctl('reload')
        if ret:
            self.call_nowait(ACTION_ON_RELOAD)
        return ret

    def check_for_startup(self):
        """Checks PostgreSQL status and returns if PostgreSQL is in the middle of startup."""
        return self.is_starting() and not self.check_startup_state_changed()

    def check_startup_state_changed(self):
        """Checks if PostgreSQL has completed starting up or failed or still starting.

        Should only be called when state == 'starting'

        :returns: True if state was changed from 'starting'
        """
        ready = self.pg_isready()

        if ready == STATE_REJECT:
            return False
        elif ready == STATE_NO_RESPONSE:
            ret = not self.is_running()
            if ret:
                self.set_state('start failed')
                self.slots_handler.schedule(False)  # TODO: can remove this?
                self.config.save_configuration_files(True)  # TODO: maybe remove this?
            return ret
        else:
            if ready != STATE_RUNNING:
                # Bad configuration or unexpected OS error. No idea of PostgreSQL status.
                # Let the main loop of run cycle clean up the mess.
                logger.warning("%s status returned from pg_isready",
                               "Unknown" if ready == STATE_UNKNOWN else "Invalid")
            self.set_state('running')
            self.slots_handler.schedule()
            self.config.save_configuration_files(True)
            # TODO: __cb_pending can be None here after PostgreSQL restarts on its own. Do we want to call the callback?
            # Previously we didn't even notice.
            action = self.__cb_pending or ACTION_ON_START
            self.call_nowait(action)
            self.__cb_pending = None

            return True

    def wait_for_startup(self, timeout=None):
        """Waits for PostgreSQL startup to complete or fail.

        :returns: True if start was successful, False otherwise"""
        if not self.is_starting():
            # Should not happen
            logger.warning("wait_for_startup() called when not in starting state")

        while not self.check_startup_state_changed():
            if self.cancellable.is_cancelled or timeout and self.time_in_state() > timeout:
                return None
            time.sleep(1)

        return self.state == 'running'

    def restart(self, timeout=None, task=None, block_callbacks=False, role=None):
        """Restarts PostgreSQL.

        When timeout parameter is set the call will block either until PostgreSQL has started, failed to start or
        timeout arrives.

        :returns: True when restart was successful and timeout did not expire when waiting.
        """
        self.set_state('restarting')
        if not block_callbacks:
            self.__cb_pending = ACTION_ON_RESTART
        ret = self.stop(block_callbacks=True) and self.start(timeout, task, True, role)
        if not ret and not self.is_starting():
            self.set_state('restart failed ({0})'.format(self.state))
        return ret

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def controldata(self):
        """ return the contents of pg_controldata, or non-True value if pg_controldata call failed """
        result = {}
        # Don't try to call pg_controldata during backup restore
        if self._version_file_exists() and self.state != 'creating replica':
            try:
                env = os.environ.copy()
                env.update(LANG='C', LC_ALL='C')
                data = subprocess.check_output([self.pgcommand('pg_controldata'), self._data_dir], env=env)
                if data:
                    data = data.decode('utf-8').splitlines()
                    # pg_controldata output depends on major verion. Some of parameters are prefixed by 'Current '
                    result = {l.split(':')[0].replace('Current ', '', 1): l.split(':', 1)[1].strip() for l in data
                              if l and ':' in l}
            except subprocess.CalledProcessError:
                logger.exception("Error when calling pg_controldata")
        return result

    @contextmanager
    def get_replication_connection_cursor(self, host='localhost', port=5432, database=None, **kwargs):
        conn_kwargs = self.config.replication.copy()
        conn_kwargs.update(host=host, port=int(port), database=database or self._database, connect_timeout=3,
                           user=conn_kwargs.pop('username'), replication=1, options='-c statement_timeout=2000')
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def get_local_timeline_lsn_from_replication_connection(self):
        timeline = lsn = None
        try:
            with self.get_replication_connection_cursor(**self.config.local_replication_address) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                timeline, lsn = cur.fetchone()[1:3]
        except Exception:
            logger.exception('Can not fetch local timeline and lsn from replication connection')
        return timeline, lsn

    def get_replica_timeline(self):
        return self.get_local_timeline_lsn_from_replication_connection()[0]

    def replica_cached_timeline(self, master_timeline):
        if not self._cached_replica_timeline or not master_timeline or self._cached_replica_timeline != master_timeline:
            self._cached_replica_timeline = self.get_replica_timeline()
        return self._cached_replica_timeline

    def get_master_timeline(self):
        return self._cluster_info_state_get('timeline')

    def get_history(self, timeline):
        history_path = 'pg_{0}/{1:08X}.history'.format(self.wal_name, timeline)
        try:
            cursor = self._connection.cursor()
            cursor.execute('SELECT isdir, modification FROM pg_catalog.pg_stat_file(%s)', (history_path,))
            isdir, modification = cursor.fetchone()
            if not isdir:
                cursor.execute('SELECT pg_catalog.pg_read_file(%s)', (history_path,))
                history = list(parse_history(cursor.fetchone()[0]))
                if history[-1][0] == timeline - 1:
                    history[-1].append(modification.isoformat())
                return history
        except Exception:
            logger.exception('Failed to read and parse %s', (history_path,))

    def follow(self, member, role='replica', timeout=None, do_reload=False):
        recovery_params = self.config.build_recovery_params(member)
        self.config.write_recovery_conf(recovery_params)

        # When we demoting the master or standby_leader to replica or promoting replica to a standby_leader
        # and we know for sure that postgres was already running before, we will only execute on_role_change
        # callback and prevent execution of on_restart/on_start callback.
        # If the role remains the same (replica or standby_leader), we will execute on_start or on_restart
        change_role = self.cb_called and (self.role in ('master', 'demoted') or
                                          not {'standby_leader', 'replica'} - {self.role, role})
        if change_role:
            self.__cb_pending = ACTION_NOOP

        if self.is_running():
            if do_reload:
                self.config.write_postgresql_conf()
                self.reload()
            else:
                self.restart(block_callbacks=change_role, role=role)
        else:
            self.start(timeout=timeout, block_callbacks=change_role, role=role)

        if change_role:
            # TODO: postpone this until start completes, or maybe do even earlier
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return True

    def _wait_promote(self, wait_seconds):
        for _ in polling_loop(wait_seconds):
            data = self.controldata()
            if data.get('Database cluster state') == 'in production':
                return True

    def promote(self, wait_seconds, on_success=None, access_is_restricted=False):
        if self.role == 'master':
            return True
        ret = self.pg_ctl('promote', '-W')
        if ret:
            self.set_role('master')
            if on_success is not None:
                on_success()
            if not access_is_restricted:
                self.call_nowait(ACTION_ON_ROLE_CHANGE)
            ret = self._wait_promote(wait_seconds)
        return ret

    def timeline_wal_position(self):
        # This method could be called from different threads (simultaneously with some other `_query` calls).
        # If it is called not from main thread we will create a new cursor to execute statement.
        if current_thread().ident == self.__thread_ident:
            return self._cluster_info_state_get('timeline'), self._cluster_info_state_get('wal_position')

        with self.connection().cursor() as cursor:
            cursor.execute(self.cluster_info_query)
            return cursor.fetchone()[:2]

    def postmaster_start_time(self):
        try:
            query = "SELECT pg_catalog.to_char(pg_catalog.pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')"
            if current_thread().ident == self.__thread_ident:
                return self.query(query).fetchone()[0]
            with self.connection().cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0]
        except psycopg2.Error:
            return None

    def last_operation(self):
        return str(self._cluster_info_state_get('wal_position'))

    def configure_server_parameters(self):
        self._major_version = self.get_major_version()
        self.config.setup_server_parameters()
        return True

    def move_data_directory(self):
        if os.path.isdir(self._data_dir) and not self.is_running():
            try:
                new_name = '{0}_{1}'.format(self._data_dir, time.strftime('%Y-%m-%d-%H-%M-%S'))
                logger.info('renaming data directory to %s', new_name)
                os.rename(self._data_dir, new_name)
            except OSError:
                logger.exception("Could not rename data directory %s", self._data_dir)

    def remove_data_directory(self):
        self.set_role('uninitialized')
        logger.info('Removing data directory: %s', self._data_dir)
        try:
            if os.path.islink(self._data_dir):
                os.unlink(self._data_dir)
            elif not os.path.exists(self._data_dir):
                return
            elif os.path.isfile(self._data_dir):
                os.remove(self._data_dir)
            elif os.path.isdir(self._data_dir):

                # let's see if pg_xlog|pg_wal is a symlink, in this case we
                # should clean the target
                for pg_wal_dir in ('pg_xlog', 'pg_wal'):
                    pg_wal_path = os.path.join(self._data_dir, pg_wal_dir)
                    if os.path.exists(pg_wal_path) and os.path.islink(pg_wal_path):
                        pg_wal_realpath = os.path.realpath(pg_wal_path)
                        logger.info('Removing WAL directory: %s', pg_wal_realpath)
                        shutil.rmtree(pg_wal_realpath)

                shutil.rmtree(self._data_dir)
        except (IOError, OSError):
            logger.exception('Could not remove data directory %s', self._data_dir)
            self.move_data_directory()

    def pick_synchronous_standby(self, cluster):
        """Finds the best candidate to be the synchronous standby.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.

        :returns tuple of candidate name or None, and bool showing if the member is the active synchronous standby.
        """
        current = cluster.sync.sync_standby
        current = current.lower() if current else current
        members = {m.name.lower(): m for m in cluster.members}
        candidates = []
        # Pick candidates based on who has flushed WAL farthest.
        # TODO: for synchronous_commit = remote_write we actually want to order on write_location
        for app_name, state, sync_state in self.query(
                "SELECT pg_catalog.lower(application_name), state, sync_state"
                " FROM pg_catalog.pg_stat_replication"
                " ORDER BY flush_{0} DESC".format(self.lsn_name)):
            member = members.get(app_name)
            if state != 'streaming' or not member or member.tags.get('nosync', False):
                continue
            if sync_state == 'sync':
                return app_name, True
            if sync_state == 'potential' and app_name == current:
                # Prefer current even if not the best one any more to avoid indecisivness and spurious swaps.
                return cluster.sync.sync_standby, False
            if sync_state in ('async', 'potential'):
                candidates.append(member.name)

        if candidates:
            return candidates[0], False
        return None, False

    def read_postmaster_opts(self):
        """returns the list of option names/values from postgres.opts, Empty dict if read failed or no file"""
        result = {}
        try:
            with open(os.path.join(self._data_dir, 'postmaster.opts')) as f:
                data = f.read()
                for opt in data.split('" "'):
                    if '=' in opt and opt.startswith('--'):
                        name, val = opt.split('=', 1)
                        result[name.strip('-')] = val.rstrip('"\n')
        except IOError:
            logger.exception('Error when reading postmaster.opts')
        return result

    def single_user_mode(self, command=None, options=None):
        """run a given command in a single-user mode. If the command is empty - then just start and stop"""
        cmd = [self.pgcommand('postgres'), '--single', '-D', self._data_dir]
        for opt, val in sorted((options or {}).items()):
            cmd.extend(['-c', '{0}={1}'.format(opt, val)])
        # need a database name to connect
        cmd.append(self._database)
        return self.cancellable.call(cmd, communicate_input=command)

    def cleanup_archive_status(self):
        status_dir = os.path.join(self._data_dir, 'pg_' + self.wal_name, 'archive_status')
        try:
            for f in os.listdir(status_dir):
                path = os.path.join(status_dir, f)
                try:
                    if os.path.islink(path):
                        os.unlink(path)
                    elif os.path.isfile(path):
                        os.remove(path)
                except OSError:
                    logger.exception('Unable to remove %s', path)
        except OSError:
            logger.exception('Unable to list %s', status_dir)

    def fix_cluster_state(self):
        self.cleanup_archive_status()

        # Start in a single user mode and stop to produce a clean shutdown
        opts = self.read_postmaster_opts()
        opts.update({'archive_mode': 'on', 'archive_command': 'false'})
        self.config.remove_recovery_conf()
        return self.single_user_mode(options=opts) == 0 or None

    def schedule_sanity_checks_after_pause(self):
        """
            After coming out of pause we have to:
            1. sync replication slots, because it might happen that slots were removed
            2. get new 'Database system identifier' to make sure that it wasn't changed
        """
        self.slots_handler.schedule()
        self._sysid = None
