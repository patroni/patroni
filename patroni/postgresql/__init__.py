import logging
import os
import psycopg2
import shlex
import shutil
import subprocess
import time

from contextlib import contextmanager
from copy import deepcopy
from dateutil import tz
from datetime import datetime
from patroni.postgresql.callback_executor import CallbackExecutor
from patroni.postgresql.bootstrap import Bootstrap
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.postgresql.config import ConfigHandler, mtime
from patroni.postgresql.connection import Connection, get_connection_cursor
from patroni.postgresql.misc import parse_history, parse_lsn, postgres_major_version_to_int
from patroni.postgresql.postmaster import PostmasterProcess
from patroni.postgresql.slots import SlotsHandler
from patroni.exceptions import PostgresConnectionException
from patroni.utils import Retry, RetryFailedError, polling_loop, data_directory_is_empty, parse_int
from psutil import TimeoutExpired
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

    POSTMASTER_START_TIME = "pg_catalog.to_char(pg_catalog.pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')"
    TL_LSN = ("CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 "
              "ELSE ('x' || pg_catalog.substr(pg_catalog.pg_{0}file_name("
              "pg_catalog.pg_current_{0}_{1}()), 1, 8))::bit(32)::int END, "  # master timeline
              "CASE WHEN pg_catalog.pg_is_in_recovery() THEN 0 "
              "ELSE pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_current_{0}_{1}(), '0/0')::bigint END, "  # write_lsn
              "pg_catalog.pg_{0}_{1}_diff(pg_catalog.pg_last_{0}_replay_{1}(), '0/0')::bigint,  "
              "pg_catalog.pg_{0}_{1}_diff(COALESCE(pg_catalog.pg_last_{0}_receive_{1}(), '0/0'), '0/0')::bigint, "
              "pg_catalog.pg_is_in_recovery() AND pg_catalog.pg_is_{0}_replay_paused()")

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
    def wal_dir(self):
        return os.path.join(self._data_dir, 'pg_' + self.wal_name)

    @property
    def wal_name(self):
        return 'wal' if self._major_version >= 100000 else 'xlog'

    @property
    def lsn_name(self):
        return 'lsn' if self._major_version >= 100000 else 'location'

    @property
    def cluster_info_query(self):
        if self._major_version >= 90600:
            extra = (", CASE WHEN latest_end_lsn IS NULL THEN NULL ELSE received_tli END,"
                     " slot_name, conninfo FROM pg_catalog.pg_stat_get_wal_receiver()")
            if self.role == 'standby_leader':
                extra = "timeline_id" + extra + ", pg_catalog.pg_control_checkpoint()"
            else:
                extra = "0" + extra
        else:
            extra = "0, NULL, NULL, NULL"

        return ("SELECT " + self.TL_LSN + ", {2}").format(self.wal_name, self.lsn_name, extra)

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
        return data_directory_is_empty(self._data_dir)

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
                self._cluster_info_state = dict(zip(['timeline', 'wal_position', 'replayed_location',
                                                     'received_location', 'replay_paused', 'pg_control_timeline',
                                                     'received_tli', 'slot_name', 'conninfo'], result))
            except RetryFailedError as e:  # SELECT failed two times
                self._cluster_info_state = {'error': str(e)}
                if not self.is_starting() and self.pg_isready() == STATE_REJECT:
                    self.set_state('starting')

        if 'error' in self._cluster_info_state:
            raise PostgresConnectionException(self._cluster_info_state['error'])

        return self._cluster_info_state.get(name)

    def replayed_location(self):
        return self._cluster_info_state_get('replayed_location')

    def received_location(self):
        return self._cluster_info_state_get('received_location')

    def primary_slot_name(self):
        return self._cluster_info_state_get('slot_name')

    def primary_conninfo(self):
        return self._cluster_info_state_get('conninfo')

    def received_timeline(self):
        return self._cluster_info_state_get('received_tli')

    def is_leader(self):
        return bool(self._cluster_info_state_get('timeline'))

    def pg_control_timeline(self):
        try:
            return int(self.controldata().get("Latest checkpoint's TimeLineID"))
        except (TypeError, ValueError):
            logger.exception('Failed to parse timeline from pg_controldata output')

    def latest_checkpoint_location(self):
        """Returns checkpoint location for the cleanly shut down primary"""

        data = self.controldata()
        lsn = data.get('Latest checkpoint location')
        if data.get('Database cluster state') == 'shut down' and lsn:
            try:
                return str(parse_lsn(lsn))
            except (IndexError, ValueError) as e:
                logger.error('Exception when parsing lsn %s: %r', lsn, e)

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

        try:
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

    def checkpoint(self, connect_kwargs=None, timeout=None):
        check_not_is_in_recovery = connect_kwargs is not None
        connect_kwargs = connect_kwargs or self.config.local_connect_kwargs
        for p in ['connect_timeout', 'options']:
            connect_kwargs.pop(p, None)
        if timeout:
            connect_kwargs['connect_timeout'] = timeout
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

    def stop(self, mode='fast', block_callbacks=False, checkpoint=None, on_safepoint=None, stop_timeout=None):
        """Stop PostgreSQL

        Supports a callback when a safepoint is reached. A safepoint is when no user backend can return a successful
        commit to users. Currently this means we wait for user backends to close. But in the future alternate mechanisms
        could be added.

        :param on_safepoint: This callback is called when no user backends are running.
        """
        if checkpoint is None:
            checkpoint = False if mode == 'immediate' else True

        success, pg_signaled = self._do_stop(mode, block_callbacks, checkpoint, on_safepoint, stop_timeout)
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

    def _do_stop(self, mode, block_callbacks, checkpoint, on_safepoint, stop_timeout):
        postmaster = self.is_running()
        if not postmaster:
            if on_safepoint:
                on_safepoint()
            return True, False

        if checkpoint and not self.is_starting():
            self.checkpoint(timeout=stop_timeout)

        if not block_callbacks:
            self.set_state('stopping')

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
            postmaster.wait_for_user_backends_to_close()
            on_safepoint()

        try:
            postmaster.wait(timeout=stop_timeout)
        except TimeoutExpired:
            logger.warning("Timeout during postmaster stop, aborting Postgres.")
            if not self.terminate_postmaster(postmaster, mode, stop_timeout):
                postmaster.wait()

        return True, True

    def terminate_postmaster(self, postmaster, mode, stop_timeout):
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

    def terminate_starting_postmaster(self, postmaster):
        """Terminates a postmaster that has not yet opened ports or possibly even written a pid file. Blocks
        until the process goes away."""
        postmaster.signal_stop('immediate', self.pgcommand('pg_ctl'))
        postmaster.wait()

    def _wait_for_connection_close(self, postmaster):
        try:
            with self.connection().cursor() as cur:
                while postmaster.is_running():  # Need a timeout here?
                    cur.execute("SELECT 1")
                    time.sleep(STOP_POLLING_INTERVAL)
        except psycopg2.Error:
            pass

    def reload(self, block_callbacks=False):
        ret = self.pg_ctl('reload')
        if ret and not block_callbacks:
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

    def get_guc_value(self, name):
        cmd = [self.pgcommand('postgres'), self._data_dir, '-C', name]
        try:
            data = subprocess.check_output(cmd)
            if data:
                return data.decode('utf-8').strip()
        except Exception as e:
            logger.error('Failed to execute %s: %r', cmd, e)

    def controldata(self):
        """ return the contents of pg_controldata, or non-True value if pg_controldata call failed """
        # Don't try to call pg_controldata during backup restore
        if self._version_file_exists() and self.state != 'creating replica':
            try:
                env = os.environ.copy()
                env.update(LANG='C', LC_ALL='C')
                data = subprocess.check_output([self.pgcommand('pg_controldata'), self._data_dir], env=env)
                if data:
                    data = filter(lambda e: ':' in e, data.decode('utf-8').splitlines())
                    # pg_controldata output depends on major version. Some of parameters are prefixed by 'Current '
                    return {k.replace('Current ', '', 1): v.strip() for k, v in map(lambda e: e.split(':', 1), data)}
            except subprocess.CalledProcessError:
                logger.exception("Error when calling pg_controldata")
        return {}

    @contextmanager
    def get_replication_connection_cursor(self, host='localhost', port=5432, **kwargs):
        conn_kwargs = self.config.replication.copy()
        conn_kwargs.update(host=host, port=int(port) if port else None, user=conn_kwargs.pop('username'),
                           connect_timeout=3, replication=1, options='-c statement_timeout=2000')
        with get_connection_cursor(**conn_kwargs) as cur:
            yield cur

    def get_replica_timeline(self):
        try:
            with self.get_replication_connection_cursor(**self.config.local_replication_address) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                return cur.fetchone()[1]
        except Exception:
            logger.exception('Can not fetch local timeline and lsn from replication connection')

    def replica_cached_timeline(self, master_timeline):
        if not self._cached_replica_timeline or not master_timeline or self._cached_replica_timeline != master_timeline:
            self._cached_replica_timeline = self.get_replica_timeline()
        return self._cached_replica_timeline

    def get_master_timeline(self):
        return self._cluster_info_state_get('timeline')

    def get_history(self, timeline):
        history_path = os.path.join(self.wal_dir, '{0:08X}.history'.format(timeline))
        history_mtime = mtime(history_path)
        if history_mtime:
            try:
                with open(history_path, 'r') as f:
                    history = f.read()
                history = list(parse_history(history))
                if history[-1][0] == timeline - 1:
                    history_mtime = datetime.fromtimestamp(history_mtime).replace(tzinfo=tz.tzlocal())
                    history[-1].append(history_mtime.isoformat())
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
                if self.reload(block_callbacks=change_role) and change_role:
                    self.set_role(role)
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

    def _pre_promote(self):
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

    def promote(self, wait_seconds, task, on_success=None, access_is_restricted=False):
        if self.role == 'master':
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

        ret = self.pg_ctl('promote', '-W')
        if ret:
            self.set_role('master')
            if on_success is not None:
                on_success()
            if not access_is_restricted:
                self.call_nowait(ACTION_ON_ROLE_CHANGE)
            ret = self._wait_promote(wait_seconds)
        return ret

    @staticmethod
    def _wal_position(is_leader, wal_position, received_location, replayed_location):
        return wal_position if is_leader else max(received_location or 0, replayed_location or 0)

    def timeline_wal_position(self):
        # This method could be called from different threads (simultaneously with some other `_query` calls).
        # If it is called not from main thread we will create a new cursor to execute statement.
        if current_thread().ident == self.__thread_ident:
            timeline = self._cluster_info_state_get('timeline')
            wal_position = self._cluster_info_state_get('wal_position')
            replayed_location = self.replayed_location()
            received_location = self.received_location()
            pg_control_timeline = self._cluster_info_state_get('pg_control_timeline')
        else:
            with self.connection().cursor() as cursor:
                cursor.execute(self.cluster_info_query)
                (timeline, wal_position, replayed_location,
                 received_location, _, pg_control_timeline) = cursor.fetchone()[:6]

        wal_position = self._wal_position(timeline, wal_position, received_location, replayed_location)
        return (timeline, wal_position, pg_control_timeline)

    def postmaster_start_time(self):
        try:
            query = "SELECT " + self.POSTMASTER_START_TIME
            if current_thread().ident == self.__thread_ident:
                return self.query(query).fetchone()[0]
            with self.connection().cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()[0]
        except psycopg2.Error:
            return None

    def last_operation(self):
        return str(self._wal_position(self.is_leader(), self._cluster_info_state_get('wal_position'),
                                      self.received_location(), self.replayed_location()))

    def configure_server_parameters(self):
        self._major_version = self.get_major_version()
        self.config.setup_server_parameters()
        return True

    def pg_wal_realpath(self):
        """Returns a dict containing the symlink (key) and target (value) for the wal directory"""
        links = {}
        for pg_wal_dir in ('pg_xlog', 'pg_wal'):
            pg_wal_path = os.path.join(self._data_dir, pg_wal_dir)
            if os.path.exists(pg_wal_path) and os.path.islink(pg_wal_path):
                pg_wal_realpath = os.path.realpath(pg_wal_path)
                links[pg_wal_path] = pg_wal_realpath
        return links

    def pg_tblspc_realpaths(self):
        """Returns a dict containing the symlink (key) and target (values) for the tablespaces"""
        links = {}
        pg_tblsp_dir = os.path.join(self._data_dir, 'pg_tblspc')
        if os.path.exists(pg_tblsp_dir):
            for tsdn in os.listdir(pg_tblsp_dir):
                pg_tsp_path = os.path.join(pg_tblsp_dir, tsdn)
                if parse_int(tsdn) and os.path.islink(pg_tsp_path):
                    pg_tsp_rpath = os.path.realpath(pg_tsp_path)
                    links[pg_tsp_path] = pg_tsp_rpath
        return links

    def move_data_directory(self):
        if os.path.isdir(self._data_dir) and not self.is_running():
            try:
                postfix = time.strftime('%Y-%m-%d-%H-%M-%S')

                # let's see if the wal directory is a symlink, in this case we
                # should move the target
                for (source, pg_wal_realpath) in self.pg_wal_realpath().items():
                    logger.info('renaming WAL directory and updating symlink: %s', pg_wal_realpath)
                    new_name = '{0}_{1}'.format(pg_wal_realpath, postfix)
                    os.rename(pg_wal_realpath, new_name)
                    os.unlink(source)
                    os.symlink(source, new_name)

                # Move user defined tablespace directory
                for (source, pg_tsp_rpath) in self.pg_tblspc_realpaths().items():
                    logger.info('renaming user defined tablespace directory and updating symlink: %s', pg_tsp_rpath)
                    new_name = '{0}_{1}'.format(pg_tsp_rpath, postfix)
                    os.rename(pg_tsp_rpath, new_name)
                    os.unlink(source)
                    os.symlink(source, new_name)

                new_name = '{0}_{1}'.format(self._data_dir, postfix)
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

    def _get_synchronous_commit_param(self):
        return self.query("SHOW synchronous_commit").fetchone()[0]

    def pick_synchronous_standby(self, cluster, sync_node_count=1):
        """Finds the best candidate to be the synchronous standby.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.

        :returns tuple of candidates list and synchronous standby list.
        """
        if self._major_version < 90600:
            sync_node_count = 1
        members = {m.name.lower(): m for m in cluster.members}
        candidates = []
        sync_nodes = []
        # Pick candidates based on who has higher replay/remote_write/flush lsn.
        sync_commit_par = self._get_synchronous_commit_param()
        sort_col = {'remote_apply': 'replay', 'remote_write': 'write'}.get(sync_commit_par, 'flush')
        # pg_stat_replication.sync_state has 4 possible states - async, potential, quorum, sync.
        # Sort clause "ORDER BY sync_state DESC" is to get the result in required order and to keep
        # the result consistent in case if a synchronous standby member is slowed down OR async node
        # receiving changes faster than the sync member (very rare but possible). Such cases would
        # trigger sync standby member swapping frequently and the sort on sync_state desc should
        # help in keeping the query result consistent.
        for app_name, state, sync_state in self.query(
                "SELECT pg_catalog.lower(application_name), state, sync_state"
                " FROM pg_catalog.pg_stat_replication"
                " WHERE state = 'streaming'"
                " ORDER BY sync_state DESC, {0}_{1} DESC".format(sort_col, self.lsn_name)):
            member = members.get(app_name)
            if not member or member.tags.get('nosync', False):
                continue
            candidates.append(member.name)
            if sync_state == 'sync':
                sync_nodes.append(member.name)
            if len(candidates) >= sync_node_count:
                break

        return candidates, sync_nodes

    def schedule_sanity_checks_after_pause(self):
        """
            After coming out of pause we have to:
            1. sync replication slots, because it might happen that slots were removed
            2. get new 'Database system identifier' to make sure that it wasn't changed
        """
        self.slots_handler.schedule()
        self._sysid = None
