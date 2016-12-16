import logging
import os
import psycopg2
import re
import shlex
import shutil
import subprocess
import tempfile
import time

from collections import defaultdict
from patroni import call_self
from patroni.callback_executor import CallbackExecutor
from patroni.exceptions import PostgresConnectionException, PostgresException
from patroni.utils import compare_values, parse_bool, parse_int, Retry, RetryFailedError, polling_loop
from six import string_types
from threading import current_thread, Lock

logger = logging.getLogger(__name__)

ACTION_ON_START = "on_start"
ACTION_ON_STOP = "on_stop"
ACTION_ON_RESTART = "on_restart"
ACTION_ON_RELOAD = "on_reload"
ACTION_ON_ROLE_CHANGE = "on_role_change"

STATE_RUNNING = 'running'
STATE_REJECT = 'rejecting connections'
STATE_NO_RESPONSE = 'not responding'
STATE_UNKNOWN = 'unknown'


def slot_name_from_member_name(member_name):
    """Translate member name to valid PostgreSQL slot name.

    PostgreSQL replication slot names must be valid PostgreSQL names. This function maps the wider space of
    member names to valid PostgreSQL names. Names are lowercased, dashes and periods common in hostnames
    are replaced with underscores, other characters are encoded as their unicode codepoint. Name is truncated
    to 64 characters. Multiple different member names may map to a single slot name."""

    def replace_char(match):
        c = match.group(0)
        return '_' if c in '-.' else "u{:04d}".format(ord(c))

    slot_name = re.sub('[^a-z0-9_]', replace_char, member_name.lower())
    return slot_name[0:64]


class Postgresql(object):

    # List of parameters which must be always passed to postmaster as command line options
    # to make it not possible to change them with 'ALTER SYSTEM'.
    # Some of these parameters have sane default value assigned and Patroni doesn't allow
    # to decrease this value. E.g. 'wal_level' can't be lower then 'hot_standby' and so on.
    # These parameters could be changed only globally, i.e. via DCS.
    # P.S. 'listen_addresses' and 'port' are added here just for convenience, to mark them
    # as a parameters which should always be passed through command line.
    #
    # Format:
    #  key - parameter name
    #  value - tuple(default_value, check_function, min_version)
    #    default_value -- some sane default value
    #    check_function -- if the new value is not correct must return `!False`
    #    min_version -- major version of PostgreSQL when parameter was introduced
    CMDLINE_OPTIONS = {
        'listen_addresses': (None, lambda _: False, 9.1),
        'port': (None, lambda _: False, 9.1),
        'cluster_name': (None, lambda _: False, 9.5),
        'wal_level': ('hot_standby', lambda v: v.lower() in ('hot_standby', 'replica', 'logical'), 9.1),
        'hot_standby': ('on', lambda _: False, 9.1),
        'max_connections': (100, lambda v: int(v) >= 100, 9.1),
        'max_wal_senders': (5, lambda v: int(v) >= 5, 9.1),
        'wal_keep_segments': (8, lambda v: int(v) >= 8, 9.1),
        'max_prepared_transactions': (0, lambda v: int(v) >= 0, 9.1),
        'max_locks_per_transaction': (64, lambda v: int(v) >= 64, 9.1),
        'track_commit_timestamp': ('off', lambda v: parse_bool(v) is not None, 9.5),
        'max_replication_slots': (5, lambda v: int(v) >= 5, 9.4),
        'max_worker_processes': (8, lambda v: int(v) >= 8, 9.4),
        'wal_log_hints': ('on', lambda _: False, 9.4)
    }

    def __init__(self, config):
        self.config = config
        self.name = config['name']
        self.scope = config['scope']
        self._bin_dir = config.get('bin_dir') or ''
        self._database = config.get('database', 'postgres')
        self._data_dir = config['data_dir']
        self._pending_restart = False
        self.__thread_ident = current_thread().ident

        self._version_file = os.path.join(self._data_dir, 'PG_VERSION')
        self._major_version = self.get_major_version()
        self._synchronous_standby_names = None
        self._server_parameters = self.get_server_parameters(config)

        self._connect_address = config.get('connect_address')
        self._superuser = config['authentication'].get('superuser', {})
        self._replication = config['authentication']['replication']
        self.resolve_connection_addresses()

        self._need_rewind = False
        self._use_slots = config.get('use_slots', True)
        self._schedule_load_slots = self.use_slots

        self._pgpass = config.get('pgpass') or os.path.join(os.path.expanduser('~'), 'pgpass')
        self._callback_executor = CallbackExecutor()
        self.__cb_called = False
        self.__cb_pending = None
        config_base_name = config.get('config_base_name', 'postgresql')
        self._postgresql_conf = os.path.join(self._data_dir, config_base_name + '.conf')
        self._postgresql_base_conf_name = config_base_name + '.base.conf'
        self._postgresql_base_conf = os.path.join(self._data_dir, self._postgresql_base_conf_name)
        self._recovery_conf = os.path.join(self._data_dir, 'recovery.conf')
        self._postmaster_pid = os.path.join(self._data_dir, 'postmaster.pid')
        self._trigger_file = config.get('recovery_conf', {}).get('trigger_file') or 'promote'
        self._trigger_file = os.path.abspath(os.path.join(self._data_dir, self._trigger_file))

        self._connection = None
        self._cursor_holder = None
        self._sysid = None
        self._replication_slots = []  # list of already existing replication slots
        self.retry = Retry(max_tries=-1, deadline=config['retry_timeout']/2.0, max_delay=1,
                           retry_exceptions=PostgresConnectionException)

        # Retry 'pg_is_in_recovery()' only once
        self._is_leader_retry = Retry(max_tries=1, deadline=config['retry_timeout']/2.0, max_delay=1,
                                      retry_exceptions=PostgresConnectionException)

        self._state_lock = Lock()
        self.set_state('stopped')
        self._role_lock = Lock()
        self.set_role(self.get_postgres_role_from_data_directory())

        self._state_entry_timestamp = None

        if self.is_running():
            self.set_state('running')
            self.set_role('master' if self.is_leader() else 'replica')
            self._write_postgresql_conf()  # we are "joining" already running postgres

    @property
    def _configuration_to_save(self):
        configuration = [self._postgresql_conf]
        if 'custom_conf' not in self.config:
            configuration.append(self._postgresql_base_conf)
        if not self.config['parameters'].get('hba_file'):
            configuration.append(os.path.join(self._data_dir, 'pg_hba.conf'))
        return configuration

    @property
    def use_slots(self):
        return self._use_slots and self._major_version >= 9.4

    @property
    def callback(self):
        return self.config.get('callbacks') or {}

    def _version_file_exists(self):
        return not self.data_directory_empty() and os.path.isfile(self._version_file)

    def get_major_version(self):
        if self._version_file_exists():
            try:
                with open(self._version_file) as f:
                    return float(f.read())
            except Exception:
                logger.exception('Failed to read PG_VERSION from %s', self._data_dir)
        return 0.0

    def get_server_parameters(self, config):
        parameters = config['parameters'].copy()
        listen_addresses, port = (config['listen'] + ':5432').split(':')[:2]
        parameters.update({'cluster_name': self.scope, 'listen_addresses': listen_addresses, 'port': port})
        if config.get('synchronous_mode', False):
            if self._synchronous_standby_names is None:
                parameters.pop('synchronous_standby_names', None)
            else:
                parameters['synchronous_standby_names'] = self._synchronous_standby_names
        return {k: v for k, v in parameters.items() if not self._major_version or
                self._major_version >= self.CMDLINE_OPTIONS.get(k, (0, 1, 9.1))[2]}

    def resolve_connection_addresses(self):
        self._local_address = self.get_local_address()
        self.connection_string = 'postgres://{0}/{1}'.format(
            self._connect_address or self._local_address['host'] + ':' + self._local_address['port'], self._database)

    def _pgcommand(self, cmd):
        """Returns path to the specified PostgreSQL command"""
        return os.path.join(self._bin_dir, cmd)

    def pg_ctl(self, cmd, *args, **kwargs):
        """Builds and executes pg_ctl command

        :returns: `!True` when return_code == 0, otherwise `!False`"""

        pg_ctl = [self._pgcommand('pg_ctl'), cmd]
        if cmd == 'stop':
            pg_ctl += ['-w']
            timeout = self.config.get('pg_ctl_timeout')
            if timeout:
                try:
                    pg_ctl += ['-t', str(int(timeout))]
                except Exception:
                    logger.error('Bad value of pg_ctl_timeout: %s', timeout)
        return subprocess.call(pg_ctl + ['-D', self._data_dir] + list(args), **kwargs) == 0

    def pg_isready(self):
        """Runs pg_isready to see if PostgreSQL is accepting connections.

        :returns: 'ok' if PostgreSQL is up, 'reject' if starting up, 'no_resopnse' if not up."""

        cmd = [self._pgcommand('pg_isready'),
               '-h', self._local_address['host'],
               '-p', self._local_address['port'],
               '-d', self._database]
        # We only need the username because pg_isready does not try to authenticate
        if 'username' in self._superuser:
            cmd.extend(['-U', self._superuser['username']])

        ret = subprocess.call(cmd)
        return_codes = {0: STATE_RUNNING,
                        1: STATE_REJECT,
                        2: STATE_NO_RESPONSE,
                        3: STATE_UNKNOWN}
        return return_codes.get(ret, STATE_UNKNOWN)

    def reload_config(self, config):
        server_parameters = self.get_server_parameters(config)

        listen_address_changed = pending_reload = pending_restart = False
        if self.state == 'running':
            changes = {p: v for p, v in server_parameters.items() if '.' not in p}
            changes.update({p: None for p, v in self._server_parameters.items() if not ('.' in p or p in changes)})
            if changes:
                if 'wal_segment_size' not in changes:
                    changes['wal_segment_size'] = '16384kB'
                # XXX: query can raise an exception
                for r in self.query("""SELECT name, setting, unit, vartype, context
                                         FROM pg_settings
                                        WHERE name IN (""" + ', '.join(['%s'] * len(changes)) + """)
                                        ORDER BY 1 DESC""", *(list(changes.keys()))):
                    if r[4] == 'internal':
                        if r[0] == 'wal_segment_size':
                            server_parameters.pop(r[0], None)
                            wal_segment_size = parse_int(r[2], 'kB')
                            if wal_segment_size is not None:
                                changes['wal_segment_size'] = '{0}kB'.format(int(r[1]) * wal_segment_size)
                    elif r[0] in changes:
                        unit = changes['wal_segment_size'] if r[0] in ('min_wal_size', 'max_wal_size') else r[2]
                        new_value = changes.pop(r[0])
                        if self._major_version >= 9.6 and r[0] == 'wal_level' and new_value == 'hot_standby':
                            new_value = 'replica'
                        if new_value is None or not compare_values(r[3], unit, r[1], new_value):
                            if r[4] == 'postmaster':
                                pending_restart = True
                                if r[0] in ('listen_addresses', 'port'):
                                    listen_address_changed = True
                            else:
                                pending_reload = True
                for param in changes:
                    if param in server_parameters:
                        logger.warning('Removing invalid parameter `%s` from postgresql.parameters', param)
                        server_parameters.pop(param)

            # Check that user-defined-paramters have changed (parameters with period in name)
            if not pending_reload:
                for p, v in server_parameters.items():
                    if '.' in p and (p not in self._server_parameters or str(v) != str(self._server_parameters[p])):
                        pending_reload = True
                        break
                if not pending_reload:
                    for p, v in self._server_parameters.items():
                        if '.' in p and (p not in server_parameters or str(v) != str(server_parameters[p])):
                            pending_reload = True
                            break

        self.config = config
        self._pending_restart = pending_restart
        self._server_parameters = server_parameters
        self._connect_address = config.get('connect_address')

        if not listen_address_changed:
            self.resolve_connection_addresses()

        if pending_reload:
            self._write_postgresql_conf()
            self.reload()
        self._is_leader_retry.deadline = self.retry.deadline = config['retry_timeout']/2.0

    @property
    def pending_restart(self):
        return self._pending_restart

    @property
    def can_rewind(self):
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        # low-hanging fruit: check if pg_rewind configuration is there
        if not (self.config.get('use_pg_rewind') and all(self._superuser.get(n) for n in ('username', 'password'))):
            return False

        cmd = [self._pgcommand('pg_rewind'), '--help']
        try:
            ret = subprocess.call(cmd, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
            if ret != 0:  # pg_rewind is not there, close up the shop and go home
                return False
        except OSError:
            return False
        # check if the cluster's configuration permits pg_rewind
        data = self.controldata()
        return data.get('wal_log_hints setting', 'off') == 'on' or data.get('Data page checksum version', '0') != '0'

    @property
    def sysid(self):
        if not self._sysid:
            data = self.controldata()
            self._sysid = data.get('Database system identifier', "")
        return self._sysid

    def get_local_address(self):
        listen_addresses = self._server_parameters['listen_addresses'].split(',')
        local_address = listen_addresses[0].strip()  # take first address from listen_addresses

        for la in listen_addresses:
            if la.strip().lower() in ('*', '0.0.0.0', '127.0.0.1', 'localhost'):  # we are listening on '*' or localhost
                local_address = 'localhost'  # connection via localhost is preferred
                break
        return {'host': local_address, 'port': self._server_parameters['port']}

    def get_postgres_role_from_data_directory(self):
        if self.data_directory_empty():
            return 'uninitialized'
        elif os.path.exists(self._recovery_conf):
            return 'replica'
        else:
            return 'master'

    @property
    def _local_connect_kwargs(self):
        ret = self._local_address.copy()
        ret.update({'database': self._database,
                    'fallback_application_name': 'Patroni',
                    'connect_timeout': 3,
                    'options': '-c statement_timeout=2000'})
        if 'username' in self._superuser:
            ret['user'] = self._superuser['username']
        if 'password' in self._superuser:
            ret['password'] = self._superuser['password']
        return ret

    def connection(self):
        if not self._connection or self._connection.closed != 0:
            self._connection = psycopg2.connect(**self._local_connect_kwargs)
            self._connection.autocommit = True
            self.server_version = self._connection.server_version
        return self._connection

    def _cursor(self):
        if not self._cursor_holder or self._cursor_holder.closed or self._cursor_holder.connection.closed != 0:
            logger.info("establishing a new patroni connection to the postgres cluster")
            self._cursor_holder = self.connection().cursor()
        return self._cursor_holder

    def close_connection(self):
        if self._connection and self._connection.closed == 0:
            self._connection.close()
            logger.info("closed patroni connection to the postgresql cluster")
        self._cursor_holder = self._connection = None

    def _query(self, sql, *params):
        """We are always using the same cursor, therefore this method is not thread-safe!!!
        You can call it from different threads only if you are holding explicit `AsyncExecutor` lock,
        because the main thread is always holding this lock when running HA cycle."""
        cursor = None
        try:
            cursor = self._cursor()
            cursor.execute(sql, params)
            return cursor
        except psycopg2.Error as e:
            if cursor and cursor.connection.closed == 0:
                raise e
            if self.state == 'restarting':
                raise RetryFailedError('cluster is being restarted')
            raise PostgresConnectionException('connection problems')

    def query(self, sql, *params):
        try:
            return self.retry(self._query, sql, *params)
        except RetryFailedError as e:
            raise PostgresConnectionException(str(e))

    def data_directory_empty(self):
        return not os.path.exists(self._data_dir) or os.listdir(self._data_dir) == []

    @staticmethod
    def initdb_allowed_option(name):
        if name in ['pgdata', 'nosync', 'pwfile', 'sync-only']:
            raise Exception('{0} option for initdb is not allowed'.format(name))
        return True

    def get_initdb_options(self, config):
        options = []
        for o in config:
            if isinstance(o, string_types) and self.initdb_allowed_option(o):
                options.append('--{0}'.format(o))
            elif isinstance(o, dict):
                keys = list(o.keys())
                if len(keys) != 1 or not isinstance(keys[0], string_types) or not self.initdb_allowed_option(keys[0]):
                    raise Exception('Invalid option: {0}'.format(o))
                options.append('--{0}={1}'.format(keys[0], o[keys[0]]))
            else:
                raise Exception('Unknown type of initdb option: {0}'.format(o))
        return options

    def _initialize(self, config):
        self.set_state('initalizing new cluster')
        options = self.get_initdb_options(config.get('initdb') or [])
        pwfile = None

        if self._superuser:
            if 'username' in self._superuser:
                options.append('--username={0}'.format(self._superuser['username']))
            if 'password' in self._superuser:
                (fd, pwfile) = tempfile.mkstemp()
                os.write(fd, self._superuser['password'].encode('utf-8'))
                os.close(fd)
                options.append('--pwfile={0}'.format(pwfile))
        options = ['-o', ' '.join(options)] if options else []

        ret = self.pg_ctl('initdb', *options)
        if pwfile:
            os.remove(pwfile)
        if ret:
            self.write_pg_hba(config.get('pg_hba', []))
            self._major_version = self.get_major_version()
        else:
            self.set_state('initdb failed')
        return ret

    def run_bootstrap_post_init(self, config):
        """
        runs a script after initdb is called and waits until completion.
        passed: cluster name, parameters
        """
        if 'post_init' in config:
            cmd = config['post_init']
            r = self._local_connect_kwargs
            if 'user' in r:
                connstring = 'postgres://{user}@{host}:{port}/{database}'.format(**r)
            else:
                connstring = 'postgres://{host}:{port}/{database}'.format(**r)
                if 'password' in r:
                    import getpass
                    r.setdefault('user', os.environ.get('PGUSER', getpass.getuser()))

            env = self.write_pgpass(r) if 'password' in r else None
            try:
                ret = subprocess.call(shlex.split(cmd) + [connstring], env=env)
            except OSError:
                logger.error('post_init script %s failed', cmd)
                return False
            if ret != 0:
                logger.error('post_init script %s returned non-zero code %d', cmd, ret)
                return False
        return True

    def delete_trigger_file(self):
        if os.path.exists(self._trigger_file):
            os.unlink(self._trigger_file)

    def write_pgpass(self, record):
        if 'user' not in record or 'password' not in record:
            return os.environ.copy()

        with open(self._pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**record))

        env = os.environ.copy()
        env['PGPASSFILE'] = self._pgpass
        return env

    def replica_method_can_work_without_replication_connection(self, method):
        return method != 'basebackup' and self.config and self.config.get(method, {}).get('no_master')

    def can_create_replica_without_replication_connection(self):
        """ go through the replication methods to see if there are ones
            that does not require a working replication connection.
        """
        replica_methods = self.config.get('create_replica_method', [])
        return any(self.replica_method_can_work_without_replication_connection(method) for method in replica_methods)

    def create_replica(self, clone_member):
        """
            create the replica according to the replica_method
            defined by the user.  this is a list, so we need to
            loop through all methods the user supplies
        """

        self.set_state('creating replica')
        self._sysid = None

        # get list of replica methods from config.
        # If there is no configuration key, or no value is specified, use basebackup
        replica_methods = self.config.get('create_replica_method') or ['basebackup']

        if clone_member and clone_member.conn_url:
            r = clone_member.conn_kwargs(self._replication)
            connstring = 'postgres://{user}@{host}:{port}/{database}'.format(**r)
            # add the credentials to connect to the replica origin to pgpass.
            env = self.write_pgpass(r)
        else:
            connstring = ''
            env = os.environ.copy()
            # if we don't have any source, leave only replica methods that work without it
            replica_methods = \
                [r for r in replica_methods if self.replica_method_can_work_without_replication_connection(r)]

        # go through them in priority order
        ret = 1
        for replica_method in replica_methods:
            # if the method is basebackup, then use the built-in
            if replica_method == "basebackup":
                ret = self.basebackup(connstring, env)
                if ret == 0:
                    logger.info("replica has been created using basebackup")
                    # if basebackup succeeds, exit with success
                    break
            else:
                if not self.data_directory_empty():
                    self.remove_data_directory()

                cmd = replica_method
                method_config = {}
                # user-defined method; check for configuration
                # not required, actually
                if replica_method in self.config:
                    method_config = self.config[replica_method].copy()
                    # look to see if the user has supplied a full command path
                    # if not, use the method name as the command
                    cmd = method_config.pop('command', cmd)

                # add the default parameters
                method_config.update({"scope": self.scope,
                                      "role": "replica",
                                      "datadir": self._data_dir,
                                      "connstring": connstring})
                params = ["--{0}={1}".format(arg, val) for arg, val in method_config.items()]
                try:
                    # call script with the full set of parameters
                    ret = subprocess.call(shlex.split(cmd) + params, env=env)
                    # if we succeeded, stop
                    if ret == 0:
                        logger.info('replica has been created using %s', replica_method)
                        break
                    else:
                        logger.error('Error creating replica using method %s: %s exited with code=%s',
                                     replica_method, cmd, ret)
                except Exception:
                    logger.exception('Error creating replica using method %s', replica_method)
                    ret = 1

        self.set_state('stopped')
        return ret

    def is_leader(self):
        try:
            return not self._is_leader_retry(self._query, 'SELECT pg_is_in_recovery()').fetchone()[0]
        except RetryFailedError as e:  # SELECT pg_is_in_recovery() failed two times
            if not self.is_starting() and self.pg_isready() == STATE_REJECT:
                self.set_state('starting')
            raise PostgresConnectionException(str(e))

    def is_running(self):
        if not (self._version_file_exists() and os.path.isfile(self._postmaster_pid)):
            return False
        return self.is_pid_running(self.read_pid_file().get('pid', 0))

    def read_pid_file(self):
        """Reads and parses postmaster.pid from the data directory

        :returns dictionary of values if successful, empty dictionary otherwise
        """
        pid_line_names = ['pid', 'data_dir', 'start_time', 'port', 'socket_dir', 'listen_addr', 'shmem_key']
        try:
            with open(self._postmaster_pid) as f:
                return {name: line.rstrip("\n") for name, line in zip(pid_line_names, f)}
        except IOError:
            return {}

    @staticmethod
    def is_pid_running(pid):
        try:
            pid = int(pid)
            if pid < 0:
                pid = -pid
            return pid > 0 and pid != os.getpid() and pid != os.getppid() and (os.kill(pid, 0) or True)
        except Exception:
            return False

    @property
    def cb_called(self):
        return self.__cb_called

    def call_nowait(self, cb_name):
        """ pick a callback command and call it without waiting for it to finish """
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

    def wait_for_port_open(self, pid, initiated, timeout):
        """Waits until PostgreSQL opens ports."""
        for _ in polling_loop(timeout):
            pid_file = self.read_pid_file()
            if len(pid_file) > 5:
                try:
                    pmpid = int(pid_file['pid'])
                    pmstart = int(pid_file['start_time'])

                    if pmstart >= initiated - 2 and pmpid == pid:
                        isready = self.pg_isready()
                        if isready != STATE_NO_RESPONSE:
                            if isready not in [STATE_REJECT, STATE_RUNNING]:
                                logger.warning("Can't determine PostgreSQL startup status, assuming running")
                            return True
                except ValueError:
                    # Garbage in the pid file
                    pass

            if not self.is_pid_running(pid):
                logger.error('postmaster is not running')
                self.set_state('start failed')
                return False

        logger.warning("Timed out waiting for PostgreSQL to start")
        return False

    def start(self, timeout=None, block_callbacks=False):
        """Start PostgreSQL

        Waits for postmaster to open ports or terminate so pg_isready can be used to check startup completion
        or failure.

        :returns: True if start was initiated and postmaster ports are open, False if start failed"""
        # make sure we close all connections established against
        # the former node, otherwise, we might get a stalled one
        # after kill -9, which would report incorrect data to
        # patroni.
        self.close_connection()

        if self.is_running():
            logger.error('Cannot start PostgreSQL because one is already running.')
            return True

        if not block_callbacks:
            self.__cb_pending = ACTION_ON_START

        self.set_role(self.get_postgres_role_from_data_directory())

        self.set_state('starting')
        self._pending_restart = False

        self._write_postgresql_conf()
        self.resolve_connection_addresses()

        opts = {p: self._server_parameters[p] for p, v in self.CMDLINE_OPTIONS.items() if self._major_version >= v[2]}
        if self._major_version >= 9.6 and opts['wal_level'] == 'hot_standby':
            opts['wal_level'] = 'replica'
        options = ['--{0}={1}'.format(p, v) for p, v in opts.items()]

        start_initiated = time.time()

        # Unfortunately `pg_ctl start` does not return postmaster pid to us. Without this information
        # it is hard to know the current state of postgres startup, so we had to reimplement pg_ctl start
        # in python. It will start postgres, wait for port to be open and wait until postgres will start
        # accepting connections.
        # Important!!! We can't just start postgres using subprocess.Popen, because in this case it
        # will be our child for the rest of our live and we will have to take care of it (`waitpid`).
        # So we will use the same approach as pg_ctl uses: start a new process, which will start postgres.
        # This process will write postmaster pid to stdout and exit immediately. Now it's responsibility
        # of init process to take care about postmaster.
        # In order to make everything portable we can't use fork&exec approach here, so  we will call
        # ourselves and pass list of arguments which must be used to start postgres.
        proc = call_self(['pg_ctl_start', self._pgcommand('postgres'), '-D', self._data_dir] + options, close_fds=True,
                         preexec_fn=os.setsid, stdout=subprocess.PIPE, env={'PATH': os.environ.get('PATH')})
        pid = int(proc.stdout.readline().strip())
        proc.wait()
        logger.info('postmaster pid=%s', pid)

        start_timeout = timeout
        if not start_timeout:
            try:
                start_timeout = float(self.config.get('pg_ctl_timeout', 60))
            except ValueError:
                start_timeout = 60

        # We want postmaster to open ports before we continue
        if not self.wait_for_port_open(pid, start_initiated, start_timeout):
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
        connect_kwargs = connect_kwargs or self._local_connect_kwargs
        for p in ['connect_timeout', 'options']:
            connect_kwargs.pop(p, None)
        try:
            with psycopg2.connect(**connect_kwargs) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = 0")
                    if check_not_is_in_recovery:
                        cur.execute('SELECT pg_is_in_recovery()')
                        if cur.fetchone()[0]:
                            return 'is_in_recovery=true'
                    return cur.execute('CHECKPOINT')
        except psycopg2.Error:
            logging.exception('Exception during CHECKPOINT')
            return 'not accessible or not healty'

    def stop(self, mode='fast', block_callbacks=False, checkpoint=True):
        if not self.is_running():
            if not block_callbacks:
                self.set_state('stopped')
            return True

        if checkpoint and not self.is_starting():
            self.checkpoint()

        if not block_callbacks:
            self.set_state('stopping')

        ret = self.pg_ctl('stop', '-m', mode)
        # block_callbacks is used during restart to avoid
        # running start/stop callbacks in addition to restart ones
        if not ret:
            logger.warning('pg_ctl stop failed')
            self.set_state('stop failed')
        elif not block_callbacks:
            self.set_state('stopped')
            self.call_nowait(ACTION_ON_STOP)
        return ret

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

        :returns: True iff state was changed from 'starting'
        """
        ready = self.pg_isready()

        if ready == STATE_REJECT:
            return False
        elif ready == STATE_NO_RESPONSE:
            self.set_state('start failed')
            self._schedule_load_slots = False  # TODO: can remove this?
            self.save_configuration_files()  # TODO: maybe remove this?
            return True
        else:
            if ready != STATE_RUNNING:
                # Bad configuration or unexpected OS error. No idea of PostgreSQL status.
                # Let the main loop of run cycle clean up the mess.
                logger.warning("%s status returned from pg_isready",
                               "Unknown" if ready == STATE_UNKNOWN else "Invalid")
            self.set_state('running')
            self._schedule_load_slots = self.use_slots
            self.save_configuration_files()
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
            if timeout and self.time_in_state() > timeout:
                return None
            time.sleep(1)

        return self.state == 'running'

    def restart(self, timeout=None):
        """Restarts PostgreSQL.

        When timeout parameter is set the call will block either until PostgreSQL has started, failed to start or
        timeout arrives.

        :returns: True when restart was successful and timeout did not expire when waiting.
        """
        self.set_state('restarting')
        self.__cb_pending = ACTION_ON_RESTART
        ret = self.stop(block_callbacks=True) and self.start(timeout=timeout, block_callbacks=True)
        if not ret and not self.is_starting():
            self.set_state('restart failed ({0})'.format(self.state))
        return ret

    def _write_postgresql_conf(self):
        # rename the original configuration if it is necessary
        if 'custom_conf' not in self.config and not os.path.exists(self._postgresql_base_conf):
            os.rename(self._postgresql_conf, self._postgresql_base_conf)

        with open(self._postgresql_conf, 'w') as f:
            f.write('# Do not edit this file manually!\n# It will be overwritten by Patroni!\n')
            f.write("include '{0}'\n\n".format(self.config.get('custom_conf') or self._postgresql_base_conf_name))
            for name, value in sorted(self._server_parameters.items()):
                f.write("{0} = '{1}'\n".format(name, value))

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def write_pg_hba(self, config):
        with open(os.path.join(self._data_dir, 'pg_hba.conf'), 'a') as f:
            f.write('\n{}\n'.format('\n'.join(config)))

    def primary_conninfo(self, member):
        if not (member and member.conn_url):
            return None
        r = member.conn_kwargs(self._replication)
        r.update({'application_name': self.name, 'sslmode': 'prefer', 'sslcompression': '1'})
        keywords = 'user password host port sslmode sslcompression application_name'.split()
        return ' '.join('{0}={{{0}}}'.format(kw) for kw in keywords).format(**r)

    def check_recovery_conf(self, primary_conninfo):
        if not os.path.isfile(self._recovery_conf):
            return False

        with open(self._recovery_conf, 'r') as f:
            for line in f:
                if line.startswith('primary_conninfo'):
                    return primary_conninfo and (primary_conninfo in line)
        return not primary_conninfo

    def write_recovery_conf(self, primary_conninfo):
        with open(self._recovery_conf, 'w') as f:
            f.write("standby_mode = 'on'\nrecovery_target_timeline = 'latest'\n")
            if primary_conninfo:
                f.write("primary_conninfo = '{0}'\n".format(primary_conninfo))
                if self.use_slots:
                    f.write("primary_slot_name = '{0}'\n".format(slot_name_from_member_name(self.name)))
            for name, value in self.config.get('recovery_conf', {}).items():
                if name not in ('standby_mode', 'recovery_target_timeline', 'primary_conninfo', 'primary_slot_name'):
                    f.write("{0} = '{1}'\n".format(name, value))

    def rewind(self, r):
        # prepare pg_rewind connection
        env = self.write_pgpass(r)
        dsn_attrs = [
            ('user', r.get('user')),
            ('host', r.get('host')),
            ('port', r.get('port')),
            ('dbname', r.get('database')),
            ('sslmode', 'prefer'),
            ('sslcompression', '1'),
        ]
        dsn = " ".join("{0}={1}".format(k, v) for k, v in dsn_attrs if v is not None)
        logger.info('running pg_rewind from %s', dsn)
        try:
            return subprocess.call([self._pgcommand('pg_rewind'),
                                    '-D', self._data_dir,
                                    '--source-server', dsn,
                                    ], env=env) == 0
        except OSError:
            return False

    def controldata(self):
        """ return the contents of pg_controldata, or non-True value if pg_controldata call failed """
        result = {}
        # Don't try to call pg_controldata during backup restore
        if self._version_file_exists() and self.state != 'creating replica':
            try:
                data = subprocess.check_output([self._pgcommand('pg_controldata'), self._data_dir])
                if data:
                    data = data.decode('utf-8').splitlines()
                    result = {l.split(':')[0].replace('Current ', '', 1): l.split(':')[1].strip() for l in data if l}
            except subprocess.CalledProcessError:
                logger.exception("Error when calling pg_controldata")
        return result

    def read_postmaster_opts(self):
        """ returns the list of option names/values from postgres.opts, Empty dict if read failed or no file """
        result = {}
        try:
            with open(os.path.join(self._data_dir, "postmaster.opts")) as f:
                data = f.read()
                opts = [opt.strip('"\n') for opt in data.split(' "')]
                for opt in opts:
                    if '=' in opt and opt.startswith('--'):
                        name, val = opt.split('=', 1)
                        name = name.strip('-')
                        result[name] = val
        except IOError:
            logger.exception('Error when reading postmaster.opts')
        return result

    def single_user_mode(self, command=None, options=None):
        """ run a given command in a single-user mode. If the command is empty - then just start and stop """
        cmd = [self._pgcommand('postgres'), '--single', '-D', self._data_dir]
        for opt, val in sorted((options or {}).items()):
            cmd.extend(['-c', '{0}={1}'.format(opt, val)])
        # need a database name to connect
        cmd.append(self._database)
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
        if p:
            if command:
                p.communicate('{0}\n'.format(command))
            p.stdin.close()
            return p.wait()
        return 1

    def cleanup_archive_status(self):
        status_dir = os.path.join(self._data_dir, 'pg_xlog', 'archive_status')
        try:
            for f in os.listdir(status_dir):
                path = os.path.join(status_dir, f)
                try:
                    if os.path.islink(path):
                        os.unlink(path)
                    elif os.path.isfile(path):
                        os.remove(path)
                except OSError:
                    logger.exception("Unable to remove %s", path)
        except OSError:
            logger.exception("Unable to list %s", status_dir)

    @property
    def need_rewind(self):
        return self._need_rewind

    def follow(self, member, leader, recovery=False, async_executor=None, need_rewind=None, timeout=None):
        if need_rewind is not None:
            self._need_rewind = need_rewind

        primary_conninfo = self.primary_conninfo(member)

        if self.check_recovery_conf(primary_conninfo) and not recovery:
            return True

        if async_executor:
            async_executor.schedule('changing primary_conninfo and restarting')
            async_executor.run_async(self._do_follow, (primary_conninfo, leader, recovery, timeout))
        else:
            return self._do_follow(primary_conninfo, leader, recovery, timeout)

    def _do_follow(self, primary_conninfo, leader, recovery=False, timeout=None):
        change_role = self.role in ('master', 'demoted')

        if leader and leader.name == self.name:
            primary_conninfo = None
            self._need_rewind = False
            if self.is_running():
                return
        elif change_role:
            self._need_rewind = True

        if self._need_rewind and not self.can_rewind:
            logger.warning("Data directory may be out of sync master, rewind may be needed.")

        if self._need_rewind and leader and leader.conn_url and self.can_rewind:
            logger.info("rewind flag is set")

            if self.is_running() and not self.stop(checkpoint=False):
                return logger.warning('Can not run pg_rewind because postgres is still running')

            # prepare pg_rewind connection
            r = leader.conn_kwargs(self._superuser)

            # first make sure that we are really trying to rewind
            # from the master and run a checkpoint on a t in order to
            # make it store the new timeline (5540277D.8020309@iki.fi)
            leader_status = self.checkpoint(r)
            if leader_status:
                return logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)

            # at present, pg_rewind only runs when the cluster is shut down cleanly
            # and not shutdown in recovery. We have to remove the recovery.conf if present
            # and start/shutdown in a single user mode to emulate this.
            # XXX: if recovery.conf is linked, it will be written anew as a normal file.
            if os.path.isfile(self._recovery_conf) or os.path.islink(self._recovery_conf):
                os.unlink(self._recovery_conf)

            # Archived segments might be useful to pg_rewind,
            # clean the flags that tell we should remove them.
            self.cleanup_archive_status()

            # Start in a single user mode and stop to produce a clean shutdown
            opts = self.read_postmaster_opts()
            opts.update({'archive_mode': 'on', 'archive_command': 'false'})
            self.single_user_mode(options=opts)

            if self.rewind(r) or not self.config.get('remove_data_directory_on_rewind_failure', False):
                self.write_recovery_conf(primary_conninfo)
                self.start()
            else:
                logger.error('unable to rewind the former master')
                self.remove_data_directory()
            self._need_rewind = False
        else:
            self.write_recovery_conf(primary_conninfo)
            if recovery:
                self.start(timeout=timeout)
            else:
                self.restart()
            self.set_role('replica')

        if change_role:
            # TODO: postpone this until start completes, or maybe do even earlier
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return True

    def save_configuration_files(self):
        """
            copy postgresql.conf to postgresql.conf.backup to be able to retrive configuration files
            - originally stored as symlinks, those are normally skipped by pg_basebackup
            - in case of WAL-E basebackup (see http://comments.gmane.org/gmane.comp.db.postgresql.wal-e/239)
        """
        try:
            for f in self._configuration_to_save:
                if os.path.isfile(f):
                    shutil.copy(f, f + '.backup')
        except IOError:
            logger.exception('unable to create backup copies of configuration files')

    def restore_configuration_files(self):
        """ restore a previously saved postgresql.conf """
        try:
            for f in self._configuration_to_save:
                if not os.path.isfile(f) and os.path.isfile(f + '.backup'):
                    shutil.copy(f + '.backup', f)
        except IOError:
            logger.exception('unable to restore configuration files from backup')

    def promote(self):
        if self.role == 'master':
            return True
        ret = self.pg_ctl('promote')
        if ret:
            self.set_role('master')
            logger.info("cleared rewind flag after becoming the leader")
            self._need_rewind = False
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return ret

    def create_or_update_role(self, name, password, options):
        options = list(map(str.upper, options))
        if 'NOLOGIN' not in options and 'LOGIN' not in options:
            options.append('LOGIN')

        self.query("""DO $$
BEGIN
    SET local synchronous_commit = 'local';
    PERFORM * FROM pg_authid WHERE rolname = %s;
    IF FOUND THEN
        ALTER ROLE "{0}" WITH {1} PASSWORD %s;
    ELSE
        CREATE ROLE "{0}" WITH {1} PASSWORD %s;
    END IF;
END;
$$""".format(name, ' '.join(options)), name, password, password)

    def xlog_position(self, retry=True):
        stmt = """SELECT CASE WHEN pg_is_in_recovery()
                              THEN GREATEST(pg_xlog_location_diff(COALESCE(pg_last_xlog_receive_location(), '0/0'),
                                                                  '0/0')::bigint,
                                            pg_xlog_location_diff(pg_last_xlog_replay_location(), '0/0')::bigint)
                              ELSE pg_xlog_location_diff(pg_current_xlog_location(), '0/0')::bigint
                          END"""

        # This method could be called from different threads (simultaneously with some other `_query` calls).
        # If it is called not from main thread we will create a new cursor to execute statement.
        if current_thread().ident == self.__thread_ident:
            return (self.query(stmt) if retry else self._query(stmt)).fetchone()[0]

        with self.connection().cursor() as cursor:
            cursor.execute(stmt)
            return cursor.fetchone()[0]

    def load_replication_slots(self):
        if self.use_slots and self._schedule_load_slots:
            cursor = self._query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
            self._replication_slots = [r[0] for r in cursor]
            self._schedule_load_slots = False

    def postmaster_start_time(self):
        try:
            cursor = self.query("""SELECT to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')""")
            return cursor.fetchone()[0]
        except psycopg2.Error:
            return None

    def sync_replication_slots(self, cluster):
        if self.use_slots:
            try:
                self.load_replication_slots()
                # if the replicatefrom tag is set on the member - we should not create the replication slot for it on
                # the current master, because that member would replicate from elsewhere. We still create the slot if
                # the replicatefrom destination member is currently not a member of the cluster (fallback to the
                # master), or if replicatefrom destination member happens to be the current master
                if self.role == 'master':
                    slot_members = [m.name for m in cluster.members if m.name != self.name and
                                    (m.replicatefrom is None or m.replicatefrom == self.name or
                                     not cluster.has_member(m.replicatefrom))]
                else:
                    # only manage slots for replicas that replicate from this one, except for the leader among them
                    slot_members = [m.name for m in cluster.members if m.replicatefrom == self.name and
                                    m.name != cluster.leader.name]
                slots = set(slot_name_from_member_name(name) for name in slot_members)

                if len(slots) < len(slot_members):
                    # Find which names are conflicting for a nicer error message
                    slot_conflicts = defaultdict(list)
                    for name in slot_members:
                        slot_conflicts[slot_name_from_member_name(name)].append(name)
                    logger.error("Following cluster members share a replication slot name: %s",
                                 "; ".join("{} map to {}".format(", ".join(v), k)
                                           for k, v in slot_conflicts.items() if len(v) > 1))

                # drop unused slots
                for slot in set(self._replication_slots) - slots:
                    self._query("""SELECT pg_drop_replication_slot(%s)
                                    WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                                    WHERE slot_name = %s AND NOT active)""", slot, slot)

                # create new slots
                for slot in slots - set(self._replication_slots):
                    self._query("""SELECT pg_create_physical_replication_slot(%s)
                                    WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                                    WHERE slot_name = %s)""", slot, slot)

                self._replication_slots = slots
            except Exception:
                logger.exception('Exception when changing replication slots')
                self._schedule_load_slots = True

    def last_operation(self):
        return str(self.xlog_position())

    def clone(self, clone_member):
        """
             - initialize the replica from an existing member (master or replica)
             - initialize the replica using the replica creation method that
               works without the replication connection (i.e. restore from on-disk
               base backup)
        """

        ret = self.create_replica(clone_member) == 0
        if ret:
            self._major_version = self.get_major_version()
            self.delete_trigger_file()
            self.restore_configuration_files()
        return ret

    def bootstrap(self, config):
        """ Initialize a new node from scratch and start it. """
        if self._initialize(config) and self.start() and self.run_bootstrap_post_init(config):
            for name, value in (config.get('users') or {}).items():
                if name not in (self._superuser.get('username'), self._replication['username']):
                    self.create_or_update_role(name, value['password'], value.get('options', []))
            self.create_or_update_role(self._replication['username'], self._replication['password'], ['REPLICATION'])
        else:
            raise PostgresException("Could not bootstrap master PostgreSQL")

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
                shutil.rmtree(self._data_dir)
        except (IOError, OSError):
            logger.exception('Could not remove data directory %s', self._data_dir)
            self.move_data_directory()

    def basebackup(self, conn_url, env):
        # creates a replica data dir using pg_basebackup.
        # this is the default, built-in create_replica_method
        # tries twice, then returns failure (as 1)
        # uses "stream" as the xlog-method to avoid sync issues
        maxfailures = 2
        ret = 1
        for bbfailures in range(0, maxfailures):
            if not self.data_directory_empty():
                self.remove_data_directory()

            try:
                ret = subprocess.call([self._pgcommand('pg_basebackup'), '--pgdata=' + self._data_dir,
                                       '--xlog-method=stream', "--dbname=" + conn_url], env=env)
                if ret == 0:
                    break
                else:
                    logger.error('Error when fetching backup: pg_basebackup exited with code=%s', ret)

            except Exception as e:
                logger.error('Error when fetching backup with pg_basebackup: %s', e)

            if bbfailures < maxfailures - 1:
                logger.warning('Trying again in 5 seconds')
                time.sleep(5)

        return ret

    def pick_synchronous_standby(self, cluster):
        """Finds the best candidate to be the synchronous standby.

        Current synchronous standby is always preferred, unless it has disconnected or does not want to be a
        synchronous standby any longer.

        :returns tuple of candidate name or None, and bool showing if the member is the active synchronous standby.
        """
        current = cluster.sync.sync_standby
        members = {m.name: m for m in cluster.members}
        candidates = []
        # Pick candidates based on who has flushed WAL farthest.
        # TODO: for synchronous_commit = remote_write we actually want to order on write_location
        for app_name, state, sync_state in self.query(
                """SELECT application_name, state, sync_state
                     FROM pg_stat_replication
                    ORDER BY flush_location DESC"""):
            member = members.get(app_name)
            if state != 'streaming' or not member or member.tags.get('nosync', False):
                continue
            if sync_state == 'sync':
                return app_name, True
            if sync_state == 'potential' and app_name == current:
                # Prefer current even if not the best one any more to avoid indecisivness and spurious swaps.
                return current, False
            if sync_state == 'async':
                candidates.append(app_name)

        if candidates:
            return candidates[0], False
        return None, False

    def set_synchronous_standby(self, name):
        """Sets a node to be synchronous standby and if changed does a reload for PostgreSQL."""
        if name != self._synchronous_standby_names:
            if name is None:
                self._server_parameters.pop('synchronous_standby_names', None)
            else:
                self._server_parameters['synchronous_standby_names'] = name
            self._synchronous_standby_names = name
            self._write_postgresql_conf()
            self.reload()

    @staticmethod
    def postgres_version_to_int(pg_version):
        """ Convert the server_version to integer

        >>> Postgresql.postgres_version_to_int('9.5.3')
        90503
        >>> Postgresql.postgres_version_to_int('9.3.13')
        90313
        >>> Postgresql.postgres_version_to_int('10.1')
        100001
        >>> Postgresql.postgres_version_to_int('10')
        Traceback (most recent call last):
            ...
        Exception: Invalid PostgreSQL format: X.Y or X.Y.Z is accepted: 10
        >>> Postgresql.postgres_version_to_int('a.b.c')
        Traceback (most recent call last):
            ...
        Exception: Invalid PostgreSQL version: a.b.c
        """
        components = pg_version.split('.')

        result = []
        if len(components) < 2 or len(components) > 3:
            raise Exception("Invalid PostgreSQL format: X.Y or X.Y.Z is accepted: {0}".format(pg_version))
        if len(components) == 2:
            # new style verion numbers, i.e. 10.1 becomes 100001
            components.insert(1, '0')
        try:
            result = [c if int(c) > 10 else '0{0}'.format(c) for c in components]
            result = int(''.join(result))
        except ValueError:
            raise Exception("Invalid PostgreSQL version: {0}".format(pg_version))
        return result
