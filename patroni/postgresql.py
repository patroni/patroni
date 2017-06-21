import logging
import errno
import os
import psycopg2
import psutil
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import time

from collections import defaultdict
from contextlib import contextmanager
from patroni import call_self
from patroni.callback_executor import CallbackExecutor
from patroni.exceptions import PostgresConnectionException, PostgresException
from patroni.utils import compare_values, parse_bool, parse_int, Retry, RetryFailedError, polling_loop, null_context
from six import string_types
from six.moves.urllib.parse import quote_plus
from threading import current_thread, Lock, Event

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

STOP_SIGNALS = {
    'smart': signal.SIGTERM,
    'fast': signal.SIGINT,
    'immediate': signal.SIGQUIT,
}
STOP_POLLING_INTERVAL = 1
REWIND_STATUS = type('Enum', (), {'INITIAL': 0, 'CHECK': 1, 'NEED': 2, 'NOT_NEED': 3, 'SUCCESS': 4, 'FAILED': 5})


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
        'listen_addresses': (None, lambda _: False, 90100),
        'port': (None, lambda _: False, 90100),
        'cluster_name': (None, lambda _: False, 90500),
        'wal_level': ('hot_standby', lambda v: v.lower() in ('hot_standby', 'replica', 'logical'), 90100),
        'hot_standby': ('on', lambda _: False, 90100),
        'max_connections': (100, lambda v: int(v) >= 100, 90100),
        'max_wal_senders': (5, lambda v: int(v) >= 5, 90100),
        'wal_keep_segments': (8, lambda v: int(v) >= 8, 90100),
        'max_prepared_transactions': (0, lambda v: int(v) >= 0, 90100),
        'max_locks_per_transaction': (64, lambda v: int(v) >= 64, 90100),
        'track_commit_timestamp': ('off', lambda v: parse_bool(v) is not None, 90500),
        'max_replication_slots': (5, lambda v: int(v) >= 5, 90400),
        'max_worker_processes': (8, lambda v: int(v) >= 8, 90400),
        'wal_log_hints': ('on', lambda _: False, 90400)
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
        self.resolve_connection_addresses()

        self._rewind_state = REWIND_STATUS.INITIAL
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

        self._connection_lock = Lock()
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

        # This event is set to true when no backends are running. Could be set in parallel by
        # multiple processes, like when demote is racing with async restart. Needs to be cleared
        # before invoking stop if wait for this event is desired.
        self.stop_safepoint_reached = Event()
        self.stop_safepoint_reached.set()

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
        return self._use_slots and self._major_version >= 90400

    @property
    def _replication(self):
        return self.config['authentication']['replication']

    @property
    def callback(self):
        return self.config.get('callbacks') or {}

    @staticmethod
    def _wal_name(version):
        return 'wal' if version >= 100000 else 'xlog'

    @property
    def wal_name(self):
        return self._wal_name(self._major_version)

    @property
    def lsn_name(self):
        return 'lsn' if self._major_version >= 100000 else 'location'

    def _version_file_exists(self):
        return not self.data_directory_empty() and os.path.isfile(self._version_file)

    def get_major_version(self):
        if self._version_file_exists():
            try:
                with open(self._version_file) as f:
                    return self.postgres_major_version_to_int(f.read().strip())
            except Exception:
                logger.exception('Failed to read PG_VERSION from %s', self._data_dir)
        return 0

    def get_server_parameters(self, config):
        parameters = config['parameters'].copy()
        listen_addresses, port = (config['listen'] + ':5432').split(':')[:2]
        parameters.update({'cluster_name': self.scope, 'listen_addresses': listen_addresses, 'port': port})
        if config.get('synchronous_mode', False):
            if self._synchronous_standby_names is None:
                if config.get('synchronous_mode_strict', False):
                    parameters['synchronous_standby_names'] = '*'
                else:
                    parameters.pop('synchronous_standby_names', None)
            else:
                parameters['synchronous_standby_names'] = self._synchronous_standby_names
        if self._major_version >= 90600 and parameters['wal_level'] == 'hot_standby':
            parameters['wal_level'] = 'replica'
        return {k: v for k, v in parameters.items() if not self._major_version or
                self._major_version >= self.CMDLINE_OPTIONS.get(k, (0, 1, 90100))[2]}

    def resolve_connection_addresses(self):
        port = self._server_parameters['port']
        tcp_local_address = self._get_tcp_local_address()

        local_address = {'port': port}
        if self.config.get('use_unix_socket', True):
            unix_local_address = self._get_unix_local_address()
            # if unix_socket_directories is not specified, but use_unix_socket is set to true - do our best
            # to use default value, i.e. don't specify a host neither in connection url nor arguments
            if unix_local_address is not None:
                # fallback to tcp if unix_socket_directories is set, but there are no sutable values
                local_address['host'] = unix_local_address or tcp_local_address
        else:
            local_address['host'] = tcp_local_address

        self._local_address = local_address
        self._local_replication_address = {'host': tcp_local_address, 'port': port}

        self.connection_string = 'postgres://{0}/{1}'.format(
            self._connect_address or tcp_local_address + ':' + port, self._database)

    def _pgcommand(self, cmd):
        """Returns path to the specified PostgreSQL command"""
        return os.path.join(self._bin_dir, cmd)

    def pg_ctl(self, cmd, *args, **kwargs):
        """Builds and executes pg_ctl command

        :returns: `!True` when return_code == 0, otherwise `!False`"""

        pg_ctl = [self._pgcommand('pg_ctl'), cmd]
        return subprocess.call(pg_ctl + ['-D', self._data_dir] + list(args), **kwargs) == 0

    def pg_isready(self):
        """Runs pg_isready to see if PostgreSQL is accepting connections.

        :returns: 'ok' if PostgreSQL is up, 'reject' if starting up, 'no_resopnse' if not up."""

        cmd = [self._pgcommand('pg_isready'), '-p', self._local_address['port'], '-d', self._database]

        # Host is not set if we are connecting via default unix socket
        if 'host' in self._local_address:
            cmd.extend(['-h', self._local_address['host']])

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
        self._superuser = config['authentication'].get('superuser', {})
        server_parameters = self.get_server_parameters(config)

        local_connection_address_changed = pending_reload = pending_restart = False
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
                        if new_value is None or not compare_values(r[3], unit, r[1], new_value):
                            if r[4] == 'postmaster':
                                pending_restart = True
                                if r[0] in ('listen_addresses', 'port') or\
                                        config.get('use_unix_socket', True) and r[0] == 'unix_socket_directories':
                                    local_connection_address_changed = True
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

        if not local_connection_address_changed:
            self.resolve_connection_addresses()

        if pending_reload:
            self._write_postgresql_conf()
            self.reload()
        self._is_leader_retry.deadline = self.retry.deadline = config['retry_timeout']/2.0

    @property
    def pending_restart(self):
        return self._pending_restart

    @staticmethod
    def configuration_allows_rewind(data):
        return data.get('wal_log_hints setting', 'off') == 'on' \
            or data.get('Data page checksum version', '0') != '0'

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
        return self.configuration_allows_rewind(self.controldata())

    @property
    def sysid(self):
        if not self._sysid:
            data = self.controldata()
            self._sysid = data.get('Database system identifier', "")
        return self._sysid

    def _get_unix_local_address(self):
        if 'unix_socket_directories' in self._server_parameters:
            for d in self._server_parameters['unix_socket_directories'].split(','):
                d = d.strip()
                if d.startswith('/'):  # Only absolute path can be used to connect via unix-socket
                    return d
            return ''

    def _get_tcp_local_address(self):
        listen_addresses = self._server_parameters['listen_addresses'].split(',')

        for la in listen_addresses:
            if la.strip().lower() in ('*', '0.0.0.0', '127.0.0.1', 'localhost'):  # we are listening on '*' or localhost
                return 'localhost'  # connection via localhost is preferred
        return listen_addresses[0].strip()  # can't use localhost, take first address from listen_addresses

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
        with self._connection_lock:
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
                # When connected via unix socket, psycopg2 can't recoginze 'connection lost'
                # and leaves `_cursor_holder.connection.closed == 0`, but psycopg2.OperationalError
                # is still raised (what is correct). It doesn't make sense to continiue with existing
                # connection and we will close it, to avoid its reuse by the `_cursor` method.
                if isinstance(e, psycopg2.OperationalError):
                    self.close_connection()
                else:
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
            self._server_parameters = self.get_server_parameters(self.config)
        else:
            self.set_state('initdb failed')
        return ret

    def run_bootstrap_post_init(self, config):
        """
        runs a script after initdb or custom bootstrap script is called and waits until completion.
        """
        cmd = config.get('post_bootstrap') or config.get('post_init')
        if cmd:
            r = self._local_connect_kwargs

            if 'host' in r:
                # '/tmp' => '%2Ftmp' for unix socket path
                host = quote_plus(r['host']) if r['host'].startswith('/') else r['host']
            else:
                host = ''

                # https://www.postgresql.org/docs/current/static/libpq-pgpass.html
                # A host name of localhost matches both TCP (host name localhost) and Unix domain socket
                # (pghost empty or the default socket directory) connections coming from the local machine.
                r['host'] = 'localhost'  # set it to localhost to write into pgpass

            if 'user' in r:
                user = r['user'] + '@'
            else:
                user = ''
                if 'password' in r:
                    import getpass
                    r.setdefault('user', os.environ.get('PGUSER', getpass.getuser()))

            connstring = 'postgres://{0}{1}:{2}/{3}'.format(user, host, r['port'], r['database'])
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
            # XXX: This is dangerous in case somebody deletes the data directory while PostgreSQL is still running.
            return False
        return self.is_pid_running(self.get_pid())

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

    def get_pid(self):
        """Fetches pid value from postmaster.pid using read_pid_file

        :returns pid if successful, 0 if pid file is not present"""
        # TODO: figure out what to do on permission errors
        pid = self.read_pid_file().get('pid', 0)
        try:
            return int(pid)
        except ValueError:
            logger.warning("Garbage pid in postmaster.pid: {0!r}".format(pid))
            return 0

    @staticmethod
    def is_pid_running(pid):
        try:
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

    def start(self, timeout=None, block_callbacks=False, task=None):
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

        opts = {p: self._server_parameters[p] for p in self.CMDLINE_OPTIONS if p in self._server_parameters}
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
        with task or null_context():
            if task and task.is_cancelled:
                logger.info("PostgreSQL start cancelled.")
                return False

            start_initiated = time.time()
            proc = call_self(['pg_ctl_start', self._pgcommand('postgres'), '-D', self._data_dir] + options,
                             close_fds=True, preexec_fn=os.setsid, stdout=subprocess.PIPE,
                             env={p: os.environ[p] for p in ('PATH', 'LC_ALL', 'LANG') if p in os.environ})
            pid = int(proc.stdout.readline().strip())
            proc.wait()
            logger.info('postmaster pid=%s', pid)

            if task:
                task.complete(pid)

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
            with self._get_connection_cursor(**connect_kwargs) as cur:
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
        success, pg_signaled = self._do_stop(mode, block_callbacks, checkpoint)
        if success:
            self.stop_safepoint_reached.set()  # In case we exited early. Setting twice is not a problem.
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

    def _do_stop(self, mode, block_callbacks, checkpoint):
        if not self.is_running():
            return True, False

        if checkpoint and not self.is_starting():
            self.checkpoint()

        if not block_callbacks:
            self.set_state('stopping')

        # Send signal to postmaster to stop
        pid, result = self._signal_postmaster_stop(mode)
        if result is not None:
            return result, True

        # We can skip safepoint detection if nobody is waiting for it.
        if not self.stop_safepoint_reached.is_set():
            # Wait for our connection to terminate so we can be sure that no new connections are being initiated
            self._wait_for_connection_close(pid)
            self._wait_for_user_backends_to_close(pid)
            self.stop_safepoint_reached.set()

        self._wait_for_postmaster_stop(pid)

        return True, True

    def _wait_for_postmaster_stop(self, pid):
        # This wait loop differs subtly from pg_ctl as we check for both the pid file going
        # away and if the pid is running. This seems safer.
        while pid == self.get_pid() and self.is_pid_running(pid):
            time.sleep(STOP_POLLING_INTERVAL)

    def _signal_postmaster_stop(self, mode):
        pid = self.get_pid()
        if pid == 0:
            return None, True
        elif pid < 0:
            logger.warning("Cannot stop server; single-user server is running (PID: {0})".format(-pid))
            return None, False
        try:
            os.kill(pid, STOP_SIGNALS[mode])
        except OSError as e:
            if e.errno == errno.ESRCH:
                return None, True
            else:
                logger.warning("Could not send stop signal to PostgreSQL (error: {0})".format(e.errno))
                return None, False
        return pid, None

    def terminate_starting_postmaster(self, pid):
        """Terminates a postmaster that has not yet opened ports or possibly even written a pid file. Blocks
        until the process goes away."""
        try:
            os.kill(pid, STOP_SIGNALS['immediate'])
        except OSError as e:
            if e.errno == errno.ESRCH:
                return
            logger.warning("Could not send stop signal to PostgreSQL (error: {0})".format(e.errno))

        while self.is_pid_running(pid):
            time.sleep(STOP_POLLING_INTERVAL)

    def _wait_for_connection_close(self, pid):
        try:
            with self.connection().cursor() as cur:
                while True:  # Need a timeout here?
                    if pid == self.get_pid() and self.is_pid_running(pid):
                        cur.execute("SELECT 1")
                        time.sleep(STOP_POLLING_INTERVAL)
                        continue
                    else:
                        break
        except psycopg2.Error:
            pass

    @staticmethod
    def _wait_for_user_backends_to_close(postmaster_pid):
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 9.6
        aux_proc_re = re.compile("(?:postgres:)( .*:)? (?:""(?:startup|logger|checkpointer|writer|wal writer|"
                                 "autovacuum launcher|autovacuum worker|stats collector|wal receiver|archiver|"
                                 "wal sender) process|bgworker: )")

        try:
            postmaster = psutil.Process(postmaster_pid)
            user_backends = [p for p in postmaster.children() if not aux_proc_re.match(p.cmdline()[0])]
            logger.debug("Waiting for user backends {0} to close".format(
                ",".join(p.cmdline()[0] for p in user_backends)))
            psutil.wait_procs(user_backends)
            logger.debug("Backends closed")
        except psutil.NoSuchProcess:
            return

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

    def restart(self, timeout=None, task=None):
        """Restarts PostgreSQL.

        When timeout parameter is set the call will block either until PostgreSQL has started, failed to start or
        timeout arrives.

        :returns: True when restart was successful and timeout did not expire when waiting.
        """
        self.set_state('restarting')
        self.__cb_pending = ACTION_ON_RESTART
        ret = self.stop(block_callbacks=True) and self.start(timeout=timeout, block_callbacks=True, task=task)
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
        if not (member and member.conn_url) or member.name == self.name:
            return None
        r = member.conn_kwargs(self._replication)
        r.update({'application_name': self.name, 'sslmode': 'prefer', 'sslcompression': '1'})
        keywords = 'user password host port sslmode sslcompression application_name'.split()
        return ' '.join('{0}={{{0}}}'.format(kw) for kw in keywords).format(**r)

    def check_recovery_conf(self, member):
        # TODO: recovery.conf could be stale, would be nice to detect that.
        primary_conninfo = self.primary_conninfo(member)

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

    def pg_rewind(self, r):
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
                data = subprocess.check_output([self._pgcommand('pg_controldata'), self._data_dir],
                                               env={'LANG': 'C', 'LC_ALL': 'C', 'PATH': os.environ['PATH']})
                if data:
                    data = data.decode('utf-8').splitlines()
                    # pg_controldata output depends on major verion. Some of parameters are prefixed by 'Current '
                    result = {l.split(':')[0].replace('Current ', '', 1): l.split(':', 1)[1].strip() for l in data if l}
            except subprocess.CalledProcessError:
                logger.exception("Error when calling pg_controldata")
        return result

    @property
    def need_rewind(self):
        return self._rewind_state in (REWIND_STATUS.CHECK, REWIND_STATUS.NEED)

    @staticmethod
    @contextmanager
    def _get_connection_cursor(**kwargs):
        with psycopg2.connect(**kwargs) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                yield cur

    @contextmanager
    def _get_replication_connection_cursor(self, host='localhost', port=5432, **kwargs):
        with self._get_connection_cursor(host=host, port=int(port), database=self._database, replication=1,
                                         user=self._replication['username'], password=self._replication['password'],
                                         connect_timeout=3, options='-c statement_timeout=2000') as cur:
            yield cur

    def check_leader_is_not_in_recovery(self, **kwargs):
        try:
            with self._get_connection_cursor(connect_timeout=3, options='-c statement_timeout=2000', **kwargs) as cur:
                cur.execute('SELECT pg_is_in_recovery()')
                if not cur.fetchone()[0]:
                    return True
                logger.info('Leader is still in_recovery and therefore can\'t be used for rewind')
        except Exception:
            return logger.exception('Exception when working with leader')

    def _get_local_timeline_lsn(self):
        timeline = lsn = None
        if self.is_running():  # if postgres is running - get timeline and lsn from replication connection
            try:
                with self._get_replication_connection_cursor(**self._local_replication_address) as cur:
                    cur.execute('IDENTIFY_SYSTEM')
                    timeline, lsn = cur.fetchone()[1:3]
            except Exception:
                logger.exception('Can not fetch local timeline and lsn from replication connection')
        else:  # otherwise analyze pg_controldata output
            data = self.controldata()
            try:
                if data.get('Database cluster state') == 'shut down':
                    lsn = data.get('Latest checkpoint location')
                    timeline = int(data.get("Latest checkpoint's TimeLineID"))
                elif data.get('Database cluster state') == 'shut down in recovery':
                    lsn = data.get('Minimum recovery ending location')
                    timeline = int(data.get("Min recovery ending loc's timeline"))
            except (TypeError, ValueError):
                logger.exception('Failed to get local timeline and lsn from pg_controldata output')
        logger.info('Local timeline=%s lsn=%s', timeline, lsn)
        return timeline, lsn

    def _check_timeline_and_lsn(self, leader):
        local_timeline, local_lsn = self._get_local_timeline_lsn()
        if local_timeline is None or local_lsn is None:
            return

        if not self.check_leader_is_not_in_recovery(**leader.conn_kwargs(self._superuser)):
            return

        history = need_rewind = None
        try:
            with self._get_replication_connection_cursor(**leader.conn_kwargs()) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                master_timeline = cur.fetchone()[1]
                logger.info('master_timeline=%s', master_timeline)
                if local_timeline > master_timeline:  # Not always supported by pg_rewind
                    need_rewind = True
                elif master_timeline > 1:
                    cur.execute('TIMELINE_HISTORY %s', (master_timeline,))
                    history = bytes(cur.fetchone()[1]).decode('utf-8')
                    logger.info('master: history=%s', history)
                else:  # local_timeline == master_timeline == 1
                    need_rewind = False
        except Exception:
            return logger.exception('Exception when working with master via replication connection')

        if history is not None:
            def parse_lsn(lsn):
                t = lsn.split('/')
                return int(t[0], 16) * 0x100000000 + int(t[1], 16)

            for line in history.split('\n'):
                line = line.strip().split('\t')
                if len(line) == 3:
                    try:
                        timeline = int(line[0])
                        if timeline == local_timeline:
                            try:
                                need_rewind = parse_lsn(local_lsn) >= parse_lsn(line[1])
                            except ValueError:
                                logger.exception('Exception when parsing lsn')
                            break
                        elif timeline > local_timeline:
                            break
                    except ValueError:
                        continue

        self._rewind_state = need_rewind and REWIND_STATUS.NEED or REWIND_STATUS.NOT_NEED

    def rewind(self, leader):
        if self.is_running() and not self.stop(checkpoint=False):
            return logger.warning('Can not run pg_rewind because postgres is still running')

        # prepare pg_rewind connection
        r = leader.conn_kwargs(self._superuser)

        # first make sure that we are really trying to rewind
        # from the master and run a checkpoint on it in order to
        # make it store the new timeline (5540277D.8020309@iki.fi)
        leader_status = self.checkpoint(r)
        if leader_status:
            return logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)

        if self.pg_rewind(r):
            self._rewind_state = REWIND_STATUS.SUCCESS
        elif not self.check_leader_is_not_in_recovery(**r):
            logger.warning('Failed to rewind because master %s become unreachable', leader.name)
        else:
            logger.error('Failed to rewind from healty master: %s', leader.name)

            if self.config.get('remove_data_directory_on_rewind_failure', False):
                logger.warning('remove_data_directory_on_rewind_failure is set. removing...')
                self.remove_data_directory()
                self._rewind_state = REWIND_STATUS.INITIAL
            else:
                self._rewind_state = REWIND_STATUS.FAILED
        return False

    def trigger_check_diverged_lsn(self):
        if self.can_rewind and self._rewind_state != REWIND_STATUS.NEED:
            self._rewind_state = REWIND_STATUS.CHECK

    def rewind_needed_and_possible(self, leader):
        if leader and leader.name != self.name and leader.conn_url and self._rewind_state == REWIND_STATUS.CHECK:
            self._check_timeline_and_lsn(leader)
        return leader and leader.conn_url and self._rewind_state == REWIND_STATUS.NEED

    @property
    def rewind_executed(self):
        return self._rewind_state > REWIND_STATUS.NOT_NEED

    def follow(self, member, timeout=None):
        primary_conninfo = self.primary_conninfo(member)
        change_role = self.role in ('master', 'demoted')

        self.write_recovery_conf(primary_conninfo)
        if self.is_running():
            self.restart()
        else:
            self.start(timeout=timeout)
        self.set_role('replica')

        if change_role:
            # TODO: postpone this until start completes, or maybe do even earlier
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return True

    def _do_rewind(self, leader):
        logger.info("rewind flag is set")

        if self.is_running() and not self.stop(checkpoint=False):
            logger.warning('Can not run pg_rewind because postgres is still running')
            return False

        # prepare pg_rewind connection
        r = leader.conn_kwargs(self._superuser)

        # first make sure that we are really trying to rewind
        # from the master and run a checkpoint on a t in order to
        # make it store the new timeline (5540277D.8020309@iki.fi)
        leader_status = self.checkpoint(r)
        if leader_status:
            logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)
            return False

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

        try:
            if not self.rewind(r):
                logger.error('unable to rewind the former master')
                if self.config.get('remove_data_directory_on_rewind_failure', False):
                    self.remove_data_directory()
                    return False
            return True
        finally:
            self._need_rewind = False

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
            logger.info("cleared rewind state after becoming the leader")
            self._rewind_state = REWIND_STATUS.INITIAL
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

    def wal_position(self, retry=True):
        stmt = """SELECT CASE WHEN pg_is_in_recovery()
                              THEN GREATEST(pg_{0}_{1}_diff(COALESCE(pg_last_{0}_receive_{1}(), '0/0'),
                                                                  '0/0')::bigint,
                                            pg_{0}_{1}_diff(pg_last_{0}_replay_{1}(), '0/0')::bigint)
                              ELSE pg_{0}_{1}_diff(pg_current_{0}_{1}(), '0/0')::bigint
                          END""".format(self.wal_name, self.lsn_name)

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
                    cursor = self._query("""SELECT pg_drop_replication_slot(%s)
                                             WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                                             WHERE slot_name = %s AND NOT active)""", slot, slot)

                    if cursor.rowcount != 1:  # Either slot doesn't exists or it is still active
                        self._schedule_load_slots = True  # schedule load_replication_slots on the next iteration

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
        return str(self.wal_position())

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
            self._server_parameters = self.get_server_parameters(self.config)
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
        # save environ to restore it later
        old_env = os.environ.copy()
        os.environ.clear()
        os.environ.update(env)

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
                version = 0
                with psycopg2.connect(conn_url + '?replication=1') as c:
                    version = c.server_version

                ret = subprocess.call([self._pgcommand('pg_basebackup'), '--pgdata=' + self._data_dir,
                                       '--{0}-method=stream'.format(self._wal_name(version)), '--dbname=' + conn_url])
                if ret == 0:
                    break
                else:
                    logger.error('Error when fetching backup: pg_basebackup exited with code=%s', ret)

            except psycopg2.Error:
                logger.error('Can not connect to %s', conn_url)
            except Exception as e:
                logger.error('Error when fetching backup with pg_basebackup: %s', e)

            if bbfailures < maxfailures - 1:
                logger.warning('Trying again in 5 seconds')
                time.sleep(5)

        # restore environ
        os.environ.clear()
        os.environ.update(old_env)

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
                    ORDER BY flush_{0} DESC""".format(self.lsn_name)):
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

    @staticmethod
    def postgres_major_version_to_int(pg_version):
        """
        >>> Postgresql.postgres_major_version_to_int('10')
        100000
        >>> Postgresql.postgres_major_version_to_int('9.6')
        90600
        """
        return Postgresql.postgres_version_to_int(pg_version + '.0')
