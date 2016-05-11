import logging
import os
import psycopg2
import shlex
import shutil
import subprocess
import tempfile
import time

from patroni.exceptions import PostgresConnectionException, PostgresException
from patroni.utils import Retry, RetryFailedError
from six import string_types
from six.moves.urllib_parse import urlparse
from threading import Lock

logger = logging.getLogger(__name__)

ACTION_ON_START = "on_start"
ACTION_ON_STOP = "on_stop"
ACTION_ON_RESTART = "on_restart"
ACTION_ON_RELOAD = "on_reload"
ACTION_ON_ROLE_CHANGE = "on_role_change"


def parseurl(url):
    r = urlparse(url)
    ret = {
        'host': r.hostname,
        'port': r.port or 5432,
        'database': r.path[1:],
        'fallback_application_name': 'Patroni',
        'connect_timeout': 3,
        'options': '-c statement_timeout=2000',
    }
    if r.username:
        ret['user'] = r.username
    if r.password:
        ret['password'] = r.password
    return ret


class Postgresql(object):

    def __init__(self, config):
        self.config = config
        self.name = config['name']
        self._server_parameters = self.get_server_parameters(config)
        self._listen_addresses, self._port = (config['listen'] + ':5432').split(':')[:2]

        self.scope = config['scope']
        self._data_dir = config['data_dir']
        self.replication = config['replication']
        self.superuser = config.get('superuser') or {}
        self.admin = config.get('admin') or {}

        self.initdb_options = config.get('initdb') or []
        self.pgpass = config.get('pgpass') or os.path.join(os.path.expanduser('~'), 'pgpass')
        self.pg_rewind = config.get('pg_rewind') or {}
        self.callback = config.get('callbacks') or {}
        self.use_slots = config.get('use_slots', True)
        self._schedule_load_slots = self.use_slots
        self._postgresql_conf = os.path.join(self._data_dir, 'postgresql.conf')
        self._postgresql_base_conf_name = 'postgresql.base.conf'
        self._postgresql_base_conf = os.path.join(self._data_dir, self._postgresql_base_conf_name)
        self._recovery_conf = os.path.join(self._data_dir, 'recovery.conf')
        self._configuration_to_save = (self._postgresql_conf, self._postgresql_base_conf,
                                       os.path.join(self._data_dir, 'pg_hba.conf'))
        self._postmaster_pid = os.path.join(self._data_dir, 'postmaster.pid')
        self._trigger_file = config.get('recovery_conf', {}).get('trigger_file') or 'promote'
        self._trigger_file = os.path.abspath(os.path.join(self._data_dir, self._trigger_file))

        self._pg_ctl = ['pg_ctl', '-w', '-D', self._data_dir]

        self.local_address = self.get_local_address()
        connect_address = config.get('connect_address') or self.local_address
        self.connection_string = 'postgres://{username}:{password}@{connect_address}/postgres'.format(
            connect_address=connect_address, **self.replication)

        self._connection = None
        self._cursor_holder = None
        self._sysid = None
        self._replication_slots = []  # list of already existing replication slots
        self.retry = Retry(max_tries=-1, deadline=5, max_delay=1, retry_exceptions=PostgresConnectionException)

        self._state_lock = Lock()
        self.set_state('stopped')
        self._role_lock = Lock()
        self.set_role(self.get_postgres_role_from_data_directory())

        if self.is_running():
            self.set_state('running')
            self.set_role('master' if self.is_leader() else 'replica')
            self._write_postgresql_conf()  # we are "joining" already running postgres

    @staticmethod
    def get_server_parameters(config):
        return {p: v for p, v in (config.get('parameters') or {}).items() if p not in ('listen_addresses', 'port')}

    @property
    def can_rewind(self):
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        # low-hanging fruit: check if pg_rewind configuration is there
        if not self.pg_rewind or\
           not (self.pg_rewind.get('username', '') and self.pg_rewind.get('password', '')):
            return False

        cmd = ['pg_rewind', '--help']
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
        listen_addresses = self._listen_addresses.split(',')
        local_address = listen_addresses[0].strip()  # take first address from listen_addresses

        for la in listen_addresses:
            if la.strip() in ['*', '0.0.0.0']:  # we are listening on *
                local_address = 'localhost'  # connection via localhost is preferred
                break
        return local_address + ':' + self._port

    def get_postgres_role_from_data_directory(self):
        if self.data_directory_empty():
            return 'uninitialized'
        elif os.path.exists(self._recovery_conf):
            return 'replica'
        else:
            return 'master'

    @property
    def _connect_kwargs(self):
        r = parseurl('postgres://{0}/postgres'.format(self.local_address))
        if 'username' in self.superuser:
            r['user'] = self.superuser['username']
        if 'password' in self.superuser:
            r['password'] = self.superuser['password']
        return r

    def connection(self):
        if not self._connection or self._connection.closed != 0:
            self._connection = psycopg2.connect(**self._connect_kwargs)
            self._connection.autocommit = True
            self.server_version = self._connection.server_version
        return self._connection

    def _cursor(self):
        if not self._cursor_holder or self._cursor_holder.closed or self._cursor_holder.connection.closed != 0:
            logger.info("establishing a new patroni connection to the postgres cluster")
            self._cursor_holder = self.connection().cursor()
        return self._cursor_holder

    def close_connection(self):
        if self._cursor_holder and self._cursor_holder.connection and self._cursor_holder.connection.closed == 0:
            self._cursor_holder.connection.close()
            logger.info("closed patroni connection to the postgresql cluster")

    def _query(self, sql, *params):
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

    def get_initdb_options(self):
        options = []
        for o in self.initdb_options:
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

    def initialize(self):
        self.set_state('initalizing new cluster')
        options = self.get_initdb_options()
        pwfile = None

        if self.superuser:
            if 'username' in self.superuser:
                options.append('--username={0}'.format(self.superuser['username']))
            if 'password' in self.superuser:
                (fd, pwfile) = tempfile.mkstemp()
                os.write(fd, self.superuser['password'].encode('utf-8'))
                os.close(fd)
                options.append('--pwfile={0}'.format(pwfile))

        ret = subprocess.call(self._pg_ctl + ['initdb'] + (['-o', ' '.join(options)] if options else [])) == 0
        if pwfile:
            os.remove(pwfile)
        if ret:
            self.write_pg_hba()
        else:
            self.set_state('initdb failed')
        return ret

    def delete_trigger_file(self):
        if os.path.exists(self._trigger_file):
            os.unlink(self._trigger_file)

    def write_pgpass(self, record):
        with open(self.pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**record))

        env = os.environ.copy()
        env['PGPASSFILE'] = self.pgpass
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

        if clone_member:
            connstring = clone_member.conn_url
            # add the credentials to connect to the replica origin to pgpass.
            env = self.write_pgpass(parseurl(clone_member.conn_url))
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
                try:
                    method_config.update({"scope": self.scope,
                                          "role": "replica",
                                          "datadir": self._data_dir,
                                          "connstring": connstring})
                    params = ["--{0}={1}".format(arg, val) for arg, val in method_config.items()]
                    # call script with the full set of parameters
                    ret = subprocess.call(shlex.split(cmd) + params, env=env)
                    # if we succeeded, stop
                    if ret == 0:
                        logger.info('replica has been created using %s', replica_method)
                        break
                except Exception:
                    logger.exception('Error creating replica using method %s', replica_method)
                    ret = 1

        self.set_state('stopped')
        return ret

    def is_leader(self):
        return not self.query('SELECT pg_is_in_recovery()').fetchone()[0]

    def is_running(self):
        return subprocess.call(' '.join(self._pg_ctl) + ' status > /dev/null 2>&1', shell=True) == 0

    def call_nowait(self, cb_name):
        """ pick a callback command and call it without waiting for it to finish """
        if not self.callback or cb_name not in self.callback:
            return False
        cmd = self.callback[cb_name]
        try:
            subprocess.Popen(shlex.split(cmd) + [cb_name, self.role, self.scope])
        except OSError:
            logger.exception('callback %s %s %s %s failed', cmd, cb_name, self.role, self.scope)
            return False
        return True

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

    def start(self, block_callbacks=False):
        if self.is_running():
            logger.error('Cannot start PostgreSQL because one is already running.')
            return True

        self.set_role(self.get_postgres_role_from_data_directory())
        if os.path.exists(self._postmaster_pid):
            os.remove(self._postmaster_pid)
            logger.info('Removed %s', self._postmaster_pid)

        if not block_callbacks:
            self.set_state('starting')

        env = {'PATH': os.environ.get('PATH')}
        # pg_ctl will write a FATAL if the username is incorrect. exporting PGUSER if necessary
        if 'username' in self.superuser and self.superuser['username'] != os.environ.get('USER'):
            env['PGUSER'] = self.superuser['username']
        self._write_postgresql_conf()
        server_arguments = ['-o', "--listen_addresses='{0}' --port={1}".format(self._listen_addresses, self._port)]
        ret = subprocess.call(self._pg_ctl + ['start'] + server_arguments, env=env, preexec_fn=os.setsid) == 0

        self.set_state('running' if ret else 'start failed')

        self._schedule_load_slots = ret and self.use_slots
        self.save_configuration_files()
        # block_callbacks is used during restart to avoid
        # running start/stop callbacks in addition to restart ones
        if ret and not block_callbacks:
            self.call_nowait(ACTION_ON_START)
        return ret

    def checkpoint(self, connect_kwargs=None):
        connect_kwargs = connect_kwargs or self._connect_kwargs
        for p in ['connect_timeout', 'options']:
            connect_kwargs.pop(p, None)
        try:
            with psycopg2.connect(**connect_kwargs) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = 0")
                    cur.execute('CHECKPOINT')
        except psycopg2.Error:
            logging.exception('Exception during CHECKPOINT')

    def stop(self, mode='fast', block_callbacks=False, checkpoint=True):
        # make sure we close all connections established against
        # the former node, otherwise, we might get a stalled one
        # after kill -9, which would report incorrect data to
        # patroni.

        self.close_connection()
        if not self.is_running():
            if not block_callbacks:
                self.set_state('stopped')
            return True

        if checkpoint:
            self.checkpoint()

        if not block_callbacks:
            self.set_state('stopping')

        ret = subprocess.call(self._pg_ctl + ['stop', '-m', mode]) == 0
        # block_callbacks is used during restart to avoid
        # running start/stop callbacks in addition to restart ones
        if not ret:
            self.set_state('stop failed')
        elif not block_callbacks:
            self.set_state('stopped')
            self.call_nowait(ACTION_ON_STOP)
        return ret

    def reload(self):
        ret = subprocess.call(self._pg_ctl + ['reload']) == 0
        if ret:
            self.call_nowait(ACTION_ON_RELOAD)
        return ret

    def restart(self):
        self.set_state('restarting')
        ret = self.stop(block_callbacks=True) and self.start(block_callbacks=True)
        if ret:
            self.call_nowait(ACTION_ON_RESTART)
        else:
            self.set_state('restart failed ({0})'.format(self.state))
        return ret

    def _write_postgresql_conf(self):
        # rename the original configuration if it is necessary
        if not os.path.exists(self._postgresql_base_conf):
            os.rename(self._postgresql_conf, self._postgresql_base_conf)

        with open(self._postgresql_conf, 'w') as f:
            f.write('# Do not edit this file manually!\n# It will be overwritten by Patroni!\n')
            f.write("include '{0}'\n\n".format(self._postgresql_base_conf_name))
            for setting, value in sorted(self._server_parameters.items()):
                f.write("{0} = '{1}'\n".format(setting, value))

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def check_replication_lag(self, last_leader_operation):
        return (last_leader_operation or 0) - self.xlog_position() <= self.config.get('maximum_lag_on_failover', 0)

    def write_pg_hba(self):
        with open(os.path.join(self._data_dir, 'pg_hba.conf'), 'a') as f:
            f.write('\n{}\n'.format('\n'.join(self.config.get('pg_hba', []))))

    def primary_conninfo(self, leader_url):
        r = parseurl(leader_url)
        r.update({'application_name': self.name, 'sslmode': 'prefer', 'sslcompression': '1'})
        keywords = 'user password host port sslmode sslcompression application_name'.split()
        return ' '.join('{0}={{{0}}}'.format(kw) for kw in keywords).format(**r)

    def check_recovery_conf(self, leader):
        if not os.path.isfile(self._recovery_conf):
            return False

        pattern = leader and leader.conn_url and self.primary_conninfo(leader.conn_url)

        with open(self._recovery_conf, 'r') as f:
            for line in f:
                if line.startswith('primary_conninfo'):
                    return pattern and (pattern in line)
        return not pattern

    def write_recovery_conf(self, leader):
        with open(self._recovery_conf, 'w') as f:
            f.write("standby_mode = 'on'\nrecovery_target_timeline = 'latest'\n")
            if leader and leader.conn_url:
                f.write("primary_conninfo = '{0}'\n".format(self.primary_conninfo(leader.conn_url)))
                if self.use_slots:
                    f.write("primary_slot_name = '{0}'\n".format(self.name))
            for name, value in self.config.get('recovery_conf', {}).items():
                if name not in ('standby_mode', 'recovery_target_timeline', 'primary_conninfo', 'primary_slot_name'):
                    f.write("{0} = '{1}'\n".format(name, value))

    def rewind(self, leader):
        # prepare pg_rewind connection
        r = parseurl(leader.conn_url)
        r.update(self.pg_rewind)
        r['user'] = r.pop('username')
        env = self.write_pgpass(r)
        pc = "user={user} host={host} port={port} dbname=postgres sslmode=prefer sslcompression=1".format(**r)
        # first run a checkpoint on a promoted master in order
        # to make it store the new timeline (5540277D.8020309@iki.fi)
        self.checkpoint(r)
        logger.info("running pg_rewind from %s", pc)
        pg_rewind = ['pg_rewind', '-D', self._data_dir, '--source-server', pc]
        try:
            ret = subprocess.call(pg_rewind, env=env) == 0
        except OSError:
            ret = False
        if ret:
            self.write_recovery_conf(leader)
        return ret

    def controldata(self):
        """ return the contents of pg_controldata, or non-True value if pg_controldata call failed """
        result = {}
        if self.state != 'creating replica':  # Don't try to call pg_controldata during backup restore
            try:
                data = subprocess.check_output(['pg_controldata', self._data_dir])
                if data:
                    data = data.decode('utf-8').splitlines()
                    result = {l.split(':')[0].replace('Current ', '', 1): l.split(':')[1].strip() for l in data if l}
            except subprocess.CalledProcessError:
                logger.exception("Error when calling pg_controldata")
        return result

    def single_user_mode(self, command=None, options=None):
        """ run a given command in a single-user mode. If the command is empty - then just start and stop """
        cmd = ['postgres', '--single', '-D', self._data_dir]
        for opt, val in sorted((options or {}).items()):
            cmd.extend(['-c', '{0}={1}'.format(opt, val)])
        # need a database name to connect
        cmd.append('postgres')
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

    def follow(self, leader, recovery=False):
        if self.check_recovery_conf(leader) and not recovery:
            return True
        change_role = self.role == 'master'
        need_rewind = change_role and self.can_rewind
        if need_rewind:
            logger.info("set the rewind flag after demote")
        self.write_recovery_conf(leader)
        if leader and need_rewind:  # we have a leader and need to rewind
            if self.is_running():
                self.stop()
            # at present, pg_rewind only runs when the cluster is shut down cleanly
            # and not shutdown in recovery. We have to remove the recovery.conf if present
            # and start/shutdown in a single user mode to emulate this.
            # XXX: if recovery.conf is linked, it will be written anew as a normal file.
            if os.path.islink(self._recovery_conf):
                os.unlink(self._recovery_conf)
            else:
                os.remove(self._recovery_conf)
            # Archived segments might be useful to pg_rewind,
            # clean the flags that tell we should remove them.
            self.cleanup_archive_status()
            # Start in a single user mode and stop to produce a clean shutdown
            self.single_user_mode(options={'archive_mode': 'on', 'archive_command': 'false'})
            if self.rewind(leader):
                ret = self.start()
            else:
                logger.error("unable to rewind the former master")
                self.remove_data_directory()
                ret = True
        else:  # do not rewind until the leader becomes available
            ret = self.restart()
        if change_role and ret:
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return ret

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
        ret = subprocess.call(self._pg_ctl + ['promote']) == 0
        if ret:
            self.set_role('master')
            logger.info("cleared rewind flag after becoming the leader")
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return ret

    def create_or_update_role(self, name, password, options):
        self.query("""DO $$
BEGIN
    SET local synchronous_commit = 'local';
    PERFORM * FROM pg_authid WHERE rolname = %s;
    IF FOUND THEN
        ALTER ROLE "{0}" WITH LOGIN {1} PASSWORD %s;
    ELSE
        CREATE ROLE "{0}" WITH LOGIN {1} PASSWORD %s;
    END IF;
END;
$$""".format(name, options), name, password, password)

    def create_replication_user(self):
        self.create_or_update_role(self.replication['username'], self.replication['password'], 'REPLICATION')

    def create_connection_user(self):
        if self.admin:
            self.create_or_update_role(self.admin['username'], self.admin['password'], 'CREATEDB CREATEROLE')

    def xlog_position(self):
        return self.query("""SELECT pg_xlog_location_diff(CASE WHEN pg_is_in_recovery()
                                                               THEN pg_last_xlog_replay_location()
                                                               ELSE pg_current_xlog_location()
                                                          END, '0/0')::bigint""").fetchone()[0]

    def load_replication_slots(self):
        if self.use_slots and self._schedule_load_slots:
            cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
            self._replication_slots = [r[0] for r in cursor]
            self._schedule_load_slots = False

    def sync_replication_slots(self, cluster):
        if self.use_slots:
            try:
                self.load_replication_slots()
                # if the replicatefrom tag is set on the member - we should not create the replication slot for it on
                # the current master, because that member would replicate from elsewhere. We still create the slot if
                # the replicatefrom destination member is currently not a member of the cluster (fallback to the
                # master), or if replicatefrom destination member happens to be the current master
                if self.role == 'master':
                    slots = [m.name for m in cluster.members if m.name != self.name and
                             (m.replicatefrom is None or m.replicatefrom == self.name or
                              not cluster.has_member(m.replicatefrom))]
                else:
                    # only manage slots for replicas that replicate from this one, except for the leader among them
                    slots = [m.name for m in cluster.members if m.replicatefrom == self.name and
                             m.name != cluster.leader.name]
                # drop unused slots
                for slot in set(self._replication_slots) - set(slots):
                    self.query("""SELECT pg_drop_replication_slot(%s)
                                   WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                                   WHERE slot_name = %s)""", slot, slot)

                # create new slots
                for slot in set(slots) - set(self._replication_slots):
                    self.query("""SELECT pg_create_physical_replication_slot(%s)
                                   WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                                   WHERE slot_name = %s)""", slot, slot)

                self._replication_slots = slots
            except psycopg2.Error:
                logger.exception('Exception when changing replication slots')

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
            self.delete_trigger_file()
            self.restore_configuration_files()
        return ret

    def bootstrap(self):
        """ Initialize a new node from scratch and start it. """
        if self.initialize() and self.start():
            self.create_replication_user()
            self.create_connection_user()
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
            try:
                ret = subprocess.call(['pg_basebackup', '--pgdata=' + self._data_dir,
                                       '--xlog-method=stream', "--dbname=" + conn_url], env=env)
                if ret == 0:
                    break

            except Exception as e:
                logger.error('Error when fetching backup with pg_basebackup: {0}'.format(e))

            if bbfailures < maxfailures - 1:
                logger.error('Trying again in 5 seconds')
                time.sleep(5)

        return ret
