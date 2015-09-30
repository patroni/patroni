import logging
import os
import psycopg2
import shlex
import shutil
import subprocess
import time

from patroni.exceptions import PostgresConnectionException, PostgresException
from patroni.utils import Retry, RetryFailedError
from six.moves.urllib_parse import urlparse

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


class Postgresql:

    def __init__(self, config):
        self.config = config
        self.name = config['name']
        self.scope = config['scope']
        self.listen_addresses, self.port = config['listen'].split(':')
        self.data_dir = config['data_dir']
        self.replication = config['replication']
        self.superuser = config['superuser']
        self.admin = config['admin']
        self._pg_rewind = config.get('pg_rewind', {})
        self.callback = config.get('callbacks', {})
        self.use_slots = config.get('use_slots', True)
        self.schedule_load_slots = self.use_slots
        self.recovery_conf = os.path.join(self.data_dir, 'recovery.conf')
        self.configuration_to_save = (os.path.join(self.data_dir, 'pg_hba.conf'),
                                      os.path.join(self.data_dir, 'postgresql.conf'))
        self.postmaster_pid = os.path.join(self.data_dir, 'postmaster.pid')
        self.trigger_file = config.get('recovery_conf', {}).get('trigger_file', None) or 'promote'
        self.trigger_file = os.path.abspath(os.path.join(self.data_dir, self.trigger_file))
        self._role = 'replica'

        self._pg_ctl = ['pg_ctl', '-w', '-D', self.data_dir]

        self.local_address = self.get_local_address()
        connect_address = config.get('connect_address', None) or self.local_address
        self.connection_string = 'postgres://{username}:{password}@{connect_address}/postgres'.format(
            connect_address=connect_address, **self.replication)

        self._connection = None
        self._cursor_holder = None
        self.members = []  # list of already existing replication slots
        self.retry = Retry(max_tries=-1, deadline=10, max_delay=1, retry_exceptions=PostgresConnectionException)
        self.init_pg_rewind()

    def init_pg_rewind(self):
        try:
            self._pg_rewind_present = ('username' in self._pg_rewind and
                                       ('wal_log_hints' in self.config['parameters'] or
                                        'data_checksums' in self.config['parameters']) and
                                       subprocess.call(['pg_rewind',
                                                        '--version'],
                                                       stdout=open(os.devnull, 'w'),
                                                       stderr=subprocess.STDOUT) == 0)
            if self._pg_rewind_present:
                self._pg_rewind['user'] = self._pg_rewind['username']
        except:
            self._pg_rewind_present = False
        if self._pg_rewind and not self._pg_rewind_present:
            logger.warning("pg_rewind support is disabled")

    def get_local_address(self):
        listen_addresses = self.listen_addresses.split(',')
        local_address = listen_addresses[0].strip()  # take first address from listen_addresses

        for la in listen_addresses:
            if la.strip() in ['*', '0.0.0.0']:  # we are listening on *
                local_address = 'localhost'  # connection via localhost is preferred
                break
        return local_address + ':' + self.port

    def connection(self):
        if not self._connection or self._connection.closed != 0:
            r = parseurl('postgres://{}/postgres'.format(self.local_address))
            self._connection = psycopg2.connect(**r)
            self._connection.autocommit = True
        return self._connection

    def _cursor(self):
        if not self._cursor_holder or self._cursor_holder.closed or self._cursor_holder.connection.closed != 0:
            self._cursor_holder = self.connection().cursor()
        return self._cursor_holder

    def _query(self, sql, *params):
        cursor = None
        try:
            cursor = self._cursor()
            cursor.execute(sql, params)
            return cursor
        except psycopg2.Error as e:
            if cursor and cursor.connection.closed == 0:
                raise e
            raise PostgresConnectionException('connection problems')

    def query(self, sql, *params):
        try:
            return self.retry(self._query, sql, *params)
        except RetryFailedError as e:
            raise PostgresConnectionException(str(e))

    def data_directory_empty(self):
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        ret = subprocess.call(self._pg_ctl + ['initdb', '-o', '--encoding=UTF8']) == 0
        ret and self.write_pg_hba()
        return ret

    def delete_trigger_file(self):
        os.path.exists(self.trigger_file) and os.unlink(self.trigger_file)

    def write_pgpass(self, record, append=False):
        pgpass = 'pgpass'
        with open(pgpass, 'w' if not append else 'a') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**record))
        env = os.environ.copy()
        env['PGPASSFILE'] = pgpass
        return env

    def sync_from_leader(self, leader):
        r = parseurl(leader.conn_url)

        env = self.write_pgpass(r)
        return self.create_replica(r, env) == 0

    @staticmethod
    def build_connstring(conn):
        return "host={host} port={port} user={user}".format(**conn)

    def create_replica(self, master_connection, env):
        connstring = self.build_connstring(master_connection)
        cmd = self.config['restore']
        try:
            ret = subprocess.call(shlex.split(cmd) + [self.scope, "replica", self.data_dir, connstring], env=env)
            self.delete_trigger_file()
        except:
            logger.exception('Error when creating replica')
            return 1
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
        except:
            logger.exception('callback %s %s %s %s failed', cmd, cb_name, self.role, self.scope)
            return False
        return True

    @property
    def role(self):
        return self._role

    def start(self, block_callbacks=False):
        if self.is_running():
            self._role = 'master' if self.is_leader() else 'replica'
            self.schedule_load_slots = self.use_slots
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        self._role = 'replica' if os.path.exists(self.recovery_conf) else 'master'
        if os.path.exists(self.postmaster_pid):
            os.remove(self.postmaster_pid)
            logger.info('Removed %s', self.postmaster_pid)

        ret = subprocess.call(self._pg_ctl + ['start', '-o', self.server_options()]) == 0
        self.schedule_load_slots = ret and self.use_slots
        self.save_configuration_files()
        # block_callbacks is used during restart to avoid
        # running start/stop callbacks in addition to restart ones
        ret and not block_callbacks and ret and self.call_nowait(ACTION_ON_START)
        return ret

    def stop(self, mode='fast', block_callbacks=False):
        if block_callbacks:
            try:
                self.query('SET statement_timeout TO 0')
                self.query('CHECKPOINT')
            except:
                logging.exception('Exception diring CHECKPOINT')

        ret = subprocess.call(self._pg_ctl + ['stop', '-m', mode]) == 0
        # block_callbacks is used during restart to avoid
        # running start/stop callbacks in addition to restart ones
        ret and not block_callbacks and self.call_nowait(ACTION_ON_STOP)
        return ret

    def reload(self):
        ret = subprocess.call(self._pg_ctl + ['reload']) == 0
        ret and self.call_nowait(ACTION_ON_RELOAD)
        return ret

    def restart(self):
        ret = self.stop(block_callbacks=True) and self.start(block_callbacks=True)
        ret and self.call_nowait(ACTION_ON_RESTART)
        return ret

    def server_options(self):
        options = "--listen_addresses='{}' --port={}".format(self.listen_addresses, self.port)
        for setting, value in self.config['parameters'].items():
            options += " --{}='{}'".format(setting, value)
        return options

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False
        return True

    def is_healthiest_node(self, cluster):
        if self.is_leader():
            return True

        if cluster.last_leader_operation - self.xlog_position() > self.config.get('maximum_lag_on_failover', 0):
            return False

        for member in cluster.members:
            if member.name == self.name:
                continue
            try:
                r = parseurl(member.conn_url)
                member_conn = psycopg2.connect(**r)
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute(
                    "SELECT pg_is_in_recovery(), %s - pg_xlog_location_diff(pg_last_xlog_replay_location(), '0/0')",
                    (self.xlog_position(),))
                row = member_cursor.fetchone()
                member_cursor.close()
                member_conn.close()
                logger.error([self.name, member.name, row])
                if not row[0]:
                    logger.warning('Master (%s) is still alive', member.name)
                    return False
                if row[1] < 0:
                    return False
            except psycopg2.Error:
                continue
        return True

    def write_pg_hba(self):
        with open(os.path.join(self.data_dir, 'pg_hba.conf'), 'a') as f:
            f.write('\nhost replication {username} {network} md5\n'.format(**self.replication))
            for line in self.config.get('pg_hba', []):
                if line.split()[0].strip() == 'hostssl' and self.config['parameters'].get('ssl', 'off').lower() != 'on':
                    continue
                f.write(line + '\n')

    @staticmethod
    def primary_conninfo(leader_url):
        r = parseurl(leader_url)
        return 'user={user} password={password} host={host} port={port} sslmode=prefer sslcompression=1'.format(**r)

    def check_recovery_conf(self, leader):
        if not os.path.isfile(self.recovery_conf):
            return False

        pattern = leader and leader.conn_url and self.primary_conninfo(leader.conn_url)

        with open(self.recovery_conf, 'r') as f:
            for line in f:
                if line.startswith('primary_conninfo'):
                    if not pattern:
                        return False
                    return pattern in line

        return not pattern

    def write_recovery_conf(self, leader):
        with open(self.recovery_conf, 'w') as f:
            f.write("""standby_mode = 'on'
recovery_target_timeline = 'latest'
""")
            if leader and leader.conn_url:
                f.write("""primary_conninfo = '{}'\n""".format(self.primary_conninfo(leader.conn_url)))
                if self.use_slots:
                    f.write("""primary_slot_name = '{}'\n""".format(self.name))
                for name, value in self.config.get('recovery_conf', {}).items():
                    f.write("{} = '{}'\n".format(name, value))

    def prepare_pg_rewind_connection(self, leader_url, pg_rewind):
        r = parseurl(leader_url)
        r.update(pg_rewind)
        env = self.write_pgpass(r, append=True)
        return (env, "user={user} host={host} port={port} dbname=postgres sslmode=prefer sslcompression=1".format(**r))

    def pg_rewind(self, leader):
        env, pc = self.prepare_pg_rewind_connection(leader.conn_url, self._pg_rewind)
        logger.info("running pg_rewind from {}".format(pc))
        pg_rewind = ['pg_rewind', '-D', self.data_dir, '--source-server', pc]
        try:
            ret = (subprocess.call(pg_rewind, env=env) == 0)
        except:
            ret = False
        if ret:
            self.write_recovery_conf(leader)
        return ret

    def follow_the_leader(self, leader):
        if not self.check_recovery_conf(leader):
            self.write_recovery_conf(leader)
            change_role = self.role == 'master'

            if leader and change_role and self._pg_rewind_present:
                self.stop()
                if self.pg_rewind(leader):
                    ret = self.start()
                else:
                    ret = False
                    self.remove_data_directory()
                    logger.error("unable to rewind the former leader")
            else:
                ret = self.restart()
            change_role and ret and self.call_nowait(ACTION_ON_ROLE_CHANGE)

    def save_configuration_files(self):
        """
            copy postgresql.conf to postgresql.conf.backup to preserve it in the WAL-e backup.
            see http://comments.gmane.org/gmane.comp.db.postgresql.wal-e/239
        """
        for f in self.configuration_to_save:
            shutil.copy(f, f + '.backup')

    def restore_configuration_files(self):
        """ restore a previously saved postgresql.conf """
        try:
            for f in self.configuration_to_save:
                shutil.copy(f + '.backup', f)
        except:
            logger.exception('unable to restore configuration from WAL-E backup')

    def promote(self):
        if self.role == 'master':
            return True
        ret = subprocess.call(self._pg_ctl + ['promote']) == 0
        if ret:
            self._role = 'master'
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return ret

    def demote(self, leader):
        self.follow_the_leader(leader)

    def create_replication_user(self):
        self.query('CREATE USER "{}" WITH REPLICATION ENCRYPTED PASSWORD %s'.format(
            self.replication['username']), self.replication['password'])

    def create_connection_users(self):
        if self.superuser:
            if 'username' in self.superuser:
                self.query('CREATE ROLE "{0}" WITH LOGIN SUPERUSER PASSWORD %s'.format(
                    self.superuser['username']), self.superuser['password'])
            else:
                rolsuper = self.query("""SELECT rolname FROM pg_authid WHERE rolsuper = 't'""").fetchone()[0]
                self.query('ALTER ROLE "{0}" WITH PASSWORD %s'.format(rolsuper), self.superuser['password'])
        if self.admin:
            self.query('CREATE ROLE "{0}" WITH LOGIN CREATEDB CREATEROLE PASSWORD %s'.format(
                self.admin['username']), self.admin['password'])

    def xlog_position(self):
        return self.query("""SELECT pg_xlog_location_diff(CASE WHEN pg_is_in_recovery()
                                                               THEN pg_last_xlog_replay_location()
                                                               ELSE pg_current_xlog_location()
                                                          END, '0/0')""").fetchone()[0]

    def load_replication_slots(self):
        if self.use_slots and self.schedule_load_slots:
            cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
            self.members = [r[0] for r in cursor]
            self.schedule_load_slots = False

    def sync_replication_slots(self, cluster):
        if self.use_slots:
            self.load_replication_slots()
            members = [m.name for m in cluster.members if m.name != self.name] if self.role == 'master' else []
            # drop unused slots
            for slot in set(self.members) - set(members):
                self.query("""SELECT pg_drop_replication_slot(%s)
                               WHERE EXISTS(SELECT 1 FROM pg_replication_slots
                               WHERE slot_name = %s)""", slot, slot)

            # create new slots
            for slot in set(members) - set(self.members):
                self.query("""SELECT pg_create_physical_replication_slot(%s)
                               WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                               WHERE slot_name = %s)""", slot, slot)

            self.members = members

    def last_operation(self):
        return str(self.xlog_position())

    def bootstrap(self, current_leader=None):
        """
            Initially bootstrap PostgreSQL, either by creating a data
            directory with initdb, or by initalizing a replica from an
            exiting leader. Failure in the first case always leads to
            exception, since there is no point in continuing if initdb failed.
            In the second case, however, a False is returned on failure, since
            it is normal for the replica to retry a failed attempt to initialize
            from the master.
        """
        ret = False
        if not current_leader:
            ret = self.initialize() and self.start()
            if ret:
                self.create_replication_user()
                self.create_connection_users()
            else:
                raise PostgresException("Could not bootstrap master PostgreSQL")
        else:
            if self.sync_from_leader(current_leader):
                self.write_recovery_conf(current_leader)
                ret = self.start()
        return ret

    def move_data_directory(self):
        if os.path.isdir(self.data_dir) and not self.is_running():
            try:
                new_name = '{0}_{1}'.format(self.data_dir, time.strftime('%Y-%m-%d-%H-%M-%S'))
                logger.info('renaming data directory to %s', new_name)
                os.rename(self.data_dir, new_name)
            except:
                logger.exception("Could not rename data directory %s", self.data_dir)

    def remove_data_directory(self):
        logger.info('Removing data directory: %s', self.data_dir)
        try:
            if os.path.islink(self.data_dir):
                os.unlink(self.data_dir)
            elif not os.path.exists(self.data_dir):
                return
            elif os.path.isfile(self.data_dir):
                os.remove(self.data_dir)
            elif os.path.isdir(self.data_dir):
                shutil.rmtree(self.data_dir)
        except:
            logger.exception('Could not remove data directory %s', self.data_dir)
            self.move_data_directory()
