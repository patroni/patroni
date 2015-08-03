import logging
import os
import psycopg2
import shlex
import shutil
import subprocess
import sys

from helpers.utils import sleep

if sys.hexversion >= 0x03000000:
    from urllib.parse import urlparse
    long = int
else:
    from urlparse import urlparse

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
        self.callback = config.get('callbacks', {})
        self.recovery_conf = os.path.join(self.data_dir, 'recovery.conf')
        self.configuration_to_save = (os.path.join(self.data_dir, 'pg_hba.conf'),
                                      os.path.join(self.data_dir, 'postgresql.conf'))
        self.postmaster_pid = os.path.join(self.data_dir, 'postmaster.pid')
        self.trigger_file = config.get('recovery_conf', {}).get('trigger_file', None) or 'promote'
        self.trigger_file = os.path.abspath(os.path.join(self.data_dir, self.trigger_file))
        self.is_promoted = False

        self._pg_ctl = ['pg_ctl', '-w', '-D', self.data_dir]
        self.wal_e = config.get('wal_e', None)
        if self.wal_e:
            self.wal_e_path = 'envdir {} wal-e --aws-instance-profile '.\
                format(self.wal_e.get('env_dir', '/home/postgres/etc/wal-e.d/env'))

        self.local_address = self.get_local_address()
        connect_address = config.get('connect_address', None) or self.local_address
        self.connection_string = 'postgres://{username}:{password}@{connect_address}/postgres'.format(
            connect_address=connect_address, **self.replication)

        self._connection = None
        self._cursor_holder = None
        self.members = []  # list of already existing replication slots

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
        if not self._cursor_holder or self._cursor_holder.closed:
            self._cursor_holder = self.connection().cursor()
        return self._cursor_holder

    def disconnect(self):
        self._connection and self._connection.close()
        self._connection = self._cursor_holder = None

    def query(self, sql, *params):
        max_attempts = 0
        while True:
            ex = None
            try:
                cursor = self._cursor()
                cursor.execute(sql, params)
                return cursor
            except psycopg2.InterfaceError as e:
                ex = e
            except psycopg2.OperationalError as e:
                if self._connection and self._connection.closed == 0:
                    raise e
                ex = e
            if ex:
                self.disconnect()
                max_attempts += 1
                if max_attempts >= 3:
                    raise ex
                sleep(5)

    def data_directory_empty(self):
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        ret = subprocess.call(self._pg_ctl + ['initdb', '-o', '--encoding=UTF8']) == 0
        ret and self.write_pg_hba()
        return ret

    def delete_trigger_file(self):
        os.path.exists(self.trigger_file) and os.unlink(self.trigger_file)

    def sync_from_leader(self, leader):
        r = parseurl(leader.conn_url)

        pgpass = 'pgpass'
        with open(pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{host}:{port}:*:{user}:{password}\n'.format(**r))

        env = os.environ.copy()
        env['PGPASSFILE'] = pgpass
        return self.create_replica(r, env) == 0

    @staticmethod
    def build_connstring(self, conn):
        return "host={host} port={port} user={user}".format(**conn)

    def create_replica(self, master_connection, env):
        connstring = self.build_connstring(master_connection, master_connection)
        cmd = self.config['restore']
        ret = subprocess.call(shlex.split(os.path.abspath(cmd))+[self.scope, "replica",
                              self.data_dir, connstring], env=env)
        return ret

    def is_leader(self, check_only=False):
        ret = not self.query('SELECT pg_is_in_recovery()').fetchone()[0]
        if ret and self.is_promoted and not check_only:
            self.delete_trigger_file()
            self.is_promoted = False
        return ret

    def is_running(self):
        return subprocess.call(' '.join(self._pg_ctl) + ' status > /dev/null', shell=True) == 0

    def call_nowait(self, cb_name, is_leader=None):
        """ pick a callback command and call it without waiting for it to finish """
        if not self.callback or cb_name not in self.callback:
            return False
        cmd = self.callback[cb_name]
        if is_leader is None:
            try:
                is_leader = self.is_leader(check_only=True)
            except psycopg2.OperationalError as e:
                logger.warning("unable to perform {0} action, cannot obtain the cluster role: {1}".format(cb_name, e))
                return False
        scope = self.scope
        try:
            role = "master" if is_leader else "replica"
            subprocess.Popen(shlex.split(os.path.abspath(cmd))+[cb_name, role, scope])
        except Exception as e:
            logger.warning("callback {0} {1} {2} {3} failed: {4}".format(os.path.abspath(cmd), cb_name, role, scope, e))
            return False
        return True

    def start(self):
        if self.is_running():
            self.load_replication_slots()
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        if os.path.exists(self.postmaster_pid):
            os.remove(self.postmaster_pid)
            logger.info('Removed %s', self.postmaster_pid)

        ret = subprocess.call(self._pg_ctl + ['start', '-o', self.server_options()]) == 0
        ret and self.load_replication_slots()
        self.save_configuration_files()
        if ret and ACTION_ON_START in self.callback:
            self.call_nowait(ACTION_ON_START)
        return ret

    def stop(self):
        try:
            is_leader = self.is_leader(check_only=True)
        except:
            is_leader = None
            pass
        ret = subprocess.call(self._pg_ctl + ['stop', '-m', 'fast'])
        if ret == 0 and ACTION_ON_STOP in self.callback:
            self.call_nowait(ACTION_ON_STOP, is_leader=is_leader)
        return ret == 0

    def reload(self):
        ret = subprocess.call(self._pg_ctl + ['reload'])
        if ret == 0 and ACTION_ON_RELOAD in self.callback:
            self.call_nowait(ACTION_ON_RELOAD)
        return ret == 0

    def restart(self):
        try:
            is_leader = self.is_leader(check_only=True)
        except:
            is_leader = None
            pass
        ret = subprocess.call(self._pg_ctl + ['restart', '-m', 'fast'])
        if ret == 0 and ACTION_ON_RESTART in self.callback:
            self.call_nowait(ACTION_ON_RESTART, is_leader=is_leader)
        return ret == 0

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
                    "SELECT pg_is_in_recovery(), %s - (pg_last_xlog_replay_location() - '0/0000000'::pg_lsn)",
                    (self.xlog_position(), ))
                row = member_cursor.fetchone()
                member_cursor.close()
                member_conn.close()
                logger.error([self.name, member.name, row])
                if not row[0] or row[1] < 0:
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
                f.write("""
primary_slot_name = '{}'
primary_conninfo = '{}'
""".format(self.name, self.primary_conninfo(leader.conn_url)))
                for name, value in self.config.get('recovery_conf', {}).items():
                    f.write("{} = '{}'\n".format(name, value))

    def follow_the_leader(self, leader):
        if not self.check_recovery_conf(leader):
            self.write_recovery_conf(leader)
            self.restart()
            if ACTION_ON_ROLE_CHANGE in self.callback:
                self.call_nowait(ACTION_ON_ROLE_CHANGE)

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
        except Exception as e:
            logger.error("unable to restore configuration from WAL-E backup: {}".format(e))

    def promote(self):
        self.is_promoted = subprocess.call(self._pg_ctl + ['promote']) == 0
        if self.is_promoted and ACTION_ON_ROLE_CHANGE in self.callback:
            self.call_nowait(ACTION_ON_ROLE_CHANGE)
        return self.is_promoted

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
                self.query('ALTER ROLE postgres WITH PASSWORD %s', self.superuser['password'])
        if self.admin:
            self.query('CREATE ROLE "{0}" WITH LOGIN CREATEDB CREATEROLE PASSWORD %s'.format(
                self.admin['username']), self.admin['password'])

    def xlog_position(self):
        return self.query("""SELECT CASE WHEN pg_is_in_recovery()
                                         THEN pg_last_xlog_replay_location() - '0/0000000'::pg_lsn
                                         ELSE pg_current_xlog_location() - '0/00000'::pg_lsn END""").fetchone()[0]

    def load_replication_slots(self):
        cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
        self.members = [r[0] for r in cursor]

    def sync_replication_slots(self, members):
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

    def create_replication_slots(self, cluster):
        self.sync_replication_slots([m.name for m in cluster.members if m.name != self.name])

    def drop_replication_slots(self):
        self.sync_replication_slots([])

    def last_operation(self):
        return str(self.xlog_position())
