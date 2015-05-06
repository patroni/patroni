import logging
import os
import psycopg2
import re
import time
import urlparse


logger = logging.getLogger(__name__)


def parseurl(url):
    r = urlparse.urlparse(url)
    return {
        'hostname': r.hostname,
        'port': r.port or 5432,
        'username': r.username,
        'password': r.password,
    }


class Postgresql:

    def __init__(self, config):
        self.name = config['name']
        self.host, self.port = config['listen'].split(':')
        self.data_dir = config['data_dir']
        self.replication = config['replication']

        self.config = config

        self.cursor_holder = None
        self.connection_string = 'postgres://{username}:{password}@{listen}/postgres'.format(
            listen=self.config['listen'], **self.replication)

        self.conn = None

    def cursor(self):
        if not self.cursor_holder:
            self.conn = psycopg2.connect('postgres://{}/postgres'.format(self.config['listen']))
            self.conn.autocommit = True
            self.cursor_holder = self.conn.cursor()

        return self.cursor_holder

    def disconnect(self):
        try:
            self.conn.close()
        except:
            logger.exception('Error disconnecting')

    def query(self, sql):
        max_attempts = 0
        while True:
            try:
                self.cursor().execute(sql)
                break
            except psycopg2.OperationalError as e:
                if self.conn:
                    self.disconnect()
                self.cursor_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return self.cursor()

    def data_directory_empty(self):
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        if os.system('initdb -D ' + self.data_dir) == 0:
            self.write_pg_hba()

            return True

        return False

    def sync_from_leader(self, leader):
        r = parseurl(leader['address'])

        pgpass = 'pgpass'
        with open(pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0644)
            f.write('{hostname}:{port}:*:{username}:{password}\n'.format(**r))

        return os.system('PGPASSFILE={pgpass} pg_basebackup -R -D {data_dir} --host={hostname} --port={port} -U {username}'.format(
            pgpass=pgpass, data_dir=self.data_dir, **r)) == 0

    def is_leader(self):
        return not self.query('SELECT pg_is_in_recovery()').fetchone()[0]

    def is_running(self):
        return os.system('pg_ctl status -D {} > /dev/null'.format(self.data_dir)) == 0

    def start(self):
        if self.is_running():
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        pid_path = os.path.join(self.data_dir, 'postmaster.pid')
        if os.path.exists(pid_path):
            os.remove(pid_path)
            logger.info('Removed %s', pid_path)

        command_code = os.system('postgres -D {} {} &'.format(self.data_dir, self.server_options()))
        time.sleep(5)
        return command_code != 0

    def stop(self):
        return os.system('pg_ctl stop -w -m fast -D ' + self.data_dir) != 0

    def reload(self):
        return os.system('pg_ctl reload -w -D ' + self.data_dir) == 0

    def restart(self):
        return os.system('pg_ctl restart -w -m fast -D ' + self.data_dir) == 0

    def server_options(self):
        options = '-c listen_addresses={} -c port={}'.format(self.host, self.port)
        for setting, value in self.config['parameters'].iteritems():
            options += ' -c "{}={}"'.format(setting, value)
        return options

    def is_healthy(self):
        if not self.is_running():
            logger.warning('Postgresql is not running.')
            return False

        return True

    def is_healthiest_node(self, members):
        for member in members:
            if member['hostname'] == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member['address'])
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute(
                    'SELECT %s::pg_lsn - pg_last_xlog_replay_location() AS bytes', (self.xlog_position(), ))
                xlog_diff = member_cursor.fetchone()[0]
                logger.info([self.name, member['hostname'], xlog_diff])
                if xlog_diff < 0:
                    member_cursor.close()
                    return False
                member_cursor.close()
            except psycopg2.OperationalError:
                continue
        return True

    def replication_slot_name(self):
        member = os.environ.get("MEMBER")
        (member, _) = re.subn(r'[^a-z0-9]+', r'_', member)
        return member

    def write_pg_hba(self):
        with open(os.path.join(self.data_dir, 'pg_hba.conf'), 'a') as f:
            f.write('host replication {username} {network} md5'.format(**self.replication))

    def write_recovery_conf(self, leader_hash):
        r = parseurl(leader_hash['address'])

        with open(os.path.join(self.data_dir, 'recovery.conf'), 'w') as f:
            f.write("""
standby_mode = 'on'
primary_slot_name = '{recovery_slot}'
primary_conninfo = 'user={username} password={password} host={hostname} port={port} sslmode=prefer sslcompression=1'
recovery_target_timeline = 'latest'
""".format(recovery_slot=self.name, **r))
            for name, value in self.config.get('recovery_conf', {}).iteritems():
                f.write("{} = '{}'\n".format(name, value))

    def follow_the_leader(self, leader_hash):
        r = parseurl(leader_hash['address'])
        pattern = 'host={hostname} port={port}'.format(**r)
        with open(os.path.join(self.data_dir, 'recovery.conf', 'r')) as f:
            for line in f:
                if pattern in line:
                    return
        self.write_recovery_conf(leader_hash)
        self.restart()

    def promote(self):
        return os.system('pg_ctl promote -w -D ' + self.data_dir) == 0

    def demote(self, leader):
        self.write_recovery_conf(leader)
        self.restart()

    def create_replication_user(self):
        self.query("CREATE USER \"%s\" WITH REPLICATION ENCRYPTED PASSWORD '%s';" %
                   (self.replication['username'], self.replication['password']))

    def xlog_position(self):
        return self.query('SELECT pg_last_xlog_replay_location()').fetchone()[0]
