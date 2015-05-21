import logging
import os
import psycopg2
import shutil
import subprocess
import sys
import time

is_py3 = sys.hexversion >= 0x03000000

if is_py3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse


logger = logging.getLogger(__name__)


def parseurl(url):
    r = urlparse(url)
    return {
        'hostname': r.hostname,
        'port': r.port or 5432,
        'username': r.username,
        'password': r.password,
    }


class Postgresql:

    def __init__(self, config):
        self.name = config['name']
        self.listen_addresses, self.port = config['listen'].split(':')
        self.libpq_parameters = {
            'host': self.listen_addresses.split(',')[0].strip(),
            'port': self.port,
            'fallback_application_name': 'Governor',
            'connect_timeout': 5,
            'options': '-c statement_timeout=2000'
        }
        self.data_dir = config['data_dir']
        self.replication = config['replication']
        self.superuser = config['superuser']
        self.admin = config['admin']
        self.recovery_conf = os.path.join(self.data_dir, 'recovery.conf')
        self.configuration_to_save = (os.path.join(self.data_dir, 'pg_hba.conf'),
                                      os.path.join(self.data_dir, 'postgresql.conf'))
        self._pg_ctl = 'pg_ctl -w -D ' + self.data_dir
        self.wal_e = config.get('wal_e', None)
        if self.wal_e:
            self.wal_e_path = 'envdir {} wal-e --aws-instance-profile '.\
                format(self.wal_e.get('env_dir', '/home/postgres/etc/wal-e.d/env'))

        self.config = config

        self.connection_string = 'postgres://{username}:{password}@{connect_address}/postgres'.format(
            connect_address=self.config['connect_address'], **self.replication)

        self.conn = None
        self.cursor_holder = None
        self.members = []  # list of already existing replication slots

    def cursor(self):
        if not self.cursor_holder:
            self.conn = psycopg2.connect(**self.libpq_parameters)
            self.conn.autocommit = True
            self.cursor_holder = self.conn.cursor()

        return self.cursor_holder

    def disconnect(self):
        try:
            self.conn.close()
        except:
            logger.exception('Error disconnecting')

    def query(self, sql, *params):
        max_attempts = 0
        while True:
            try:
                self.cursor().execute(sql, params)
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
        ret = os.system(self._pg_ctl + ' initdb -o --encoding=UTF8') == 0
        ret and self.write_pg_hba()
        return ret

    def sync_from_leader(self, leader):
        r = parseurl(leader.address)

        pgpass = 'pgpass'
        with open(pgpass, 'w') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write('{hostname}:{port}:*:{username}:{password}\n'.format(**r))

        try:
            os.environ['PGPASSFILE'] = pgpass
            return self.create_replica(leader.address, r) == 0
        finally:
            os.environ.pop('PGPASSFILE')

    def create_replica(self, master_connurl, master_connection):
        """ creates a new replica using either pg_basebackup or WAL-E """
        if self.should_use_s3_to_create_replica(master_connurl):
            result = self.create_replica_with_s3()
            # if restore from the backup on S3 failed - try with the pg_basebackup
            if result == 0:
                return result
        return self.create_replica_with_pg_basebackup(master_connection)

    def create_replica_with_pg_basebackup(self, master_connection):
        return os.system('pg_basebackup -R -D {data_dir} --host={hostname} --port={port} -U {username}'.format(
            data_dir=self.data_dir, **master_connection))

    def create_replica_with_s3(self):
        if not self.wal_e or not self.wal_e_path:
            return 1

        ret = os.system(self.wal_e_path + ' backup-fetch {} LATEST'.format(self.data_dir))
        self.restore_configuration_files()
        return ret

    def should_use_s3_to_create_replica(self, master_connurl):
        """ determine whether it makes sense to use S3 and not pg_basebackup """
        if not self.wal_e or not self.wal_e_path:
            return False

        threshold_megabytes = self.wal_e.get('threshold_megabytes', 10240)
        threshold_backup_size_percentage = self.wal_e.get('threshold_backup_size_percentage', 30)

        try:
            latest_backup = subprocess.check_output(self.wal_e_path.split() + ['backup-list', '--detail', 'LATEST'])
            # name    last_modified   expanded_size_bytes wal_segment_backup_start    wal_segment_offset_backup_start wal_segment_backup_stop wal_segment_offset_backup_stop
            # base_00000001000000000000007F_00000040  2015-05-18T10:13:25.000Z
            # 20310671    00000001000000000000007F    00000040
            # 00000001000000000000007F    00000240
            backup_strings = latest_backup.splitlines() if latest_backup else ()
            if len(backup_strings) != 2:
                return False

            names = backup_strings[0].split()
            vals = backup_strings[1].split()
            if (len(names) != len(vals)) or (len(names) != 7):
                return False

            backup_info = dict(zip(names, vals))
        except subprocess.CalledProcessError as e:
            logger.error("could not query wal-e latest backup: {}".format(e))
            return False

        try:
            backup_size = backup_info['expanded_size_bytes']
            backup_start_segment = backup_info['wal_segment_backup_start']
            backup_start_offset = backup_info['wal_segment_offset_backup_start']
        except Exception as e:
            logger.error("unable to get some of S3 backup parameters: {}".format(e))
            return False

        # WAL filename is XXXXXXXXYYYYYYYY000000ZZ, where X - timeline, Y - LSN logical log file,
        # ZZ - 2 high digits of LSN offset. The rest of the offset is the provided decimal offset,
        # that we have to convert to hex and 'prepend' to the high offset digits.

        lsn_segment = backup_start_segment[8:16]
        # first 2 characters of the result are 0x and the last one is L
        lsn_offset = hex((long(backup_start_segment[16:32], 16) << 24) + long(backup_start_offset))[2:-1]

        # construct the LSN from the segment and offset
        backup_start_lsn = '{}/{}'.format(lsn_segment, lsn_offset)

        conn = None
        cursor = None
        diff_in_bytes = long(backup_size)
        try:
            # get the difference in bytes between the current WAL location and the backup start offset
            conn = psycopg2.connect(master_connurl)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT pg_xlog_location_diff(pg_current_xlog_location(), %s)", (backup_start_lsn,))
            diff_in_bytes = long(cursor.fetchone()[0])
        except psycopg2.Error as e:
            logger.error('could not determine difference with the master location: {}'.format(e))
            return False
        finally:
            cursor and cursor.close()
            conn and conn.close()

        # if the size of the accumulated WAL segments is more than a certan percentage of the backup size
        # or exceeds the pre-determined size - pg_basebackup is chosen instead.
        return (diff_in_bytes < long(threshold_megabytes) * 1048576) and\
               (diff_in_bytes < long(backup_size) * float(threshold_backup_size_percentage) / 100)

    def is_leader(self):
        return not self.query('SELECT pg_is_in_recovery()').fetchone()[0]

    def is_running(self):
        return os.system(self._pg_ctl + ' status > /dev/null') == 0

    def start(self):
        if self.is_running():
            self.load_replication_slots()
            logger.error('Cannot start PostgreSQL because one is already running.')
            return False

        pid_path = os.path.join(self.data_dir, 'postmaster.pid')
        if os.path.exists(pid_path):
            os.remove(pid_path)
            logger.info('Removed %s', pid_path)

        ret = os.system(self._pg_ctl + ' start -o "{}"'.format(self.server_options())) == 0
        ret and self.load_replication_slots()
        self.save_configuration_files()
        return ret

    def stop(self):
        return os.system(self._pg_ctl + ' stop -m fast') != 0

    def reload(self):
        return os.system(self._pg_ctl + ' reload') == 0

    def restart(self):
        return os.system(self._pg_ctl + ' restart -m fast') == 0

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
        if cluster.last_leader_operation - self.xlog_position() > self.config.get('maximum_lag_on_failover', 0):
            return False

        for member in cluster.members:
            if member.hostname == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member.address)
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute(
                    "SELECT %s - (pg_last_xlog_replay_location() - '0/0000000'::pg_lsn)", (self.xlog_position(), ))
                xlog_diff = member_cursor.fetchone()[0]
                logger.info([self.name, member.hostname, xlog_diff])
                member_cursor.close()
                member_conn.close()
                if xlog_diff < 0:
                    return False
            except psycopg2.OperationalError:
                continue
        return True

    def write_pg_hba(self):
        with open(os.path.join(self.data_dir, 'pg_hba.conf'), 'a') as f:
            f.write('host replication {username} {network} md5'.format(**self.replication))
            # allow TCP connections from the host's own address
            f.write("\nhost postgres postgres samehost trust\n")
            # allow TCP connections from the rest of the world with a password, prefer ssl
            f.write("\nhostssl all all 0.0.0.0/0 md5\n")
            f.write("\nhost    all all 0.0.0.0/0 md5\n")

    @staticmethod
    def primary_conninfo(leader_url):
        r = parseurl(leader_url)
        return 'user={username} password={password} host={hostname} port={port} sslmode=prefer sslcompression=1'.format(**r)

    def check_recovery_conf(self, leader):
        if not os.path.isfile(self.recovery_conf):
            return False

        pattern = leader and leader.address and self.primary_conninfo(leader.address)

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
            if leader and leader.address:
                f.write("""
primary_slot_name = '{}'
primary_conninfo = '{}'
""".format(self.name, self.primary_conninfo(leader.address)))
                for name, value in self.config.get('recovery_conf', {}).items():
                    f.write("{} = '{}'\n".format(name, value))

    def follow_the_leader(self, leader):
        if self.check_recovery_conf(leader):
            return
        self.write_recovery_conf(leader)
        self.restart()

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
        return os.system(self._pg_ctl + ' promote') == 0

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
        return self.query("SELECT pg_last_xlog_replay_location() - '0/0000000'::pg_lsn").fetchone()[0]

    def load_replication_slots(self):
        cursor = self.query("SELECT slot_name FROM pg_replication_slots WHERE slot_type='physical'")
        self.members = [r[0] for r in cursor]

    def create_replication_slots(self, members):
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
        return self.query("SELECT pg_current_xlog_location() - '0/00000'::pg_lsn").fetchone()[0]
