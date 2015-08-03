# arguments are:
#   - cluster scope
#   - cluster role
#   - master connection string
import logging
import os
import psycopg2
import subprocess
import sys

logger = logging.getLogger(__name__)


class Restore:

    def __init__(self, scope, role, datadir, connstring):
        self.scope = scope
        self.role = role
        self.connstring = connstring
        self.datadir = datadir
        self.env = os.environ.copy()

    def parse_connstring(self):
        # the connection string is in the form host= port= user=
        # return the dictionary with all components as separare keys
        result = {}
        if self.connstring:
            for x in self.connstring.split():
                if x and '=' in x:
                    key, val = x.split('=')
                result[key.strip()] = val.strip()
        return result

    def replica_method(self):
        return self.create_replica_with_pg_basebackup

    def replica_fallback_method(self):
        return None

    def run(self):
        """ creates a new replica using either pg_basebackup or WAL-E """
        method_fn = self.replica_method()
        ret = method_fn()
        if ret != 0 and self.replica_fallback_method() is not None:
            ret = (self.replica_fallback_method())()
        return ret

    def create_replica_with_pg_basebackup(self):
        master_connection = self.parse_connstring()
        ret = subprocess.call(['pg_basebackup', '-R', '-D', self.data_dir, '--host=' + master_connection['host'],
                               '--port=' + str(master_connection['port']), '-U', master_connection['user']],
                              env=self.env)
        self.delete_trigger_file()
        return ret


class WALERestore(Restore):

    def __init__(self, scope, role, datadir, connstring):
        super(WALERestore, self).__init__(scope, role, datadir, connstring)

    def replica_method(self):
        if self.should_use_s3_to_create_replica(self):
            return self.create_replica_with_s3
        return self.create_replica_with_pg_basebackup

    def replica_fallback_method(self):
        return self.create_replica_with_pg_basebackup

    def should_use_s3_to_create_replica(self, master_connection):
        """ determine whether it makes sense to use S3 and not pg_basebackup """
        if not self.wal_e or not self.wal_e_paselfth:
            return False

        threshold_megabytes = self.wal_e.get('threshold_megabytes', 10240)
        threshold_backup_size_percentage = self.wal_e.get('threshold_backup_size_percentage', 30)

        try:
            latest_backup = subprocess.check_output(self.wal_e_path.split() + ['backup-list', '--detail', 'LATEST'])
            # name    last_modified   expanded_size_bytes wal_segment_backup_start    wal_segment_offset_backup_start
            #                                                   wal_segment_backup_stop wal_segment_offset_backup_stop
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
            conn = psycopg2.connect(**master_connection)
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

    def create_replica_with_s3(self):
        if not self.wal_e or not self.wal_e_path:
            return 1

        ret = subprocess.call(self.wal_e_path + ' backup-fetch {} LATEST'.format(self.data_dir), shell=True)
        self.restore_configuration_files()
        return ret


if __name__ == '__main__':
    if len(sys.argv) == 5:
        # scope, role, datadir, connstring
        restore = Restore(*(sys.argv[1:]))
        sys.exit(restore.run())
    sys.exit("Usage: {0} scope role datadir connstring".format(sys.argv[0]))
