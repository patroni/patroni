#!/usr/bin/python
# arguments are:
#   - cluster scope
#   - cluster role
#   - master connection string

# for the AWS, the folliowing environment variables should be defined:
# - WALE_ENV_DIR: directory where WAL-E environment is kept
# - WAL_S3_BUCKET: a name of the S3 bucket for WAL-E
# - WALE_BACKUP_THRESHOLD_MEGABYTES if WAL amount is above that - use pg_basebackup
# - WALE_BACKUP_THRESHOLD_PERCENTAGE if WAL size exceeds a certain percentage of the
#   latest backup size
from collections import namedtuple
import logging
import os
import psycopg2
import subprocess
import sys


if sys.hexversion >= 0x03000000:
    long = int

logger = logging.getLogger(__name__)


class Restore(object):

    def __init__(self, scope, role, datadir, connstring, env=None):
        self.scope = scope
        self.role = role
        self.master_connection = Restore.parse_connstring(connstring)
        self.data_dir = datadir
        self.env = os.environ.copy() if not env else env

    @staticmethod
    def parse_connstring(connstring):
        # the connection string is in the form host= port= user=
        # return the dictionary with all components as separare keys
        result = {}
        if connstring:
            for x in connstring.split():
                if x and '=' in x:
                    key, val = x.split('=')
                result[key.strip()] = val.strip()
        return result

    def setup(self):
        pass

    def replica_method(self):
        return self.create_replica_with_pg_basebackup

    def replica_fallback_method(self):
        return None

    def run(self):
        """ creates a new replica using either pg_basebackup or WAL-E """
        method_fn = self.replica_method()
        ret = method_fn() if method_fn else 1
        if ret != 0 and self.replica_fallback_method() is not None:
            ret = (self.replica_fallback_method())()
        return ret

    def create_replica_with_pg_basebackup(self):
        try:
            ret = subprocess.call(['pg_basebackup', '-R', '-D',
                                   self.data_dir, '--host=' + self.master_connection['host'],
                                   '--port=' + str(self.master_connection['port']),
                                   '-U', self.master_connection['user']],
                                  env=self.env)
        except Exception as e:
            logger.error('Error when fetching backup with pg_basebackup: {0}'.format(e))
            return 1
        return ret


class WALERestore(Restore):

    def __init__(self, scope, role, datadir, connstring, env=None):
        super(WALERestore, self).__init__(scope, role, datadir, connstring, env)
        # check the environment variables
        self.init_error = False

    def setup(self):
        if (self.env.get('WAL_S3_BUCKET') and
            self.env.get('WALE_BACKUP_THRESHOLD_PERCENTAGE') and
           self.env.get('WALE_BACKUP_THRESHOLD_MEGABYTES')) is None:
                self.init_error = True
        else:
            self.wal_e = namedtuple('WALE',
                                    'threshold_megabytes threshold_backup_size_percentage s3_bucket cmd dir env_file')

            self.wal_e.dir = self.env.get('WALE_ENV_DIR', '/home/postgres/etc/wal-e.d/env')
            self.wal_e.env_file = os.path.join(self.wal_e.dir, 'WALE_S3_PREFIX')

            self.wal_e.cmd = 'envdir {} wal-e --aws-instance-profile '.\
                format(self.wal_e.dir)
            self.wal_e.s3_bucket = self.env['WAL_S3_BUCKET']
            self.wal_e.threshold_megabytes = self.env['WALE_BACKUP_THRESHOLD_MEGABYTES']
            self.wal_e.threshold_backup_size_percentage = self.env['WALE_BACKUP_THRESHOLD_PERCENTAGE']

            # check that the env file exists, create it otherwise
            try:
                if not os.path.exists(self.wal_e.dir):
                    os.makedirs(self.wal_e.dir)
                # if this is a directory - make sure we have full access there
                elif not (os.path.isdir(self.wal_e.dir) and os.access(self.wal_e.dir, os.R_OK | os.W_OK | os.X_OK)):
                    logger.error("Unable to access {} or not a directory".format(self.wal_e.dir))
                    self.init_error = True
                # if WAL_S3_PREFIX is not there - create it and write the full path to bucket
                if not self.init_error and not os.path.exists(self.wal_e.env_file):
                    with open(self.wal_e.env_file, 'w') as f:
                        f.write("s3://{0}/spilo/{1}/wal/\n".format(self.wal_e.s3_bucket, self.scope))

            except (os.error, IOError) as e:
                logger.error("{0}: WAL-e archiving is disabled".format(e))
                self.init_error = True

    def replica_method(self):
        if self.should_use_s3_to_create_replica():
            return self.create_replica_with_s3
        return None

    def replica_fallback_method(self):
        return self.create_replica_with_pg_basebackup

    def should_use_s3_to_create_replica(self):
        """ determine whether it makes sense to use S3 and not pg_basebackup """
        if self.init_error:
            return False

        threshold_megabytes = self.wal_e.threshold_megabytes
        threshold_backup_size_percentage = self.wal_e.threshold_backup_size_percentage

        try:
            latest_backup = subprocess.check_output(self.wal_e.cmd.split() + ['backup-list', '--detail', 'LATEST'],
                                                    env=self.env)
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
            conn = psycopg2.connect(**(self.master_connection))
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
        if self.init_error:
            return 1
        try:
            ret = subprocess.call(self.wal_e.cmd + ' backup-fetch {} LATEST'.format(self.data_dir), env=self.env)
        except Exception as e:
            logger.error('Error when fetching backup with WAL-E: {0}'.format(e))
            return 1
        return ret


if __name__ == '__main__':
    if len(sys.argv) == 5:
        # scope, role, datadir, connstring
        restore = WALERestore(*(sys.argv[1:]))
        restore.setup()
        sys.exit(restore.run())
    sys.exit("Usage: {0} scope role datadir connstring".format(sys.argv[0]))
