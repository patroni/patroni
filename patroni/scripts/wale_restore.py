#!/usr/bin/env python

# sample script to clone new replicas using WAL-E restore
# falls back to pg_basebackup if WAL-E restore fails, or if
# WAL-E backup is too far behind
# note that pg_basebackup still expects to use restore from
# WAL-E for transaction logs

# theoretically should work with SWIFT, but not tested on it

# arguments are:
#   - cluster scope
#   - cluster role
#   - master connection string
#   - number of retries
#   - envdir for the WALE env
# - WALE_BACKUP_THRESHOLD_MEGABYTES if WAL amount is above that - use pg_basebackup
# - WALE_BACKUP_THRESHOLD_PERCENTAGE if WAL size exceeds a certain percentage of the

# this script depends on an envdir defining the S3 bucket (or SWIFT dir),and login
# credentials per WALE Documentation.

# currently also requires that you configure the restore_command to use wal_e, example:
#       recovery_conf:
#               restore_command: envdir /etc/wal-e.d/env wal-e wal-fetch "%f" "%p" -p 1

from collections import namedtuple
import logging
import os
import psycopg2
import subprocess
import sys
import argparse


if sys.hexversion >= 0x03000000:
    long = int

logger = logging.getLogger(__name__)


class WALERestore(object):

    def __init__(self, scope, datadir, connstring, env_dir, threshold_mb, threshold_pct, use_iam, no_master):
        self.scope = scope
        self.master_connection = connstring
        self.data_dir = datadir
        self.wal_e = namedtuple('wale', 'dir,threshold_mb,threshold_pct,iam_string,cmd')
        self.wal_e.dir = env_dir
        self.wal_e.threshold_mb = threshold_mb
        self.wal_e.threshold_pct = threshold_pct
        self.wal_e.iam_string = ' --aws-instance-profile ' if use_iam == 1 else ''
        self.no_master = no_master
        self.wal_e.cmd = 'envdir {0} wal-e {1} '.format(self.wal_e.dir, self.wal_e.iam_string)
        self.init_error = (not os.path.exists(self.wal_e.dir))

    def run(self):
        """ creates a new replica using WAL-E """
        if not self.init_error and self.should_use_s3_to_create_replica():
            return self.create_replica_with_s3()
        return 2

    def should_use_s3_to_create_replica(self):
        """ determine whether it makes sense to use S3 and not pg_basebackup """

        threshold_megabytes = self.wal_e.threshold_mb
        threshold_backup_size_percentage = self.wal_e.threshold_pct

        try:
            latest_backup = subprocess.check_output(self.wal_e.cmd.split() + ['backup-list', '--detail', 'LATEST'])
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
            logger.error("unable to get some of WALE backup parameters: {}".format(e))
            return False

        # WAL filename is XXXXXXXXYYYYYYYY000000ZZ, where X - timeline, Y - LSN logical log file,
        # ZZ - 2 high digits of LSN offset. The rest of the offset is the provided decimal offset,
        # that we have to convert to hex and 'prepend' to the high offset digits.

        lsn_segment = backup_start_segment[8:16]
        # first 2 characters of the result are 0x and the last one is L
        lsn_offset = hex((long(backup_start_segment[16:32], 16) << 24) + long(backup_start_offset))[2:-1]

        # construct the LSN from the segment and offset
        backup_start_lsn = '{0}/{1}'.format(lsn_segment, lsn_offset)

        diff_in_bytes = long(backup_size)
        if not self.no_master:
            try:
                # get the difference in bytes between the current WAL location and the backup start offset
                with psycopg2.connect(self.master_connection) as con:
                    con.autocommit = True
                    with con.cursor() as cur:
                        cur.execute("SELECT pg_xlog_location_diff(pg_current_xlog_location(), %s)", (backup_start_lsn,))
                        diff_in_bytes = long(cur.fetchone()[0])
            except psycopg2.Error as e:
                logger.error('could not determine difference with the master location: %s', e)
                return False
        else:
            # always try to use WAL-E if base backup is available
            diff_in_bytes = 0

        # if the size of the accumulated WAL segments is more than a certan percentage of the backup size
        # or exceeds the pre-determined size - pg_basebackup is chosen instead.
        return (diff_in_bytes < long(threshold_megabytes) * 1048576) and\
            (diff_in_bytes < long(backup_size) * float(threshold_backup_size_percentage) / 100)

    def create_replica_with_s3(self):
        # if we're set up, restore the replica using fetch latest
        try:
            ret = subprocess.call(self.wal_e.cmd.split() + ['backup-fetch', '{}'.format(self.data_dir), 'LATEST'])
        except Exception as e:
            logger.error('Error when fetching backup with WAL-E: {0}'.format(e))
            return 1

        return ret


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description='Script to image replicas using WAL-E')
    parser.add_argument('--scope', required=True)
    parser.add_argument('--role', required=False)
    parser.add_argument('--datadir', required=True)
    parser.add_argument('--connstring', required=True)
    parser.add_argument('--retries', type=int, default=1)
    parser.add_argument('--envdir', required=True)
    parser.add_argument('--threshold_megabytes', type=int, default=10240)
    parser.add_argument('--threshold_backup_size_percentage', type=int, default=30)
    parser.add_argument('--use_iam', type=int, default=0)
    parser.add_argument('--no_master', type=int, default=0)
    args = parser.parse_args()

    # retry cloning in a loop
    for _ in range(0, args.retries + 1):
        restore = WALERestore(scope=args.scope, datadir=args.datadir, connstring=args.connstring,
                              env_dir=args.envdir, threshold_mb=args.threshold_megabytes,
                              threshold_pct=args.threshold_backup_size_percentage, use_iam=args.use_iam,
                              no_master=args.no_master)
        ret = restore.run()
        if ret == 0:
            break

    sys.exit(ret)

if __name__ == '__main__':
    main()
