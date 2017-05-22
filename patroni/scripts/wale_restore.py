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
import argparse
import csv
import logging
import os
import psycopg2
import subprocess
import sys
import time

from collections import namedtuple

logger = logging.getLogger(__name__)

RETRY_SLEEP_INTERVAL = 1
si_prefixes = ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']


# Meaningful names to the exit codes used by WALERestore
ExitCode = type('Enum', (), {
    'SUCCESS': 0,  #: Succeeded
    'RETRY_LATER': 1,  #: External issue, retry later
    'FAIL': 2  #: Don't try again unless configuration changes
})


# We need to know the current PG version in order to figure out the correct WAL directory name
def get_major_version(data_dir):
    version_file = os.path.join(data_dir, 'PG_VERSION')
    if os.path.isfile(version_file):  # version file exists
        try:
            with open(version_file) as f:
                return float(f.read())
        except Exception:
            logger.exception('Failed to read PG_VERSION from %s', data_dir)
    return 0.0


def repr_size(n_bytes):
    """
    >>> repr_size(1000)
    '1000 Bytes'
    >>> repr_size(8257332324597)
    '7.5 TiB'
    """
    if n_bytes < 1024:
        return '{0} Bytes'.format(n_bytes)
    i = -1
    while n_bytes > 1023:
        n_bytes /= 1024.0
        i += 1
    return '{0} {1}iB'.format(round(n_bytes, 1), si_prefixes[i])


def size_as_bytes(size_, prefix):
    """
    >>> size_as_bytes(7.5, 'T')
    8246337208320
    """
    prefix = prefix.upper()

    assert prefix in si_prefixes

    exponent = si_prefixes.index(prefix) + 1

    return int(size_ * (1024.0 ** exponent))


WALEConfig = namedtuple(
    'WALEConfig',
    [
        'env_dir',
        'threshold_mb',
        'threshold_pct',
        'cmd',
    ]
)


class WALERestore(object):
    def __init__(self, scope, datadir, connstring, env_dir, threshold_mb,
                 threshold_pct, use_iam, no_master, retries):
        self.scope = scope
        self.master_connection = connstring
        self.data_dir = datadir
        self.no_master = no_master

        wale_cmd = [
            'envdir',
            env_dir,
            'wal-e',
        ]

        if use_iam == 1:
            wale_cmd += ['--aws-instance-profile']

        self.wal_e = WALEConfig(
            env_dir=env_dir,
            threshold_mb=threshold_mb,
            threshold_pct=threshold_pct,
            cmd=wale_cmd,
        )

        self.init_error = (not os.path.exists(self.wal_e.env_dir))
        self.retries = retries

    def run(self):
        """
        Creates a new replica using WAL-E

        Returns
        -------
        ExitCode
            0 = Success
            1 = Error, try again
            2 = Error, don't try again

        """
        if self.init_error:
            logger.error('init error: %r did not exist at initialization time',
                         self.wal_e.env_dir)
            return ExitCode.FAIL

        try:
            should_use_s3 = self.should_use_s3_to_create_replica()
            if should_use_s3 is None:  # Need to retry
                return ExitCode.RETRY_LATER
            elif should_use_s3:
                return self.create_replica_with_s3()
            elif not should_use_s3:
                return ExitCode.FAIL
        except Exception:
            logger.exception("Unhandled exception when running WAL-E restore")
        return ExitCode.FAIL

    def should_use_s3_to_create_replica(self):
        """ determine whether it makes sense to use S3 and not pg_basebackup """

        threshold_megabytes = self.wal_e.threshold_mb
        threshold_percent = self.wal_e.threshold_pct

        try:
            cmd = self.wal_e.cmd + ['backup-list', '--detail', 'LATEST']

            logger.debug('calling %r', cmd)
            wale_output = subprocess.check_output(cmd)

            reader = csv.DictReader(wale_output.decode('utf-8').splitlines(),
                                    dialect='excel-tab')
            rows = list(reader)
            if not len(rows):
                logger.warning('wal-e did not find any backups')
                return False

            # This check might not add much, it was performed in the previous
            # version of this code. since the old version rolled CSV parsing the
            # check may have been part of the CSV parsing.
            if len(rows) > 1:
                logger.warning(
                    'wal-e returned more than one row of backups: %r',
                    rows)
                return False

            backup_info = rows[0]
        except subprocess.CalledProcessError:
            logger.exception("could not query wal-e latest backup")
            return None

        try:
            backup_size = int(backup_info['expanded_size_bytes'])
            backup_start_segment = backup_info['wal_segment_backup_start']
            backup_start_offset = backup_info['wal_segment_offset_backup_start']
        except KeyError:
            logger.exception("unable to get some of WALE backup parameters")
            return None

        # WAL filename is XXXXXXXXYYYYYYYY000000ZZ, where X - timeline, Y - LSN logical log file,
        # ZZ - 2 high digits of LSN offset. The rest of the offset is the provided decimal offset,
        # that we have to convert to hex and 'prepend' to the high offset digits.

        lsn_segment = backup_start_segment[8:16]
        # first 2 characters of the result are 0x and the last one is L
        lsn_offset = hex((int(backup_start_segment[16:32], 16) << 24) + int(backup_start_offset))[2:-1]

        # construct the LSN from the segment and offset
        backup_start_lsn = '{0}/{1}'.format(lsn_segment, lsn_offset)

        diff_in_bytes = backup_size
        attempts_no = 0
        while True:
            if self.master_connection:
                try:
                    # get the difference in bytes between the current WAL location and the backup start offset
                    with psycopg2.connect(self.master_connection) as con:
                        if con.server_version >= 100000:
                            wal_name = 'wal'
                            lsn_name = 'lsn'
                        else:
                            wal_name = 'xlog'
                            lsn_name = 'location'
                        con.autocommit = True
                        with con.cursor() as cur:
                            cur.execute("""SELECT CASE WHEN pg_is_in_recovery()
                                                       THEN GREATEST(
                                                                pg_{0}_{1}_diff(COALESCE(
                                                                  pg_last_{0}_receive_{1}(), '0/0'), %s)::bigint,
                                                                pg_{0}_{1}_diff(pg_last_{0}_replay_{1}(), %s)::bigint)
                                                       ELSE pg_{0}_{1}_diff(pg_current_{0}_{1}(), %s)::bigint
                                                   END""".format(wal_name, lsn_name),
                                        (backup_start_lsn, backup_start_lsn, backup_start_lsn))

                            diff_in_bytes = int(cur.fetchone()[0])
                except psycopg2.Error:
                    logger.exception('could not determine difference with the master location')
                    if attempts_no < self.retries:  # retry in case of a temporarily connection issue
                        attempts_no = attempts_no + 1
                        time.sleep(RETRY_SLEEP_INTERVAL)
                        continue
                    else:
                        if not self.no_master:
                            return False  # do no more retries on the outer level
                        logger.info("continue with base backup from S3 since master is not available")
                        diff_in_bytes = 0
                        break
            else:
                # always try to use WAL-E if master connection string is not available
                diff_in_bytes = 0
            break

        # if the size of the accumulated WAL segments is more than a certan percentage of the backup size
        # or exceeds the pre-determined size - pg_basebackup is chosen instead.
        is_size_thresh_ok = diff_in_bytes < int(threshold_megabytes) * 1048576
        threshold_pct_bytes = backup_size * threshold_percent / 100.0
        is_percentage_thresh_ok = float(diff_in_bytes) < int(threshold_pct_bytes)
        are_thresholds_ok = is_size_thresh_ok and is_percentage_thresh_ok

        class Size(object):
            def __init__(self, n_bytes, prefix=None):
                self.n_bytes = n_bytes
                self.prefix = prefix

            def __repr__(self):
                if self.prefix is not None:
                    n_bytes = size_as_bytes(self.n_bytes, self.prefix)
                else:
                    n_bytes = self.n_bytes
                return repr_size(n_bytes)

        class HumanContext(object):
            def __init__(self, items):
                self.items = items

            def __repr__(self):
                return ', '.join('{}={!r}'.format(key, value)
                                 for key, value in self.items)

        human_context = repr(HumanContext([
            ('threshold_size', Size(threshold_megabytes, 'M')),
            ('threshold_percent', threshold_percent),
            ('threshold_percent_size', Size(threshold_pct_bytes)),
            ('backup_size', Size(backup_size)),
            ('backup_diff', Size(diff_in_bytes)),
            ('is_size_thresh_ok', is_size_thresh_ok),
            ('is_percentage_thresh_ok', is_percentage_thresh_ok),
        ]))

        if not are_thresholds_ok:
            logger.info('wal-e backup size diff is over threshold, falling back '
                        'to other means of restore: %s', human_context)
        else:
            logger.info('Thresholds are OK, using wal-e basebackup: %s', human_context)
        return are_thresholds_ok

    def fix_subdirectory_path_if_broken(self, dirname):
        # in case it is a symlink pointing to a non-existing location, remove it and create the actual directory
        path = os.path.join(self.data_dir, dirname)
        if not os.path.exists(path):
            if os.path.islink(path):  # broken xlog symlink, to remove
                try:
                    os.remove(path)
                except OSError:
                    logger.exception("could not remove broken %s symlink pointing to %s",
                                     dirname, os.readlink(path))
                    return False
            try:
                os.mkdir(path)
            except OSError:
                logger.exception("coud not create missing %s directory path", dirname)
                return False
        return True

    def create_replica_with_s3(self):
        # if we're set up, restore the replica using fetch latest
        try:
            cmd = self.wal_e.cmd + ['backup-fetch',
                                    '{}'.format(self.data_dir),
                                    'LATEST']
            logger.debug('calling: %r', cmd)
            exit_code = subprocess.call(cmd)
        except Exception as e:
            logger.error('Error when fetching backup with WAL-E: {0}'.format(e))
            return ExitCode.RETRY_LATER

        if (exit_code == 0 and not
           self.fix_subdirectory_path_if_broken('pg_xlog' if get_major_version(self.data_dir) < 10 else 'pg_wal')):
            return ExitCode.FAIL
        return exit_code


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

    exit_code = None
    assert args.retries >= 0

    # Retry cloning in a loop. We do separate retries for the master
    # connection attempt inside should_use_s3_to_create_replica,
    # because we need to differentiate between the last attempt and
    # the rest and make a decision when the last attempt fails on
    # whether to use WAL-E or not depending on the no_master flag.
    for _ in range(0, args.retries + 1):
        restore = WALERestore(scope=args.scope, datadir=args.datadir, connstring=args.connstring,
                              env_dir=args.envdir, threshold_mb=args.threshold_megabytes,
                              threshold_pct=args.threshold_backup_size_percentage, use_iam=args.use_iam,
                              no_master=args.no_master, retries=args.retries)
        exit_code = restore.run()
        if not exit_code == ExitCode.RETRY_LATER:  # only WAL-E failures lead to the retry
            logger.debug('exit_code is %r, not retrying', exit_code)
            break
        time.sleep(RETRY_SLEEP_INTERVAL)

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
