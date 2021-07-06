import logging
import os
import shlex
import six
import subprocess

from threading import Lock, Thread

from .connection import get_connection_cursor
from .misc import format_lsn, parse_history, parse_lsn
from ..async_executor import CriticalTask
from ..dcs import Leader

logger = logging.getLogger(__name__)

REWIND_STATUS = type('Enum', (), {'INITIAL': 0, 'CHECKPOINT': 1, 'CHECK': 2, 'NEED': 3,
                                  'NOT_NEED': 4, 'SUCCESS': 5, 'FAILED': 6})


class Rewind(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._checkpoint_task_lock = Lock()
        self.reset_state()

    @staticmethod
    def configuration_allows_rewind(data):
        return data.get('wal_log_hints setting', 'off') == 'on' or data.get('Data page checksum version', '0') != '0'

    @property
    def can_rewind(self):
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        # low-hanging fruit: check if pg_rewind configuration is there
        if not self._postgresql.config.get('use_pg_rewind'):
            return False

        cmd = [self._postgresql.pgcommand('pg_rewind'), '--help']
        try:
            ret = subprocess.call(cmd, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
            if ret != 0:  # pg_rewind is not there, close up the shop and go home
                return False
        except OSError:
            return False
        return self.configuration_allows_rewind(self._postgresql.controldata())

    @property
    def can_rewind_or_reinitialize_allowed(self):
        return self._postgresql.config.get('remove_data_directory_on_diverged_timelines') or self.can_rewind

    def trigger_check_diverged_lsn(self):
        if self.can_rewind_or_reinitialize_allowed and self._state != REWIND_STATUS.NEED:
            self._state = REWIND_STATUS.CHECK

    @staticmethod
    def check_leader_is_not_in_recovery(conn_kwargs):
        try:
            with get_connection_cursor(connect_timeout=3, options='-c statement_timeout=2000', **conn_kwargs) as cur:
                cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                if not cur.fetchone()[0]:
                    return True
                logger.info('Leader is still in_recovery and therefore can\'t be used for rewind')
        except Exception:
            return logger.exception('Exception when working with leader')

    def _get_checkpoint_end(self, timeline, lsn):
        """The checkpoint record size in WAL depends on postgres major version and platform (memory alignment).
        Hence, the only reliable way to figure out where it ends, read the record from file with the help of pg_waldump
        and parse the output. We are trying to read two records, and expect that it will fail to read the second one:
        `pg_waldump: fatal: error in WAL record at 0/182E220: invalid record length at 0/182E298: wanted 24, got 0`
        The error message contains information about LSN of the next record, which is exactly where checkpoint ends."""

        lsn8 = format_lsn(lsn, True)
        lsn = format_lsn(lsn)
        out, err = self._postgresql.waldump(timeline, lsn, 2)
        if out is not None and err is not None:
            out = out.decode('utf-8').rstrip().split('\n')
            err = err.decode('utf-8').rstrip().split('\n')
            pattern = 'error in WAL record at {0}: invalid record length at '.format(lsn)

            if len(out) == 1 and len(err) == 1 and ', lsn: {0}, prev '.format(lsn8) in out[0] and pattern in err[0]:
                i = err[0].find(pattern) + len(pattern)
                j = err[0].find(": wanted ", i)
                if j > -1:
                    try:
                        return parse_lsn(err[0][i:j])
                    except Exception as e:
                        logger.error('Failed to parse lsn %s: %r', err[0][i:j], e)
            logger.error('Failed to parse pg_%sdump output', self._postgresql.wal_name)
            logger.error(' stdout=%s', '\n'.join(out))
            logger.error(' stderr=%s', '\n'.join(err))

        return 0

    def _get_local_timeline_lsn_from_controldata(self):
        in_recovery = timeline = lsn = None
        data = self._postgresql.controldata()
        try:
            if data.get('Database cluster state') == 'shut down in recovery':
                in_recovery = True
                lsn = data.get('Minimum recovery ending location')
                timeline = int(data.get("Min recovery ending loc's timeline"))
                if lsn == '0/0' or timeline == 0:  # it was a master when it crashed
                    data['Database cluster state'] = 'shut down'
            if data.get('Database cluster state') == 'shut down':
                in_recovery = False
                lsn = data.get('Latest checkpoint location')
                timeline = int(data.get("Latest checkpoint's TimeLineID"))
        except (TypeError, ValueError):
            logger.exception('Failed to get local timeline and lsn from pg_controldata output')

        if lsn is not None:
            try:
                lsn = parse_lsn(lsn)
            except (IndexError, ValueError) as e:
                logger.error('Exception when parsing lsn %s: %r', lsn, e)
                lsn = None

        return in_recovery, timeline, lsn

    def _get_local_timeline_lsn(self):
        if self._postgresql.is_running():  # if postgres is running - get timeline from replication connection
            in_recovery = True
            timeline = self._postgresql.received_timeline() or self._postgresql.get_replica_timeline()
            lsn = self._postgresql.replayed_location()
        else:  # otherwise analyze pg_controldata output
            in_recovery, timeline, lsn = self._get_local_timeline_lsn_from_controldata()

        log_lsn = format_lsn(lsn) if isinstance(lsn, six.integer_types) else lsn
        logger.info('Local timeline=%s lsn=%s', timeline, log_lsn)
        return in_recovery, timeline, lsn

    @staticmethod
    def _log_master_history(history, i):
        start = max(0, i - 3)
        end = None if i + 4 >= len(history) else i + 2
        history_show = []

        def format_history_line(line):
            return '{0}\t{1}\t{2}'.format(line[0], format_lsn(line[1]), line[2])

        for line in history[start:end]:
            history_show.append(format_history_line(line))

        if line != history[-1]:
            history_show.append('...')
            history_show.append(format_history_line(history[-1]))

        logger.info('master: history=%s', '\n'.join(history_show))

    def _conn_kwargs(self, member, auth):
        ret = member.conn_kwargs(auth)
        if not ret.get('database'):
            ret['database'] = self._postgresql.database
        return ret

    def _check_timeline_and_lsn(self, leader):
        in_recovery, local_timeline, local_lsn = self._get_local_timeline_lsn()
        if local_timeline is None or local_lsn is None:
            return

        if isinstance(leader, Leader):
            if leader.member.data.get('role') != 'master':
                return
        # standby cluster
        elif not self.check_leader_is_not_in_recovery(self._conn_kwargs(leader, self._postgresql.config.replication)):
            return

        history = need_rewind = None
        try:
            with self._postgresql.get_replication_connection_cursor(**leader.conn_kwargs()) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                master_timeline = cur.fetchone()[1]
                logger.info('master_timeline=%s', master_timeline)
                if local_timeline > master_timeline:  # Not always supported by pg_rewind
                    need_rewind = True
                elif local_timeline == master_timeline:
                    need_rewind = False
                elif master_timeline > 1:
                    cur.execute('TIMELINE_HISTORY %s', (master_timeline,))
                    history = cur.fetchone()[1]
                    if not isinstance(history, six.string_types):
                        history = bytes(history).decode('utf-8')
                    logger.debug('master: history=%s', history)
        except Exception:
            return logger.exception('Exception when working with master via replication connection')

        if history is not None:
            history = list(parse_history(history))
            for i, (parent_timeline, switchpoint, _) in enumerate(history):
                if parent_timeline == local_timeline:
                    # We don't need to rewind when:
                    # 1. for replica: replayed location is not ahead of switchpoint
                    # 2. for the former primary: end of checkpoint record is the same as switchpoint
                    if in_recovery:
                        need_rewind = local_lsn > switchpoint
                    elif local_lsn >= switchpoint:
                        need_rewind = True
                    else:
                        need_rewind = switchpoint != self._get_checkpoint_end(local_timeline, local_lsn)
                    break
                elif parent_timeline > local_timeline:
                    break
            self._log_master_history(history, i)

        self._state = need_rewind and REWIND_STATUS.NEED or REWIND_STATUS.NOT_NEED

    def rewind_or_reinitialize_needed_and_possible(self, leader):
        if leader and leader.name != self._postgresql.name and leader.conn_url and self._state == REWIND_STATUS.CHECK:
            self._check_timeline_and_lsn(leader)
        return leader and leader.conn_url and self._state == REWIND_STATUS.NEED

    def __checkpoint(self, task, wakeup):
        try:
            result = self._postgresql.checkpoint()
        except Exception as e:
            result = 'Exception: ' + str(e)
        with task:
            task.complete(not bool(result))
            if task.result:
                wakeup()

    def ensure_checkpoint_after_promote(self, wakeup):
        """After promote issue a CHECKPOINT from a new thread and asynchronously check the result.
        In case if CHECKPOINT failed, just check that timeline in pg_control was updated."""

        if self._state == REWIND_STATUS.INITIAL and self._postgresql.is_leader():
            with self._checkpoint_task_lock:
                if self._checkpoint_task:
                    with self._checkpoint_task:
                        if self._checkpoint_task.result:
                            self._state = REWIND_STATUS.CHECKPOINT
                        if self._checkpoint_task.result is not False:
                            return
                else:
                    self._checkpoint_task = CriticalTask()
                    return Thread(target=self.__checkpoint, args=(self._checkpoint_task, wakeup)).start()

            if self._postgresql.get_master_timeline() == self._postgresql.pg_control_timeline():
                self._state = REWIND_STATUS.CHECKPOINT

    def checkpoint_after_promote(self):
        return self._state == REWIND_STATUS.CHECKPOINT

    def _fetch_missing_wal(self, restore_command, wal_filename):
        cmd = ''
        length = len(restore_command)
        i = 0
        while i < length:
            if restore_command[i] == '%' and i + 1 < length:
                i += 1
                if restore_command[i] == 'p':
                    cmd += os.path.join(self._postgresql.wal_dir, wal_filename)
                elif restore_command[i] == 'f':
                    cmd += wal_filename
                elif restore_command[i] == 'r':
                    cmd += '000000010000000000000001'
                elif restore_command[i] == '%':
                    cmd += '%'
                else:
                    cmd += '%'
                    i -= 1
            else:
                cmd += restore_command[i]
            i += 1

        logger.info('Trying to fetch the missing wal: %s', cmd)
        return self._postgresql.cancellable.call(shlex.split(cmd)) == 0

    def _find_missing_wal(self, data):
        # could not open file "$PGDATA/pg_wal/0000000A00006AA100000068": No such file or directory
        pattern = 'could not open file "'
        for line in data.decode('utf-8').split('\n'):
            b = line.find(pattern)
            if b > -1:
                b += len(pattern)
                e = line.find('": ', b)
                if e > -1 and '/' in line[b:e]:
                    waldir, wal_filename = line[b:e].rsplit('/', 1)
                    if waldir.endswith('/pg_' + self._postgresql.wal_name) and len(wal_filename) == 24:
                        return wal_filename

    def pg_rewind(self, r):
        # prepare pg_rewind connection
        env = self._postgresql.config.write_pgpass(r)
        env.update(LANG='C', LC_ALL='C', PGOPTIONS='-c statement_timeout=0')
        dsn = self._postgresql.config.format_dsn(r, True)
        logger.info('running pg_rewind from %s', dsn)

        restore_command = self._postgresql.config.get('recovery_conf', {}).get('restore_command') \
            if self._postgresql.major_version < 120000 else self._postgresql.get_guc_value('restore_command')

        cmd = [self._postgresql.pgcommand('pg_rewind')]
        if self._postgresql.major_version >= 130000 and restore_command:
            cmd.append('--restore-target-wal')
        cmd.extend(['-D', self._postgresql.data_dir, '--source-server', dsn])

        while True:
            results = {}
            ret = self._postgresql.cancellable.call(cmd, env=env, communicate=results)

            logger.info('pg_rewind exit code=%s', ret)
            if ret is None:
                return False

            logger.info(' stdout=%s', results['stdout'].decode('utf-8'))
            logger.info(' stderr=%s', results['stderr'].decode('utf-8'))
            if ret == 0:
                return True

            if not restore_command or self._postgresql.major_version >= 130000:
                return False

            missing_wal = self._find_missing_wal(results['stderr']) or self._find_missing_wal(results['stdout'])
            if not missing_wal:
                return False

            if not self._fetch_missing_wal(restore_command, missing_wal):
                logger.info('Failed to fetch WAL segment %s required for pg_rewind', missing_wal)
                return False

    def execute(self, leader):
        if self._postgresql.is_running() and not self._postgresql.stop(checkpoint=False):
            return logger.warning('Can not run pg_rewind because postgres is still running')

        # prepare pg_rewind connection
        r = self._conn_kwargs(leader, self._postgresql.config.rewind_credentials)

        # 1. make sure that we are really trying to rewind from the master
        # 2. make sure that pg_control contains the new timeline by:
        #   running a checkpoint or
        #   waiting until Patroni on the master will expose checkpoint_after_promote=True
        checkpoint_status = leader.checkpoint_after_promote if isinstance(leader, Leader) else None
        if checkpoint_status is None:  # master still runs the old Patroni
            leader_status = self._postgresql.checkpoint(self._conn_kwargs(leader, self._postgresql.config.superuser))
            if leader_status:
                return logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)
        elif not checkpoint_status:
            return logger.info('Waiting for checkpoint on %s before rewind', leader.name)
        elif not self.check_leader_is_not_in_recovery(r):
            return

        if self.pg_rewind(r):
            self._state = REWIND_STATUS.SUCCESS
        elif not self.check_leader_is_not_in_recovery(r):
            logger.warning('Failed to rewind because master %s become unreachable', leader.name)
        else:
            logger.error('Failed to rewind from healty master: %s', leader.name)

            for name in ('remove_data_directory_on_rewind_failure', 'remove_data_directory_on_diverged_timelines'):
                if self._postgresql.config.get(name):
                    logger.warning('%s is set. removing...', name)
                    self._postgresql.remove_data_directory()
                    self._state = REWIND_STATUS.INITIAL
                    break
            else:
                self._state = REWIND_STATUS.FAILED
        return False

    def reset_state(self):
        self._state = REWIND_STATUS.INITIAL
        with self._checkpoint_task_lock:
            self._checkpoint_task = None

    @property
    def is_needed(self):
        return self._state in (REWIND_STATUS.CHECK, REWIND_STATUS.NEED)

    @property
    def executed(self):
        return self._state > REWIND_STATUS.NOT_NEED

    @property
    def failed(self):
        return self._state == REWIND_STATUS.FAILED

    def read_postmaster_opts(self):
        """returns the list of option names/values from postgres.opts, Empty dict if read failed or no file"""
        result = {}
        try:
            with open(os.path.join(self._postgresql.data_dir, 'postmaster.opts')) as f:
                data = f.read()
                for opt in data.split('" "'):
                    if '=' in opt and opt.startswith('--'):
                        name, val = opt.split('=', 1)
                        result[name.strip('-')] = val.rstrip('"\n')
        except IOError:
            logger.exception('Error when reading postmaster.opts')
        return result

    def single_user_mode(self, communicate=None, options=None):
        """run a given command in a single-user mode. If the command is empty - then just start and stop"""
        cmd = [self._postgresql.pgcommand('postgres'), '--single', '-D', self._postgresql.data_dir]
        for opt, val in sorted((options or {}).items()):
            cmd.extend(['-c', '{0}={1}'.format(opt, val)])
        # need a database name to connect
        cmd.append('template1')
        return self._postgresql.cancellable.call(cmd, communicate=communicate)

    def cleanup_archive_status(self):
        status_dir = os.path.join(self._postgresql.wal_dir, 'archive_status')
        try:
            for f in os.listdir(status_dir):
                path = os.path.join(status_dir, f)
                try:
                    if os.path.islink(path):
                        os.unlink(path)
                    elif os.path.isfile(path):
                        os.remove(path)
                except OSError:
                    logger.exception('Unable to remove %s', path)
        except OSError:
            logger.exception('Unable to list %s', status_dir)

    def ensure_clean_shutdown(self):
        self.cleanup_archive_status()

        # Start in a single user mode and stop to produce a clean shutdown
        opts = self.read_postmaster_opts()
        opts.update({'archive_mode': 'on', 'archive_command': 'false'})
        self._postgresql.config.remove_recovery_conf()
        output = {}
        ret = self.single_user_mode(communicate=output, options=opts)
        if ret != 0:
            logger.error('Crash recovery finished with code=%s', ret)
            logger.info(' stdout=%s', output['stdout'].decode('utf-8'))
            logger.info(' stderr=%s', output['stderr'].decode('utf-8'))
        return ret == 0 or None
