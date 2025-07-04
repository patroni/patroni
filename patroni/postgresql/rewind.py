import logging
import os
import re
import shlex
import shutil
import subprocess

from enum import IntEnum
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ..async_executor import CriticalTask
from ..collections import EMPTY_DICT
from ..dcs import Leader, RemoteMember
from . import Postgresql
from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir, parse_history, parse_lsn, PostgresqlRole

logger = logging.getLogger(__name__)


class REWIND_STATUS(IntEnum):
    INITIAL = 0
    CHECKPOINT = 1
    CHECK = 2
    NEED = 3
    NOT_NEED = 4
    SUCCESS = 5
    FAILED = 6


class Rewind(object):

    def __init__(self, postgresql: Postgresql) -> None:
        self._postgresql = postgresql
        self._checkpoint_task_lock = Lock()
        self.reset_state()

    @staticmethod
    def configuration_allows_rewind(data: Dict[str, str]) -> bool:
        return data.get('wal_log_hints setting', 'off') == 'on' or data.get('Data page checksum version', '0') != '0'

    @property
    def enabled(self) -> bool:
        return bool(self._postgresql.config.get('use_pg_rewind'))

    @property
    def can_rewind(self) -> bool:
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        # low-hanging fruit: check if pg_rewind configuration is there
        if not self.enabled:
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
    def should_remove_data_directory_on_diverged_timelines(self) -> bool:
        return bool(self._postgresql.config.get('remove_data_directory_on_diverged_timelines'))

    @property
    def can_rewind_or_reinitialize_allowed(self) -> bool:
        return self.should_remove_data_directory_on_diverged_timelines or self.can_rewind

    def trigger_check_diverged_lsn(self) -> None:
        if self.can_rewind_or_reinitialize_allowed and self._state != REWIND_STATUS.NEED:
            self._state = REWIND_STATUS.CHECK
        with self._checkpoint_task_lock:
            self._checkpoint_task = None

    @staticmethod
    def check_leader_is_not_in_recovery(conn_kwargs: Dict[str, Any]) -> Optional[bool]:
        try:
            with get_connection_cursor(connect_timeout=3, options='-c statement_timeout=2000', **conn_kwargs) as cur:
                cur.execute('SELECT pg_catalog.pg_is_in_recovery()')
                row = cur.fetchone()
                if not row or not row[0]:
                    return True
                logger.info('Leader is still in_recovery and therefore can\'t be used for rewind')
        except Exception:
            return logger.exception('Exception when working with leader')

    @staticmethod
    def check_leader_has_run_checkpoint(conn_kwargs: Dict[str, Any]) -> Optional[str]:
        try:
            with get_connection_cursor(connect_timeout=3, options='-c statement_timeout=2000', **conn_kwargs) as cur:
                cur.execute("SELECT NOT pg_catalog.pg_is_in_recovery()"
                            " AND ('x' || pg_catalog.substr(pg_catalog.pg_walfile_name("
                            " pg_catalog.pg_current_wal_lsn()), 1, 8))::bit(32)::int = timeline_id"
                            " FROM pg_catalog.pg_control_checkpoint()")
                row = cur.fetchone()
                if not row or not row[0]:
                    return 'leader has not run a checkpoint yet'
        except Exception:
            logger.exception('Exception when working with leader')
            return 'not accessible or not healthy'

    def _get_checkpoint_end(self, timeline: int, lsn: int) -> int:
        """Get the end of checkpoint record from WAL.

        .. note::
            The checkpoint record size in WAL depends on postgres major version and platform (memory alignment).
            Hence, the only reliable way to figure out where it ends, is to read the record from file with the
            help of ``pg_waldump`` and parse the output.

            We are trying to read two records, and expect that it will fail to read the second record with message:

                fatal: error in WAL record at 0/182E220: invalid record length at 0/182E298: wanted 24, got 0; or

                fatal: error in WAL record at 0/182E220: invalid record length at 0/182E298: expected at least 24, got 0

            The error message contains information about LSN of the next record, which is exactly where checkpoint ends.

        :param timeline: the checkpoint *timeline* from ``pg_controldata``.
        :param lsn: the checkpoint *location* as :class:`int` from ``pg_controldata``.

        :returns: the end of checkpoint record as :class:`int` or ``0`` if failed to parse ``pg_waldump`` output.
        """
        lsn8 = format_lsn(lsn, True)
        lsn_str = format_lsn(lsn)
        out, err = self._postgresql.waldump(timeline, lsn_str, 2)
        if out is not None and err is not None:
            out = out.decode('utf-8').rstrip().split('\n')
            err = err.decode('utf-8').rstrip().split('\n')
            pattern = 'error in WAL record at {0}: invalid record length at '.format(lsn_str)

            if len(out) == 1 and len(err) == 1 and ', lsn: {0}, prev '.format(lsn8) in out[0] and pattern in err[0]:
                i = err[0].find(pattern) + len(pattern)
                # Message format depends on the major version:
                # * expected at least -- starting from v16
                # * wanted -- before v16
                # * nothing (end of message) 9.5 and older
                # We will simply check all possible combinations.
                for pattern in (': expected at least ', ': wanted ', '\n'):
                    j = (err[0] + '\n').find(pattern, i)
                    if j > -1:
                        try:
                            return parse_lsn(err[0][i:j])
                        except Exception as e:
                            logger.error('Failed to parse lsn %s: %r', err[0][i:j], e)
            logger.error('Failed to parse pg_%sdump output', self._postgresql.wal_name)
            logger.error(' stdout=%s', '\n'.join(out))
            logger.error(' stderr=%s', '\n'.join(err))

        return 0

    def _get_local_timeline_lsn_from_controldata(self) -> Tuple[Optional[bool], Optional[int], Optional[int]]:
        in_recovery = timeline = lsn = None
        data = self._postgresql.controldata()
        try:
            if data.get('Database cluster state') in ('shut down in recovery', 'in archive recovery'):
                in_recovery = True
                lsn = data.get('Minimum recovery ending location')
                timeline = int(data.get("Min recovery ending loc's timeline", ""))
                if lsn == '0/0' or timeline == 0:  # it was a primary when it crashed
                    data['Database cluster state'] = 'shut down'
            if data.get('Database cluster state') == 'shut down':
                in_recovery = False
                lsn = data.get('Latest checkpoint location')
                timeline = int(data.get("Latest checkpoint's TimeLineID", ""))
        except (TypeError, ValueError):
            logger.exception('Failed to get local timeline and lsn from pg_controldata output')

        if lsn is not None:
            try:
                lsn = parse_lsn(lsn)
            except (IndexError, ValueError) as e:
                logger.error('Exception when parsing lsn %s: %r', lsn, e)
                lsn = None

        return in_recovery, timeline, lsn

    def _get_local_timeline_lsn(self) -> Tuple[Optional[bool], Optional[int], Optional[int]]:
        if self._postgresql.is_running():  # if postgres is running - get timeline from replication connection
            in_recovery = True
            timeline = self._postgresql.get_replica_timeline()
            lsn = self._postgresql.replay_lsn()
        else:  # otherwise analyze pg_controldata output
            in_recovery, timeline, lsn = self._get_local_timeline_lsn_from_controldata()

        log_lsn = format_lsn(lsn) if isinstance(lsn, int) else lsn
        logger.info('Local timeline=%s lsn=%s', timeline, log_lsn)
        return in_recovery, timeline, lsn

    @staticmethod
    def _log_primary_history(history: List[Tuple[int, int, str]], i: int) -> None:
        start = max(0, i - 3)
        end = None if i + 4 >= len(history) else i + 2
        history_show: List[str] = []

        def format_history_line(line: Tuple[int, int, str]) -> str:
            return '{0}\t{1}\t{2}'.format(line[0], format_lsn(line[1]), line[2])

        line = None
        for line in history[start:end]:
            history_show.append(format_history_line(line))

        if line != history[-1]:
            history_show.append('...')
            history_show.append(format_history_line(history[-1]))

        logger.info('primary: history=%s', '\n'.join(history_show))

    def _conn_kwargs(self, member: Union[Leader, RemoteMember], auth: Dict[str, Any]) -> Dict[str, Any]:
        ret = member.conn_kwargs(auth)
        if not ret.get('dbname'):
            ret['dbname'] = self._postgresql.database
        # Add target_session_attrs to make sure we hit the primary.
        # It is not strictly necessary for starting from PostgreSQL v14, which made it possible
        # to rewind from standby, but doing it from the real primary is always safer.
        if self._postgresql.major_version >= 100000:
            ret['target_session_attrs'] = 'read-write'
        return ret

    def _check_timeline_and_lsn(self, leader: Union[Leader, RemoteMember]) -> None:
        in_recovery, local_timeline, local_lsn = self._get_local_timeline_lsn()
        if local_timeline is None or local_lsn is None:
            return

        if isinstance(leader, Leader) and leader.member.data.get('role') not in (PostgresqlRole.MASTER,
                                                                                 PostgresqlRole.PRIMARY):
            return

        # We want to use replication credentials when connecting to the "postgres" database in case if
        # `use_pg_rewind` isn't enabled and only `remove_data_directory_on_diverged_timelines` is set
        # for Postgresql older than v11 (where Patroni can't use a dedicated user for rewind).
        # In all other cases we will use rewind or superuser credentials.
        check_credentials = self._postgresql.config.replication if not self.enabled and\
            self.should_remove_data_directory_on_diverged_timelines and\
            self._postgresql.major_version < 110000 else self._postgresql.config.rewind_credentials
        if not self.check_leader_is_not_in_recovery(self._conn_kwargs(leader, check_credentials)):
            return

        history = need_rewind = None
        try:
            with self._postgresql.get_replication_connection_cursor(**leader.conn_kwargs()) as cur:
                cur.execute('IDENTIFY_SYSTEM')
                row = cur.fetchone()
                if row:
                    primary_timeline = row[1]
                    logger.info('primary_timeline=%s', primary_timeline)
                    if local_timeline > primary_timeline:  # Not always supported by pg_rewind
                        need_rewind = True
                    elif local_timeline == primary_timeline:
                        need_rewind = False
                    elif primary_timeline > 1:
                        cur.execute('TIMELINE_HISTORY {0}'.format(primary_timeline).encode('utf-8'))
                        row = cur.fetchone()
                        if row:
                            history = row[1]
                            if not isinstance(history, str):
                                history = bytes(history).decode('utf-8')
                            logger.debug('primary: history=%s', history)
        except Exception:
            return logger.exception('Exception when working with primary via replication connection')

        if history is not None:
            history = list(parse_history(history))
            i = len(history)
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
                    need_rewind = True
                    break
            else:
                need_rewind = True
            self._log_primary_history(history, i)

        self._state = need_rewind and REWIND_STATUS.NEED or REWIND_STATUS.NOT_NEED

    def rewind_or_reinitialize_needed_and_possible(self, leader: Union[Leader, RemoteMember, None]) -> bool:
        if leader and leader.name != self._postgresql.name and leader.conn_url and self._state == REWIND_STATUS.CHECK:
            self._check_timeline_and_lsn(leader)
        return bool(leader and leader.conn_url) and self._state == REWIND_STATUS.NEED

    def __checkpoint(self, task: CriticalTask, wakeup: Callable[..., Any]) -> None:
        try:
            result = self._postgresql.checkpoint()
        except Exception as e:
            result = 'Exception: ' + str(e)
        with task:
            task.complete(not bool(result))
            if task.result:
                wakeup()

    def ensure_checkpoint_after_promote(self, wakeup: Callable[..., Any]) -> None:
        """After promote issue a CHECKPOINT from a new thread and asynchronously check the result.
        In case if CHECKPOINT failed, just check that timeline in pg_control was updated."""

        if self._state != REWIND_STATUS.CHECKPOINT and self._postgresql.is_primary():
            with self._checkpoint_task_lock:
                if self._checkpoint_task:
                    result = None

                    with self._checkpoint_task:
                        result = self._checkpoint_task.result

                    if result is True:
                        self._state = REWIND_STATUS.CHECKPOINT

                    if result is not None:
                        self._checkpoint_task = None
                elif self._postgresql.get_primary_timeline() == self._postgresql.pg_control_timeline():
                    self._state = REWIND_STATUS.CHECKPOINT
                else:
                    self._checkpoint_task = CriticalTask()
                    Thread(target=self.__checkpoint, args=(self._checkpoint_task, wakeup)).start()

    def checkpoint_after_promote(self) -> bool:
        return self._state == REWIND_STATUS.CHECKPOINT

    def get_archive_command(self) -> Optional[str]:
        """Get ``archive_command`` GUC value if defined and archiving is enabled.

        :returns: ``archive_command`` defined in the Postgres configuration or None.
        """
        archive_mode = self._postgresql.get_guc_value('archive_mode')
        archive_cmd = self._postgresql.get_guc_value('archive_command')
        if archive_mode in ('on', 'always') and archive_cmd:
            return archive_cmd

    def _build_archiver_command(self, command: str, wal_filename: str) -> str:
        """Replace placeholders in the given archiver command's template.
        Applicable for archive_command and restore_command.
        Can also be used for archive_cleanup_command and recovery_end_command,
        however %r value is always set to 000000010000000000000001."""
        cmd = ''
        length = len(command)
        i = 0
        while i < length:
            if command[i] == '%' and i + 1 < length:
                i += 1
                if command[i] == 'p':
                    cmd += os.path.join(self._postgresql.wal_dir, wal_filename)
                elif command[i] == 'f':
                    cmd += wal_filename
                elif command[i] == 'r':
                    cmd += '000000010000000000000001'
                elif command[i] == '%':
                    cmd += '%'
                else:
                    cmd += '%'
                    i -= 1
            else:
                cmd += command[i]
            i += 1

        return cmd

    def _fetch_missing_wal(self, restore_command: str, wal_filename: str) -> bool:
        cmd = self._build_archiver_command(restore_command, wal_filename)

        logger.info('Trying to fetch the missing wal: %s', cmd)
        return self._postgresql.cancellable.call(shlex.split(cmd)) == 0

    def _find_missing_wal(self, data: bytes) -> Optional[str]:
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

    def _archive_ready_wals(self) -> None:
        """Try to archive WALs that have .ready files just in case
        archive_mode was not set to 'always' before promote, while
        after it the WALs were recycled on the promoted replica.
        With this we prevent the entire loss of such WALs and the
        consequent old leader's start failure."""
        archive_cmd = self.get_archive_command()
        if not archive_cmd:
            return

        walseg_regex = re.compile(r'^[0-9A-F]{24}(\.partial){0,1}\.ready$')
        status_dir = os.path.join(self._postgresql.wal_dir, 'archive_status')
        try:
            wals_to_archive = [f[:-6] for f in os.listdir(status_dir) if walseg_regex.match(f)]
        except OSError as e:
            return logger.error('Unable to list %s: %r', status_dir, e)

        # skip fsync, as postgres --single or pg_rewind will anyway run it
        for wal in sorted(wals_to_archive):
            old_name = os.path.join(status_dir, wal + '.ready')
            # wal file might have already been archived
            if os.path.isfile(old_name) and os.path.isfile(os.path.join(self._postgresql.wal_dir, wal)):
                cmd = self._build_archiver_command(archive_cmd, wal)
                # it is the author of archive_command, who is responsible
                # for not overriding the WALs already present in archive
                logger.info('Trying to archive %s: %s', wal, cmd)
                if self._postgresql.cancellable.call([cmd], shell=True) == 0:
                    new_name = os.path.join(status_dir, wal + '.done')
                    try:
                        shutil.move(old_name, new_name)
                    except Exception as e:
                        logger.error('Unable to rename %s to %s: %r', old_name, new_name, e)
                else:
                    logger.info('Failed to archive WAL segment %s', wal)

    def _maybe_clean_pg_replslot(self) -> None:
        """Clean pg_replslot directory if pg version is less then 11
        (pg_rewind deletes $PGDATA/pg_replslot content only since pg11)."""
        if self._postgresql.major_version < 110000:
            replslot_dir = self._postgresql.slots_handler.pg_replslot_dir
            try:
                for f in os.listdir(replslot_dir):
                    shutil.rmtree(os.path.join(replslot_dir, f))
                fsync_dir(replslot_dir)
            except Exception as e:
                logger.warning('Unable to clean %s: %r', replslot_dir, e)

    def pg_rewind(self, conn_kwargs: Dict[str, Any]) -> bool:
        """Do pg_rewind.

        .. note::
            If ``pg_rewind`` doesn't support ``--restore-target-wal`` parameter and exited with non zero code,
            Patroni will parse stderr/stdout to figure out if it failed due to a missing WAL file and will
            repeat an attempt after downloading the missing file using ``restore_command``.

        :param conn_kwargs: :class:`dict` object with connection parameters.

        :returns: ``True`` if ``pg_rewind`` finished successfully, ``False`` otherwise.
        """
        # prepare pg_rewind connection string
        env = self._postgresql.config.write_pgpass(conn_kwargs)
        env.update(LANG='C', LC_ALL='C', PGOPTIONS='-c statement_timeout=0')
        dsn = self._postgresql.config.format_dsn({**conn_kwargs, 'password': None})
        logger.info('running pg_rewind from %s', dsn)

        restore_command = (self._postgresql.config.get('recovery_conf') or EMPTY_DICT).get('restore_command') \
            if self._postgresql.major_version < 120000 else self._postgresql.get_guc_value('restore_command')

        # Until v15 pg_rewind expected postgresql.conf to be inside $PGDATA, which is not the case on e.g. Debian
        pg_rewind_can_restore = restore_command and (self._postgresql.major_version >= 150000
                                                     or (self._postgresql.major_version >= 130000
                                                         and self._postgresql.config.config_dir
                                                         == self._postgresql.data_dir))

        cmd = [self._postgresql.pgcommand('pg_rewind')]
        if pg_rewind_can_restore:
            cmd.append('--restore-target-wal')
            if self._postgresql.major_version >= 150000 and\
                    self._postgresql.config.config_dir != self._postgresql.data_dir:
                cmd.append('--config-file={0}'.format(self._postgresql.config.postgresql_conf))

        cmd.extend(['-D', self._postgresql.data_dir, '--source-server', dsn])

        while True:
            results: Dict[str, bytes] = {}
            ret = self._postgresql.cancellable.call(cmd, env=env, communicate=results)

            logger.info('pg_rewind exit code=%s', ret)
            if ret is None:
                return False

            logger.info(' stdout=%s', results['stdout'].decode('utf-8'))
            logger.info(' stderr=%s', results['stderr'].decode('utf-8'))
            if ret == 0:
                return True

            if not restore_command or pg_rewind_can_restore:
                return False

            missing_wal = self._find_missing_wal(results['stderr']) or self._find_missing_wal(results['stdout'])
            if not missing_wal:
                return False

            if not self._fetch_missing_wal(restore_command, missing_wal):
                logger.info('Failed to fetch WAL segment %s required for pg_rewind', missing_wal)
                return False

    def execute(self, leader: Union[Leader, RemoteMember]) -> Optional[bool]:
        if self._postgresql.is_running() and not self._postgresql.stop(checkpoint=False):
            return logger.warning('Can not run pg_rewind because postgres is still running')

        self._archive_ready_wals()

        # prepare pg_rewind connection
        r = self._conn_kwargs(leader, self._postgresql.config.rewind_credentials)

        # 1. make sure that we are really trying to rewind from the primary
        # 2. make sure that pg_control contains the new timeline by:
        #   running a checkpoint or
        #   waiting until Patroni on the primary will expose checkpoint_after_promote=True
        checkpoint_status = leader.checkpoint_after_promote if isinstance(leader, Leader) else None
        if checkpoint_status is None:  # we are the standby-cluster leader or primary still runs the old Patroni
            # superuser credentials match rewind_credentials if the latter are not provided or we run 10 or older
            if self._postgresql.config.superuser == self._postgresql.config.rewind_credentials:
                leader_status = self._postgresql.checkpoint(
                    self._conn_kwargs(leader, self._postgresql.config.superuser))
            else:  # we run 11+ and have a dedicated pg_rewind user
                leader_status = self.check_leader_has_run_checkpoint(r)
            if leader_status:  # we tried to run/check for a checkpoint on the remote leader, but it failed
                return logger.warning('Can not use %s for rewind: %s', leader.name, leader_status)
        elif not checkpoint_status:
            return logger.info('Waiting for checkpoint on %s before rewind', leader.name)
        elif not self.check_leader_is_not_in_recovery(r):
            return

        if self.pg_rewind(r):
            self._maybe_clean_pg_replslot()
            self._state = REWIND_STATUS.SUCCESS
        else:
            if not self.check_leader_is_not_in_recovery(r):
                logger.warning('Failed to rewind because primary %s become unreachable', leader.name)
                if not self.can_rewind:  # It is possible that the previous attempt damaged pg_control file!
                    self._state = REWIND_STATUS.FAILED
            else:
                logger.error('Failed to rewind from healthy primary: %s', leader.name)
                self._state = REWIND_STATUS.FAILED

            if self.failed:
                for name in ('remove_data_directory_on_rewind_failure', 'remove_data_directory_on_diverged_timelines'):
                    if self._postgresql.config.get(name):
                        logger.warning('%s is set. removing...', name)
                        self._postgresql.remove_data_directory()
                        self._state = REWIND_STATUS.INITIAL
                        break
        return False

    def reset_state(self) -> None:
        self._state = REWIND_STATUS.INITIAL
        with self._checkpoint_task_lock:
            self._checkpoint_task = None

    @property
    def is_needed(self) -> bool:
        return self._state in (REWIND_STATUS.CHECK, REWIND_STATUS.NEED)

    @property
    def executed(self) -> bool:
        return self._state > REWIND_STATUS.NOT_NEED

    @property
    def failed(self) -> bool:
        return self._state == REWIND_STATUS.FAILED

    def read_postmaster_opts(self) -> Dict[str, str]:
        """returns the list of option names/values from postgres.opts, Empty dict if read failed or no file"""
        result: Dict[str, str] = {}
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

    def single_user_mode(self, communicate: Optional[Dict[str, Any]] = None,
                         options: Optional[Dict[str, str]] = None) -> Optional[int]:
        """run a given command in a single-user mode. If the command is empty - then just start and stop"""
        cmd = [self._postgresql.pgcommand('postgres'), '--single', '-D', self._postgresql.data_dir]
        for opt, val in sorted((options or {}).items()):
            cmd.extend(['-c', '{0}={1}'.format(opt, val)])
        # need a database name to connect
        cmd.append('template1')
        return self._postgresql.cancellable.call(cmd, communicate=communicate)

    def cleanup_archive_status(self) -> None:
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

    def ensure_clean_shutdown(self) -> Optional[bool]:
        self._archive_ready_wals()
        self.cleanup_archive_status()

        # Start in a single user mode and stop to produce a clean shutdown
        opts = self.read_postmaster_opts()
        opts.update({'archive_mode': 'on', 'archive_command': 'false'})
        self._postgresql.config.remove_recovery_conf()
        output: Dict[str, bytes] = {}
        ret = self.single_user_mode(communicate=output, options=opts)
        if ret != 0:
            logger.error('Crash recovery finished with code=%s', ret)
            logger.info(' stdout=%s', output['stdout'].decode('utf-8'))
            logger.info(' stderr=%s', output['stderr'].decode('utf-8'))
        return ret == 0 or None

    def archive_shutdown_checkpoint_wal(self, archive_cmd: str) -> None:
        """Archive WAL file with the shutdown checkpoint.

        :param archive_cmd: archiver command to use
        """
        data = self._postgresql.controldata()
        wal_file = data.get("Latest checkpoint's REDO WAL file", '')
        if not wal_file:
            logger.error("Cannot extract latest checkpoint's WAL file name")
            return
        cmd = self._build_archiver_command(archive_cmd, wal_file)
        if self._postgresql.cancellable.call([cmd], shell=True):
            logger.error("Failed to archive WAL file with the shutdown checkpoint")
