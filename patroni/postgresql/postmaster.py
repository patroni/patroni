import logging
import multiprocessing
import os
import pathlib
import re
import signal
import subprocess
import sys

from multiprocessing.connection import Connection
from typing import Dict, List, Optional

import psutil

from patroni import KUBERNETES_ENV_PREFIX, PATRONI_ENV_PREFIX

# avoid spawning the resource tracker process
if sys.version_info >= (3, 8):  # pragma: no cover
    import multiprocessing.resource_tracker
    multiprocessing.resource_tracker.getfd = lambda: 0
elif sys.version_info >= (3, 4):  # pragma: no cover
    import multiprocessing.semaphore_tracker
    multiprocessing.semaphore_tracker.getfd = lambda: 0

logger = logging.getLogger(__name__)

STOP_SIGNALS = {
    'smart': 'TERM',
    'fast': 'INT',
    'immediate': 'QUIT',
}


def pg_ctl_start(conn: Connection, cmdline: List[str], env: Dict[str, str]) -> None:
    if os.name != 'nt':
        os.setsid()
    try:
        postmaster = subprocess.Popen(cmdline, close_fds=True, env=env)
        conn.send(postmaster.pid)
    except Exception:
        logger.exception('Failed to execute %s', cmdline)
        conn.send(None)
    conn.close()


class PostmasterProcess(psutil.Process):

    def __init__(self, pid: int) -> None:
        self._postmaster_pid: Dict[str, str]
        self.is_single_user = False
        if pid < 0:
            pid = -pid
            self.is_single_user = True
        super(PostmasterProcess, self).__init__(pid)

    @staticmethod
    def _read_postmaster_pidfile(data_dir: str) -> Dict[str, str]:
        """Reads and parses postmaster.pid from the data directory

        :returns dictionary of values if successful, empty dictionary otherwise
        """
        pid_line_names = ['pid', 'data_dir', 'start_time', 'port', 'socket_dir', 'listen_addr', 'shmem_key']
        try:
            with open(os.path.join(data_dir, 'postmaster.pid')) as f:
                return {name: line.rstrip('\n') for name, line in zip(pid_line_names, f)}
        except IOError:
            return {}

    def _is_postmaster_process(self, pgcommand: str, data_dir: str) -> bool:
        """Determine whether this process is the postmaster.

        This method applies several heuristics to decide if the PID read from ``postmaster.pid``
        corresponds to the PostgreSQL postmaster:

            * Excludes the Patroni process itself, its parent, and its direct children.
            * Compares the process start time with the value stored in ``postmaster.pid``,
              treating a small time difference as a positive match.
            * Checks that the executable name matches the expected binary derived from
              ``pgcommand`` (``postgres`` by default) or ``postmaster``.
            * When possible, verifies that the process current working directory matches the given data directory.

        :param pgcommand: name of the postgres/postmaster executable that should be running.
        :param data_dir: PostgreSQL data directory that contains.
        :returns: ``True`` if the process is likely the correct postmaster, ``False`` otherwise.
        """
        if self.pid == os.getpid() or self.pid == os.getppid() or self.ppid() == os.getpid():
            logger.info('Patroni (pid=%s, ppid=%s), "fake postmaster" (pid=%s, ppid=%s)',
                        os.getpid(), os.getppid(), self.pid, self.ppid())
            return False

        try:
            start_time = int(self._postmaster_pid.get('start_time', 0))
            if start_time and abs(self.create_time() - start_time) < 3:
                return True
        except ValueError:
            logger.warning('Garbage start time value in pid file: %r', self._postmaster_pid.get('start_time'))

        try:
            exe = self.exe()
        except Exception as e:
            logger.warning('Failed to get executable file for PID=%d: %r', self.pid, e)
            exe = None

        try:
            cwd = self.cwd()
        except Exception as e:
            logger.warning('Failed to get CWD for PID=%d: %r', self.pid, e)
            cwd = None

        if exe:
            def normalize_exe(value: str) -> str:
                if os.name == 'nt':
                    value = value.lower()
                    base, ext = os.path.splitext(value)
                    if ext == '.exe':
                        value = base
                return os.path.basename(value)

            valid_exes = frozenset([normalize_exe(os.path.basename(pgcommand)), 'postmaster'])
            if normalize_exe(exe) not in valid_exes:
                logger.info('Process %d from postmaster.pid with executable file "%s" does not look like postgres',
                            self.pid, exe)
                return False

            if cwd:
                def normalize_path(path: str) -> Optional[pathlib.PurePath]:
                    try:
                        return pathlib.PurePath(os.path.realpath(path))
                    except Exception as e:
                        logger.warning('Failed to normalize "%s" path: %r', path, e)
                    return None

                data_dir_path = normalize_path(data_dir)
                cwd_path = normalize_path(cwd)
                if data_dir_path and cwd_path:
                    if data_dir_path == cwd_path:
                        logger.debug('Process %d from postmaster.pid with executable file "%s" and CWD="%s"'
                                     ' looks like a correct postgres instance', self.pid, exe, cwd)
                        return True
                    else:
                        logger.info('Process %d from postmaster.pid with executable file "%s" does not look like '
                                    'postgres because CWD="%s" is not PGDATA="%s"', self.pid, exe, cwd, data_dir)
                        return False

            logger.warning('Could not determine if process %d from postmaster.pid with executable file "%s"'
                           ' and CWD="%s" is correct postgres instance. Assuming it is', self.pid, exe, cwd)
            return True

        logger.info('Process %d from postmaster.pid with executable file "%s" and CWD="%s" does not look like postgres',
                    self.pid, exe, cwd)
        return False

    @classmethod
    def _from_pidfile(cls, data_dir: str) -> Optional['PostmasterProcess']:
        postmaster_pid = PostmasterProcess._read_postmaster_pidfile(data_dir)
        try:
            pid = int(postmaster_pid.get('pid', 0))
            if pid:
                proc = cls(pid)
                proc._postmaster_pid = postmaster_pid
                return proc
        except ValueError:
            return None

    @staticmethod
    def from_pidfile(pgcommand: str, data_dir: str) -> Optional['PostmasterProcess']:
        try:
            proc = PostmasterProcess._from_pidfile(data_dir)
            return proc if proc and proc._is_postmaster_process(pgcommand, data_dir) else None
        except psutil.NoSuchProcess:
            return None

    @classmethod
    def from_pid(cls, pid: int) -> Optional['PostmasterProcess']:
        try:
            return cls(pid)
        except psutil.NoSuchProcess:
            return None

    def signal_kill(self) -> bool:
        """to suspend and kill postmaster and all children

        :returns True if postmaster and children are killed, False if error
        """
        try:
            self.suspend()
        except psutil.NoSuchProcess:
            return True
        except psutil.Error as e:
            logger.warning('Failed to suspend postmaster: %s', e)

        try:
            children = self.children(recursive=True)
        except psutil.NoSuchProcess:
            return True
        except psutil.Error as e:
            logger.warning('Failed to get a list of postmaster children: %s', e)
            children = []

        try:
            self.kill()
        except psutil.NoSuchProcess:
            return True
        except psutil.Error as e:
            logger.warning('Could not kill postmaster: %s', e)
            return False

        for child in children:
            try:
                child.kill()
            except psutil.Error:
                pass
        psutil.wait_procs(children + [self])
        return True

    def signal_stop(self, mode: str, pg_ctl: str = 'pg_ctl') -> Optional[bool]:
        """Signal postmaster process to stop

        :returns None if signaled, True if process is already gone, False if error
        """
        if self.is_single_user:
            logger.warning("Cannot stop server; single-user server is running (PID: %s)", self.pid)
            return False
        if os.name != 'posix':
            return self.pg_ctl_kill(mode, pg_ctl)
        try:
            self.send_signal(getattr(signal, 'SIG' + STOP_SIGNALS[mode]))
        except psutil.NoSuchProcess:
            return True
        except psutil.AccessDenied as e:
            logger.warning("Could not send stop signal to PostgreSQL: %r", e)
            return False

        return None

    def pg_ctl_kill(self, mode: str, pg_ctl: str) -> Optional[bool]:
        try:
            status = subprocess.call([pg_ctl, "kill", STOP_SIGNALS[mode], str(self.pid)])
        except OSError:
            return False
        if status == 0:
            return None
        else:
            return not self.is_running()

    def wait_for_user_backends_to_close(self, stop_timeout: Optional[float]) -> None:
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 18
        aux_proc_re = re.compile("(?:postgres:)( .*:)? (?:(?:archiver|startup|autovacuum launcher|autovacuum worker|"
                                 "checkpointer|logger|stats collector|wal receiver|wal writer|writer)(?: process  )?|"
                                 "walreceiver|wal sender process|walsender|walwriter|background writer|"
                                 "logical replication launcher|logical replication worker for subscription|"
                                 "logical replication tablesync worker for subscription|"
                                 "logical replication parallel apply worker for subscription|"
                                 "logical replication apply worker for subscription|"
                                 "slotsync worker|walsummarizer|io worker|bgworker:) ")

        try:
            children = self.children()
        except psutil.Error:
            return logger.debug('Failed to get list of postmaster children')

        user_backends: List[psutil.Process] = []
        user_backends_cmdlines: Dict[int, str] = {}
        for child in children:
            try:
                cmdline = child.cmdline()
                if cmdline and not aux_proc_re.match(cmdline[0]):
                    user_backends.append(child)
                    user_backends_cmdlines[child.pid] = cmdline[0]
            except psutil.NoSuchProcess:
                pass
        if user_backends:
            logger.debug('Waiting for user backends %s to close', ', '.join(user_backends_cmdlines.values()))
            _, live = psutil.wait_procs(user_backends, stop_timeout)
            if stop_timeout and live:
                live = [user_backends_cmdlines[b.pid] for b in live]
                logger.warning('Backends still alive after %s: %s', stop_timeout, ', '.join(live))
            else:
                logger.debug("Backends closed")

    @staticmethod
    def start(pgcommand: str, data_dir: str, conf: str, options: List[str]) -> Optional['PostmasterProcess']:
        # Unfortunately `pg_ctl start` does not return postmaster pid to us. Without this information
        # it is hard to know the current state of postgres startup, so we had to reimplement pg_ctl start
        # in python. It will start postgres, wait for port to be open and wait until postgres will start
        # accepting connections.
        # Important!!! We can't just start postgres using subprocess.Popen, because in this case it
        # will be our child for the rest of our live and we will have to take care of it (`waitpid`).
        # So we will use the same approach as pg_ctl uses: start a new process, which will start postgres.
        # This process will write postmaster pid to stdout and exit immediately. Now it's responsibility
        # of init process to take care about postmaster.
        # In order to make everything portable we can't use fork&exec approach here, so  we will call
        # ourselves and pass list of arguments which must be used to start postgres.
        # On Windows, in order to run a side-by-side assembly the specified env must include a valid SYSTEMROOT.
        env = {p: os.environ[p] for p in os.environ if not p.startswith(
            PATRONI_ENV_PREFIX) and not p.startswith(KUBERNETES_ENV_PREFIX)}
        try:
            proc = PostmasterProcess._from_pidfile(data_dir)
            if proc and not proc._is_postmaster_process(pgcommand, data_dir):
                # Upon start postmaster process performs various safety checks if there is a postmaster.pid
                # file in the data directory. Although Patroni already detected that the running process
                # corresponding to the postmaster.pid is not a postmaster, the new postmaster might fail
                # to start, because it thinks that postmaster.pid is already locked.
                # Important!!! Unlink of postmaster.pid isn't an option, because it has a lot of nasty race conditions.
                # Luckily there is a workaround to this problem, we can pass the pid from postmaster.pid
                # in the `PG_GRANDPARENT_PID` environment variable and postmaster will ignore it.
                logger.info("Telling pg_ctl that it is safe to ignore postmaster.pid for process %s", proc.pid)
                env['PG_GRANDPARENT_PID'] = str(proc.pid)
        except psutil.NoSuchProcess:
            pass
        cmdline = [pgcommand, '-D', data_dir, '--config-file={}'.format(conf)] + options
        logger.debug("Starting postgres: %s", " ".join(cmdline))
        ctx = multiprocessing.get_context('spawn')
        parent_conn, child_conn = ctx.Pipe(False)
        proc = ctx.Process(target=pg_ctl_start, args=(child_conn, cmdline, env))
        proc.start()
        pid = parent_conn.recv()
        proc.join()
        if pid is None:
            return
        logger.info('postmaster pid=%s', pid)

        # TODO: In an extremely unlikely case, the process could have exited and the pid reassigned. The start
        # initiation time is not accurate enough to compare to create time as start time would also likely
        # be relatively close. We need the subprocess extract pid+start_time in a race free manner.
        return PostmasterProcess.from_pid(pid)
