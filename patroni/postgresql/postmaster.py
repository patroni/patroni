import logging
import multiprocessing
import os
import pathlib
import re
import signal
import subprocess
import sys

from multiprocessing.connection import Connection
from typing import Callable, Dict, List, Optional

import psutil

from patroni import KUBERNETES_ENV_PREFIX, PATRONI_ENV_PREFIX
from patroni.daemon import SOCKET_ENV_VARIABLE

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
    """Wait for Patroni's permission to start PostgreSQL, then send it the new postmaster's PID.

    Once Patroni confirms it is tracking the postmaster, the helper exits and leaves PostgreSQL running.
    If Patroni disconnects before confirming, the helper kills the postmaster rather than leave it unmanaged.

    :param conn: the helper's end of the pipe used to receive permission, send the PID, and receive acknowledgment.
    :param cmdline: the PostgreSQL executable and arguments to launch.
    :param env: environment variables for the new postmaster.
    """
    if os.name != 'nt':
        os.setsid()
    parent_pid = os.getppid()
    postmaster = None
    try:
        if conn.recv() is not True:
            return
        try:
            postmaster = subprocess.Popen(cmdline, close_fds=True, env=env)
        except OSError:
            logger.exception('Failed to execute %s', cmdline)
            return
        conn.send(postmaster.pid)
        # Another child can inherit Patroni's end of the pipe and keep it open after Patroni exits.
        # On POSIX the helper is then reparented, so a changed parent PID tells us to stop waiting for EOF or a reply.
        while not conn.poll(0.1) and os.getppid() == parent_pid:
            if postmaster.poll() is not None:
                postmaster = None
                return
        # Honor an acknowledgment that Patroni sent just before it exited.
        if conn.poll() and conn.recv() is True:
            postmaster = None
    except (EOFError, OSError):
        pass
    except Exception:
        logger.exception('Failed to execute %s', cmdline)
    finally:
        conn.close()
        if postmaster is not None:
            # No acknowledgment: the helper is still responsible for the postmaster.
            postmaster.kill()


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

    def wait_for_user_backends_to_close(self, stop_timeout: Optional[float]) -> Optional[bool]:
        """Wait for all user backend processes to close before the postmaster shuts down.

        Auxiliary/background PostgreSQL processes (archiver, checkpointer, wal senders, logical
        replication workers, and so on) are identified via their command line and excluded, so that
        only genuine client backends are awaited.

        :param stop_timeout: maximum number of seconds to wait for the user backends to close.
            If ``None``, wait indefinitely.

        :returns: ``True`` if there were no user backends or all of them closed in time; ``None`` if
            the list of children could not be retrieved or some backends were still alive after
            *stop_timeout*.
        """
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 19
        aux_proc_re = re.compile("(?:postgres:)( .*:)? (?:(?:archiver|startup|autovacuum launcher|autovacuum worker|"
                                 "checkpointer|logger|stats collector|wal receiver|wal writer|writer)(?: process  )?|"
                                 "syslogger|walreceiver|wal sender process|walsender|walwriter|background writer|"
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
                return True
        else:
            return True

    @staticmethod
    def alive(process: psutil.Process) -> bool:
        """Check whether a previously identified process may still be alive.

        :param process: the launcher or postmaster being tracked.
        :returns: ``False`` if the process is gone, dead, or a zombie; ``True`` if alive or access is denied.
        """
        try:
            return process.is_running() and str(process.status()) not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return True

    @staticmethod
    def start(pgcommand: str, data_dir: str, conf: str, options: List[str],
              publish: Callable[[psutil.Process], bool] = lambda process: True) -> Optional['PostmasterProcess']:
        """Start PostgreSQL through a short-lived helper so Patroni need not reap the postmaster.

        The helper waits for permission before starting PostgreSQL, then reports the PID and waits for acknowledgment.
        Acknowledge a published postmaster even if shutdown cancelled startup: the parent now owns its PID.

        :param pgcommand: path or name of the PostgreSQL server executable.
        :param data_dir: data directory to pass to PostgreSQL with ``-D``.
        :param conf: path to the PostgreSQL configuration file.
        :param options: additional server command-line arguments.
        :param publish: callback invoked first with the launcher, then with the verified postmaster. Returning
                ``False`` for the launcher prevents startup; for the postmaster it cancels startup but
                still acknowledges that Patroni is tracking the process.
        :returns: the verified postmaster, or ``None`` if startup was refused or the helper did not hand it over.
        """
        # On Windows, in order to run a side-by-side assembly the specified env must include a valid SYSTEMROOT.
        # We also remove NOTIFY_SOCKET environment variable so that PostgreSQL doesn't send READY=1 or STOPPING=1
        # to systemd, because it is exclusively responsibility of Patroni to do it.
        env = {p: os.environ[p] for p in os.environ if p != SOCKET_ENV_VARIABLE
               and not p.startswith(PATRONI_ENV_PREFIX) and not p.startswith(KUBERNETES_ENV_PREFIX)}
        if 'PG_MALLOC_ARENA_MAX' in env:
            env['MALLOC_ARENA_MAX'] = env.pop('PG_MALLOC_ARENA_MAX')
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
        parent_conn, child_conn = ctx.Pipe()
        with parent_conn, child_conn:
            launcher = ctx.Process(target=pg_ctl_start, args=(child_conn, cmdline, env))
            launcher.start()
            # Leaving our copy open would prevent recv() from seeing EOF if the helper dies before sending its PID.
            child_conn.close()
            try:
                helper = psutil.Process(launcher.pid)
                if helper.ppid() != os.getpid() or not publish(helper):
                    parent_conn.send(False)
                    return None
                parent_conn.send(True)
                pid = parent_conn.recv()
                postmaster = PostmasterProcess.from_pid(pid)
                # Check parentage before acknowledging the PID. Once the helper exits, the postmaster is reparented
                # and that check is no longer possible.
                if not postmaster or postmaster.ppid() != helper.pid:
                    return None
                accepted = publish(postmaster)
                parent_conn.send(True)
                if not accepted:
                    return None
                logger.info('postmaster pid=%s', pid)
                return postmaster
            except (EOFError, OSError, psutil.Error):
                return None
            finally:
                parent_conn.close()
                launcher.join()
