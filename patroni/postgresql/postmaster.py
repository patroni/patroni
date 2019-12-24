import logging
import multiprocessing
import os
import psutil
import re
import signal
import subprocess
import sys

from patroni import PATRONI_ENV_PREFIX

# avoid spawning the resource tracker process
if sys.version_info >= (3, 8):  # pragma: no cover
    import multiprocessing.resource_tracker
    multiprocessing.resource_tracker.getfd = lambda: 0
elif sys.version_info >= (3, 4):  # pragma: no cover
    import multiprocessing.semaphore_tracker
    multiprocessing.semaphore_tracker.getfd = lambda: 0

logger = logging.getLogger(__name__)

STOP_SIGNALS = {
    'smart': signal.SIGTERM,
    'fast': signal.SIGINT,
    'immediate': signal.SIGQUIT if os.name != 'nt' else signal.SIGABRT,
}


def pg_ctl_start(conn, cmdline, env):
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

    def __init__(self, pid):
        self.is_single_user = False
        if pid < 0:
            pid = -pid
            self.is_single_user = True
        super(PostmasterProcess, self).__init__(pid)

    @staticmethod
    def _read_postmaster_pidfile(data_dir):
        """Reads and parses postmaster.pid from the data directory

        :returns dictionary of values if successful, empty dictionary otherwise
        """
        pid_line_names = ['pid', 'data_dir', 'start_time', 'port', 'socket_dir', 'listen_addr', 'shmem_key']
        try:
            with open(os.path.join(data_dir, 'postmaster.pid')) as f:
                return {name: line.rstrip('\n') for name, line in zip(pid_line_names, f)}
        except IOError:
            return {}

    def _is_postmaster_process(self):
        try:
            start_time = int(self._postmaster_pid.get('start_time', 0))
            if start_time and abs(self.create_time() - start_time) > 3:
                logger.info('Process %s is not postmaster, too much difference between PID file start time %s and '
                            'process start time %s', self.pid, self.create_time(), start_time)
                return False
        except ValueError:
            logger.warning('Garbage start time value in pid file: %r', self._postmaster_pid.get('start_time'))

        # Extra safety check. The process can't be ourselves, our parent or our direct child.
        if self.pid == os.getpid() or self.pid == os.getppid() or self.ppid() == os.getpid():
            logger.info('Patroni (pid=%s, ppid=%s), "fake postmaster" (pid=%s, ppid=%s)',
                        os.getpid(), os.getppid(), self.pid, self.ppid())
            return False

        return True

    @classmethod
    def _from_pidfile(cls, data_dir):
        postmaster_pid = PostmasterProcess._read_postmaster_pidfile(data_dir)
        try:
            pid = int(postmaster_pid.get('pid', 0))
            if pid:
                proc = cls(pid)
                proc._postmaster_pid = postmaster_pid
                return proc
        except ValueError:
            pass

    @staticmethod
    def from_pidfile(data_dir):
        try:
            proc = PostmasterProcess._from_pidfile(data_dir)
            return proc if proc and proc._is_postmaster_process() else None
        except psutil.NoSuchProcess:
            return None

    @classmethod
    def from_pid(cls, pid):
        try:
            return cls(pid)
        except psutil.NoSuchProcess:
            return None

    def signal_stop(self, mode):
        """Signal postmaster process to stop

        :returns None if signaled, True if process is already gone, False if error
        """
        if self.is_single_user:
            logger.warning("Cannot stop server; single-user server is running (PID: {0})".format(self.pid))
            return False
        try:
            self.send_signal(STOP_SIGNALS[mode])
        except psutil.NoSuchProcess:
            return True
        except psutil.AccessDenied as e:
            logger.warning("Could not send stop signal to PostgreSQL (error: {0})".format(e))
            return False

        return None

    def wait_for_user_backends_to_close(self):
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 11
        aux_proc_re = re.compile("(?:postgres:)( .*:)? (?:(?:archiver|startup|autovacuum launcher|autovacuum worker|"
                                 "checkpointer|logger|stats collector|wal receiver|wal writer|writer)(?: process  )?|"
                                 "walreceiver|wal sender process|walsender|walwriter|background writer|"
                                 "logical replication launcher|logical replication worker for|bgworker:) ")

        try:
            children = self.children()
        except psutil.Error:
            return logger.debug('Failed to get list of postmaster children')

        user_backends = []
        user_backends_cmdlines = []
        for child in children:
            try:
                cmdline = child.cmdline()[0]
                if not aux_proc_re.match(cmdline):
                    user_backends.append(child)
                    user_backends_cmdlines.append(cmdline)
            except psutil.NoSuchProcess:
                pass
        if user_backends:
            logger.debug('Waiting for user backends %s to close', ', '.join(user_backends_cmdlines))
            psutil.wait_procs(user_backends)
        logger.debug("Backends closed")

    @staticmethod
    def start(pgcommand, data_dir, conf, options):
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
        env = {p: os.environ[p] for p in os.environ if not p.startswith(PATRONI_ENV_PREFIX)}
        try:
            proc = PostmasterProcess._from_pidfile(data_dir)
            if proc and not proc._is_postmaster_process():
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
        ctx = multiprocessing.get_context('spawn') if sys.version_info >= (3, 4) else multiprocessing
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
