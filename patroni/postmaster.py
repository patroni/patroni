import logging
import os
import psutil
import re
import signal
import subprocess

from patroni import call_self

logger = logging.getLogger(__name__)

STOP_SIGNALS = {
    'smart': signal.SIGTERM,
    'fast': signal.SIGINT,
    'immediate': signal.SIGQUIT,
}


class PostmasterProcess(psutil.Process):
    def __init__(self, pid):
        self.is_single_user = False
        if pid < 0:
            pid = -pid
            self.is_single_user = True
        super(PostmasterProcess, self).__init__(pid)

    @classmethod
    def from_pidfile(cls, pidfile):
        try:
            pid = int(pidfile.get('pid', 0))
            if not pid:
                return None
        except ValueError:
            return None

        try:
            proc = cls(pid)
        except psutil.NoSuchProcess:
            return None

        try:
            start_time = int(pidfile.get('start_time', 0))
            if start_time and abs(proc.create_time() - start_time) > 3:
                return None
        except ValueError:
            logger.warning("Garbage start time value in pid file: %r", pidfile.get('start_time'))

        # Extra safety check. The process can't be ourselves, our parent or our direct child.
        if proc.pid == os.getpid() or proc.pid == os.getppid() or proc.parent() == os.getpid():
            return None

        return proc

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
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 9.6
        aux_proc_re = re.compile("(?:postgres:)( .*:)? (?:""(?:startup|logger|checkpointer|writer|wal writer|"
                                 "autovacuum launcher|autovacuum worker|stats collector|wal receiver|archiver|"
                                 "wal sender) process|bgworker: )")

        try:
            user_backends = []
            user_backends_cmdlines = []
            for child in self.children():
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
        except psutil.Error:
            logger.exception('wait_for_user_backends_to_close')

    @classmethod
    def start(cls, pgcommand, data_dir, conf, options):
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
        proc = call_self(['pg_ctl_start', pgcommand, '-D', data_dir,
                          '--config-file={}'.format(conf)] + options, close_fds=True,
                         preexec_fn=os.setsid, stdout=subprocess.PIPE,
                         env={p: os.environ[p] for p in ('PATH', 'LC_ALL', 'LANG') if p in os.environ})
        pid = int(proc.stdout.readline().strip())
        proc.wait()
        logger.info('postmaster pid=%s', pid)

        # TODO: In an extremely unlikely case, the process could have exited and the pid reassigned. The start
        # initiation time is not accurate enough to compare to create time as start time would also likely
        # be relatively close. We need the subprocess extract pid+start_time in a race free manner.
        return PostmasterProcess.from_pid(pid)
