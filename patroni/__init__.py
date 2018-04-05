import logging
import os
import signal
import sys
import time

logger = logging.getLogger(__name__)


class Patroni(object):

    def __init__(self):
        from patroni.api import RestApiServer
        from patroni.config import Config
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.postgresql import Postgresql
        from patroni.version import __version__
        from patroni.watchdog import Watchdog

        self.setup_signal_handlers()

        self.version = __version__
        self.config = Config()
        self.dcs = get_dcs(self.config)
        self.watchdog = Watchdog(self.config)
        self.load_dynamic_configuration()

        self.postgresql = Postgresql(self.config['postgresql'])
        self.api = RestApiServer(self, self.config['restapi'])
        self.ha = Ha(self)

        self.tags = self.get_tags()
        self.next_run = time.time()
        self.scheduled_restart = {}

    def load_dynamic_configuration(self):
        from patroni.exceptions import DCSError
        while True:
            try:
                cluster = self.dcs.get_cluster()
                if cluster and cluster.config and cluster.config.data:
                    if self.config.set_dynamic_configuration(cluster.config):
                        self.dcs.reload_config(self.config)
                        self.watchdog.reload_config(self.config)
                elif not self.config.dynamic_configuration and 'bootstrap' in self.config:
                    if self.config.set_dynamic_configuration(self.config['bootstrap']['dcs']):
                        self.dcs.reload_config(self.config)
                break
            except DCSError:
                logger.warning('Can not get cluster from dcs')

    def get_tags(self):
        return {tag: value for tag, value in self.config.get('tags', {}).items()
                if tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync') or value}

    @property
    def nofailover(self):
        return bool(self.tags.get('nofailover', False))

    @property
    def nosync(self):
        return bool(self.tags.get('nosync', False))

    def reload_config(self):
        try:
            self.tags = self.get_tags()
            self.dcs.reload_config(self.config)
            self.watchdog.reload_config(self.config)
            self.api.reload_config(self.config['restapi'])
            self.postgresql.reload_config(self.config['postgresql'])
        except Exception:
            logger.exception('Failed to reload config_file=%s', self.config.config_file)

    @property
    def replicatefrom(self):
        return self.tags.get('replicatefrom')

    def sighup_handler(self, *args):
        self._received_sighup = True

    def sigterm_handler(self, *args):
        if not self._received_sigterm:
            self._received_sigterm = True
            sys.exit()

    @property
    def noloadbalance(self):
        return bool(self.tags.get('noloadbalance', False))

    def schedule_next_run(self):
        self.next_run += self.dcs.loop_wait
        current_time = time.time()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
            # Release the GIL so we don't starve anyone waiting on async_executor lock
            time.sleep(0.001)
            # Warn user that Patroni is not keeping up
            logger.warning("Loop time exceeded, rescheduling immediately.")
        elif self.ha.watch(nap_time):
            self.next_run = time.time()

    def run(self):
        self.api.start()
        self.next_run = time.time()

        while not self._received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                if self.config.reload_local_configuration():
                    self.reload_config()

            logger.info(self.ha.run_cycle())

            if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                    and self.config.set_dynamic_configuration(self.dcs.cluster.config):
                self.reload_config()

            if self.postgresql.role != 'uninitialized':
                self.config.save_cache()

            self.schedule_next_run()

    def setup_signal_handlers(self):
        self._received_sighup = False
        self._received_sigterm = False
        signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def shutdown(self):
        self.api.shutdown()
        self.ha.shutdown()


def patroni_main():
    logformat = os.environ.get('PATRONI_LOGFORMAT', '%(asctime)s %(levelname)s: %(message)s')
    loglevel = os.environ.get('PATRONI_LOGLEVEL', 'INFO')
    requests_loglevel = os.environ.get('PATRONI_REQUESTS_LOGLEVEL', 'WARNING')
    logging.basicConfig(format=logformat, level=loglevel)
    logging.getLogger('requests').setLevel(requests_loglevel)

    patroni = Patroni()
    try:
        patroni.run()
    except KeyboardInterrupt:
        pass
    finally:
        patroni.shutdown()


def pg_ctl_start(args):
    import subprocess
    postmaster = subprocess.Popen(args)
    print(postmaster.pid)


def call_self(args, **kwargs):
    """This function executes Patroni once again with provided arguments.

    :args: list of arguments to call Patroni with.
    :returns: `Popen` object"""

    exe = [sys.executable]
    if not getattr(sys, 'frozen', False):  # Binary distribution?
        exe.append(sys.argv[0])

    import subprocess
    return subprocess.Popen(exe + args, **kwargs)


def main():
    if os.getpid() != 1:
        if len(sys.argv) > 5 and sys.argv[1] == 'pg_ctl_start':
            return pg_ctl_start(sys.argv[2:])
        return patroni_main()

    pid = 0

    # Looks like we are in a docker, so we will act like init
    def sigchld_handler(signo, stack_frame):
        try:
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
                elif ret[0] != pid:
                    logging.info('Reaped pid=%s, exit status=%s', *ret)
        except OSError:
            pass

    def passtochild(signo, stack_frame):
        if pid:
            os.kill(pid, signo)

    signal.signal(signal.SIGCHLD, sigchld_handler)
    signal.signal(signal.SIGHUP, passtochild)
    signal.signal(signal.SIGINT, passtochild)
    signal.signal(signal.SIGUSR1, passtochild)
    signal.signal(signal.SIGUSR2, passtochild)
    signal.signal(signal.SIGQUIT, passtochild)
    signal.signal(signal.SIGTERM, passtochild)

    patroni = call_self(sys.argv[1:])
    pid = patroni.pid
    patroni.wait()
