import logging
import os
import signal
import sys
import time

from .daemon import AbstractPatroniDaemon, abstract_main
from .version import __version__

logger = logging.getLogger(__name__)

PATRONI_ENV_PREFIX = 'PATRONI_'
KUBERNETES_ENV_PREFIX = 'KUBERNETES_'


class Patroni(AbstractPatroniDaemon):

    def __init__(self, config):
        from patroni.api import RestApiServer
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.postgresql import Postgresql
        from patroni.request import PatroniRequest
        from patroni.watchdog import Watchdog

        super(Patroni, self).__init__(config)

        self.version = __version__
        self.dcs = get_dcs(self.config)
        self.watchdog = Watchdog(self.config)
        self.load_dynamic_configuration()

        self.postgresql = Postgresql(self.config['postgresql'])
        self.api = RestApiServer(self, self.config['restapi'])
        self.request = PatroniRequest(self.config, True)
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
                time.sleep(5)

    def get_tags(self):
        return {tag: value for tag, value in self.config.get('tags', {}).items()
                if tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync') or value}

    @property
    def nofailover(self):
        return bool(self.tags.get('nofailover', False))

    @property
    def nosync(self):
        return bool(self.tags.get('nosync', False))

    def reload_config(self, sighup=False, local=False):
        try:
            super(Patroni, self).reload_config(sighup, local)
            if local:
                self.tags = self.get_tags()
                self.request.reload_config(self.config)
            if local or self.api.reload_local_certificate():
                self.api.reload_config(self.config['restapi'])
            self.watchdog.reload_config(self.config)
            self.postgresql.reload_config(self.config['postgresql'], sighup)
            self.dcs.reload_config(self.config)
        except Exception:
            logger.exception('Failed to reload config_file=%s', self.config.config_file)

    @property
    def replicatefrom(self):
        return self.tags.get('replicatefrom')

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
        super(Patroni, self).run()

    def _run_cycle(self):
        logger.info(self.ha.run_cycle())

        if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                and self.config.set_dynamic_configuration(self.dcs.cluster.config):
            self.reload_config()

        if self.postgresql.role != 'uninitialized':
            self.config.save_cache()

        self.schedule_next_run()

    def _shutdown(self):
        try:
            self.api.shutdown()
        except Exception:
            logger.exception('Exception during RestApi.shutdown')
        try:
            self.ha.shutdown()
        except Exception:
            logger.exception('Exception during Ha.shutdown')


def patroni_main():
    from multiprocessing import freeze_support
    from patroni.validator import schema

    freeze_support()
    abstract_main(Patroni, schema)


def fatal(string, *args):
    sys.stderr.write('FATAL: ' + string.format(*args) + '\n')
    sys.exit(1)


def check_psycopg2():
    min_psycopg2 = (2, 5, 4)
    min_psycopg2_str = '.'.join(map(str, min_psycopg2))

    def parse_version(version):
        for e in version.split('.'):
            try:
                yield int(e)
            except ValueError:
                break

    try:
        import psycopg2
        version_str = psycopg2.__version__.split(' ')[0]
        version = tuple(parse_version(version_str))
        if version < min_psycopg2:
            fatal('Patroni requires psycopg2>={0}, but only {1} is available', min_psycopg2_str, version_str)
    except ImportError:
        fatal('Patroni requires psycopg2>={0} or psycopg2-binary', min_psycopg2_str)


def main():
    if os.getpid() != 1:
        check_psycopg2()
        return patroni_main()

    # Patroni started with PID=1, it looks like we are in the container
    pid = 0

    # Looks like we are in a docker, so we will act like init
    def sigchld_handler(signo, stack_frame):
        try:
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
                elif ret[0] != pid:
                    logger.info('Reaped pid=%s, exit status=%s', *ret)
        except OSError:
            pass

    def passtochild(signo, stack_frame):
        if pid:
            os.kill(pid, signo)

    if os.name != 'nt':
        signal.signal(signal.SIGCHLD, sigchld_handler)
        signal.signal(signal.SIGHUP, passtochild)
        signal.signal(signal.SIGQUIT, passtochild)
        signal.signal(signal.SIGUSR1, passtochild)
        signal.signal(signal.SIGUSR2, passtochild)
    signal.signal(signal.SIGINT, passtochild)
    signal.signal(signal.SIGABRT, passtochild)
    signal.signal(signal.SIGTERM, passtochild)

    import multiprocessing
    patroni = multiprocessing.Process(target=patroni_main)
    patroni.start()
    pid = patroni.pid
    patroni.join()
