import logging
import os
import signal
import sys
import time

from patroni.version import __version__

logger = logging.getLogger(__name__)

PATRONI_ENV_PREFIX = 'PATRONI_'


class Patroni(object):

    def __init__(self, conf):
        from patroni.api import RestApiServer
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.log import PatroniLogger
        from patroni.postgresql import Postgresql
        from patroni.request import PatroniRequest
        from patroni.watchdog import Watchdog

        self.setup_signal_handlers()

        self.version = __version__
        self.logger = PatroniLogger()
        self.config = conf
        self.logger.reload_config(self.config.get('log', {}))
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

    def reload_config(self, sighup=False):
        try:
            self.tags = self.get_tags()
            self.logger.reload_config(self.config.get('log', {}))
            self.watchdog.reload_config(self.config)
            if sighup:
                self.request.reload_config(self.config)
            self.api.reload_config(self.config['restapi'])
            self.postgresql.reload_config(self.config['postgresql'], sighup)
            self.dcs.reload_config(self.config)
        except Exception:
            logger.exception('Failed to reload config_file=%s', self.config.config_file)

    @property
    def replicatefrom(self):
        return self.tags.get('replicatefrom')

    def sighup_handler(self, *args):
        self._received_sighup = True

    def sigterm_handler(self, *args):
        with self._sigterm_lock:
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

    @property
    def received_sigterm(self):
        with self._sigterm_lock:
            return self._received_sigterm

    def run(self):
        self.api.start()
        self.logger.start()
        self.next_run = time.time()

        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                if self.config.reload_local_configuration():
                    self.reload_config(True)
                else:
                    self.postgresql.config.reload_config(self.config['postgresql'], True)

            logger.info(self.ha.run_cycle())

            if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                    and self.config.set_dynamic_configuration(self.dcs.cluster.config):
                self.reload_config()

            if self.postgresql.role != 'uninitialized':
                self.config.save_cache()

            self.schedule_next_run()

    def setup_signal_handlers(self):
        from threading import Lock

        self._received_sighup = False
        self._sigterm_lock = Lock()
        self._received_sigterm = False
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def shutdown(self):
        with self._sigterm_lock:
            self._received_sigterm = True
        try:
            self.api.shutdown()
        except Exception:
            logger.exception('Exception during RestApi.shutdown')
        self.ha.shutdown()
        self.logger.shutdown()


def patroni_main():
    import argparse
    from patroni.config import Config, ConfigParseError

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))
    parser.add_argument('configfile', nargs='?', default='',
                        help='Patroni may also read the configuration from the {0} environment variable'
                        .format(Config.PATRONI_CONFIG_VARIABLE))
    args = parser.parse_args()
    try:
        conf = Config(args.configfile)
    except ConfigParseError as e:
        if e.value:
            print(e.value)
        parser.print_help()
        sys.exit(1)
    patroni = Patroni(conf)
    try:
        patroni.run()
    except KeyboardInterrupt:
        pass
    finally:
        patroni.shutdown()


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
