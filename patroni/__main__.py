import logging
import os
import signal
import sys
import time

from argparse import Namespace
from typing import Any, Dict, Optional, TYPE_CHECKING

from patroni.daemon import AbstractPatroniDaemon, abstract_main, get_base_arg_parser

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config

logger = logging.getLogger(__name__)


class Patroni(AbstractPatroniDaemon):

    def __init__(self, config: 'Config') -> None:
        from patroni.api import RestApiServer
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.postgresql import Postgresql
        from patroni.request import PatroniRequest
        from patroni.version import __version__
        from patroni.watchdog import Watchdog

        super(Patroni, self).__init__(config)

        self.version = __version__
        self.dcs = get_dcs(self.config)
        self.request = PatroniRequest(self.config, True)

        self.ensure_unique_name()

        self.watchdog = Watchdog(self.config)
        self.load_dynamic_configuration()

        self.postgresql = Postgresql(self.config['postgresql'])
        self.api = RestApiServer(self, self.config['restapi'])
        self.ha = Ha(self)

        self.tags = self.get_tags()
        self.next_run = time.time()
        self.scheduled_restart: Dict[str, Any] = {}

    def load_dynamic_configuration(self) -> None:
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
                        self.watchdog.reload_config(self.config)
                break
            except DCSError:
                logger.warning('Can not get cluster from dcs')
                time.sleep(5)

    def ensure_unique_name(self) -> None:
        """A helper method to prevent splitbrain from operator naming error."""
        from patroni.dcs import Member

        cluster = self.dcs.get_cluster()
        if not cluster:
            return
        member = cluster.get_member(self.config['name'], False)
        if not isinstance(member, Member):
            return
        try:
            _ = self.request(member, endpoint="/liveness")
            logger.fatal("Can't start; there is already a node named '%s' running", self.config['name'])
            sys.exit(1)
        except Exception:
            return

    def get_tags(self) -> Dict[str, Any]:
        return {tag: value for tag, value in self.config.get('tags', {}).items()
                if tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync') or value}

    @property
    def nofailover(self) -> bool:
        return bool(self.tags.get('nofailover', False))

    @property
    def nosync(self) -> bool:
        return bool(self.tags.get('nosync', False))

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        try:
            super(Patroni, self).reload_config(sighup, local)
            if local:
                self.tags = self.get_tags()
                self.request.reload_config(self.config)
            if local or sighup and self.api.reload_local_certificate():
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

    def schedule_next_run(self) -> None:
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

    def run(self) -> None:
        self.api.start()
        self.next_run = time.time()
        super(Patroni, self).run()

    def _run_cycle(self) -> None:
        logger.info(self.ha.run_cycle())

        if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                and self.config.set_dynamic_configuration(self.dcs.cluster.config):
            self.reload_config()

        if self.postgresql.role != 'uninitialized':
            self.config.save_cache()

        self.schedule_next_run()

    def _shutdown(self) -> None:
        try:
            self.api.shutdown()
        except Exception:
            logger.exception('Exception during RestApi.shutdown')
        try:
            self.ha.shutdown()
        except Exception:
            logger.exception('Exception during Ha.shutdown')


def patroni_main(configfile: str) -> None:
    from multiprocessing import freeze_support

    freeze_support()
    abstract_main(Patroni, configfile)


def process_arguments() -> Namespace:
    parser = get_base_arg_parser()
    parser.add_argument('--validate-config', action='store_true', help='Run config validator and exit')
    args = parser.parse_args()

    if args.validate_config:
        from patroni.validator import schema
        from patroni.config import Config, ConfigParseError

        try:
            Config(args.configfile, validator=schema)
            sys.exit()
        except ConfigParseError as e:
            sys.exit(e.value)

    return args


def main() -> None:
    from patroni import check_psycopg

    args = process_arguments()

    check_psycopg()

    if os.getpid() != 1:
        return patroni_main(args.configfile)

    # Patroni started with PID=1, it looks like we are in the container
    from types import FrameType
    pid = 0

    # Looks like we are in a docker, so we will act like init
    def sigchld_handler(signo: int, stack_frame: Optional[FrameType]) -> None:
        try:
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
                elif ret[0] != pid:
                    logger.info('Reaped pid=%s, exit status=%s', *ret)
        except OSError:
            pass

    def passtochild(signo: int, stack_frame: Optional[FrameType]):
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
    patroni = multiprocessing.Process(target=patroni_main, args=(args.configfile,))
    patroni.start()
    pid = patroni.pid
    patroni.join()


if __name__ == '__main__':
    main()
