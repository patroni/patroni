"""Patroni main entry point.

Implement ``patroni`` main daemon and expose its entry point.
"""

import logging
import os
import signal
import sys
import time

from argparse import Namespace
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from patroni import global_config, MIN_PSYCOPG2, MIN_PSYCOPG3, parse_version
from patroni.daemon import abstract_main, AbstractPatroniDaemon, get_base_arg_parser
from patroni.tags import Tags

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config
    from .dcs import Cluster

logger = logging.getLogger(__name__)


class Patroni(AbstractPatroniDaemon, Tags):
    """Implement ``patroni`` command daemon.

    :ivar version: Patroni version.
    :ivar dcs: DCS object.
    :ivar watchdog: watchdog handler, if configured to use watchdog.
    :ivar postgresql: managed Postgres instance.
    :ivar api: REST API server instance of this node.
    :ivar request: wrapper for performing HTTP requests.
    :ivar ha: HA handler.
    :ivar next_run: time when to run the next HA loop cycle.
    :ivar scheduled_restart: when a restart has been scheduled to occur, if any. In that case, should contain two keys:
        * ``schedule``: timestamp when restart should occur;
        * ``postmaster_start_time``: timestamp when Postgres was last started.
    """

    def __init__(self, config: 'Config') -> None:
        """Create a :class:`Patroni` instance with the given *config*.

        Get a connection to the DCS, configure watchdog (if required), set up Patroni interface with Postgres, configure
        the HA loop and bring the REST API up.

        .. note::
            Expected to be instantiated and run through :func:`~patroni.daemon.abstract_main`.

        :param config: Patroni configuration.
        """
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

        cluster = self.ensure_dcs_access()
        self.ensure_unique_name(cluster)

        self.watchdog = Watchdog(self.config)
        self.apply_dynamic_configuration(cluster)

        # Initialize global config
        global_config.update(None, self.config.dynamic_configuration)

        self.postgresql = Postgresql(self.config['postgresql'], self.dcs.mpp)
        self.api = RestApiServer(self, self.config['restapi'])
        self.ha = Ha(self)

        self._tags = self._get_tags()
        self.next_run = time.time()
        self.scheduled_restart: Dict[str, Any] = {}

    def ensure_dcs_access(self, sleep_time: int = 5) -> 'Cluster':
        """Continuously attempt to retrieve cluster from DCS with delay.

        :param sleep_time: seconds to wait between retry attempts after dcs connection raise :exc:`DCSError`.

        :returns: a PostgreSQL or MPP implementation of :class:`Cluster`.
        """
        from patroni.exceptions import DCSError

        while True:
            try:
                return self.dcs.get_cluster()
            except DCSError:
                logger.warning('Can not get cluster from dcs')
                time.sleep(sleep_time)

    def apply_dynamic_configuration(self, cluster: 'Cluster') -> None:
        """Apply Patroni dynamic configuration.

        Apply dynamic configuration from the DCS, if `/config` key is available in the DCS, otherwise fall back to
        ``bootstrap.dcs`` section from the configuration file.

        .. note::
            This method is called only once, at the time when Patroni is started.

        :param cluster: a PostgreSQL or MPP implementation of :class:`Cluster`.
        """
        if cluster and cluster.config and cluster.config.data:
            if self.config.set_dynamic_configuration(cluster.config):
                self.dcs.reload_config(self.config)
                self.watchdog.reload_config(self.config)
        elif not self.config.dynamic_configuration and 'bootstrap' in self.config:
            if self.config.set_dynamic_configuration(self.config['bootstrap']['dcs']):
                self.dcs.reload_config(self.config)
                self.watchdog.reload_config(self.config)

    def ensure_unique_name(self, cluster: 'Cluster') -> None:
        """A helper method to prevent splitbrain from operator naming error.

        :param cluster: a PostgreSQL or MPP implementation of :class:`Cluster`.
        """
        from patroni.dcs import Member

        if not cluster:
            return
        member = cluster.get_member(self.config['name'], False)
        if not isinstance(member, Member):
            return
        try:
            # Silence annoying WARNING: Retrying (...) messages when Patroni is quickly restarted.
            # At this moment we don't have custom log levels configured and hence shouldn't lose anything useful.
            self.logger.update_loggers({'urllib3.connectionpool': 'ERROR'})
            _ = self.request(member, endpoint="/liveness", timeout=3)
            logger.fatal("Can't start; there is already a node named '%s' running", self.config['name'])
            sys.exit(1)
        except Exception:
            self.logger.update_loggers({})

    def _get_tags(self) -> Dict[str, Any]:
        """Get tags configured for this node, if any.

        :returns: a dictionary of tags set for this node.
        """
        return self._filter_tags(self.config.get('tags', {}))

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        """Apply new configuration values for ``patroni`` daemon.

        Reload:
            * Cached tags;
            * Request wrapper configuration;
            * REST API configuration;
            * Watchdog configuration;
            * Postgres configuration;
            * DCS configuration.

        :param sighup: if it is related to a SIGHUP signal.
        :param local: if there has been changes to the local configuration file.
        """
        try:
            super(Patroni, self).reload_config(sighup, local)
            if local:
                self._tags = self._get_tags()
                self.request.reload_config(self.config)
            if local or sighup and self.api.reload_local_certificate():
                self.api.reload_config(self.config['restapi'])
            self.watchdog.reload_config(self.config)
            self.postgresql.reload_config(self.config['postgresql'], sighup)
            self.dcs.reload_config(self.config)
        except Exception:
            logger.exception('Failed to reload config_file=%s', self.config.config_file)

    @property
    def tags(self) -> Dict[str, Any]:
        """Tags configured for this node, if any."""
        return self._tags

    def schedule_next_run(self) -> None:
        """Schedule the next run of the ``patroni`` daemon main loop.

        Next run is scheduled based on previous run plus value of ``loop_wait`` configuration from DCS. If that has
        already been exceeded, run the next cycle immediately.
        """
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
        """Run ``patroni`` daemon process main loop.

        Start the REST API and keep running HA cycles every ``loop_wait`` seconds.
        """
        self.api.start()
        self.next_run = time.time()
        super(Patroni, self).run()

    def _run_cycle(self) -> None:
        """Run a cycle of the ``patroni`` daemon main loop.

        Run an HA cycle and schedule the next cycle run. If any dynamic configuration change request is detected, apply
        the change and cache the new dynamic configuration values in ``patroni.dynamic.json`` file under Postgres data
        directory.
        """
        from patroni.postgresql.misc import PostgresqlRole

        logger.info(self.ha.run_cycle())

        if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                and self.config.set_dynamic_configuration(self.dcs.cluster.config):
            self.reload_config()

        if self.postgresql.role != PostgresqlRole.UNINITIALIZED:
            self.config.save_cache()

        self.schedule_next_run()

    def _shutdown(self) -> None:
        """Perform shutdown of ``patroni`` daemon process.

        Shut down the REST API and the HA handler.
        """
        try:
            self.api.shutdown()
        except Exception:
            logger.exception('Exception during RestApi.shutdown')
        try:
            self.ha.shutdown()
        except Exception:
            logger.exception('Exception during Ha.shutdown')


def patroni_main(configfile: str) -> None:
    """Configure and start ``patroni`` main daemon process.

    :param configfile: path to Patroni configuration file.
    """
    abstract_main(Patroni, configfile)


def process_arguments() -> Namespace:
    """Process command-line arguments.

    Create a basic command-line parser through :func:`~patroni.daemon.get_base_arg_parser`, extend its capabilities by
    adding these flags and parse command-line arguments.:

      * ``--validate-config`` -- used to validate the Patroni configuration file
      * ``--generate-config`` -- used to generate Patroni configuration from a running PostgreSQL instance
      * ``--generate-sample-config`` -- used to generate a sample Patroni configuration
      * ``--ignore-listen-port`` | ``-i`` -- used to ignore ``listen`` ports already in use.
          Can be used only with ``--validate-config``
      * ``--print`` | ``-p`` -- used to print out local configuration (incl. environment configuration overrides).
          Can be used only with ``--validate-config``

    .. note::
        If running with ``--generate-config``, ``--generate-sample-config`` or ``--validate-flag`` will exit
        after generating or validating configuration.

    :returns: parsed arguments, if not running with ``--validate-config`` flag.
    """
    from patroni.config_generator import generate_config

    parser = get_base_arg_parser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--validate-config', action='store_true', help='Run config validator and exit')
    group.add_argument('--generate-sample-config', action='store_true',
                       help='Generate a sample Patroni yaml configuration file')
    group.add_argument('--generate-config', action='store_true',
                       help='Generate a Patroni yaml configuration file for a running instance')
    parser.add_argument('--dsn', help='Optional DSN string of the instance to be used as a source \
                                    for config generation. Superuser connection is required.')
    parser.add_argument('--ignore-listen-port', '-i', action='store_true',
                        help='Ignore `listen` ports already in use.\
                              Can only be used with --validate-config')
    parser.add_argument('--print', '-p', action='store_true',
                        help='Print out local configuration (incl. environment configuration overrides).\
                              Can only be used with --validate-config')
    args = parser.parse_args()

    if args.generate_sample_config:
        generate_config(args.configfile, True, None)
        sys.exit(0)
    elif args.generate_config:
        generate_config(args.configfile, False, args.dsn)
        sys.exit(0)
    elif args.validate_config:
        from patroni.config import Config, ConfigParseError
        from patroni.validator import populate_validate_params, schema

        populate_validate_params(ignore_listen_port=args.ignore_listen_port)

        try:
            config = Config(args.configfile, validator=schema)
        except ConfigParseError as e:
            sys.exit(e.value)

        if args.print:
            import yaml
            yaml.safe_dump(config.local_configuration, sys.stdout, default_flow_style=False, allow_unicode=True)
        sys.exit()

    return args


def check_psycopg() -> None:
    """Ensure at least one among :mod:`psycopg2` or :mod:`psycopg` libraries are available in the environment.

    .. note::
        Patroni chooses :mod:`psycopg2` over :mod:`psycopg`, if possible.

        If nothing meeting the requirements is found, then exit with a fatal message.
    """
    min_psycopg2_str = '.'.join(map(str, MIN_PSYCOPG2))
    min_psycopg3_str = '.'.join(map(str, MIN_PSYCOPG3))

    available_versions: List[str] = []

    # try psycopg2
    try:
        from psycopg2 import __version__
        if parse_version(__version__) >= MIN_PSYCOPG2:
            return
        available_versions.append('psycopg2=={0}'.format(__version__.split(' ')[0]))
    except ImportError:
        logger.debug('psycopg2 module is not available')

    # try psycopg3
    try:
        from psycopg import __version__
        if parse_version(__version__) >= MIN_PSYCOPG3:
            return
        available_versions.append('psycopg=={0}'.format(__version__.split(' ')[0]))
    except ImportError:
        logger.debug('psycopg module is not available')

    error = f'FATAL: Patroni requires psycopg2>={min_psycopg2_str}, psycopg2-binary, or psycopg>={min_psycopg3_str}'
    if available_versions:
        error += ', but only {0} {1} available'.format(
            ' and '.join(available_versions),
            'is' if len(available_versions) == 1 else 'are')
    sys.exit(error)


def main() -> None:
    """Main entrypoint of :mod:`patroni.__main__`.

    Process command-line arguments, ensure :mod:`psycopg2` (or :mod:`psycopg`) attendee the pre-requisites and start
    ``patroni`` daemon process.

    .. note::
        If running through a Docker container, make the main process take care of init process duties and run
        ``patroni`` daemon as another process. In that case relevant signals received by the main process and forwarded
        to ``patroni`` daemon process.
    """
    from multiprocessing import freeze_support

    # Executables created by PyInstaller are frozen, thus we need to enable frozen support for
    # :mod:`multiprocessing` to avoid :class:`RuntimeError` exceptions.
    freeze_support()

    check_psycopg()

    args = process_arguments()

    if os.getpid() != 1:
        return patroni_main(args.configfile)

    # Patroni started with PID=1, it looks like we are in the container
    from types import FrameType
    pid = 0

    # Looks like we are in a docker, so we will act like init
    def sigchld_handler(signo: int, stack_frame: Optional[FrameType]) -> None:
        """Handle ``SIGCHLD`` received by main process from ``patroni`` daemon when the daemon terminates.

        :param signo: signal number.
        :param stack_frame: current stack frame.
        """
        try:
            # log exit code of all children processes, and break loop when there is none left
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
                elif ret[0] != pid:
                    logger.info('Reaped pid=%s, exit status=%s', *ret)
        except OSError:
            pass

    def passtochild(signo: int, stack_frame: Optional[FrameType]) -> None:
        """Forward a signal *signo* from main process to child process.

        :param signo: signal number.
        :param stack_frame: current stack frame.
        """
        if pid:
            os.kill(pid, signo)

    import multiprocessing
    patroni = multiprocessing.Process(target=patroni_main, args=(args.configfile,))
    patroni.start()
    pid = patroni.pid

    # Set up signal handlers after fork to prevent child from inheriting them
    if os.name != 'nt':
        signal.signal(signal.SIGCHLD, sigchld_handler)
        signal.signal(signal.SIGHUP, passtochild)
        signal.signal(signal.SIGQUIT, passtochild)
        signal.signal(signal.SIGUSR1, passtochild)
        signal.signal(signal.SIGUSR2, passtochild)
    signal.signal(signal.SIGINT, passtochild)
    signal.signal(signal.SIGABRT, passtochild)
    signal.signal(signal.SIGTERM, passtochild)
    patroni.join()


if __name__ == '__main__':
    main()
