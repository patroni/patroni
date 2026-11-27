"""Patroni main entry point.

Implement ``patroni`` main daemon and expose its entry point.
"""

import logging
import os
import signal
import socket
import sys
import time

from argparse import Namespace
from contextlib import suppress
from enum import Enum
from threading import current_thread, Event, local, Lock, main_thread, Thread
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from patroni import global_config, MIN_PSYCOPG2, MIN_PSYCOPG3, parse_version
from patroni.collections import EMPTY_DICT
from patroni.daemon import abstract_main, AbstractPatroniDaemon, get_base_arg_parser, notify_systemd
from patroni.site import ClusterSite
from patroni.tags import Tags

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config
    from .dcs import Cluster
    from .log import PatroniLogger

logger = logging.getLogger(__name__)


_guard: Optional['ShutdownGuard'] = None
_fork = local()


def _before_fork() -> None:
    """Prevent a SIGTERM sent to the child from waking the parent's guard.

    A worker can fork while the main thread is installing the guard. The child inherits the forking thread's
    signal mask, so it can detach before unblocking SIGTERM. Leave an already blocked SIGTERM blocked.
    """
    if _guard is not None or current_thread() is not main_thread():
        _fork.unblock = signal.SIGTERM not in signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})


def _after_fork() -> None:
    """Unblock SIGTERM only if the pre-fork hook blocked it."""
    if getattr(_fork, 'unblock', False):
        _fork.unblock = False
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})


def _after_fork_in_child() -> None:
    """Disconnect the child from the parent's shutdown guard before restoring the child's signal mask."""
    global _guard
    if _guard is not None:
        # Both CPython and our Python SIGTERM handler can write to the parent's socket. Disconnect both in
        # the child before restoring its signal mask. Closing the child's copies leaves the parent untouched.
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        _guard.detach()
        _guard = None
    _after_fork()


class _ShutdownOwner(Enum):
    """Whether the stop is unclaimed, owned by HA, or owned by the guard."""

    WAITING = 'waiting'
    NORMAL = 'normal'
    GUARD = 'guard'


class ShutdownGuard(Thread):
    """Monitor shutdown even when the daemon's main thread cannot handle SIGTERM.

    The guard stays armed while the main thread shuts down the API and waits for HA work to finish. HA calls
    :meth:`claim_normal` just before stopping PostgreSQL. If the guard took over first, HA leaves the stop to it.
    """

    def __init__(self, *, is_requested: Callable[[], bool], on_request: Callable[[], float],
                 on_timeout: Callable[[], None], force_stop: Callable[[], None]) -> None:
        """Create the wakeup sockets without arming the guard.

        :param is_requested: callback that checks whether shutdown was requested, even before the guard started.
        :param on_request: callback that records the request, wakes HA, and returns the handoff grace period in seconds.
        :param on_timeout: callback invoked once to report takeover before the first emergency stop attempt.
        :param force_stop: callback that returns only when emergency shutdown is complete and Patroni can exit.
        """
        super().__init__(name='shutdown-guard', daemon=True)
        self._is_requested = is_requested
        self._on_request = on_request
        self._on_timeout = on_timeout
        self._force_stop = force_stop
        self._reader, self._writer = socket.socketpair()
        self._writer.setblocking(False)
        self._previous_fd: Optional[int] = None
        self._closed = Event()
        self._lock = Lock()
        self._owner: _ShutdownOwner = _ShutdownOwner.WAITING
        self._grace_until = 0.0

    def start(self, set_armed: Callable[[bool], None] = lambda _: None) -> None:
        """Arm the guard and start its thread. Repeated calls after startup have no effect.

        On POSIX, call this from the main thread. Publish the guard to the fork hooks before arming the daemon's
        signal callback so a forked child cannot send a shutdown request to its parent.

        :param set_armed: callback invoked with ``True`` to enable signal notifications, and with ``False`` to undo
                          that step if startup fails. It is not called when another guard is already installed.
        :raises RuntimeError: if another guard is already installed on POSIX.
        :raises ValueError: if :func:`signal.set_wakeup_fd` rejects the wakeup fd or the calling thread.
        """
        global _guard
        if self.ident is not None:
            return
        if os.name == 'posix' and _guard is not None:
            self.close()
            raise RuntimeError('shutdown guard already installed')
        try:
            if os.name == 'posix':
                _guard = self
            set_armed(True)
            if os.name == 'posix':
                # CPython writes the signal number here even if the main thread cannot run its Python handler yet.
                self._previous_fd = signal.set_wakeup_fd(self._writer.fileno())
            super().start()
        except BaseException:
            set_armed(False)
            self.close()
            raise

    def detach(self) -> None:
        """Close the forked child's copies of the wakeup sockets without taking the guard lock or waking the parent."""
        self._reader.close()
        self._writer.close()

    def notify(self) -> None:
        """Send a shutdown wakeup byte without blocking or taking a lock, so the Python signal handler can call it."""
        with suppress(OSError):
            self._writer.send(bytes((signal.SIGTERM,)))

    def extend_grace(self, timeout: float) -> None:
        """Give HA more time to prepare for the normal stop, for example while a synchronous standby is replaced.

        Wake the reader to apply the updated deadline without requesting shutdown.

        :param timeout: seconds from now to allow HA to claim the stop. This does not shorten the original grace
                        period or impose a timeout on PostgreSQL once HA has claimed the stop.
        """
        self._grace_until = time.monotonic() + timeout
        with suppress(OSError):
            self._writer.send(b'\0')

    def _claim(self, owner: _ShutdownOwner) -> bool:
        """Claim responsibility for stopping PostgreSQL if the stop is still unclaimed.

        :param owner: ``NORMAL`` for HA's normal stop, or ``GUARD`` for emergency shutdown.
        :returns: ``True`` if *owner* acquired or already held the claim; ``False`` if the other owner holds it.
        """
        # The grace period can expire just as HA is about to stop PostgreSQL. Only the winner may proceed.
        with self._lock:
            if self._owner is _ShutdownOwner.WAITING:
                self._owner = owner
            return self._owner is owner

    def claim_normal(self) -> bool:
        """Reserve the normal PostgreSQL stop for HA.

        :returns: ``True`` if HA owns the stop and may proceed; ``False`` if the guard has already taken over.
        """
        return self._claim(_ShutdownOwner.NORMAL)

    def owns_stop(self) -> bool:
        """:returns: ``True`` if the guard has claimed emergency shutdown, otherwise ``False``."""
        return self._owner is _ShutdownOwner.GUARD

    def close(self) -> None:
        """Disarm the guard and wake its thread without waiting for it to finish.

        If the guard installed a POSIX signal wakeup fd, call this from the main thread to restore the previous fd
        before closing the socket.
        """
        global _guard
        with self._lock:
            if _guard is self:
                # Restore the previous fd before closing ours: a later signal must not write to a reused descriptor.
                if self._previous_fd is not None:
                    signal.set_wakeup_fd(self._previous_fd)
                _guard = None
            self._closed.set()
        self.notify()
        self._writer.close()
        if self.ident is None:
            self._reader.close()

    def finish(self) -> None:
        """Finish shutdown on the main thread after daemon teardown.

        Close the guard if it never started or if HA claimed the stop. Otherwise the guard owns the stop;
        wait for it to exit the process or be closed.
        """
        if self.ident is None or not self._claim(_ShutdownOwner.GUARD):
            self.close()
            return
        self.notify()
        while not self._closed.is_set():
            with suppress(KeyboardInterrupt):
                self._closed.wait()

    def _wait_for_takeover(self) -> bool:
        """Wake HA and wait for it to claim the stop, counting the wakeup time toward its grace period.

        If waking HA or waiting for its claim fails, try to take over immediately. If HA wins, drain the signal
        wakeup socket until the main thread disarms the guard.

        :returns: ``True`` if the guard owns the emergency stop, ``False`` if HA owns it or the guard was closed.
        """
        with suppress(Exception):
            deadline = time.monotonic() + self._on_request()
            while self._owner is _ShutdownOwner.WAITING and not self._closed.is_set():
                remaining = max(deadline, self._grace_until) - time.monotonic()
                if remaining <= 0:
                    break
                self._reader.settimeout(remaining)
                with suppress(socket.timeout):
                    if not self._reader.recv(4096):
                        return False

        if self._closed.is_set():
            return False
        if self._claim(_ShutdownOwner.GUARD):
            return True

        self._reader.settimeout(None)
        while not self._closed.is_set() and self._reader.recv(4096):
            pass
        return False

    def run(self) -> None:
        """Wait for a shutdown request, wake HA, and take over if HA misses the deadline to claim the stop.

        If HA claims the stop, leave it in charge and wait for :meth:`finish` to close the guard.
        On takeover, request one systemd timeout extension and exit with status 1 once emergency shutdown is complete.
        """
        with self._reader:
            while not self._is_requested():
                data = self._reader.recv(4096)
                if not data or self._closed.is_set():
                    return
                # The wakeup fd also receives other handled signals, such as SIGHUP. Only SIGTERM starts shutdown;
                # one or more SIGTERM bytes all represent the same request.
                if signal.SIGTERM in data:
                    break
            if self._closed.is_set() or not self._wait_for_takeover():
                return

            with suppress(Exception):
                notify_systemd('EXTEND_TIMEOUT_USEC=30000000')
            with suppress(Exception):
                self._on_timeout()
            while not self._closed.is_set():
                try:
                    self._force_stop()
                    break
                except Exception:
                    self._closed.wait(0.1)
            with self._lock:
                if not self._closed.is_set():
                    os._exit(1)


class Patroni(AbstractPatroniDaemon, ClusterSite, Tags):
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

    def __init__(self, config: 'Config', patroni_logger: 'PatroniLogger') -> None:
        """Create a :class:`Patroni` instance with the given *config*.

        Get a connection to the DCS, configure watchdog (if required), set up Patroni interface with Postgres, configure
        the HA loop and bring the REST API up.

        .. note::
            Expected to be instantiated and run through :func:`~patroni.daemon.abstract_main`.

        :param config: Patroni configuration.
        :param patroni_logger: the logging handler for this daemon.
        """
        from patroni import thread_pool
        from patroni.api import RestApiServer
        from patroni.dcs import get_dcs
        from patroni.ha import Ha
        from patroni.postgresql import Postgresql
        from patroni.request import PatroniRequest
        from patroni.version import __version__
        from patroni.watchdog import Watchdog

        try:
            thread_pool_size = max(5, int(config.get('thread_pool_size', 5)))
        except Exception as e:
            logger.warning('Failed to parse thread_pool_size value "%s": %r', config.get('thread_pool_size'), e)
            thread_pool_size = 5
        logger.info('Patroni global thread_pool_size = %d', thread_pool_size)
        thread_pool.configure_global_pool(thread_pool_size)

        AbstractPatroniDaemon.__init__(self, config, patroni_logger)
        ClusterSite.__init__(self, config.get('site'))

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
        self.postgresql.stopping = lambda: self.received_sigterm
        self._api_started = False
        self.shutdown_guard = ShutdownGuard(
            is_requested=lambda: self.received_sigterm, on_request=self.shutdown_requested,
            on_timeout=self.logger.report_shutdown_timeout, force_stop=self.ha.shutdown_fence)
        self.postgresql.guard_owns_stop = self.shutdown_guard.owns_stop

        self._tags = self._get_tags()
        self.next_run = time.monotonic()
        self.scheduled_restart: Dict[str, Any] = {}

        self._last_effective_role = None
        self._last_effective_pg_config = self.config['postgresql']

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

        .. note::
            The check is skipped with the Kubernetes DCS: Pod names are unique, and the member record is a Pod
            annotation without TTL, so the ``api_url`` in it may be stale and point to another node after a K8s
            node reboot.

        :param cluster: a PostgreSQL or MPP implementation of :class:`Cluster`.
        """
        from patroni.dcs import Member

        if not cluster or self.dcs.__class__.__name__ == 'Kubernetes':
            return
        member = cluster.get_member(self.config['name'], False)
        if not isinstance(member, Member):
            return
        # Silence annoying WARNING: Retrying (...) messages when Patroni is quickly restarted.
        configured_loggers: Dict[str, Any] = (self.config.get('log') or EMPTY_DICT).get('loggers') or {}
        try:
            self.logger.update_loggers({**configured_loggers, 'urllib3.connectionpool': 'ERROR'})
            _ = self.request(member, endpoint="/liveness", timeout=3)
            logger.fatal("Can't start; there is already a node named '%s' running", self.config['name'])
            sys.exit(1)
        except Exception:
            self.logger.update_loggers(configured_loggers)

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
        from patroni.config import ROLE_CONFIG_SUFFIX_MAP

        try:
            super(Patroni, self).reload_config(sighup, local)
            if local:
                self._tags = self._get_tags()
                self.request.reload_config(self.config)
            received_new_cert = sighup and self.api.reload_local_certificate()
            if local or received_new_cert:
                self.api.reload_config(self.config['restapi'])
            self.watchdog.reload_config(self.config)
            if self.watchdog.needs_immediate_keepalive:
                # Not every DCS starts the next cycle right away when the TTL changes, so the HA loop would sleep for
                # the new loop_wait while the watchdog is still armed with the timeout of the previous configuration.
                self.ha.wakeup()
            self._last_effective_role = ROLE_CONFIG_SUFFIX_MAP.get(self.postgresql.role)
            self._last_effective_pg_config = \
                self.config.build_effective_postgresql_configuration(self.postgresql.role)
            self.postgresql.reload_config(self._last_effective_pg_config, sighup)
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
        current_time = time.monotonic()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
            # Release the GIL so we don't starve anyone waiting on async_executor lock
            time.sleep(0.001)
            # Warn user that Patroni is not keeping up
            logger.warning("Loop time exceeded, rescheduling immediately.")
        elif self.ha.watch(nap_time):
            self.next_run = time.monotonic()

    def shutdown_requested(self) -> float:
        """Record the shutdown request and wake the HA loop from the guard, outside the signal handler.

        :returns: seconds to allow HA to claim the normal stop, not a time limit on PostgreSQL shutdown.
        """
        self._received_sigterm = True
        self.dcs.wakeup()
        return max(self.dcs.loop_wait, 2 * self.config['retry_timeout'])

    def run(self) -> None:
        """Run ``patroni`` daemon process main loop.

        Start the REST API and keep running HA cycles every ``loop_wait`` seconds.
        """
        def set_armed(armed: bool) -> None:
            """Set the signal handler's wakeup callback.

            :param armed: ``True`` to notify the guard on SIGTERM, ``False`` to remove the callback.
            """
            self._on_sigterm = self.shutdown_guard.notify if armed else None

        # Arm the guard before starting services: even joining their threads during shutdown may block.
        self.shutdown_guard.start(set_armed)
        self.api.start()
        self._api_started = True
        self.next_run = time.monotonic()
        super(Patroni, self).run()

    def _run_cycle(self) -> None:
        """Run a cycle of the ``patroni`` daemon main loop.

        Run an HA cycle and schedule the next cycle run. If any dynamic configuration change request is detected, apply
        the change and cache the new dynamic configuration values in ``patroni.dynamic.json`` file under Postgres data
        directory.
        """
        from patroni.config import ROLE_CONFIG_SUFFIX_MAP
        from patroni.postgresql.misc import PostgresqlRole
        from patroni.utils import deep_compare

        logger.info(self.ha.run_cycle())

        self.check_shutdown()
        if self.dcs.cluster and self.dcs.cluster.config and self.dcs.cluster.config.data \
                and self.config.set_dynamic_configuration(self.dcs.cluster.config):
            self.reload_config()
        elif self._last_effective_role != ROLE_CONFIG_SUFFIX_MAP.get(self.postgresql.role):
            self._last_effective_role = ROLE_CONFIG_SUFFIX_MAP.get(self.postgresql.role)
            new_effective_pg_config = self.config.build_effective_postgresql_configuration(self.postgresql.role)
            if not deep_compare(self._last_effective_pg_config, new_effective_pg_config):
                self._last_effective_pg_config = new_effective_pg_config
                self.postgresql.reload_config(self._last_effective_pg_config)

        if self.postgresql.role != PostgresqlRole.UNINITIALIZED:
            self.config.save_cache()

        self.check_shutdown()
        self.schedule_next_run()

    def _shutdown(self) -> None:
        """Perform shutdown of ``patroni`` daemon process.

        Shut down the REST API and the HA handler.
        """
        from patroni import thread_pool

        if self._api_started:
            try:
                self.api.shutdown()
            except Exception:
                logger.exception('Exception during RestApi.shutdown')
        try:
            self.ha.shutdown()
        except Exception:
            logger.exception('Exception during Ha.shutdown')

        thread_pool.get_executor().shutdown(wait=False)

    def shutdown(self) -> None:
        """Keep the monitor armed until normal shutdown, including logger teardown, has finished."""
        # abstract_main() also calls shutdown() after Ctrl-C or an exception. Neither has to deliver a SIGTERM byte.
        self.shutdown_guard.notify()
        try:
            super(Patroni, self).shutdown()
        finally:
            self.shutdown_guard.finish()


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
    # HA and the REST API import patroni.__main__; use the same guard and fork hooks when invoked with -m.
    from patroni.__main__ import main as _main
    _main()
elif hasattr(os, 'register_at_fork'):
    os.register_at_fork(before=_before_fork, after_in_child=_after_fork_in_child, after_in_parent=_after_fork)
