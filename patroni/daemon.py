"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
import abc
import argparse
import logging
import os
import signal
import sys

from threading import Lock, stack_size, Thread
from typing import Any, Optional, Type, TYPE_CHECKING

try:  # pragma: no cover
    from time import monotonic_ns

    def monotonic_us() -> int:
        """Return the current monotonic clock value in microseconds.

        .. note::
            Uses :func:`time.monotonic_ns` available since Python 3.7.

        :returns: monotonic time in microseconds.
        """
        return monotonic_ns() // 1000
except ImportError:  # pragma: no cover
    from time import monotonic

    def monotonic_us() -> int:
        """Return the current monotonic clock value in microseconds.

        .. note::
            Fallback for Python 3.6 where :func:`time.monotonic_ns` is not available.

        :returns: monotonic time in microseconds.
        """
        return int(monotonic() * 1000000)

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config
    from .log import PatroniLogger

logger = logging.getLogger(__name__)

SOCKET_ENV_VARIABLE = 'NOTIFY_SOCKET'
__systemd_available = False

if os.environ.get(SOCKET_ENV_VARIABLE) is not None:
    try:  # pragma: no cover
        from systemd import daemon  # pyright: ignore
        __systemd_available = True
    except ImportError:  # pragma: no cover
        pass


def notify_systemd(msg: str) -> None:
    if __systemd_available:  # pragma: no cover
        daemon.notify(msg)  # pyright: ignore


def get_base_arg_parser() -> argparse.ArgumentParser:
    """Create a basic argument parser with the arguments used for both patroni and raft controller daemon.

    :returns: 'argparse.ArgumentParser' object
    """
    from .config import Config
    from .version import __version__

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))
    parser.add_argument('configfile', nargs='?', default='',
                        help='Patroni may also read the configuration from the {0} environment variable'
                        .format(Config.PATRONI_CONFIG_VARIABLE))
    return parser


class AbstractPatroniDaemon(abc.ABC):
    """A Patroni daemon process.

    .. note::

        When inheriting from :class:`AbstractPatroniDaemon` you are expected to define the methods :func:`_run_cycle`
        to determine what it should do in each execution cycle, and :func:`_shutdown` to determine what it should do
        when shutting down.

    :ivar logger: log handler used by this daemon.
    :ivar config: configuration options for this daemon.
    """

    def __init__(self, config: 'Config', patroni_logger: 'PatroniLogger') -> None:
        """Set up signal handlers, logging handler and configuration.

        :param config: configuration options for this daemon.
        :param patroni_logger: the logging handler for this daemon.
        """
        self.setup_signal_handlers()

        self.logger = patroni_logger
        self.config = config

    def sighup_handler(self, *_: Any) -> None:
        """Handle SIGHUP signals.

        Flag the daemon as "SIGHUP received".
        """
        self._received_sighup = True
        notify_systemd("RELOADING=1\nMONOTONIC_USEC={0}".format(monotonic_us()))

    def api_sigterm(self) -> bool:
        """Guarantee only a single SIGTERM is being processed.

        Flag the daemon as "SIGTERM received" with a lock-based approach.

        :returns: ``True`` if the daemon was flagged as "SIGTERM received".
        """
        ret = False
        with self._sigterm_lock:
            if not self._received_sigterm:
                self._received_sigterm = True
                ret = True
        return ret

    def sigterm_handler(self, *_: Any) -> None:
        """Handle SIGTERM signals.

        Until :func:`enable_graceful_sigterm` is called, terminate the daemon process immediately by raising
        :exc:`SystemExit`. The daemon could be blocked during the start, for example, waiting for DCS, and there is
        nothing to shut down gracefully yet.

        Afterwards, flag the daemon as "SIGTERM received" and wake up the main loop, which exits and shuts down
        the daemon.

        .. note::
            Python runs signal handlers in the main thread between arbitrary bytecode instructions. An exception
            raised from the handler could be raised after the interrupted code acquired a lock, but before it entered
            the ``with`` or ``try`` block that releases it, for example, the mutex of the logging queue. Such a lock
            remains locked forever and the shutdown deadlocks. The handler must not acquire locks either, because the
            interrupted code could be holding them.
        """
        if self._received_sigterm:
            return
        self._received_sigterm = True
        if not self._graceful_sigterm:
            sys.exit()
        try:
            self._sigterm_wakeup_lock.release()
        except RuntimeError:  # already released by a nested call of the handler
            pass

    def enable_graceful_sigterm(self) -> None:
        """Make SIGTERM shut down the daemon gracefully.

        From now on :func:`sigterm_handler` doesn't raise :exc:`SystemExit`, but only flags the daemon as
        "SIGTERM received". Start a thread that wakes up the main loop after that happened, because the signal
        handler can't safely do it.

        .. note::
            Should be called after the daemon is initialized and before the main loop is started.
        """
        thread = Thread(target=self._wakeup_on_sigterm, name='sigterm-waiter')
        thread.daemon = True
        thread.start()
        self._graceful_sigterm = True

    def _wakeup_on_sigterm(self) -> None:
        """Wait until SIGTERM is received and wake up the main loop."""
        self._sigterm_wakeup_lock.acquire()  # released by sigterm_handler()
        self._wakeup()

    def _wakeup(self) -> None:
        """Wake up the main loop if it is waiting for the next execution cycle.

        .. note::
            Called from a separate thread when SIGTERM is received. Does nothing by default, override it if
            :func:`_run_cycle` could wait for a long time.
        """

    def setup_signal_handlers(self) -> None:
        """Set up daemon signal handlers.

        Set up SIGHUP and SIGTERM signal handlers.

        .. note::

            SIGHUP is only handled in non-Windows environments.
        """
        self._received_sighup = False
        self._sigterm_lock = Lock()
        self._received_sigterm = False
        self._graceful_sigterm = False
        # Used to wake up the thread started by enable_graceful_sigterm() from sigterm_handler(). The C implementation
        # of Lock.release() is safe to call from the signal handler, unlike most other synchronization primitives.
        self._sigterm_wakeup_lock = Lock()
        self._sigterm_wakeup_lock.acquire()
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @property
    def received_sigterm(self) -> bool:
        """If daemon was signaled with SIGTERM."""
        with self._sigterm_lock:
            return self._received_sigterm

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        """Reload configuration.

        :param sighup: if it is related to a SIGHUP signal.
                       The sighup parameter could be used in the method overridden in a child class.
        :param local: will be ``True`` if there are changes in the local configuration file.
        """
        if local:
            self.logger.reload_config(self.config.get('log', {}))

    @abc.abstractmethod
    def _run_cycle(self) -> None:
        """Define what the daemon should do in each execution cycle.

        Keep being called in the daemon's main loop until the daemon is eventually terminated.
        """

    def run(self) -> None:
        """Run the daemon process.

        Start the logger thread and keep running execution cycles until a SIGTERM is eventually received. Also reload
        configuration upon receiving SIGHUP.
        """
        notify_systemd("READY=1")
        self.logger.start()
        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                self.reload_config(True, self.config.reload_local_configuration())
                notify_systemd("READY=1")

            self._run_cycle()

    @abc.abstractmethod
    def _shutdown(self) -> None:
        """Define what the daemon should do when shutting down."""

    def shutdown(self) -> None:
        """Shut the daemon down when a SIGTERM is received.

        Shut down the daemon process and the logger thread.
        """
        with self._sigterm_lock:
            self._received_sigterm = True
        notify_systemd("STOPPING=1")
        self._shutdown()
        self.logger.shutdown()


def abstract_main(cls: Type[AbstractPatroniDaemon], configfile: str) -> None:
    """Create the main entry point of a given daemon process.

    :param cls: a class that should inherit from :class:`AbstractPatroniDaemon`.
    :param configfile:
    """
    from .config import Config, ConfigParseError
    from .log import PatroniLogger
    from .utils import parse_int

    patroni_logger = PatroniLogger()
    try:
        config = Config(configfile)
    except ConfigParseError as e:
        sys.exit(e.value)
    patroni_logger.reload_config(config.get('log', {}))

    if not __systemd_available and os.environ.get(SOCKET_ENV_VARIABLE):
        logger.warning('Running under systemd but python-systemd package is not installed')

    thread_stack_size = None
    if 'thread_stack_size' in config:
        thread_stack_size = parse_int(config.get('thread_stack_size'), 'B')
        if thread_stack_size is None:
            logger.warning('Failed to parse thread_stack_size value "%s"', config.get('thread_stack_size'))

    if thread_stack_size is None:
        thread_stack_size = 524288
        logger.info('Using default value thread_stack_size = %s', thread_stack_size)

    thread_stack_size = max(65536, thread_stack_size)
    try:
        stack_size(thread_stack_size)
    except Exception as e:
        logger.warning('Failed to set threading.stack_size(%s): %r', thread_stack_size, e)

    controller = cls(config, patroni_logger)
    controller.enable_graceful_sigterm()
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
