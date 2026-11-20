"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
import abc
import argparse
import logging
import os
import signal
import socket
import sys
import time

from contextlib import suppress
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

        Flag the daemon as "SIGTERM received" with a lock-based approach and wake up the main loop with :func:`_wakeup`.

        .. note::
            Only called by REST API threads, never by the main thread, see :attr:`received_sigterm`.

        :returns: ``True`` if the daemon was flagged as "SIGTERM received".
        """
        ret = False
        with self._sigterm_lock:
            if not self._received_sigterm:
                self._received_sigterm = True
                ret = True
        if ret:
            self._wakeup()
        return ret

    def sigterm_handler(self, *_: Any) -> None:
        """Handle SIGTERM signals.

        Until :func:`enable_graceful_sigterm` is called, terminate the daemon process immediately by raising
        :exc:`SystemExit`. The daemon could be blocked during the start, for example, waiting for DCS, and there is
        nothing to shut down gracefully yet.

        Afterwards, flag the daemon as "SIGTERM received" and wake up the main loop with :func:`_wakeup`. The main
        loop exits and shuts down the daemon. If the main loop doesn't exit within ``loop_wait`` seconds, for example
        because it hangs, :func:`_sigterm_guard` terminates the daemon with :func:`_emergency_exit`.

        .. note::
            Python runs signal handlers in the main thread between arbitrary bytecode instructions. An exception
            raised from the handler could leave a lock held by the interrupted code locked forever: if it is raised
            after the lock was acquired, but before the ``with`` or ``try`` block that releases it was entered, or, on
            Python 3.9 and older, after the ``with`` block was left, but before ``__exit__()`` was called. For example,
            the ``Postgresql.state`` property could leak the lock that ``Postgresql.stop()`` needs to set the
            "stopping" state, and the shutdown deadlocks without stopping Postgres. The handler must not acquire locks
            either, because the interrupted code could be holding them.
        """
        if self._received_sigterm:
            return
        self._received_sigterm = True
        if not self._graceful_sigterm:
            sys.exit()
        self._wakeup()

    def _start_sigterm_guard(self) -> None:
        """Start a thread running :func:`_sigterm_guard`.

        The C-level signal handler writes the number of every signal that has a Python handler to the socket registered
        with :func:`signal.set_wakeup_fd` immediately, even if the main thread is blocked in C code and therefore can't
        run :func:`sigterm_handler`.

        Also set the default action of SIGALRM, which :func:`_emergency_exit` relies on. ``SIG_IGN`` could be inherited
        from the parent process.

        .. note::
            Must be called from the main thread.
        """
        self._sigterm_guard_rsock, wsock = socket.socketpair()
        wsock.setblocking(False)
        self._sigterm_guard_wsock = wsock
        self._prev_wakeup_fd = signal.set_wakeup_fd(wsock.fileno())
        self._prev_sigalrm_handler = signal.signal(signal.SIGALRM, signal.SIG_DFL)
        Thread(target=self._sigterm_guard, name='sigterm-guard', daemon=True).start()

    def _stop_sigterm_guard(self) -> None:
        """Restore the previous wakeup fd and SIGALRM handler, and make :func:`_sigterm_guard` exit by closing the write
        end of its socket.

        .. note::
            Must be called from the main thread.
        """
        signal.set_wakeup_fd(self._prev_wakeup_fd)
        if self._prev_sigalrm_handler is not None:  # None if the previous handler wasn't installed from Python
            signal.signal(signal.SIGALRM, self._prev_sigalrm_handler)
        self._sigterm_guard_wsock.close()

    def _sigterm_guard(self) -> None:
        """Terminate the daemon if the main loop is still running ``loop_wait`` seconds after SIGTERM.

        Runs in a separate thread, because Python runs signal handlers only in the main thread and only between
        bytecode instructions. When the main loop hangs in C code, for example in libpq, which retries system calls
        interrupted by signals, :func:`sigterm_handler` doesn't run at all. Calls :func:`_emergency_exit` on timeout.

        Exits when the main loop closes the socket, see :func:`_stop_sigterm_guard`.
        """
        sock = self._sigterm_guard_rsock
        deadline = None
        with sock:
            while True:
                if deadline is None:
                    sock.settimeout(None)
                else:
                    timeout = deadline - time.monotonic()
                    if timeout <= 0:
                        break
                    sock.settimeout(timeout)
                try:
                    data = sock.recv(64)
                except socket.timeout:
                    break
                if not data:
                    return
                if deadline is None and signal.SIGTERM in data:
                    deadline = time.monotonic() + self.config['loop_wait']
            if self._main_loop_running:
                self._emergency_exit()

    def _emergency_exit(self) -> None:
        """Terminate the daemon process immediately, because its main loop hangs after SIGTERM.

        Log a critical message, call :func:`_emergency_shutdown`, give the logger thread up to a second to write queued
        records, and exit with :func:`os._exit`, even if any of these steps fails. Logging is best effort.

        .. note::
            The regular :func:`shutdown` can't be used: the main loop could hold locks that it needs, and
            :exc:`SystemExit` raised in the main thread could leak them, see :func:`sigterm_handler`.

            The stack of the main thread isn't logged: formatting it reads source files, and a stalled filesystem could
            be the reason why the main loop hangs.
        """
        try:
            # The default action of SIGALRM, set by _start_sigterm_guard(), terminates the process in the kernel,
            # without a thread or the GIL, if the steps below block anyway, e.g. while reading postmaster.pid or
            # waiting for killed processes. SIGALRM is unblocked in case the signal mask was inherited from the parent.
            signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGALRM])
            signal.alarm(5)
            with suppress(Exception):  # must not prevent the emergency shutdown
                logger.critical('The main loop did not exit within %s seconds after SIGTERM, terminating the process',
                                self.config['loop_wait'])
            try:
                self._emergency_shutdown()
            except Exception:
                logger.exception('Emergency shutdown failed')
            self.logger.flush(1)
        finally:
            os._exit(1)

    def _emergency_shutdown(self) -> None:
        """Define what the daemon should do before :func:`_emergency_exit` terminates the process.

        .. note::
            Called from the :func:`_sigterm_guard` thread while the main loop is still running, therefore it must not
            take locks that the main thread could hold. Does nothing by default.
        """

    def enable_graceful_sigterm(self) -> None:
        """Make SIGTERM shut down the daemon gracefully.

        From now on :func:`sigterm_handler` doesn't raise :exc:`SystemExit`, but only flags the daemon as
        "SIGTERM received" and wakes up the main loop.

        .. note::
            Should be called after the daemon is initialized and before the main loop is started.
        """
        self._graceful_sigterm = True

    def _wakeup(self) -> None:
        """Wake up the main loop if it is waiting for the next execution cycle.

        .. note::
            Called from :func:`sigterm_handler`, therefore it must not block, acquire locks or raise exceptions.
            Does nothing by default, override it if :func:`_run_cycle` could wait for a long time.
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
        self._main_loop_running = False
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @property
    def received_sigterm(self) -> bool:
        """If daemon was signaled with SIGTERM.

        .. note::
            The main thread must not take ``_sigterm_lock``: if :exc:`KeyboardInterrupt` was raised by SIGINT while it
            is held, the lock would be leaked and :func:`shutdown` would deadlock.
        """
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
        if os.name != 'nt':
            self._start_sigterm_guard()
        self._main_loop_running = True
        try:
            while not self.received_sigterm:
                if self._received_sighup:
                    self._received_sighup = False
                    self.reload_config(True, self.config.reload_local_configuration())
                    notify_systemd("READY=1")
                    if self.received_sigterm:  # don't start the next cycle if SIGTERM was received during the reload
                        break

                self._run_cycle()
        finally:
            self._main_loop_running = False
            if os.name != 'nt':
                self._stop_sigterm_guard()

    @abc.abstractmethod
    def _shutdown(self) -> None:
        """Define what the daemon should do when shutting down."""

    def shutdown(self) -> None:
        """Shut the daemon down when a SIGTERM is received.

        Shut down the daemon process and the logger thread.
        """
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
