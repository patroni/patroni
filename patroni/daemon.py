"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
import abc
import argparse
import faulthandler
import logging
import os
import signal
import socket
import sys
import time

from contextlib import suppress
from threading import get_ident, local, Lock, main_thread, stack_size, Thread
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


_sigterm_guard_daemon: Optional['AbstractPatroniDaemon'] = None
_traceback_dump_fd = -1  # see AbstractPatroniDaemon._emergency_exit()
_fork_local = local()


def _before_fork() -> None:
    """Block SIGTERM in the thread that forks, unless it is the main thread and the SIGTERM guard isn't running.

    The child inherits the signal mask, and SIGTERM stays blocked until :func:`_after_fork_in_child` has detached the
    child from the guard. Otherwise, a SIGTERM received by the child in the meantime would reach the guard of the
    parent. Pending signals aren't inherited, so a SIGTERM sent to the parent is received after it unblocks SIGTERM.

    Other threads block SIGTERM even if the guard isn't running yet, because the main thread could start it after this
    function has returned, but before the process forks. The main thread can't start the guard while it forks, and
    blocks SIGTERM only while the guard is running: :func:`signal.pthread_sigmask` runs pending signal handlers in the
    main thread, and :func:`os.fork` would ignore :exc:`SystemExit` raised by
    :func:`AbstractPatroniDaemon.sigterm_handler` before :func:`AbstractPatroniDaemon.run` has started.

    .. note::
        Registered with :func:`os.register_at_fork`. For example, :mod:`pysyncobj` forks to write Raft snapshots.
    """
    if _sigterm_guard_daemon is not None or get_ident() != main_thread().ident:
        # pthread_sigmask() runs pending signal handlers in the main thread, a raising one would skip the assignment
        _fork_local.unblock_sigterm = True
        _fork_local.unblock_sigterm = signal.SIGTERM not in signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})


def _unblock_sigterm_after_fork() -> None:
    """Unblock SIGTERM if :func:`_before_fork` has blocked it."""
    if getattr(_fork_local, 'unblock_sigterm', False):
        _fork_local.unblock_sigterm = False
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})


def _after_fork_in_child() -> None:
    """Detach a forked child process from the SIGTERM guard of the parent, see
    :func:`AbstractPatroniDaemon._detach_sigterm_guard`, and unblock SIGTERM, see :func:`_before_fork`.

    A SIGTERM received by the child before then terminates it now. SIGTERM stays blocked if detaching fails.
    """
    global _sigterm_guard_daemon
    owner = _sigterm_guard_daemon
    if owner is not None:
        _sigterm_guard_daemon = None
        owner._detach_sigterm_guard()  # pyright: ignore [reportPrivateUsage]
    _unblock_sigterm_after_fork()


if hasattr(os, 'register_at_fork'):  # not available on Windows and Python 3.6, see patroni.dcs.raft
    os.register_at_fork(before=_before_fork, after_in_parent=_unblock_sigterm_after_fork,
                        after_in_child=_after_fork_in_child)


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

        Until :func:`run` calls :func:`enable_graceful_sigterm`, terminate the daemon process immediately by raising
        :exc:`SystemExit`. The daemon could be blocked during the start, for example, waiting for DCS, and there is
        nothing to shut down gracefully yet.

        Afterwards, flag the daemon as "SIGTERM received" and wake up the main loop with :func:`_wakeup`. The main
        loop exits and shuts down the daemon. If :func:`run` doesn't return within :attr:`sigterm_timeout`, for example
        because the main loop hangs, :func:`_sigterm_guard` terminates the daemon with :func:`_emergency_exit`.

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

        Also create the pipe that :func:`_emergency_exit` needs in advance, because the process could run out of file
        descriptors by then.

        The guard is published before the wakeup fd is set, so that a child forked by another thread in the meantime
        detaches from it, see :func:`_before_fork`.

        :raises Exception: if the guard can't be started, after restoring the previous wakeup fd.

        .. note::
            Must be called from the main thread.
        """
        global _sigterm_guard_daemon, _traceback_dump_fd
        if _traceback_dump_fd < 0:
            _, fd = os.pipe()
            os.set_blocking(fd, False)  # the dump is discarded when the pipe is full
            _traceback_dump_fd = fd
        self._sigterm_guard_rsock, self._sigterm_guard_wsock = socket.socketpair()
        self._prev_wakeup_fd = -1
        try:
            self._sigterm_guard_wsock.setblocking(False)
            _sigterm_guard_daemon = self
            self._prev_wakeup_fd = signal.set_wakeup_fd(self._sigterm_guard_wsock.fileno())
            Thread(target=self._sigterm_guard, name='sigterm-guard', daemon=True).start()
        except Exception:
            if _sigterm_guard_daemon is self:
                self._stop_sigterm_guard()
            self._sigterm_guard_rsock.close()
            self._sigterm_guard_wsock.close()
            raise

    def _stop_sigterm_guard(self) -> None:
        """Restore the previous wakeup fd and make :func:`_sigterm_guard` exit by closing the write end of its socket.

        .. note::
            Must be called from the main thread.
        """
        global _sigterm_guard_daemon
        signal.set_wakeup_fd(self._prev_wakeup_fd)
        self._sigterm_guard_wsock.close()
        _sigterm_guard_daemon = None

    def _detach_sigterm_guard(self) -> None:
        """Detach a forked child process from :func:`_sigterm_guard` of the parent.

        The child inherits the wakeup fd and the socket of the guard. Otherwise, a SIGTERM sent only to the child would
        make the guard of the parent terminate the healthy parent. Restore the previous wakeup fd, close the inherited
        sockets and restore the default action of SIGTERM, which terminates the child. The main loop that would
        handle SIGTERM doesn't run in the child.

        .. note::
            Called in the child from the thread that forked, which becomes the main thread of the child. SIGTERM is
            blocked until then, see :func:`_before_fork`. Other signals received by the child in the meantime still
            reach the guard of the parent, which ignores them.

            If the parent forked while :func:`_start_sigterm_guard` was running, the child might not have the wakeup
            fd of the guard, and ``_prev_wakeup_fd`` might still be ``-1``. Patroni doesn't use other wakeup fds.
        """
        signal.set_wakeup_fd(self._prev_wakeup_fd)
        self._sigterm_guard_rsock.close()
        self._sigterm_guard_wsock.close()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    @property
    def sigterm_timeout(self) -> float:
        """Grace period in seconds for the main loop to return after SIGTERM.

        An in-flight HA cycle can still be retrying DCS and PostgreSQL operations. Allow for both retries without
        shortening the existing ``loop_wait`` grace period.
        """
        return max(self.config['loop_wait'], 2 * self.config['retry_timeout'])

    def _sigterm_guard(self) -> None:
        """Terminate the daemon if :func:`run` is still running :attr:`sigterm_timeout` seconds after SIGTERM.

        Runs in a separate thread, because Python runs signal handlers only in the main thread and only between
        bytecode instructions. When the main thread hangs in C code, for example in libpq, which retries system calls
        interrupted by signals, :func:`sigterm_handler` doesn't run at all. Calls :func:`_emergency_exit` on timeout.

        Also wakes up the main loop with :func:`_wakeup`, because a signal delivered to another thread doesn't
        interrupt the main thread waiting for the next execution cycle.

        Exits when the main loop closes the socket, see :func:`_stop_sigterm_guard`.
        """
        sock = self._sigterm_guard_rsock
        # SIGTERM could have been received before the wakeup fd was set
        deadline = time.monotonic() + self.sigterm_timeout if self._received_sigterm else None
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
                    deadline = time.monotonic() + self.sigterm_timeout
                    with suppress(Exception):  # must not prevent the emergency exit
                        self._wakeup()
            if self._running:
                self._emergency_exit()

    def _emergency_exit(self) -> None:
        """Terminate the daemon process immediately, because :func:`run` hangs after SIGTERM.

        Call :func:`_emergency_shutdown`, log a critical message, give the logger thread up to a second to write queued
        records, and exit with :func:`os._exit`, even if any of these steps fails. Logging is best effort and happens
        last, because it could block, for example, if the main thread holds the lock of a log handler.

        Allow ``retry_timeout`` seconds for the managed service to stop, plus five seconds for the rest of the exit
        path. If it blocks, for example while reading ``postmaster.pid``, the watchdog thread of
        :func:`faulthandler.dump_traceback_later` terminates the process. It doesn't need the GIL or depend on signal
    dispositions and masks inherited from the parent. If the watchdog can't be started, all loggers are disabled
    before :func:`_emergency_shutdown` is called, and the optional wait is skipped.

        .. note::
            The regular :func:`shutdown` can't be used: the main loop could hold locks that it needs, and
            :exc:`SystemExit` raised in the main thread could leak them, see :func:`sigterm_handler`.

            Stacks of threads aren't logged: formatting them reads source files, and a stalled filesystem could be the
            reason why the main loop hangs. The watchdog thread dumps them, without source lines, to a pipe that
            :func:`_start_sigterm_guard` created, and that is never read or closed, because writing to stderr could
            block, and a closed fd could be reused.
        """
        try:
            timeout = self.config['retry_timeout']
            armed = False
            with suppress(Exception):
                faulthandler.dump_traceback_later(timeout + 5, exit=True, file=_traceback_dump_fd)
                armed = True
            if not armed:  # nothing would terminate the process if logging blocks
                with suppress(Exception):  # logging.disable() would take the lock of the logging module
                    for lgr in [logging.root, *logging.Logger.manager.loggerDict.values()]:
                        if isinstance(lgr, logging.Logger):
                            lgr.disabled = True
            error = None
            try:
                self._emergency_shutdown(timeout if armed else 0)
            except Exception as e:
                error = e
            if armed:
                with suppress(Exception):
                    logger.critical('The main loop did not exit within %s seconds after SIGTERM, terminating the '
                                    'process', self.sigterm_timeout)
                    if error is not None:
                        logger.error('Emergency shutdown failed', exc_info=error)
                    self.logger.flush(1)
        finally:
            os._exit(1)

    def _emergency_shutdown(self, timeout: float) -> None:
        """Define what the daemon should do before :func:`_emergency_exit` terminates the process.

        :param timeout: seconds to wait for the service to stop. Zero skips the optional wait and logging when the
                        exit watchdog couldn't be armed.

        .. note::
            Called from the :func:`_sigterm_guard` thread while the main loop is still running, therefore it must not
            take locks that the main thread could hold. It should not log before doing anything important either,
            because logging could block. Loggers are disabled if :func:`_emergency_exit` couldn't start its watchdog.
            Does nothing by default.
        """

    def enable_graceful_sigterm(self) -> None:
        """Make SIGTERM shut down the daemon gracefully.

        From now on :func:`sigterm_handler` doesn't raise :exc:`SystemExit`, but only flags the daemon as
        "SIGTERM received" and wakes up the main loop.

        .. note::
            Called by :func:`run` before it starts :func:`_sigterm_guard`.
        """
        self._graceful_sigterm = True

    def _wakeup(self) -> None:
        """Wake up the main loop if it is waiting for the next execution cycle.

        .. note::
            Called from :func:`sigterm_handler` and :func:`_sigterm_guard`, therefore it must not block, acquire locks
            or raise exceptions. Does nothing by default, override it if :func:`_run_cycle` could wait for a long time.
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
        self._running = False  # True while run() is running, see _sigterm_guard()
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

    def _startup(self) -> None:
        """Define what the daemon should do before it is ready and the main loop starts. Does nothing by default.

        .. note::
            Called by :func:`run` when SIGTERM already shuts down the daemon gracefully. If the daemon doesn't exit
            within :attr:`sigterm_timeout` seconds after SIGTERM, for example because this method hangs,
            :func:`_sigterm_guard` terminates it.
        """

    def run(self) -> None:
        """Run the daemon process.

        Make SIGTERM shut down the daemon gracefully, start :func:`_sigterm_guard`, call :func:`_startup`, start the
        logger thread and keep running execution cycles until a SIGTERM is eventually received. Also reload
        configuration upon receiving SIGHUP.
        """
        self.enable_graceful_sigterm()
        if os.name != 'nt':
            self._start_sigterm_guard()
        try:
            self._running = True
            self._startup()
            notify_systemd("READY=1")
            self.logger.start()
            while not self.received_sigterm:
                if self._received_sighup:
                    self._received_sighup = False
                    self.reload_config(True, self.config.reload_local_configuration())
                    notify_systemd("READY=1")
                    if self.received_sigterm:  # don't start the next cycle if SIGTERM was received during the reload
                        break

                self._run_cycle()
        finally:
            self._running = False
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
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
