import faulthandler
import logging
import os
import signal
import sys
import time

from threading import Condition, current_thread, main_thread, Thread
from typing import Callable, Optional

from .base import WatchdogBase, WatchdogError

logger = logging.getLogger(__name__)

HANDLER_TIMEOUT = 5


class ThreadWatchdog(WatchdogBase):
    """Best-effort watchdog for a stalled HA loop, not a stalled process or operating system."""

    def __init__(self, timeout: int, on_timeout: Optional[Callable[[], None]] = None) -> None:
        self._timeout = timeout
        self._on_timeout = on_timeout
        self._condition = Condition()
        self._deadline: Optional[float] = None
        self._expired = False
        self._last_keepalive = 0.0
        self._thread = Thread(target=self._run, name='watchdog', daemon=True)
        try:
            self._thread.start()
        except RuntimeError as e:
            raise WatchdogError('Could not start watchdog thread: {0}'.format(e))

    @property
    def is_running(self) -> bool:
        with self._condition:
            return self._deadline is not None

    @property
    def is_healthy(self) -> bool:
        return True

    def open(self) -> None:
        if os.name != 'nt' and current_thread() is main_thread() and \
                signal.getsignal(signal.SIGALRM) == signal.SIG_IGN:
            # An ignored SIGALRM is inherited across exec and would make the alarm in _run() a no-op. Reset it only
            # when arming, so that Patroni doesn't change it unless the fallback is used.
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
        with self._condition:
            if self._deadline is not None:
                return
            self._last_keepalive = time.monotonic()
            self._deadline = self._last_keepalive + self._timeout
            self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            self._deadline = None
            self._condition.notify_all()

    def keepalive(self) -> None:
        with self._condition:
            now = time.monotonic()
            if self._deadline is not None and now < self._deadline:
                self._last_keepalive = now
                self._deadline = now + self._timeout

    def wait(self) -> None:
        """Block while armed. Expiry terminates the process, so this only returns if the watchdog is disarmed."""
        with self._condition:
            if self._deadline is not None:
                logger.warning('Watchdog thread is still armed, waiting for it to expire')
            while self._deadline is not None or self._expired:
                try:
                    self._condition.wait()
                except KeyboardInterrupt:
                    # Exiting the interpreter would end this daemon thread before it stops PostgreSQL
                    logger.warning('Ignoring interrupt, PostgreSQL must be stopped by the watchdog thread first')

    def get_timeout(self) -> int:
        return self._timeout

    def has_set_timeout(self) -> bool:
        return True

    def set_timeout(self, timeout: int) -> None:
        with self._condition:
            if self._deadline is not None and time.monotonic() < self._deadline:
                self._deadline = self._last_keepalive + timeout
            self._timeout = timeout

    def _run(self) -> None:
        while True:
            with self._condition:
                while self._deadline is None:
                    self._condition.wait()
                remaining = self._deadline - time.monotonic()
                if remaining <= 0:
                    self._deadline = None
                    self._expired = True
                    break
            # Condition.wait(timeout) sleeps until an absolute CLOCK_REALTIME deadline when Python lacks
            # sem_clockwait() (before 3.9.8 or with glibc older than 2.30), so stepping the clock back would delay
            # expiry. time.sleep() is monotonic, and short sleeps pick up a reduced timeout without a wakeup.
            time.sleep(min(remaining, 1))
        if os.name != 'nt':
            # Armed first, because arming it can't block. faulthandler can still block while writing the dump to a
            # stalled stderr pipe, but the default action of SIGALRM terminates the process in the kernel, without a
            # thread or the GIL.
            signal.alarm(2 * HANDLER_TIMEOUT)
        try:
            # os._exit() must not depend on the handler returning, e.g. while it waits for a process stuck in
            # uninterruptible I/O. faulthandler also dumps the stacks of all threads, including the stalled HA loop.
            # Unlike the default sys.stderr, a file descriptor isn't flushed when arming: the flush would wait for the
            # lock that the logger thread holds while its write to a stalled stderr pipe blocks.
            faulthandler.dump_traceback_later(HANDLER_TIMEOUT, exit=True, file=sys.stderr.fileno())
        except Exception as e:
            logger.warning('Could not limit watchdog timeout handler duration: %s', e)
        try:
            if self._on_timeout is not None:
                self._on_timeout()
        except Exception:
            logger.exception('Watchdog timeout handler failed')
        finally:
            os._exit(1)
