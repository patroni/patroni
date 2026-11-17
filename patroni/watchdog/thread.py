import logging
import os
import time

from threading import Condition, Thread
from typing import Callable, Optional

from .base import WatchdogBase, WatchdogError

logger = logging.getLogger(__name__)


class ThreadWatchdog(WatchdogBase):
    """Best-effort watchdog for a stalled HA loop, not a stalled process or operating system."""

    def __init__(self, timeout: int, on_timeout: Optional[Callable[[], None]] = None) -> None:
        self._timeout = timeout
        self._on_timeout = on_timeout
        self._condition = Condition()
        self._deadline: Optional[float] = None
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
            self._condition.notify_all()

    def get_timeout(self) -> int:
        return self._timeout

    def has_set_timeout(self) -> bool:
        return True

    def set_timeout(self, timeout: int) -> None:
        with self._condition:
            if self._deadline is not None and time.monotonic() < self._deadline:
                self._deadline = self._last_keepalive + timeout
            self._timeout = timeout
            self._condition.notify_all()

    def _run(self) -> None:
        with self._condition:
            while True:
                if self._deadline is None:
                    self._condition.wait()
                else:
                    remaining = self._deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)
            self._deadline = None
        try:
            if self._on_timeout is not None:
                self._on_timeout()
        except Exception:
            logger.exception('Watchdog timeout handler failed')
        finally:
            os._exit(1)
