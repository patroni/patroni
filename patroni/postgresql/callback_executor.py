import logging
import sys

from enum import Enum
from threading import Condition, Thread
from typing import Any, Dict, List

from .cancellable import CancellableExecutor, CancellableSubprocess

logger = logging.getLogger(__name__)


class CallbackAction(str, Enum):
    NOOP = "noop"
    ON_START = "on_start"
    ON_STOP = "on_stop"
    ON_RESTART = "on_restart"
    ON_RELOAD = "on_reload"
    ON_ROLE_CHANGE = "on_role_change"

    def __repr__(self) -> str:
        return self.value


class OnReloadExecutor(CancellableSubprocess):

    def call_nowait(self, cmd: List[str]) -> None:
        """Run one `on_reload` callback at most.

        To achieve it we always kill already running command including child processes."""
        self.cancel(kill=True)
        self._kill_children()
        with self._lock:
            started = self._start_process(cmd, close_fds=True)
        if started and self._process is not None:
            Thread(target=self._process.wait).start()


class CallbackExecutor(CancellableExecutor, Thread):

    def __init__(self):
        CancellableExecutor.__init__(self)
        Thread.__init__(self)
        self.daemon = True
        self._on_reload_executor = OnReloadExecutor()
        self._cmd = None
        self._condition = Condition()
        self.start()

    def call(self, cmd: List[str]) -> None:
        """Executes one callback at a time.

        Already running command is killed (including child processes).
        If it couldn't be killed we wait until it finishes.

        :param cmd: command to be executed"""
        kwargs: Dict[str, Any] = {'stacklevel': 3} if sys.version_info >= (3, 8) else {}
        logger.debug('CallbackExecutor.call(%s)', cmd, **kwargs)

        if cmd[-3] == CallbackAction.ON_RELOAD:
            return self._on_reload_executor.call_nowait(cmd)

        self._kill_process()
        with self._condition:
            self._cmd = cmd
            self._condition.notify()

    def run(self) -> None:
        while True:
            with self._condition:
                if self._cmd is None:
                    self._condition.wait()
                cmd, self._cmd = self._cmd, None

            if cmd is not None:
                with self._lock:
                    if not self._start_process(cmd, close_fds=True):
                        continue
                if self._process:
                    self._process.wait()
                    self._kill_children()
