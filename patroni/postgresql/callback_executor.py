import logging

from patroni.postgresql.cancellable import CancellableExecutor, CancellableSubprocess
from threading import Condition, Thread
from typing import List

logger = logging.getLogger(__name__)

ACTION_NOOP = "noop"
ACTION_ON_START = "on_start"
ACTION_ON_STOP = "on_stop"
ACTION_ON_RESTART = "on_restart"
ACTION_ON_RELOAD = "on_reload"
ACTION_ON_ROLE_CHANGE = "on_role_change"


class OnReloadExecutor(CancellableSubprocess):

    def call_nowait(self, cmd: List[str]) -> None:
        """Run one `on_reload` callback at most.

        To achive it we always kill already running command including child processes."""
        self.cancel(kill=True)
        self._kill_children()
        self._start_process(cmd, close_fds=True)


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

        if cmd[-3] == ACTION_ON_RELOAD:
            return self._on_reload_executor.call_nowait(cmd)

        self._kill_process()
        with self._condition:
            self._cmd = cmd
            self._condition.notify()

    def run(self):
        while True:
            with self._condition:
                if self._cmd is None:
                    self._condition.wait()
                cmd, self._cmd = self._cmd, None

            with self._lock:
                if not self._start_process(cmd, close_fds=True):
                    continue
            self._process.wait()
            self._kill_children()
