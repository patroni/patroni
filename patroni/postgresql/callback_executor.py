import logging

from patroni.postgresql.cancellable import CancellableExecutor
from threading import Condition, Thread

logger = logging.getLogger(__name__)


class CallbackExecutor(CancellableExecutor, Thread):

    def __init__(self):
        CancellableExecutor.__init__(self)
        Thread.__init__(self)
        self.daemon = True
        self._cmd = None
        self._condition = Condition()
        self.start()

    def call(self, cmd):
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
