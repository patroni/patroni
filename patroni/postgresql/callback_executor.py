import logging

from patroni.postgresql.cancellable import CancellableExecutor
from threading import Event, Thread

logger = logging.getLogger(__name__)


class CallbackExecutor(CancellableExecutor, Thread):

    def __init__(self):
        CancellableExecutor.__init__(self)
        Thread.__init__(self)
        self.daemon = True
        self._cmd = None
        self._callback_event = Event()
        self.start()

    def call(self, cmd):
        self._kill_process()
        self._cmd = cmd
        self._callback_event.set()

    def run(self):
        while True:
            self._callback_event.wait()
            self._callback_event.clear()
            with self._lock:
                if not self._start_process(self._cmd, close_fds=True):
                    continue
            self._process.wait()
            self._kill_children()
