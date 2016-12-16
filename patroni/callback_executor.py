import logging
import subprocess
from threading import Event, Lock, Thread

logger = logging.getLogger(__name__)


class CallbackExecutor(Thread):

    def __init__(self):
        super(CallbackExecutor, self).__init__()
        self.daemon = True
        self._lock = Lock()
        self._cmd = None
        self._process = None
        self._callback_event = Event()
        self.start()

    def call(self, cmd):
        with self._lock:
            if self._process and self._process.poll() is None:
                self._process.kill()
                logger.warning('Killed the old callback process because it was still running: %s', self._cmd)
        self._cmd = cmd
        self._callback_event.set()

    def run(self):
        while True:
            self._callback_event.wait()
            self._callback_event.clear()
            with self._lock:
                try:
                    self._process = subprocess.Popen(self._cmd, close_fds=True)
                except Exception:
                    logger.exception('Failed to execute %s',  self._cmd)
                    continue
            self._process.wait()
