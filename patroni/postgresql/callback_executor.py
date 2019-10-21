import logging
import psutil

from threading import Event, Lock, Thread

logger = logging.getLogger(__name__)


class CallbackExecutor(Thread):

    def __init__(self):
        super(CallbackExecutor, self).__init__()
        self.daemon = True
        self._lock = Lock()
        self._cmd = None
        self._process = None
        self._process_cmd = None
        self._process_children = []
        self._callback_event = Event()
        self.start()

    def call(self, cmd):
        self._cancel()
        self._cmd = cmd
        self._callback_event.set()

    def _cancel(self):
        with self._lock:
            if self._process and self._process.poll() is None and not self._process_children:
                try:
                    self._process_children = self._process.children(recursive=True)
                except psutil.Error:
                    pass

                try:
                    self._process.kill()
                    logger.warning('Killed the old callback process because it was still running: %s',
                                   self._process_cmd)
                except psutil.NoSuchProcess:
                    return
                except psutil.AccessDenied:
                    logger.exception('Failed to kill the old callback')

    def _wait(self):
        self._process.wait()

        waitlist = []
        with self._lock:
            for child in self._process_children:
                if child.is_running():
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        continue
                    except psutil.AccessDenied:
                        pass
                    waitlist.append(child)
        psutil.wait_procs(waitlist)

    def run(self):
        while True:
            self._callback_event.wait()
            self._callback_event.clear()
            with self._lock:
                try:
                    self._process_children = []
                    self._process_cmd = self._cmd
                    self._process = psutil.Popen(self._process_cmd, close_fds=True)
                except Exception:
                    logger.exception('Failed to execute %s',  self._cmd)
                    continue
            self._wait()
