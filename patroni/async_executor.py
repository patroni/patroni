import logging
from threading import Lock, Thread

logger = logging.getLogger(__name__)


class AsyncExecutor(object):

    def __init__(self):
        self._busy = False
        self._thread_lock = Lock()
        self._scheduled_action = None
        self._scheduled_action_lock = Lock()

    @property
    def busy(self):
        return self._busy

    def schedule(self, action, immediately=False):
        with self._scheduled_action_lock:
            if self._scheduled_action is not None:
                return self._scheduled_action
            self._scheduled_action = action
            self._busy = immediately
        return None

    @property
    def scheduled_action(self):
        with self._scheduled_action_lock:
            return self._scheduled_action

    def reset_scheduled_action(self):
        with self._scheduled_action_lock:
            self._scheduled_action = None

    def run(self, func, args=()):
        try:
            return func(*args) if args else func()
        except:
            logger.exception('Exception during execution of long running task %s', self.scheduled_action)
        finally:
            with self:
                self._busy = False
                self.reset_scheduled_action()

    def run_async(self, func, args=()):
        self._busy = True
        Thread(target=self.run, args=(func, args)).start()

    def __enter__(self):
        self._thread_lock.acquire()

    def __exit__(self, *args):
        self._thread_lock.release()
