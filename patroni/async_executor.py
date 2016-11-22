import logging
from threading import RLock, Thread

logger = logging.getLogger(__name__)


class AsyncExecutor(object):

    def __init__(self, ha_wakeup):
        self._ha_wakeup = ha_wakeup
        self._thread_lock = RLock()
        self._scheduled_action = None
        self._scheduled_action_lock = RLock()

    @property
    def busy(self):
        return self.scheduled_action is not None

    def schedule(self, action, immediately=False):
        with self._scheduled_action_lock:
            if self._scheduled_action is not None:
                return self._scheduled_action
            self._scheduled_action = action
        return None

    @property
    def scheduled_action(self):
        with self._scheduled_action_lock:
            return self._scheduled_action

    def reset_scheduled_action(self):
        with self._scheduled_action_lock:
            self._scheduled_action = None

    def run(self, func, args=()):
        wakeup = False
        try:
            # if the func returned something (not None) - wake up main HA loop
            wakeup = func(*args) if args else func()
            return wakeup
        except:
            logger.exception('Exception during execution of long running task %s', self.scheduled_action)
        finally:
            with self:
                self.reset_scheduled_action()
            if wakeup is not None:
                self._ha_wakeup()

    def run_async(self, func, args=()):
        Thread(target=self.run, args=(func, args)).start()

    def __enter__(self):
        self._thread_lock.acquire()

    def __exit__(self, *args):
        self._thread_lock.release()
