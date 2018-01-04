import logging
from threading import Event, Lock, RLock, Thread

logger = logging.getLogger(__name__)


class CriticalTask(object):
    """Represents a critical task in a background process that we either need to cancel or get the result of.

    Fields of this object may be accessed only when holding a lock on it. To perform the critical task the background
    thread must, while holding lock on this object, check `is_cancelled` flag, run the task and mark the task as
    complete using `complete()`.

    The main thread must hold async lock to prevent the task from completing, hold lock on critical task object,
    call cancel. If the task has completed `cancel()` will return False and `result` field will contain the result of
    the task. When cancel returns True it is guaranteed that the background task will notice the `is_cancelled` flag.
    """
    def __init__(self):
        self._lock = Lock()
        self.is_cancelled = False
        self.result = None

    def reset(self):
        """Must be called every time the background task is finished.

        Must be called from async thread. Caller must hold lock on async executor when calling."""
        self.is_cancelled = False
        self.result = None

    def cancel(self):
        """Tries to cancel the task, returns True if the task has already run.

        Caller must hold lock on async executor and the task when calling."""
        if self.result is not None:
            return False
        self.is_cancelled = True
        return True

    def complete(self, result):
        """Mark task as completed along with a result.

        Must be called from async thread. Caller must hold lock on task when calling."""
        self.result = result

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()


class AsyncExecutor(object):

    def __init__(self, state_handler, ha_wakeup):
        self.state_handler = state_handler
        self._ha_wakeup = ha_wakeup
        self._thread_lock = RLock()
        self._scheduled_action = None
        self._scheduled_action_lock = RLock()
        self._is_cancelled = False
        self._finish_event = Event()
        self.critical_task = CriticalTask()

    @property
    def busy(self):
        return self.scheduled_action is not None

    def schedule(self, action):
        with self._scheduled_action_lock:
            if self._scheduled_action is not None:
                return self._scheduled_action
            self._scheduled_action = action
            self._is_cancelled = False
            self._finish_event.set()
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
            with self:
                if self._is_cancelled:
                    return
                self._finish_event.clear()

            self.state_handler.reset_is_cancelled()
            # if the func returned something (not None) - wake up main HA loop
            wakeup = func(*args) if args else func()
            return wakeup
        except Exception:
            logger.exception('Exception during execution of long running task %s', self.scheduled_action)
        finally:
            with self:
                self.reset_scheduled_action()
                self._finish_event.set()
                with self.critical_task:
                    self.critical_task.reset()
            if wakeup is not None:
                self._ha_wakeup()

    def run_async(self, func, args=()):
        Thread(target=self.run, args=(func, args)).start()

    def cancel(self):
        with self:
            with self._scheduled_action_lock:
                if self._scheduled_action is None:
                    return
                logger.warning('Cancelling long running task %s', self._scheduled_action)
            self._is_cancelled = True

        self.state_handler.cancel()
        self._finish_event.wait()

        with self:
            self.reset_scheduled_action()

    def __enter__(self):
        self._thread_lock.acquire()

    def __exit__(self, *args):
        self._thread_lock.release()
