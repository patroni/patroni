import logging

from threading import Event, Lock, RLock, Thread
from types import TracebackType
from typing import Any, Callable, Optional, Tuple, Type

from .postgresql.cancellable import CancellableSubprocess

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
    def __init__(self) -> None:
        self._lock = Lock()
        self.is_cancelled = False
        self.result = None

    def reset(self) -> None:
        """Must be called every time the background task is finished.

        Must be called from async thread. Caller must hold lock on async executor when calling."""
        self.is_cancelled = False
        self.result = None

    def cancel(self) -> bool:
        """Tries to cancel the task, returns True if the task has already run.

        Caller must hold lock on async executor and the task when calling."""
        if self.result is not None:
            return False
        self.is_cancelled = True
        return True

    def complete(self, result: Any) -> None:
        """Mark task as completed along with a result.

        Must be called from async thread. Caller must hold lock on task when calling."""
        self.result = result

    def __enter__(self) -> 'CriticalTask':
        self._lock.acquire()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self._lock.release()


class AsyncExecutor(object):

    def __init__(self, cancellable: CancellableSubprocess, ha_wakeup: Callable[..., None]) -> None:
        self._cancellable = cancellable
        self._ha_wakeup = ha_wakeup
        self._thread_lock = RLock()
        self._scheduled_action: Optional[str] = None
        self._scheduled_action_lock = RLock()
        self._is_cancelled = False
        self._finish_event = Event()
        self.critical_task = CriticalTask()

    @property
    def busy(self) -> bool:
        return self.scheduled_action is not None

    def schedule(self, action: str) -> Optional[str]:
        with self._scheduled_action_lock:
            if self._scheduled_action is not None:
                return self._scheduled_action
            self._scheduled_action = action
            self._is_cancelled = False
            self._finish_event.set()
        return None

    @property
    def scheduled_action(self) -> Optional[str]:
        with self._scheduled_action_lock:
            return self._scheduled_action

    def reset_scheduled_action(self) -> None:
        with self._scheduled_action_lock:
            self._scheduled_action = None

    def run(self, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[bool]:
        wakeup = False
        try:
            with self:
                if self._is_cancelled:
                    return
                self._finish_event.clear()

            self._cancellable.reset_is_cancelled()
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

    def run_async(self, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> None:
        Thread(target=self.run, args=(func, args)).start()

    def try_run_async(self, action: str, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[str]:
        prev = self.schedule(action)
        if prev is None:
            return self.run_async(func, args)
        return 'Failed to run {0}, {1} is already in progress'.format(action, prev)

    def cancel(self) -> None:
        with self:
            with self._scheduled_action_lock:
                if self._scheduled_action is None:
                    return
                logger.warning('Cancelling long running task %s', self._scheduled_action)
            self._is_cancelled = True

        self._cancellable.cancel()
        self._finish_event.wait()

        with self:
            self.reset_scheduled_action()

    def __enter__(self) -> 'AsyncExecutor':
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self._thread_lock.release()
