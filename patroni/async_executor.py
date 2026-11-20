"""Implement facilities for executing asynchronous tasks."""
import logging

from threading import Event, Lock, RLock
from types import TracebackType
from typing import Any, Callable, Optional, Tuple, Type

from . import thread_pool
from .postgresql.cancellable import CancellableSubprocess

logger = logging.getLogger(__name__)


class CriticalTask(object):
    """Represents a critical task in a background process that we either need to cancel or get the result of.

    Fields of this object may be accessed only when holding a lock on it. To perform the critical task the background
    thread must, while holding lock on this object, check ``is_cancelled`` flag, run the task and mark the task as
    complete using :func:`complete`.

    The main thread must hold async lock to prevent the task from completing, hold lock on critical task object,
    call :func:`cancel`. If the task has completed :func:`cancel` will return ``False`` and ``result`` field will
    contain the result of the task. When :func:`cancel` returns ``True`` it is guaranteed that the background task will
    notice the ``is_cancelled`` flag.

    :ivar is_cancelled: if the critical task has been cancelled.
    :ivar result: contains the result of the task, if it has already been completed.
    """

    def __init__(self) -> None:
        """Create a new instance of :class:`CriticalTask`.

        Instantiate the lock and the task control attributes.
        """
        self._lock = Lock()
        self.is_cancelled = False
        self.result = None

    def reset(self) -> None:
        """Must be called every time the background task is finished.

        .. note::
            Must be called from async thread. Caller must hold lock on async executor when calling.
        """
        self.is_cancelled = False
        self.result = None

    def cancel(self) -> bool:
        """Tries to cancel the task.

        .. note::
            Caller must hold lock on async executor and the task when calling.

        :returns: ``False`` if the task has already run, or ``True`` it has been cancelled.
        """
        if self.result is not None:
            return False
        self.is_cancelled = True
        return True

    def complete(self, result: Any) -> None:
        """Mark task as completed along with a *result*.

        .. note::
            Must be called from async thread. Caller must hold lock on task when calling.
        """
        self.result = result

    def __enter__(self) -> 'CriticalTask':
        """Acquire the object lock when entering the context manager."""
        self._lock.acquire()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        """Release the object lock when exiting the context manager."""
        self._lock.release()


class AsyncExecutor(object):
    """Asynchronous executor of (long) tasks.

    :ivar critical_task: a :class:`CriticalTask` instance to handle execution of critical background tasks.
    """

    def __init__(self, cancellable: CancellableSubprocess, ha_wakeup: Callable[..., None]) -> None:
        """Create a new instance of :class:`AsyncExecutor`.

        Configure the given *cancellable* and *ha_wakeup*, initializes the control attributes, and instantiate the lock
        and event objects that are used to access attributes and manage communication between threads.

        :param cancellable: a subprocess that supports being cancelled.
        :param ha_wakeup: function to wake up the HA loop.
        """
        self._cancellable = cancellable
        self._ha_wakeup = ha_wakeup
        self._thread_lock = RLock()
        self._scheduled_action: Optional[str] = None
        self._scheduled_action_lock = RLock()
        self._is_cancelled = False
        self._is_shut_down = False
        # Number of tasks being executed and an event that is set when there are none.
        # Usually there is at most one task, but it is possible to get more, for example when a pending REST API
        # restart gets cancelled by a forced reinitialize and starts afterwards anyway.
        self._running = 0
        self._finish_event = Event()
        self._finish_event.set()
        self.critical_task = CriticalTask()

    @property
    def busy(self) -> bool:
        """``True`` if there is an action scheduled to occur, else ``False``."""
        return self.scheduled_action is not None

    def schedule(self, action: str) -> Optional[str]:
        """Schedule *action* to be executed.

        .. note::
            Must be called before executing a task.

        .. note::
            *action* can only be scheduled if there is no other action currently scheduled, and the executor isn't
            shut down.

        :param action: action to be executed.

        :returns: ``None`` if *action* has been successfully scheduled, ``shutdown`` if the executor is shut down, or
            the previously scheduled action, if any.
        """
        with self._scheduled_action_lock:
            if self._is_shut_down:
                return 'shutdown'
            if self._scheduled_action is not None:
                return self._scheduled_action
            self._scheduled_action = action
            self._is_cancelled = False
        return None

    @property
    def is_shut_down(self) -> bool:
        """``True`` if the executor is shut down and refuses to schedule or run tasks."""
        with self._scheduled_action_lock:
            return self._is_shut_down

    @property
    def scheduled_action(self) -> Optional[str]:
        """The currently scheduled action, if any, else ``None``."""
        with self._scheduled_action_lock:
            return self._scheduled_action

    def reset_scheduled_action(self) -> None:
        """Unschedule a previously scheduled action, if any.

        .. note::
            Must be called once the scheduled task finishes or is cancelled.
        """
        with self._scheduled_action_lock:
            self._scheduled_action = None

    def run(self, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[Any]:
        """Run *func* with *args*.

        .. note::
            Expected to be executed through a thread.

        :param func: function to be run. If it returns anything other than ``None``, HA loop will be woken up at the end
            of :func:`run` execution.
        :param args: arguments to be passed to *func*.

        :returns: ``None`` if *func* execution has been cancelled, the executor is shut down, or *func* faced any
            exception, otherwise the result of *func*.
        """
        wakeup = False
        started = False
        try:
            with self:
                if self._is_cancelled or self._is_shut_down:
                    return
                self._running += 1
                started = True
                self._finish_event.clear()
                # must be done while holding the lock, otherwise we could reset the cancellation done by cancel()
                self._cancellable.reset_is_cancelled()

            # if the func returned something (not None) - wake up main HA loop
            wakeup = func(*args) if args else func()
            return wakeup
        except Exception:
            logger.exception('Exception during execution of long running task %s', self.scheduled_action)
        finally:
            with self:
                self.reset_scheduled_action()
                if started:
                    self._running -= 1
                if not self._running:
                    self._finish_event.set()
                with self.critical_task:
                    self.critical_task.reset()
            if wakeup is not None:
                self._ha_wakeup()

    def run_async(self, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> None:
        """Start an async thread that runs *func* with *args*.

        :param func: function to be run.
        :param args: arguments to be passed along with *func*.
        """
        thread_pool.get_executor().submit(self.run, func, args)

    def try_run_async(self, action: str, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[str]:
        """Try to run an async task, if none is currently being executed.

        :param action: name of the task to be executed.
        :param func: actual function that performs the task *action*.
        :param args: arguments to be passed to *func*.

        :returns: ``None`` if *func* was scheduled successfully, otherwise an error message informing of an already
            ongoing task.
        """
        prev = self.schedule(action)
        if prev is None:
            return self.run_async(func, args)
        return 'Failed to run {0}, {1} is already in progress'.format(action, prev)

    def cancel(self, unless: Optional[Callable[[], bool]] = None) -> bool:
        """Request cancellation of a scheduled async task and running tasks, if any.

        .. note::
            Wait until all running tasks are finished before returning control to caller. The lock isn't held while
            waiting. Tasks scheduled meanwhile, for example a promotion by the HA loop, are not affected.

        :param unless: called while holding the lock, right before the cancellation is requested. Nothing is cancelled
            if it returns ``True``.

        :returns: ``False`` if *unless* prevented the cancellation, otherwise ``True``.
        """
        with self:
            if unless and unless():
                return False
            with self._scheduled_action_lock:
                if self._scheduled_action is not None:
                    logger.warning('Cancelling long running task %s', self._scheduled_action)
                elif not self._running:
                    return True
            self._is_cancelled = True

        self._cancellable.cancel()
        self._finish_event.wait()

        with self, self._scheduled_action_lock:
            # the flag is reset by schedule(), a task scheduled while waiting must stay scheduled
            if self._is_cancelled:
                self.reset_scheduled_action()
        return True

    def shutdown(self) -> None:
        """Cancel the scheduled async task and running tasks, if any, and refuse to schedule or run new tasks.

        .. note::
            Wait until all running tasks are finished before returning control to caller.
        """
        with self:
            with self._scheduled_action_lock:
                self._is_shut_down = True
        self.cancel()

    def __enter__(self) -> 'AsyncExecutor':
        """Acquire the thread lock when entering the context manager."""
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        """Release the thread lock when exiting the context manager.

        .. note::
            The arguments are not used, but we need them to match the expected method signature.
        """
        self._thread_lock.release()
