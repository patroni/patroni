"""Implement facilities for executing asynchronous tasks."""
import logging

from threading import Condition, Lock, RLock
from types import TracebackType
from typing import Any, Callable, Optional, Tuple, Type, Union

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
        """Clear the result and cancellation flag when the scheduled action is retired.

        .. note::
            Caller must hold the async executor and critical task locks.
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


class _ScheduledAction(object):
    """One reservation. Its name may be reused while a cancelled callback is still queued."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.running = False
        self.condition = Condition()


class AsyncExecutor(object):
    """Asynchronous executor of (long) tasks.

    :ivar critical_task: a :class:`CriticalTask` instance to handle execution of critical background tasks.
    """

    def __init__(self, cancellable: CancellableSubprocess, ha_wakeup: Callable[..., None]) -> None:
        """Create a new instance of :class:`AsyncExecutor`.

        Configure the given *cancellable* and *ha_wakeup* and initialize the locks protecting the current action.

        :param cancellable: a subprocess that supports being cancelled.
        :param ha_wakeup: function to wake up the HA loop.
        """
        self._cancellable = cancellable
        self._ha_wakeup = ha_wakeup
        self._thread_lock = RLock()
        self._scheduled_action: Optional[_ScheduledAction] = None
        self._scheduled_action_lock = RLock()
        self.critical_task = CriticalTask()

    @property
    def busy(self) -> bool:
        """``True`` if there is an action scheduled to occur, else ``False``."""
        return self.scheduled_action is not None

    def schedule(self, action: str) -> Union[_ScheduledAction, str]:
        """Schedule *action* to be executed.

        .. note::
            Must be called before executing a task.

        .. note::
            *action* can only be scheduled if there is no other action currently scheduled.

        :param action: action to be executed.

        :returns: the reservation to pass to :meth:`run` or :meth:`run_async`, or the name of the conflicting action.
        """
        with self._scheduled_action_lock:
            if self._scheduled_action is not None:
                return self._scheduled_action.name
            self._scheduled_action = _ScheduledAction(action)
            return self._scheduled_action

    @property
    def current_action(self) -> Optional[_ScheduledAction]:
        """The current reservation, which can be passed to :meth:`cancel`."""
        with self._scheduled_action_lock:
            return self._scheduled_action

    @property
    def scheduled_action(self) -> Optional[str]:
        """The name of the currently scheduled action, if any."""
        action = self.current_action
        return action.name if action else None

    def reset_scheduled_action(self) -> None:
        """Unschedule a previously scheduled action, if any.

        .. note::
            Caller must hold the async executor lock. A running action is retired by its worker, a queued one by
            the thread that cancels it.
        """
        with self._scheduled_action_lock:
            with self.critical_task:
                self.critical_task.reset()
            self._scheduled_action = None

    def run(self, action: _ScheduledAction, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[Any]:
        """Run *func* with *args*.

        .. note::
            Expected to be executed through a thread.

        :param action: reservation returned by :meth:`schedule`.
        :param func: function to be run. If it returns anything other than ``None``, HA loop will be woken up at the end
            of :func:`run` execution.
        :param args: arguments to be passed to *func*.

        :returns: ``None`` if *func* execution has been cancelled or faced any exception, otherwise the result of
            *func*.
        """
        with self:
            if self._scheduled_action is not action or action.running:
                return None
            self._cancellable.reset_is_cancelled()
            action.running = True

        wakeup = False
        try:
            # if the func returned something (not None) - wake up main HA loop
            wakeup = func(*args) if args else func()
            return wakeup
        except Exception:
            logger.exception('Exception during execution of long running task %s', action.name)
        finally:
            with action.condition:
                with self:
                    self.reset_scheduled_action()
                action.condition.notify_all()
            if wakeup is not None:
                self._ha_wakeup()

    def run_async(self, action: _ScheduledAction, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> None:
        """Start an async thread that runs *func* with *args*.

        :param action: reservation returned by :meth:`schedule`.
        :param func: function to be run.
        :param args: arguments to be passed along with *func*.
        """
        thread_pool.get_executor().submit(self.run, action, func, args)

    def try_run_async(self, action: str, func: Callable[..., Any], args: Tuple[Any, ...] = ()) -> Optional[str]:
        """Try to run an async task, if none is currently being executed.

        :param action: name of the task to be executed.
        :param func: actual function that performs the task *action*.
        :param args: arguments to be passed to *func*.

        :returns: ``None`` if *func* was scheduled successfully, otherwise an error message informing of an already
            ongoing task.
        """
        reservation = self.schedule(action)
        if isinstance(reservation, str):
            return 'Failed to run {0}, {1} is already in progress'.format(action, reservation)
        return self.run_async(reservation, func, args)

    def cancel(self, expected: Optional[_ScheduledAction] = None) -> None:
        """Request cancellation of a scheduled async task, if any.

        .. note::
            Wait until task is cancelled before returning control to caller.

        :param expected: if given, cancel only this reservation, never its replacement.
        """
        with self, self._scheduled_action_lock:
            action = self._scheduled_action
            if action is None or expected is not None and action is not expected:
                return
            logger.warning('Cancelling long running task %s', action.name)
            if not action.running:
                self.reset_scheduled_action()
                return

        # Keep the shared subprocess assigned to this action until cancel() finishes signalling it.
        # The worker alone releases a running reservation; wait without holding the HA lock.
        with action.condition:
            if self._scheduled_action is action:
                self._cancellable.cancel()
                action.condition.wait_for(lambda: self._scheduled_action is not action)

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
