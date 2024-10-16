import logging
import subprocess

from threading import Lock
from typing import Any, Dict, List, Optional

import psutil

from patroni.exceptions import PostgresException
from patroni.utils import polling_loop

logger = logging.getLogger(__name__)


class CancellableExecutor(object):

    """
    There must be only one such process so that AsyncExecutor can easily cancel it.
    """

    def __init__(self) -> None:
        self._process = None
        self._process_cmd = None
        self._process_children: List[psutil.Process] = []
        self._lock = Lock()

    def _start_process(self, cmd: List[str], *args: Any, **kwargs: Any) -> Optional[bool]:
        """This method must be executed only when the `_lock` is acquired"""

        try:
            self._process_children = []
            self._process_cmd = cmd
            self._process = psutil.Popen(cmd, *args, **kwargs)
        except Exception:
            return logger.exception('Failed to execute %s', cmd)
        return True

    def _kill_process(self) -> None:
        with self._lock:
            if self._process is not None and self._process.is_running() and not self._process_children:
                try:
                    self._process.suspend()  # Suspend the process before getting list of children
                except psutil.Error as e:
                    logger.info('Failed to suspend the process: %s', e.msg)

                try:
                    self._process_children = self._process.children(recursive=True)
                except psutil.Error:
                    pass

                try:
                    self._process.kill()
                    logger.warning('Killed %s because it was still running', self._process_cmd)
                except psutil.NoSuchProcess:
                    pass
                except psutil.AccessDenied as e:
                    logger.warning('Failed to kill the process: %s', e.msg)

    def _kill_children(self) -> None:
        waitlist: List[psutil.Process] = []
        with self._lock:
            for child in self._process_children:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    continue
                except psutil.AccessDenied as e:
                    logger.info('Failed to kill child process: %s', e.msg)
                waitlist.append(child)
        psutil.wait_procs(waitlist)


class CancellableSubprocess(CancellableExecutor):

    def __init__(self) -> None:
        super(CancellableSubprocess, self).__init__()
        self._is_cancelled = False

    def call(self, *args: Any, **kwargs: Any) -> Optional[int]:
        for s in ('stdin', 'stdout', 'stderr'):
            kwargs.pop(s, None)

        communicate: Optional[Dict[str, str]] = kwargs.pop('communicate', None)
        input_data = None
        if isinstance(communicate, dict):
            input_data = communicate.get('input')
            if input_data:
                if input_data[-1] != '\n':
                    input_data += '\n'
                input_data = input_data.encode('utf-8')
            kwargs['stdin'] = subprocess.PIPE
            kwargs['stdout'] = subprocess.PIPE
            kwargs['stderr'] = subprocess.PIPE

        try:
            with self._lock:
                if self._is_cancelled:
                    raise PostgresException('cancelled')

                self._is_cancelled = False
                started = self._start_process(*args, **kwargs)

            if started and self._process is not None:
                if isinstance(communicate, dict):
                    communicate['stdout'], communicate['stderr'] = \
                        self._process.communicate(input_data)  # pyright: ignore [reportGeneralTypeIssues]
                return self._process.wait()
        finally:
            with self._lock:
                self._process = None
            self._kill_children()

    def reset_is_cancelled(self) -> None:
        with self._lock:
            self._is_cancelled = False

    @property
    def is_cancelled(self) -> bool:
        with self._lock:
            return self._is_cancelled

    def cancel(self, kill: bool = False) -> None:
        with self._lock:
            self._is_cancelled = True
            if self._process is None or not self._process.is_running():
                return

            logger.info('Terminating %s', self._process_cmd)
            self._process.terminate()

        for _ in polling_loop(10):
            with self._lock:
                if self._process is None or not self._process.is_running():
                    return
            if kill:
                break

        self._kill_process()
