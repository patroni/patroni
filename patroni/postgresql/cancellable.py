import logging
import os
import select
import subprocess

from threading import Lock
from typing import Any, Callable, cast, Dict, List, Optional, Tuple

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
                    logger.info('Failed to suspend the process: %s', e)

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

    @classmethod
    def _stream_lines(cls, lines: List[bytes], stream_output: Callable[[str], None]) -> None:
        for line in lines:
            if line.strip():
                stream_output(line.decode('utf-8', errors='replace'))

    @classmethod
    def _stream_data(cls, data: bytes, buf: bytearray, stream_output: Callable[[str], None]) -> None:
        start = 0
        for index, byte in enumerate(data):
            if byte in b'\r\n':
                buf.extend(data[start:index])
                cls._stream_lines([bytes(buf)], stream_output)
                buf.clear()
                start = index + 1
        buf.extend(data[start:])

    def _communicate_streaming(self, process: psutil.Popen, communicate: Dict[str, Any],
                               stream_output: Callable[[str], None]) -> None:
        """Read stdout and stderr of the running *process* in parallel until EOF.

        Every received non-empty line is immediately decoded (UTF-8, ``errors='replace'``) and passed to the
        *stream_output* callable, while the whole raw output is accumulated and stored to the *communicate*
        dict, like :meth:`~psutil.Popen.communicate` would do it.
        Both pipes are switched to the non-blocking mode and are read as data arrives (:func:`~select.poll`),
        therefore the process can't be blocked on writing to either of them.

        On Windows :func:`~select.poll` is not available, so this method falls back to
        :meth:`~psutil.Popen.communicate` and passes the output to *stream_output* after the process exits.

        .. note::
            In addition to LF, CR is also treated as a line separator when calling *stream_output* because
            ``pg_rewind --progress`` (like other PostgreSQL tools) terminates progress lines with CR.

        :param process: the running process to read the output from.
        :param communicate: :class:`dict` object where accumulated ``stdout`` and ``stderr`` will be stored.
        :param stream_output: a callable that will be executed for every line of stdout/stderr.
        """
        if os.name != 'posix':
            result = cast(Tuple[bytes, bytes], process.communicate())
            communicate['stdout'], communicate['stderr'] = result
            for data in result:
                buf = bytearray()
                self._stream_data(data, buf, stream_output)
                self._stream_lines([bytes(buf)], stream_output)
            return

        if process.stdin:
            process.stdin.close()

        output: Dict[str, bytearray] = {}
        pipes: Dict[int, Tuple[str, bytearray]] = {}
        poller = select.poll()
        for name in ('stdout', 'stderr'):
            fd = getattr(process, name).fileno()
            os.set_blocking(fd, False)
            output[name] = bytearray()
            pipes[fd] = (name, bytearray())
            poller.register(fd, select.POLLIN)

        while pipes:
            for fd, _ in poller.poll():
                try:
                    data = os.read(fd, 8192)
                except BlockingIOError:
                    continue
                name, buf = pipes[fd]
                if data:
                    output[name].extend(data)
                    self._stream_data(data, buf, stream_output)
                else:  # EOF
                    poller.unregister(fd)
                    del pipes[fd]
                    self._stream_lines([bytes(buf)], stream_output)

        communicate['stdout'], communicate['stderr'] = bytes(output['stdout']), bytes(output['stderr'])

    def call(self, *args: Any, **kwargs: Any) -> Optional[int]:
        for s in ('stdin', 'stdout', 'stderr'):
            kwargs.pop(s, None)

        stream_output: Optional[Callable[[str], None]] = kwargs.pop('stream_output', None)
        communicate: Optional[Dict[str, Any]] = kwargs.pop('communicate', None)
        if stream_output is not None:
            if not isinstance(communicate, dict):
                raise ValueError('stream_output requires a communicate dictionary')
            if communicate.get('input'):
                raise ValueError('stream_output is incompatible with communicate input')
            if kwargs.get('text') or kwargs.get('universal_newlines') \
                    or kwargs.get('encoding') is not None or kwargs.get('errors') is not None:
                raise ValueError('stream_output is incompatible with text mode')

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

        process_finished = False
        try:
            with self._lock:
                if self._is_cancelled:
                    raise PostgresException('cancelled')

                self._is_cancelled = False
                started = self._start_process(*args, **kwargs)

            if started and self._process is not None:
                if isinstance(communicate, dict):
                    if stream_output is not None and not input_data:
                        self._communicate_streaming(self._process, communicate, stream_output)
                    else:
                        communicate['stdout'], communicate['stderr'] = \
                            self._process.communicate(input_data)  # pyright: ignore [reportGeneralTypeIssues]
                return_code = self._process.wait()
                process_finished = True
                return return_code
        finally:
            if not process_finished:
                self._kill_process()
            with self._lock:
                process = self._process
                self._process = None
            if process is not None and not process_finished:
                process.wait()
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
