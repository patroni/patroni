import logging
import subprocess

from io import BufferedReader
from threading import Lock
from typing import Any, Callable, cast, Dict, List, Optional, Tuple

import psutil

from patroni import thread_pool
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

    def _communicate_streaming(self, stream_cb: Callable[[str, bytes], None]) -> Tuple[bytes, bytes]:
        """Read the running process' stdout/stderr line-by-line as it arrives.

        ``stderr`` is drained by a single helper thread while ``stdout`` is read inline, so both pipes are drained
        concurrently with only one extra thread. ``read1()`` returns as soon as any data is available, so slowly
        produced lines are delivered promptly. Exceptions from ``stream_cb`` are swallowed so streaming continues;
        only read/OS errors abort (after both streams are drained).

        :param stream_cb: called as ``stream_cb(stream_name, line)`` for every complete line of *stdout*/*stderr*.

        :returns: ``(stdout_bytes, stderr_bytes)``.
        """
        process = cast(psutil.Popen, self._process)

        stdout_chunks: List[bytes] = []
        stderr_chunks: List[bytes] = []

        def emit(name: str, line: bytes) -> None:
            try:
                stream_cb(name, line.rstrip(b'\r'))  # drop trailing CR (CRLF on Windows)
            except Exception:
                pass  # keep streaming despite a bad callback

        def reader(stream: BufferedReader, name: str, sink: List[bytes]) -> None:
            buf = b''
            try:
                while True:
                    # read1() -> a single underlying read: returns as soon as data is available
                    chunk = stream.read1(32768)
                    if not chunk:  # b'' -> EOF
                        break
                    sink.append(chunk)
                    buf += chunk
                    *lines, buf = buf.split(b'\n')
                    for line in lines:
                        emit(name, line)
                if buf:  # trailing data without a final newline
                    emit(name, buf)
            finally:
                stream.close()

        # A single helper task (from the global pool) drains stderr; stdout is read inline below.
        stderr_future = thread_pool.get_executor().submit(reader, process.stderr, 'stderr', stderr_chunks) \
            if process.stderr else None

        try:
            if process.stdout:
                reader(process.stdout, 'stdout', stdout_chunks)
        finally:
            if stderr_future is not None:
                stderr_future.result()

        return b''.join(stdout_chunks), b''.join(stderr_chunks)

    def call(self, *args: Any, **kwargs: Any) -> Optional[int]:
        """Start a cancellable subprocess, optionally capture/stream its output, and wait for it to finish.

        Positional and keyword arguments are forwarded to :class:`psutil.Popen`, except for the two special
        keyword arguments below, which are consumed here.

        :param communicate: optional :class:`dict` used to exchange data with the process. When provided,
                            *stdout* and *stderr* are captured and stored back into it under the ``stdout`` and
                            ``stderr`` keys, and an optional ``input`` value is sent to *stdin*.
        :param stream_cb: optional callback invoked as ``stream_cb(stream_name, line)`` for every complete line
                          of *stdout*/*stderr* as it arrives. Effective only when there is no ``input`` to send;
                          forces binary pipes.

        :raises PostgresException: if the executor was already cancelled.

        :returns: the process' exit code, or ``None`` if it could not be started.
        """
        communicate: Optional[Dict[str, Any]] = kwargs.pop('communicate', None)
        input_data = None
        if isinstance(communicate, dict):
            input_data = communicate.get('input')
            if input_data:
                if input_data[-1] != '\n':
                    input_data += '\n'
                input_data = input_data.encode('utf-8')

        stream_cb: Optional[Callable[[str, bytes], None]] = kwargs.pop('stream_cb', None)
        if stream_cb or isinstance(communicate, dict):
            kwargs['stdin'] = subprocess.PIPE if input_data else subprocess.DEVNULL
            kwargs['stdout'] = subprocess.PIPE
            kwargs['stderr'] = subprocess.PIPE

        if stream_cb and not input_data:
            # streaming reads raw bytes, so force binary pipes
            for key in ('text', 'universal_newlines', 'encoding', 'errors'):
                kwargs.pop(key, None)

        try:
            with self._lock:
                if self._is_cancelled:
                    raise PostgresException('cancelled')

                self._is_cancelled = False
                started = self._start_process(*args, **kwargs)

            if started and self._process is not None:
                if stream_cb and not input_data:
                    stdout, stderr = self._communicate_streaming(stream_cb)
                    if isinstance(communicate, dict):
                        # hand the full captured output back to the caller, if requested
                        communicate.update(stdout=stdout, stderr=stderr)
                elif isinstance(communicate, dict):
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
