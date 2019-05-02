import logging
import os
import subprocess

from patroni.exceptions import PostgresException
from patroni.utils import polling_loop
from six import string_types
from threading import Lock

logger = logging.getLogger(__name__)


class CancellableSubprocess(object):

    def __init__(self):
        self._is_cancelled = False
        self._process = None
        self._lock = Lock()

    def call(self, *args, **kwargs):
        for s in ('stdin', 'stdout', 'stderr'):
            kwargs.pop(s, None)

        communicate_input = 'communicate_input' in kwargs
        if communicate_input:
            input_data = kwargs.pop('communicate_input', None)
            if not isinstance(input_data, string_types):
                input_data = ''
            if input_data and input_data[-1] != '\n':
                input_data += '\n'
            kwargs['stdin'] = subprocess.PIPE
            kwargs['stdout'] = open(os.devnull, 'w')
            kwargs['stderr'] = subprocess.STDOUT

        try:
            with self._lock:
                if self._is_cancelled:
                    raise PostgresException('cancelled')

                self._is_cancelled = False
                self._process = subprocess.Popen(*args, **kwargs)

            if communicate_input:
                if input_data:
                    self._process.communicate(input_data)
                self._process.stdin.close()

            return self._process.wait()
        finally:
            with self._lock:
                self._process = None

    def reset_is_cancelled(self):
        with self._lock:
            self._is_cancelled = False

    @property
    def is_cancelled(self):
        with self._lock:
            return self._is_cancelled

    def cancel(self):
        with self._lock:
            self._is_cancelled = True
            if self._process is None or self._process.returncode is not None:
                return
            self._process.terminate()

        for _ in polling_loop(10):
            with self._lock:
                if self._process is None or self._process.returncode is not None:
                    return

        with self._lock:
            if self._process is not None and self._process.returncode is None:
                self._process.kill()
