"""Interruption of blocking reads of HTTP responses from other threads and signal handlers.

Watching for changes in DCS involves HTTP requests, which block in a socket read for a long time: long-poll requests
(etcd v2, Consul) and watch streams (etcd v3, Kubernetes). Raising an exception from a signal handler to abort the read
could leave a lock held by the interrupted code locked forever, while shutting the socket down from another thread
doesn't wake the reader up on all platforms, for example on Windows.

Instead, HTTP responses received while an :class:`Interrupter` is :func:`~Interrupter.armed` use nonblocking socket
reads. If a read would block, they wait for both the socket and the interrupter. Calling :func:`~Interrupter.interrupt`
wakes a blocked raw read or makes the next one raise :exc:`WatchInterrupted`; it does not discard already buffered HTTP
data. It only writes to a socket, which is safe to do from other threads and signal handlers on all platforms.

Connection pools of the HTTP client must be made interruptible with :func:`make_interruptible`.
"""
import errno
import http.client
import io
import select
import socket
import ssl
import sys
import time

from contextlib import contextmanager
from threading import Condition, local, Thread
from typing import Any, Generator, List, Optional

import urllib3

from urllib3.connection import HTTPConnection, HTTPSConnection


class WatchInterrupted(BaseException):
    """Raised by a read of the HTTP response after :func:`Interrupter.interrupt` was called.

    Derived from :exc:`BaseException`, so that ``except Exception`` clauses of HTTP clients don't catch it, for example,
    to retry the request.
    """


def _wait(read: List[int], write: List[int], timeout: Optional[float]) -> List[int]:
    """Wait for sockets to become readable or writable.

    :param read: file descriptors to wait for readability.
    :param write: file descriptors to wait for writability.
    :param timeout: timeout in seconds, ``None`` means to wait forever.

    :returns: ready file descriptors, empty if *timeout* has expired.
    """
    if hasattr(select, 'poll'):
        poller = select.poll()
        for fd in read:
            poller.register(fd, select.POLLIN)
        for fd in write:
            poller.register(fd, select.POLLOUT)
        return [fd for fd, _ in poller.poll(None if timeout is None else timeout * 1000)]
    readable, writable, _ = select.select(read, write, [], timeout)
    return readable + writable


class Interrupter:
    """Interrupts reads of HTTP responses received while it was :func:`armed`.

    The interruption is sticky: until :func:`clear` is called, raw reads of such responses and :func:`armed` raise
    :exc:`WatchInterrupted` right away.
    """

    def __init__(self) -> None:
        """Create a new interrupter, which is not interrupted."""
        self._interrupted = False
        self._rsock, self._wsock = socket.socketpair()
        self._rsock.setblocking(False)
        self._wsock.setblocking(False)

    def __del__(self) -> None:
        """Close the sockets."""
        for sock in (getattr(self, '_rsock', None), getattr(self, '_wsock', None)):
            if sock:
                sock.close()

    def fileno(self) -> int:
        """Get the wakeup file descriptor. A delayed byte can make it readable even after :func:`clear`.

        :returns: the file descriptor.
        """
        return self._rsock.fileno()

    def _wakeup(self) -> None:
        """Wake blocked readers without changing the interrupt flag."""
        try:
            self._wsock.send(b'\0')
        except OSError:  # the buffer is full, i.e. a wakeup is already pending
            pass

    def interrupt(self) -> None:
        """Interrupt reads of HTTP responses received while it was :func:`armed`.

        .. note::
            Safe to call from other threads and signal handlers: it doesn't block, acquire locks or raise exceptions.
        """
        self._interrupted = True
        self._wakeup()

    def clear(self) -> None:
        """Clear the interruption."""
        self._interrupted = False
        self.drain_stale_wakeups()

    def drain_stale_wakeups(self) -> None:
        """Discard old wakeup bytes, leaving the socket readable for other waiters if recv() consumes a new one."""
        try:
            while not self._interrupted and self._rsock.recv(4096):
                if self._interrupted:
                    self._wakeup()
                    break
        except OSError:  # nothing left to read
            pass

    def is_set(self) -> bool:
        """Check if it was interrupted.

        :returns: ``True`` if it was interrupted and not cleared since then.
        """
        return self._interrupted

    @contextmanager
    def armed(self) -> Generator[None, None, None]:
        """Make HTTP responses received by the current thread in this context interruptible.

        Only works for connection pools that were made interruptible with :func:`make_interruptible`. Reads of the
        response remain interruptible after the context is left.

        :raises:
            :exc:`WatchInterrupted`: if it was already interrupted.
        """
        if self.is_set():
            raise WatchInterrupted
        previous = _armed.interrupter
        _armed.interrupter = self
        try:
            yield
        finally:
            _armed.interrupter = previous


class _Armed(local):
    """The :class:`Interrupter` armed in the current thread."""

    interrupter: Optional[Interrupter] = None


_armed = _Armed()


class WatchStreamThread(Thread):
    """Daemon thread with an independently interruptible HTTP watch stream.

    Readers check readiness and publishers install snapshots while holding :attr:`_condition`.
    """

    def __init__(self, condition: Optional[Condition] = None) -> None:
        super().__init__(daemon=True)
        self._condition = condition if condition is not None else Condition()
        self._is_ready = False
        self._watch_interrupter = Interrupter()

    def kill_stream(self) -> None:
        """Invalidate the cache for readers and interrupt the watch so it can rebuild."""
        with self._condition:
            self._is_ready = False
            self._watch_interrupter.interrupt()

    @staticmethod
    def _finish_response(response: urllib3.response.HTTPResponse) -> None:
        """Close the watch response and release its connection back to the pool."""
        try:
            response.close()
        finally:
            response.release_conn()


class _InterruptibleSocketIO(io.RawIOBase):
    """Raw socket stream whose reads can be interrupted while waiting for socket I/O."""

    def __init__(self, sock: Any, interrupter: Interrupter) -> None:
        """Create a new stream.

        :param sock: :class:`~socket.socket`, :class:`~ssl.SSLSocket`, or ``WrappedSocket`` of
            :mod:`urllib3.contrib.pyopenssl`.
        :param interrupter: interrupts reads.
        """
        super().__init__()
        # The socket counts its file objects and doesn't close the connection while there are any.
        self._io = sock.makefile('rb', buffering=0)
        self._sock = sock
        self._raw: socket.socket = sock if isinstance(sock, socket.socket) else sock.socket
        self._interrupter = interrupter

    def readable(self) -> bool:
        """Check if the stream is readable.

        :returns: ``True``.
        """
        return True

    def fileno(self) -> int:
        """Get the file descriptor of the socket.

        :returns: the file descriptor.
        """
        return self._io.fileno()

    def close(self) -> None:
        """Close the stream."""
        if not self.closed:
            try:
                self._io.close()
            finally:
                super().close()

    @staticmethod
    def _want_write(exc: Exception) -> bool:
        """Check whether a nonblocking TLS read needs to wait for writability.

        urllib3 can re-raise pyOpenSSL's ``WantWriteError`` as :exc:`ssl.SSLError`.
        """
        if isinstance(exc, ssl.SSLWantWriteError):
            return True
        want_write = getattr(sys.modules.get('OpenSSL.SSL'), 'WantWriteError', ())
        return isinstance(exc, want_write) or isinstance(exc, ssl.SSLError) and \
            isinstance(exc.__cause__ or exc.__context__, want_write)

    def readinto(self, buffer: Any) -> int:
        """Read from the socket into *buffer*.

        Honors the timeout of the socket.

        :param buffer: buffer to read into.

        :returns: number of bytes read, 0 at the end of stream.

        :raises:
            :exc:`WatchInterrupted`: if the :class:`Interrupter` was interrupted.
            :exc:`socket.timeout`: if the timeout of the socket has expired.
        """
        if self._interrupter.is_set():
            raise WatchInterrupted
        interrupter_fd = self._interrupter.fileno()
        fd = self._raw.fileno()
        if fd < 0:
            raise OSError(errno.EBADF, 'Bad file descriptor')
        timeout = self._raw.gettimeout()
        deadline = None if timeout is None else time.monotonic() + timeout
        self._raw.settimeout(0)
        try:
            while True:
                try:
                    return self._sock.recv_into(buffer)
                except (BlockingIOError, ssl.SSLWantReadError, socket.timeout, TimeoutError):
                    write = False
                except Exception as exc:
                    if not self._want_write(exc):
                        raise
                    write = True

                remaining = None if deadline is None else max(0, deadline - time.monotonic())
                ready = _wait([interrupter_fd] if write else [interrupter_fd, fd], [fd] if write else [], remaining)
                if interrupter_fd in ready:
                    self._interrupter.drain_stale_wakeups()
                if self._interrupter.is_set():
                    raise WatchInterrupted
                if not ready or remaining == 0:
                    raise socket.timeout('timed out')
        finally:
            self._raw.settimeout(timeout)


class _SocketWithInterruptibleFile:
    """Socket for :class:`http.client.HTTPResponse`, which only needs :func:`makefile`."""

    def __init__(self, sock: Any, interrupter: Interrupter) -> None:
        """Create a new socket.

        :param sock: the real socket.
        :param interrupter: interrupts reads.
        """
        self._sock = sock
        self._interrupter = interrupter

    def makefile(self, mode: str) -> io.BufferedReader:
        """Create a file object for reading from the socket.

        :param mode: always ``rb``.

        :returns: buffered :class:`_InterruptibleSocketIO`.
        """
        return io.BufferedReader(_InterruptibleSocketIO(self._sock, self._interrupter))


def _is_supported(sock: Any) -> bool:
    """Check if reads from *sock* could be interrupted.

    :param sock: socket of the HTTP connection.

    :returns: ``True`` for :class:`~socket.socket`, :class:`~ssl.SSLSocket`, and ``WrappedSocket`` of pyOpenSSL.
    """
    return isinstance(sock, socket.socket)\
        or isinstance(getattr(sock, 'socket', None), socket.socket) and hasattr(sock, 'connection')


class _HTTPResponse(http.client.HTTPResponse):
    """HTTP response, which is interruptible if it was received while an :class:`Interrupter` was armed."""

    def __init__(self, sock: Any, *args: Any, **kwargs: Any) -> None:
        """Create a new response.

        :param sock: socket of the HTTP connection.
        :param args: positional arguments for :class:`http.client.HTTPResponse`.
        :param kwargs: keyword arguments for :class:`http.client.HTTPResponse`.
        """
        interrupter = _armed.interrupter
        if interrupter is not None and _is_supported(sock):
            sock = _SocketWithInterruptibleFile(sock, interrupter)
        super().__init__(sock, *args, **kwargs)


class _HTTPConnection(HTTPConnection):
    response_class = _HTTPResponse


class _HTTPSConnection(HTTPSConnection):
    response_class = _HTTPResponse


class _HTTPConnectionPool(urllib3.HTTPConnectionPool):
    ConnectionCls = _HTTPConnection


class InterruptibleHTTPSConnectionPool(urllib3.HTTPSConnectionPool):
    """HTTPS connection pool of pool managers made interruptible with :func:`make_interruptible`.

    Custom HTTPS connection pools of such pool managers must be based on it to keep responses interruptible.
    """

    ConnectionCls = _HTTPSConnection


def make_interruptible(pool_manager: urllib3.PoolManager) -> urllib3.PoolManager:
    """Make HTTP responses from *pool_manager* interruptible by :class:`Interrupter`.

    :param pool_manager: the pool manager of the HTTP client.

    :returns: *pool_manager*.
    """
    pool_manager.clear()
    pool_manager.pool_classes_by_scheme = {'http': _HTTPConnectionPool, 'https': InterruptibleHTTPSConnectionPool}
    return pool_manager
