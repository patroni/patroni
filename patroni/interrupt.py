"""Interruption of blocking reads of HTTP responses from other threads and signal handlers.

Watching for changes in DCS involves HTTP requests, which block in a socket read for a long time: long-poll requests
(etcd v2, Consul) and watch streams (etcd v3, Kubernetes). Raising an exception from a signal handler to abort the read
could leave a lock held by the interrupted code locked forever, while shutting the socket down from another thread
doesn't wake the reader up on all platforms, for example on Windows.

Instead, HTTP responses received while an :class:`Interrupter` is :func:`~Interrupter.armed` wait for their socket and
for the interrupter at the same time before every read, and raise :exc:`WatchInterrupted` once
:func:`~Interrupter.interrupt` was called. The latter only writes to a socket, which is safe to do from other threads
and signal handlers on all platforms.

Connection pools of the HTTP client must be made interruptible with :func:`make_interruptible`.
"""
import errno
import http.client
import io
import select
import socket
import ssl
import time

from contextlib import contextmanager
from threading import local
from typing import Any, Generator, List, Optional

import urllib3

from urllib3.connection import HTTPConnection, HTTPSConnection


class WatchInterrupted(BaseException):
    """Raised by a read of the HTTP response after :func:`Interrupter.interrupt` was called.

    Derived from :exc:`BaseException`, so that ``except Exception`` clauses of HTTP clients don't catch it, for example,
    to retry the request.
    """


if hasattr(select, 'poll'):
    def _wait_readable(fds: List[int], timeout: Optional[float]) -> List[int]:
        """Wait until any of file descriptors *fds* is readable.

        :param fds: file descriptors to wait for.
        :param timeout: timeout in seconds, ``None`` means to wait forever.

        :returns: readable file descriptors, empty if *timeout* has expired.
        """
        poller = select.poll()
        for fd in fds:
            poller.register(fd, select.POLLIN)
        return [fd for fd, _ in poller.poll(None if timeout is None else timeout * 1000)]
else:  # pragma: no cover
    def _wait_readable(fds: List[int], timeout: Optional[float]) -> List[int]:
        """Wait until any of sockets *fds* is readable.

        :param fds: file descriptors of sockets to wait for.
        :param timeout: timeout in seconds, ``None`` means to wait forever.

        :returns: readable file descriptors, empty if *timeout* has expired.
        """
        return select.select(fds, [], [], timeout)[0]


class Interrupter:
    """Interrupts reads of HTTP responses received while it was :func:`armed`.

    The interruption is sticky: until :func:`clear` is called, reads of such responses and :func:`armed` raise
    :exc:`WatchInterrupted` right away.
    """

    def __init__(self) -> None:
        """Create a new interrupter, which is not interrupted."""
        self._rsock, self._wsock = socket.socketpair()
        self._rsock.setblocking(False)
        self._wsock.setblocking(False)

    def __del__(self) -> None:
        """Close the sockets."""
        for sock in (getattr(self, '_rsock', None), getattr(self, '_wsock', None)):
            if sock:
                sock.close()

    def fileno(self) -> int:
        """Get the file descriptor that becomes readable when interrupted.

        :returns: the file descriptor.
        """
        return self._rsock.fileno()

    def interrupt(self) -> None:
        """Interrupt reads of HTTP responses received while it was :func:`armed`.

        .. note::
            Safe to call from other threads and signal handlers: it doesn't block, acquire locks or raise exceptions.
        """
        try:
            self._wsock.send(b'\0')
        except OSError:  # the buffer is full, i.e. it was already interrupted
            pass

    def clear(self) -> None:
        """Clear the interruption."""
        try:
            while self._rsock.recv(4096):
                pass
        except OSError:  # nothing left to read
            pass

    def is_set(self) -> bool:
        """Check if it was interrupted.

        :returns: ``True`` if it was interrupted and not cleared since then.
        """
        return bool(_wait_readable([self.fileno()], 0))

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


class _InterruptibleSocketIO(io.RawIOBase):
    """Raw binary stream of a socket, which waits for the socket and for the :class:`Interrupter` before every read."""

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

    def _pending(self) -> int:
        """Get the number of decrypted bytes that could be read without waiting for the socket.

        :returns: number of bytes buffered by TLS.
        """
        if isinstance(self._sock, ssl.SSLSocket):
            return self._sock.pending()
        connection = getattr(self._sock, 'connection', None)  # pyOpenSSL
        return connection.pending() if connection is not None else 0

    def readinto(self, buffer: Any) -> int:
        """Read from the socket into *buffer*.

        Honors the timeout of the socket.

        :param buffer: buffer to read into.

        :returns: number of bytes read, 0 at the end of stream.

        :raises:
            :exc:`WatchInterrupted`: if the :class:`Interrupter` was interrupted.
            :exc:`socket.timeout`: if the timeout of the socket has expired.
        """
        timeout = self._raw.gettimeout()
        deadline = None if timeout is None else time.monotonic() + timeout
        interrupter_fd = self._interrupter.fileno()
        while True:
            if not self._pending():
                fd = self._raw.fileno()
                if fd < 0:
                    raise OSError(errno.EBADF, 'Bad file descriptor')
                ready = _wait_readable([fd, interrupter_fd],
                                       None if deadline is None else max(deadline - time.monotonic(), 0))
                if interrupter_fd in ready:
                    raise WatchInterrupted
                if not ready:
                    raise socket.timeout('timed out')
            # A readable socket doesn't guarantee that TLS can decrypt application data, the socket could have received
            # only a part of the record, or a TLS 1.3 session ticket. Therefore we don't block in the read.
            self._raw.settimeout(0)
            try:
                return self._sock.recv_into(buffer)
            except (BlockingIOError, ssl.SSLWantReadError, socket.timeout, TimeoutError):  # pyOpenSSL raises timeout
                pass
            except ssl.SSLWantWriteError:  # pragma: no cover
                self._raw.settimeout(timeout)  # TLS renegotiation, rare enough to block for it as usual
                return self._sock.recv_into(buffer)
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


class _HTTPSConnectionPool(urllib3.HTTPSConnectionPool):
    ConnectionCls = _HTTPSConnection


def make_interruptible(pool_manager: urllib3.PoolManager) -> urllib3.PoolManager:
    """Make HTTP responses from *pool_manager* interruptible by :class:`Interrupter`.

    :param pool_manager: the pool manager of the HTTP client.

    :returns: *pool_manager*.
    """
    pool_manager.clear()
    pool_manager.pool_classes_by_scheme = {'http': _HTTPConnectionPool, 'https': _HTTPSConnectionPool}
    return pool_manager
