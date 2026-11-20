import errno
import select
import socket
import ssl
import unittest

from threading import Timer
from unittest.mock import Mock, patch

import urllib3

from urllib3.exceptions import ReadTimeoutError

from patroni.interrupt import _InterruptibleSocketIO, Interrupter, make_interruptible, WatchInterrupted

from . import HangingHTTPServer

STREAM = b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n{}\n\r\n'


class TestInterrupter(unittest.TestCase):

    def setUp(self):
        self.interrupter = Interrupter()
        self.pool_manager = make_interruptible(urllib3.PoolManager())

    def tearDown(self):
        self.pool_manager.clear()

    def request(self, server, read_timeout=10, **kwargs):
        return self.pool_manager.request('GET', server.url, retries=False,
                                         timeout=urllib3.Timeout(connect=5, read=read_timeout), **kwargs)

    def assertInterrupted(self, func, *args, **kwargs):
        Timer(0.1, self.interrupter.interrupt).start()
        self.assertRaises(WatchInterrupted, func, *args, **kwargs)

    def test_long_poll(self):
        for response in (b'', b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n'):  # waiting for headers or for body
            self.interrupter.clear()
            with HangingHTTPServer(response) as server, self.interrupter.armed():
                self.assertInterrupted(self.request, server)

    def test_stream(self):
        with HangingHTTPServer(STREAM) as server:
            with self.interrupter.armed():
                response = self.request(server, preload_content=False)
            chunks = response.read_chunked(decode_content=False)
            self.assertEqual(next(chunks), b'{}\n')
            self.assertInterrupted(next, chunks)
            self.assertIsNone(response.connection)

    def test_interrupt(self):
        self.interrupter.interrupt()
        with HangingHTTPServer(b'HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok') as server:
            self.assertEqual(self.request(server).data, b'ok')  # the request wasn't armed
            with self.assertRaises(WatchInterrupted), self.interrupter.armed():
                pass
            self.interrupter.clear()
            with self.interrupter.armed():
                self.assertEqual(self.request(server).data, b'ok')
        self.interrupter._wsock.close()
        self.interrupter.interrupt()  # doesn't raise

    def test_interrupt_before_wakeup_is_readable(self):
        # Windows emulates socketpair() over TCP loopback, where the wakeup byte could become readable later
        with patch.object(self.interrupter, '_wsock') as delayed:
            self.interrupter.interrupt()
            self.assertTrue(self.interrupter.is_set())
            self.interrupter.clear()
            self.assertFalse(self.interrupter.is_set())
        self.interrupter._wsock.send(*delayed.send.call_args[0])
        with HangingHTTPServer() as server, self.interrupter.armed():
            self.assertRaises(ReadTimeoutError, self.request, server, read_timeout=0.1)
        self.assertFalse(select.select([self.interrupter], [], [], 0)[0])

    def test_interrupt_while_draining_stale_wakeup(self):
        rsock = self.interrupter._rsock
        self.interrupter._wsock.send(b'\0')

        def recv(size):
            if not self.interrupter.is_set():
                self.interrupter.interrupt()
            return rsock.recv(size)

        with HangingHTTPServer() as server, self.interrupter.armed(), \
                patch.object(self.interrupter, '_rsock', wraps=rsock) as reader:
            reader.recv.side_effect = recv
            self.assertRaises(WatchInterrupted, self.request, server, read_timeout=1)
        self.assertTrue(self.interrupter.is_set())
        self.assertTrue(select.select([self.interrupter], [], [], 1)[0])

    def test_read_timeout(self):
        with HangingHTTPServer() as server, self.interrupter.armed():
            self.assertRaises(ReadTimeoutError, self.request, server, read_timeout=0.1)


class TestInterruptibleSocketIO(unittest.TestCase):

    def test_interrupt_before_recv(self):
        interrupter = Interrupter()
        tls = Mock(spec=ssl.SSLSocket, gettimeout=Mock(return_value=None))
        for sock in (tls, Mock(socket=tls)):
            with _InterruptibleSocketIO(sock, interrupter) as raw:
                interrupter.interrupt()
                self.assertRaises(WatchInterrupted, raw.readinto, bytearray(1))
                sock.recv_into.assert_not_called()  # even data already buffered by TLS isn't returned
                interrupter.clear()

    def test_tls_want_write(self):
        rsock, wsock = socket.socketpair()
        with rsock, wsock:
            interrupter = Interrupter()
            tls = Mock(spec=ssl.SSLSocket, fileno=rsock.fileno, gettimeout=rsock.gettimeout,
                       settimeout=rsock.settimeout)
            rsock.settimeout(1)
            results = Mock(side_effect=[ssl.SSLWantWriteError, 1])

            def recv_into(buffer):
                self.assertEqual(rsock.gettimeout(), 0)
                return results(buffer)

            def interrupt_on_write(buffer):
                interrupter.interrupt()
                raise ssl.SSLWantWriteError

            with _InterruptibleSocketIO(tls, interrupter) as raw:
                tls.recv_into.side_effect = recv_into
                self.assertEqual(raw.readinto(bytearray(1)), 1)
                self.assertEqual(rsock.gettimeout(), 1)
                rsock.settimeout(None)
                tls.recv_into.side_effect = interrupt_on_write
                self.assertRaises(WatchInterrupted, raw.readinto, bytearray(1))
                self.assertIsNone(rsock.gettimeout())

    def test_wrapped_tls_want_write(self):
        class WantWriteError(Exception):
            pass

        rsock, wsock = socket.socketpair()
        with rsock, wsock, patch.dict('sys.modules', {'OpenSSL.SSL': Mock(WantWriteError=WantWriteError)}), \
                patch('patroni.interrupt.select', Mock(spec=['select'], select=select.select)):
            rsock.settimeout(1)
            sock = Mock(socket=rsock)
            wrapped = ssl.SSLError('read error')
            wrapped.__context__ = WantWriteError()
            sock.recv_into.side_effect = [WantWriteError, wrapped, 1, ssl.SSLError('other TLS error')]
            with _InterruptibleSocketIO(sock, Interrupter()) as raw:
                self.assertEqual(raw.readinto(bytearray(1)), 1)
                self.assertRaises(ssl.SSLError, raw.readinto, bytearray(1))
            self.assertEqual(rsock.gettimeout(), 1)

    def test_readinto(self):
        rsock, wsock = socket.socketpair()
        wsock.send(b'1')
        tls = Mock(spec=ssl.SSLSocket, fileno=rsock.fileno, gettimeout=Mock(return_value=None))
        pyopenssl = Mock(socket=rsock)
        for sock, error in ((tls, ssl.SSLWantReadError), (pyopenssl, TimeoutError)):
            # decrypted data could be buffered, or application data could be unavailable even if the socket is readable
            sock.recv_into.side_effect = [1, error, 2]
            with _InterruptibleSocketIO(sock, Interrupter()) as raw:
                self.assertEqual(raw.fileno(), sock.makefile.return_value.fileno.return_value)
                self.assertEqual(raw.readinto(bytearray(2)), 1)
                self.assertEqual(raw.readinto(bytearray(2)), 2)
            sock.makefile.return_value.close.assert_called_once_with()
        self.assertIsNone(rsock.gettimeout())
        rsock.close()
        wsock.close()
        with _InterruptibleSocketIO(pyopenssl, Interrupter()) as raw:
            with self.assertRaises(OSError) as e:
                raw.readinto(bytearray(1))
            self.assertEqual(e.exception.errno, errno.EBADF)
