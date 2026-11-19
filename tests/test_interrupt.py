import errno
import socket
import ssl
import unittest

from threading import Timer
from unittest.mock import Mock

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

    def test_read_timeout(self):
        with HangingHTTPServer() as server, self.interrupter.armed():
            self.assertRaises(ReadTimeoutError, self.request, server, read_timeout=0.1)


class TestInterruptibleSocketIO(unittest.TestCase):

    def test_readinto(self):
        rsock, wsock = socket.socketpair()
        wsock.send(b'1')
        tls = Mock(spec=ssl.SSLSocket, fileno=rsock.fileno, gettimeout=Mock(return_value=None))
        tls.pending.side_effect = [1, 0, 0]
        pyopenssl = Mock(socket=rsock)
        pyopenssl.connection.pending.side_effect = [1, 0, 0]
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
        pyopenssl.connection.pending.side_effect = None
        pyopenssl.connection.pending.return_value = 0
        with _InterruptibleSocketIO(pyopenssl, Interrupter()) as raw:
            with self.assertRaises(OSError) as e:
                raw.readinto(bytearray(1))
            self.assertEqual(e.exception.errno, errno.EBADF)
