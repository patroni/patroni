import psycopg2
import unittest
import ssl

from patroni.api import RestApiHandler, RestApiServer
from six import BytesIO as IO
from six.moves import BaseHTTPServer
from test_postgresql import psycopg2_connect


def nop(*args, **kwargs):
    pass


def throws(*args, **kwargs):
    raise psycopg2.OperationalError()


def ssl_wrap_socket(socket, *args, **kwargs):
    return socket


class Mock_BaseServer__is_shut_down:

    def set(self):
        pass

    def clear(self):
        pass


class MockPostgresql:

    def connection(self):
        return psycopg2_connect()

    def is_running(self):
        return True


class MockPatroni:

    def __init__(self):
        self.postgresql = MockPostgresql()


class MockRequest:

    def __init__(self, path):
        self.path = path

    def makefile(self, *args, **kwargs):
        return IO(self.path)


class MockRestApiServer(RestApiServer):

    def __init__(self, Handler, path, *args):
        config = {'listen': '127.0.0.1:8008', 'auth': 'test:test', 'certfile': 'dumb'}
        super(MockRestApiServer, self).__init__(MockPatroni(), config)
        if len(args) > 0:
            self.query = args[0]
        Handler(MockRequest(path), ('0.0.0.0', 8080), self)


class TestRestApiHandler(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestRestApiHandler, self).__init__(method_name)

    def set_up(self):
        BaseHTTPServer.HTTPServer.__init__ = nop
        RestApiServer._BaseServer__is_shut_down = Mock_BaseServer__is_shut_down()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        ssl.wrap_socket = ssl_wrap_socket

    def test_do_GET(self):
        MockRestApiServer(RestApiHandler, b'GET /')
        MockRestApiServer(RestApiHandler, b'GET /', throws)

    def test_do_GET_sampleauth(self):
        MockRestApiServer(RestApiHandler, b'GET /sampleauth')
        MockRestApiServer(RestApiHandler, b'GET /sampleauth\nAuthorization:')
        MockRestApiServer(RestApiHandler, b'GET /sampleauth\nAuthorization: Basic dGVzdDp0ZXN0')
