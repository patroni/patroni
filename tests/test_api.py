import psycopg2
import unittest

from mock import Mock, patch
from patroni.api import RestApiHandler, RestApiServer
from six import BytesIO as IO
from six.moves import BaseHTTPServer
from test_postgresql import psycopg2_connect, MockCursor


class MockPostgresql(Mock):

    def connection(self):
        return psycopg2_connect()

    def is_running(self):
        return True


class MockPatroni:

    postgresql = MockPostgresql()


class MockRequest:

    def __init__(self, path):
        self.path = path

    def makefile(self, *args, **kwargs):
        return IO(self.path)


class MockRestApiServer(RestApiServer):

    def __init__(self, Handler, path):
        self.socket = 0
        BaseHTTPServer.HTTPServer.__init__ = Mock()
        MockRestApiServer._BaseServer__is_shut_down = Mock()
        MockRestApiServer._BaseServer__shutdown_request = True
        config = {'listen': '127.0.0.1:8008', 'auth': 'test:test', 'certfile': 'dumb'}
        super(MockRestApiServer, self).__init__(MockPatroni(), config)
        Handler(MockRequest(path), ('0.0.0.0', 8080), self)


@patch('ssl.wrap_socket', Mock(return_value=0))
class TestRestApiHandler(unittest.TestCase):

    def test_do_GET(self):
        MockRestApiServer(RestApiHandler, b'GET /')
        with patch.object(RestApiServer, 'query', Mock(side_effect=psycopg2.OperationalError())):
            MockRestApiServer(RestApiHandler, b'GET /')

    def test_do_GET_sampleauth(self):
        MockRestApiServer(RestApiHandler, b'GET /sampleauth')
        MockRestApiServer(RestApiHandler, b'GET /sampleauth\nAuthorization:')
        MockRestApiServer(RestApiHandler, b'GET /sampleauth\nAuthorization: Basic dGVzdDp0ZXN0')

    def test_do_GET_patroni(self):
        MockRestApiServer(RestApiHandler, b'GET /patroni')

    @patch('time.sleep', Mock())
    def test_RestApiServer_query(self):
        with patch.object(MockCursor, 'execute', Mock(side_effect=psycopg2.OperationalError)):
            MockRestApiServer(RestApiHandler, b'GET /patroni')
        with patch.object(MockPostgresql, 'connection', Mock(side_effect=psycopg2.OperationalError)):
            MockRestApiServer(RestApiHandler, b'GET /patroni')
