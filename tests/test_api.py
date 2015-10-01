import psycopg2
import unittest

from mock import Mock, patch
from patroni.api import RestApiHandler, RestApiServer
from six import BytesIO as IO
from six.moves import BaseHTTPServer
from test_postgresql import psycopg2_connect, MockCursor


class MockPostgresql(Mock):

    name = 'test'
    state = 'running'
    role = 'master'

    def connection(self):
        return psycopg2_connect()

    def is_running(self):
        return True


class MockHa(Mock):

    dcs = Mock()
    state_handler = MockPostgresql()

    def schedule_restart(self):
        return 'restart'

    def schedule_reinitialize(self):
        return 'reinitialize'

    def restart(self):
        return (True, '')

    def restart_scheduled(self):
        return False


class MockPatroni:

    postgresql = MockPostgresql()
    ha = MockHa()
    dcs = Mock()


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
        MockRestApiServer(RestApiHandler, b'GET /master')
        MockRestApiServer(RestApiHandler, b'GET /replica')
        with patch.object(MockHa, 'restart_scheduled', Mock(return_value=True)):
            MockRestApiServer(RestApiHandler, b'GET /master')

    def test_do_GET_patroni(self):
        MockRestApiServer(RestApiHandler, b'GET /patroni')

    def test_basicauth(self):
        MockRestApiServer(RestApiHandler, b'POST /restart HTTP/1.0')
        MockRestApiServer(RestApiHandler, b'POST /restart HTTP/1.0\nAuthorization:')

    def test_do_POST_restart(self):
        request = b'POST /restart HTTP/1.0\nAuthorization: Basic dGVzdDp0ZXN0'
        MockRestApiServer(RestApiHandler, request)
        with patch.object(MockHa, 'restart', Mock(side_effect=Exception)):
            MockRestApiServer(RestApiHandler, request)

    @patch.object(MockHa, 'dcs')
    def test_do_POST_reinitialize(self, dcs):
        cluster = dcs.get_cluster.return_value
        request = b'POST /reinitialize HTTP/1.0\nAuthorization: Basic dGVzdDp0ZXN0'
        MockRestApiServer(RestApiHandler, request)
        cluster.is_unlocked.return_value = False
        MockRestApiServer(RestApiHandler, request)
        with patch.object(MockHa, 'schedule_reinitialize', Mock(return_value=None)):
            MockRestApiServer(RestApiHandler, request)
        cluster.leader.name = 'test'
        MockRestApiServer(RestApiHandler, request)

    @patch('time.sleep', Mock())
    def test_RestApiServer_query(self):
        with patch.object(MockCursor, 'execute', Mock(side_effect=psycopg2.OperationalError)):
            MockRestApiServer(RestApiHandler, b'GET /patroni')
        with patch.object(MockPostgresql, 'connection', Mock(side_effect=psycopg2.OperationalError)):
            MockRestApiServer(RestApiHandler, b'GET /patroni')
