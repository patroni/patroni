import psycopg2
import unittest

from patroni.api import RestApiHandler, RestApiServer
from six import BytesIO as IO
from test_postgresql import psycopg2_connect


def throws(*args, **kwargs):
    raise psycopg2.OperationalError()


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
        self.patroni = MockPatroni()
        if len(args) > 0:
            self.query = args[0]
        Handler(MockRequest(path), ('0.0.0.0', 8080), self)


class TestRestApiHandler(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(TestRestApiHandler, self).__init__(method_name)

    def test_do_GET(self):
        MockRestApiServer(RestApiHandler, b'GET /')
        MockRestApiServer(RestApiHandler, b'GET /', throws)
