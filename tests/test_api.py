import psycopg2
import sys
import unittest

from helpers.api import RestApiHandler, RestApiServer
from test_postgresql import psycopg2_connect

if sys.hexversion >= 0x03000000:
    from io import BytesIO as IO
else:
    from StringIO import StringIO as IO


def throws(*args, **kwargs):
    raise psycopg2.OperationalError()


class MockPostgresql:

    def connection(self):
        return psycopg2_connect()

    def is_running(self):
        return True


class MockGovernor:

    def __init__(self):
        self.postgresql = MockPostgresql()


class MockRequest:

    def __init__(self, path):
        self.path = path

    def makefile(self, *args, **kwargs):
        return IO(self.path)


class MockRestApiServer(RestApiServer):

    def __init__(self, Handler, path, *args):
        self.governor = MockGovernor()
        if len(args) > 0:
            self.query = args[0]
        Handler(MockRequest(path), ('0.0.0.0', 8080), self)


class TestRestApiHandler(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(TestRestApiHandler, self).__init__(method_name)

    def test_do_GET(self):
        MockRestApiServer(RestApiHandler, b'GET /')
        MockRestApiServer(RestApiHandler, b'GET /', throws)
