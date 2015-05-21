#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import sys

from helpers.statuspage import StatusPage
from test_postgresql import MockConnect

if sys.hexversion >= 0x03000000:
    from io import BytesIO as IO
else:
    from StringIO import StringIO as IO


class TestStatusPage(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestStatusPage, self).__init__(method_name)

    def set_up(self):
        pass

    def tear_down(self):
        pass

    def test_statuspage_main(self):
        pass

    def test_statuspage_initialize(self):
        pass

    def test_do_GET(self):
        for mock_recovery in [True, False]:
            for page in [b'GET /pg_master', b'GET /pg_slave', b'GET /pg_status', b'GET /not_found']:
                self.http_server = MockServer(('0.0.0.0', 8888), StatusPage, page, mock_recovery)


class MockRequest(object):

    def __init__(self, path):
        self.path = path

    def makefile(self, *args, **kwargs):
        return IO(self.path)


class MockServer(object):

    def __init__(self, ip_port, Handler, path, mock_recovery=False):
        self.postgresql = MockConnect()
        self.postgresql.mock_values['mock_recovery'] = mock_recovery
        Handler(MockRequest(path), ip_port, self)


if __name__ == '__main__':
    unittest.main()
