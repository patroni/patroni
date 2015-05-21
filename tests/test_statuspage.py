#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from helpers.statuspage import StatusPage
from test_postgresql import MockConnect
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
        self.http_server = MockServer(('0.0.0.0', 8888), StatusPage, '/pg_master')
        self.http_server = MockServer(('0.0.0.0', 8888), StatusPage, '/pg_slave')
        self.http_server = MockServer(('0.0.0.0', 8888), StatusPage, '/pg_status')
        self.http_server = MockServer(('0.0.0.0', 8888), StatusPage, '/not_found')

class MockRequest(object):

    def __init__(self, path):
        self.path = path

    def makefile(self, *args, **kwargs):
        return IO(b"GET " + self.path)


class MockServer(object):

    def __init__(self, ip_port, Handler, path):
        self.postgresql = MockConnect()
        Handler(MockRequest(path), ip_port, self)


if __name__ == '__main__':
    unittest.main()
