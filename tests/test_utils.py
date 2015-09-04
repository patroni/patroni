import os
import time
import unittest

from patroni.utils import reap_children, sigchld_handler, sigterm_handler, sleep


def nop(*args, **kwargs):
    pass


def os_waitpid(a, b):
    return (0, 0)


def time_sleep(_):
    sigchld_handler(None, None)


class TestUtils(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestUtils, self).__init__(method_name)

    def set_up(self):
        self.time_sleep = time.sleep
        time.sleep = nop

    def tear_down(self):
        time.sleep = self.time_sleep

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, sigterm_handler, None, None)

    def test_reap_children(self):
        reap_children()
        os.waitpid = os_waitpid
        sigchld_handler(None, None)
        reap_children()

    def test_sleep(self):
        time.sleep = time_sleep
        sleep(0.01)
