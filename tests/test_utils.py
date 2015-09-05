import os
import time
import unittest

from patroni.exceptions import DCSError
from patroni.utils import Retry, RetryFailedError, reap_children, sigchld_handler, sigterm_handler, sleep


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


class TestRetrySleeper(unittest.TestCase):

    def _pass(self):
        pass

    def _fail(self, times=1):
        scope = dict(times=0)

        def inner():
            if scope['times'] >= times:
                pass
            else:
                scope['times'] += 1
                raise DCSError('Failed!')
        return inner

    def _makeOne(self, *args, **kwargs):
        return Retry(*args, **kwargs)

    def test_reset(self):
        retry = self._makeOne(delay=0, max_tries=2)
        retry(self._fail())
        self.assertEquals(retry._attempts, 1)
        retry.reset()
        self.assertEquals(retry._attempts, 0)

    def test_too_many_tries(self):
        retry = self._makeOne(delay=0)
        self.assertRaises(RetryFailedError, retry, self._fail(times=999))
        self.assertEquals(retry._attempts, 1)

    def test_maximum_delay(self):
        def sleep_func(_time):
            pass

        retry = self._makeOne(delay=10, max_tries=100, sleep_func=sleep_func)
        retry(self._fail(times=10))
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        self.assertEquals(type(retry._cur_delay), float)

    def test_deadline(self):
        def sleep_func(_time):
            pass

        retry = self._makeOne(deadline=0.0001, sleep_func=sleep_func)
        self.assertRaises(RetryFailedError, retry, self._fail(times=100))

    def test_copy(self):
        _sleep = lambda t: None
        retry = self._makeOne(sleep_func=_sleep)
        rcopy = retry.copy()
        self.assertTrue(rcopy.sleep_func is _sleep)
