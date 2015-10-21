import unittest

from mock import Mock, patch
from patroni.exceptions import PatroniException
from patroni.utils import Retry, RetryFailedError, reap_children, sigchld_handler, sigterm_handler, sleep


def time_sleep(_):
    sigchld_handler(None, None)


class TestUtils(unittest.TestCase):

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, sigterm_handler, None, None)

    @patch('time.sleep', Mock())
    def test_reap_children(self):
        reap_children()
        with patch('os.waitpid', Mock(return_value=(0, 0))):
            sigchld_handler(None, None)
            reap_children()

    @patch('time.sleep', time_sleep)
    def test_sleep(self):
        sleep(0.01)


@patch('time.sleep', Mock())
class TestRetrySleeper(unittest.TestCase):

    def _fail(self, times=1):
        scope = dict(times=0)

        def inner():
            if scope['times'] >= times:
                pass
            else:
                scope['times'] += 1
                raise PatroniException('Failed!')
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
        retry = self._makeOne(delay=10, max_tries=100)
        retry(self._fail(times=10))
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        self.assertEquals(type(retry._cur_delay), float)

    def test_deadline(self):
        retry = self._makeOne(deadline=0.0001)
        self.assertRaises(RetryFailedError, retry, self._fail(times=100))

    def test_copy(self):
        def _sleep(t):
            None

        retry = self._makeOne(sleep_func=_sleep)
        rcopy = retry.copy()
        self.assertTrue(rcopy.sleep_func is _sleep)
