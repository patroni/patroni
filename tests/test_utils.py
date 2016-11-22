import unittest

from mock import Mock, patch
from patroni.exceptions import PatroniException
from patroni.utils import Retry, RetryFailedError


@patch('time.sleep', Mock())
class TestRetrySleeper(unittest.TestCase):

    @staticmethod
    def _fail(times=1):
        scope = dict(times=0)

        def inner():
            if scope['times'] >= times:
                pass
            else:
                scope['times'] += 1
                raise PatroniException('Failed!')
        return inner

    def test_reset(self):
        retry = Retry(delay=0, max_tries=2)
        retry(self._fail())
        self.assertEquals(retry._attempts, 1)
        retry.reset()
        self.assertEquals(retry._attempts, 0)

    def test_too_many_tries(self):
        retry = Retry(delay=0)
        self.assertRaises(RetryFailedError, retry, self._fail(times=999))
        self.assertEquals(retry._attempts, 1)

    def test_maximum_delay(self):
        retry = Retry(delay=10, max_tries=100)
        retry(self._fail(times=10))
        self.assertTrue(retry._cur_delay < 4000, retry._cur_delay)
        # gevent's sleep function is picky about the type
        self.assertEquals(type(retry._cur_delay), float)

    def test_deadline(self):
        retry = Retry(deadline=0.0001)
        self.assertRaises(RetryFailedError, retry, self._fail(times=100))

    def test_copy(self):
        def _sleep(t):
            pass

        retry = Retry(sleep_func=_sleep)
        rcopy = retry.copy()
        self.assertTrue(rcopy.sleep_func is _sleep)
