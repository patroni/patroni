import unittest

from unittest.mock import Mock, patch

from patroni.async_executor import AsyncExecutor, CriticalTask


class TestAsyncExecutor(unittest.TestCase):

    def setUp(self):
        self.a = AsyncExecutor(Mock(), Mock())

    @patch('patroni.thread_pool.get_executor', Mock())
    def test_run_async(self):
        self.a.run_async(Mock(return_value=True))

    def test_run(self):
        self.a.run(Mock(side_effect=Exception()))

    def test_cancel(self):
        self.a.cancel()
        self.a.schedule('foo')
        self.a.cancel()
        self.a.run(Mock())


class TestCriticalTask(unittest.TestCase):

    def test_completed_task(self):
        ct = CriticalTask()
        ct.complete(1)
        self.assertFalse(ct.cancel())
