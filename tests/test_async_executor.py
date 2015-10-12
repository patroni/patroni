import unittest

from mock import Mock, patch
from patroni.async_executor import AsyncExecutor
from threading import Thread


class TestAsyncExecutor(unittest.TestCase):

    def setUp(self):
        self.a = AsyncExecutor()

    @patch.object(Thread, 'start', Mock())
    def test_run_async(self):
        self.a.run_async(Mock(return_value=True))

    def test_run(self):
        self.a.run(Mock(side_effect=Exception()))
