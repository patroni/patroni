import psutil
import unittest

from mock import Mock, patch
from patroni.exceptions import PostgresException
from patroni.postgresql.cancellable import CancellableSubprocess


class TestCancellableSubprocess(unittest.TestCase):

    def setUp(self):
        self.c = CancellableSubprocess()

    def test_call(self):
        self.c.cancel()
        self.assertRaises(PostgresException, self.c.call, communicate_input=None)

    @patch('patroni.postgresql.cancellable.polling_loop', Mock(return_value=[0, 0]))
    def test_cancel(self):
        self.c._process = Mock()
        self.c._process.is_running.return_value = True

        self.c._process.children.return_value = [Mock()]
        self.c.cancel()
        self.c._process.children.return_value[0].kill.side_effect = psutil.AccessDenied()
        self.c.cancel()
        self.c._process.children.return_value[0].kill.side_effect = psutil.NoSuchProcess(123)
        self.c.cancel()
        self.c._process.children.side_effect = psutil.Error()
        self.c.cancel()
        self.c._process.is_running.side_effect = [True, False]
        self.c.cancel()
