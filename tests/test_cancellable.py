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
        self.assertRaises(PostgresException, self.c.call)

    def test__kill_children(self):
        self.c._process_children = [Mock()]
        self.c._kill_children()
        self.c._process_children[0].kill.side_effect = psutil.AccessDenied()
        self.c._kill_children()
        self.c._process_children[0].kill.side_effect = psutil.NoSuchProcess(123)
        self.c._kill_children()

    @patch('patroni.postgresql.cancellable.polling_loop', Mock(return_value=[0, 0]))
    def test_cancel(self):
        self.c._process = Mock()
        self.c._process.is_running.return_value = True
        self.c._process.children.side_effect = psutil.NoSuchProcess(123)
        self.c._process.suspend.side_effect = psutil.AccessDenied()
        self.c.cancel()
        self.c._process.is_running.side_effect = [True, False]
        self.c.cancel()
