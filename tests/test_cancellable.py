import unittest

from mock import Mock, PropertyMock, patch
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
        self.c._process.returncode = None
        self.c.cancel()
        type(self.c._process).returncode = PropertyMock(side_effect=[None, -15])
        self.c.cancel()
