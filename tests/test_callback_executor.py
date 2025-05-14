import unittest

from unittest import mock
from unittest.mock import Mock, patch

import psutil

from patroni.postgresql.callback_executor import CallbackAction, CallbackExecutor
from patroni.postgresql.misc import PostgresqlRole


class TestCallbackExecutor(unittest.TestCase):

    @patch('psutil.Popen')
    def test_callback_executor(self, mock_popen):
        mock_popen.return_value.children.return_value = []
        mock_popen.return_value.is_running.return_value = True

        callback = ['test.sh', CallbackAction.ON_START, PostgresqlRole.REPLICA, 'foo']
        ce = CallbackExecutor()
        ce._kill_children = Mock(side_effect=Exception)
        ce._invoke_excepthook = Mock()
        self.assertIsNone(ce.call(callback))
        ce.join()

        self.assertIsNone(ce.call(callback))
        mock_popen.assert_called_with(['test.sh', 'on_start', 'replica', 'foo'], close_fds=True)
        mock_popen.reset_mock()

        mock_popen.return_value.kill.side_effect = psutil.AccessDenied()
        self.assertIsNone(ce.call(callback))

        ce._process_children = []
        mock_popen.return_value.children.side_effect = psutil.Error()
        mock_popen.return_value.kill.side_effect = psutil.NoSuchProcess(123)
        self.assertIsNone(ce.call(callback))

        mock_popen.side_effect = Exception
        ce = CallbackExecutor()
        ce._condition.wait = Mock(side_effect=[None, Exception])
        ce._invoke_excepthook = Mock()
        self.assertIsNone(ce.call(callback))
        ce.join()
        mock_popen.reset_mock()

        mock_popen.side_effect = [Mock()]
        self.assertIsNone(ce.call(['test.sh', CallbackAction.ON_RELOAD, PostgresqlRole.REPLICA, 'foo']))
        self.assertEqual(mock_popen.call_args_list[0],
                         mock.call(['test.sh', 'on_reload', 'replica', 'foo'], close_fds=True))
        ce.join()
