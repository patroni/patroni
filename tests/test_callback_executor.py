import unittest
from mock import mock

from patroni.postgresql.callback_executor import CallbackExecutor
import time


class TestCallbackExecutor(unittest.TestCase):

    def test_callback_executor_calls_commands_in_order(self):
        mock_popen = mock.MagicMock('subprocess.Popen')
        # First command sleep, second return immidately, third throws exception
        mock_popen.return_value.wait.side_effect = [lambda: time.sleep(1), mock.Mock(), Exception]

        ce = CallbackExecutor(mock_popen)
        for x in range(1,4):
          ce.call([str(x)])

        ce._cmds_queue.join()

        calls = [
          mock.call(['1'], close_fds=True),
          mock.call().wait(),
          mock.call(['2'], close_fds=True),
          mock.call().wait(),
          mock.call(['3'], close_fds=True),
          mock.call().wait(),
        ]
        print(mock_popen.mock_calls)
        mock_popen.assert_has_calls(calls)
