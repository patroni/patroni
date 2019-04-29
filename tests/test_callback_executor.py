import unittest

from mock import Mock, patch
from patroni.callback_executor import CallbackExecutor


class TestCallbackExecutor(unittest.TestCase):

    @patch('subprocess.Popen')
    def test_callback_executor(self, mock_popen):
        mock_popen.return_value.wait.side_effect = Exception
        mock_popen.return_value.poll.return_value = None

        ce = CallbackExecutor()
        self.assertIsNone(ce.call([]))
        ce.join()

        self.assertIsNone(ce.call([]))

        mock_popen.return_value.kill.side_effect = OSError
        self.assertIsNone(ce.call([]))

        mock_popen.side_effect = Exception
        ce = CallbackExecutor()
        ce._callback_event.wait = Mock(side_effect=[None, Exception])
        self.assertIsNone(ce.call([]))
        ce.join()
