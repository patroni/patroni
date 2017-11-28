import unittest

from mock import Mock, patch
from patroni.postmaster import PostmasterProcess
import psutil


class TestPostmasterProcess(unittest.TestCase):
    @patch('psutil.Process.__init__', Mock())
    def test_init(self):
        proc = PostmasterProcess(-123)
        self.assertTrue(proc.is_single_user)

    @patch('psutil.Process.create_time')
    @patch('psutil.Process.__init__')
    def test_from_pidfile(self, mock_init, mock_create_time):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        self.assertEquals(PostmasterProcess.from_pidfile({}), None)
        self.assertEquals(PostmasterProcess.from_pidfile({"pid": "foo"}), None)
        self.assertEquals(PostmasterProcess.from_pidfile({"pid": "123"}), None)

        mock_init.side_effect = None
        with patch.object(psutil.Process, 'pid', 123), \
                patch.object(psutil.Process, 'parent', return_value=124), \
                patch('os.getpid', return_value=125) as mock_ospid, \
                patch('os.getppid', return_value=126):

            self.assertNotEquals(PostmasterProcess.from_pidfile({"pid": "123"}), None)

            mock_create_time.return_value = 100000
            self.assertEquals(PostmasterProcess.from_pidfile({"pid": "123", "start_time": "200000"}), None)
            self.assertNotEquals(PostmasterProcess.from_pidfile({"pid": "123", "start_time": "foobar"}), None)

            mock_ospid.return_value = 123
            self.assertEquals(PostmasterProcess.from_pidfile({"pid": "123", "start_time": "100000"}), None)

    @patch('psutil.Process.__init__')
    def test_from_pid(self, mock_init):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        self.assertEquals(PostmasterProcess.from_pid(123), None)
        mock_init.side_effect = None
        self.assertNotEquals(PostmasterProcess.from_pid(123), None)

    @patch('psutil.Process.__init__', Mock())
    @patch('psutil.Process.send_signal')
    @patch('psutil.Process.pid', Mock(return_value=123))
    def test_signal_stop(self, mock_send_signal):
        proc = PostmasterProcess(-123)
        self.assertEquals(proc.signal_stop('immediate'), False)

        mock_send_signal.side_effect = [None, psutil.NoSuchProcess(123), psutil.AccessDenied()]
        proc = PostmasterProcess(123)
        self.assertEquals(proc.signal_stop('immediate'), None)
        self.assertEquals(proc.signal_stop('immediate'), True)
        self.assertEquals(proc.signal_stop('immediate'), False)

    @patch('psutil.Process.__init__', Mock())
    @patch('psutil.wait_procs')
    def test_wait_for_user_backends_to_close(self, mock_wait):
        c1 = Mock()
        c1.cmdline = Mock(return_value=["postgres: startup process"])
        c2 = Mock()
        c2.cmdline = Mock(return_value=["postgres: postgres postgres [local] idle"])
        with patch('psutil.Process.children', Mock(return_value=[c1, c2])):
            proc = PostmasterProcess(123)
            self.assertIsNone(proc.wait_for_user_backends_to_close())
            mock_wait.assert_called_with([c2])

    @patch('subprocess.Popen')
    @patch.object(PostmasterProcess, 'from_pid')
    def test_start(self, mock_frompid, mock_popen):
        mock_frompid.return_value = "proc 123"
        mock_popen.return_value.stdout.readline.return_value = '123'
        self.assertEquals(
            PostmasterProcess.start('/bin/true', '/tmp/', '/tmp/test.conf', ['--foo=bar', '--bar=baz']),
            "proc 123"
        )
        mock_frompid.assert_called_with(123)
