import multiprocessing
import psutil
import unittest

from mock import Mock, patch, mock_open
from patroni.postgresql.postmaster import PostmasterProcess
from six.moves import builtins


class MockProcess(object):
    def __init__(self, target, args):
        self.target = target
        self.args = args

    def start(self):
        self.target(*self.args)

    def join(self):
        pass


class TestPostmasterProcess(unittest.TestCase):
    @patch('psutil.Process.__init__', Mock())
    def test_init(self):
        proc = PostmasterProcess(-123)
        self.assertTrue(proc.is_single_user)

    @patch('psutil.Process.create_time')
    @patch('psutil.Process.__init__')
    @patch.object(PostmasterProcess, '_read_postmaster_pidfile')
    def test_from_pidfile(self, mock_read, mock_init, mock_create_time):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        mock_read.return_value = {}
        self.assertIsNone(PostmasterProcess.from_pidfile(''))
        mock_read.return_value = {"pid": "foo"}
        self.assertIsNone(PostmasterProcess.from_pidfile(''))
        mock_read.return_value = {"pid": "123"}
        self.assertIsNone(PostmasterProcess.from_pidfile(''))

        mock_init.side_effect = None
        with patch.object(psutil.Process, 'pid', 123), \
                patch.object(psutil.Process, 'ppid', return_value=124), \
                patch('os.getpid', return_value=125) as mock_ospid, \
                patch('os.getppid', return_value=126):

            self.assertIsNotNone(PostmasterProcess.from_pidfile(''))

            mock_create_time.return_value = 100000
            mock_read.return_value = {"pid": "123", "start_time": "200000"}
            self.assertIsNone(PostmasterProcess.from_pidfile(''))

            mock_read.return_value = {"pid": "123", "start_time": "foobar"}
            self.assertIsNotNone(PostmasterProcess.from_pidfile(''))

            mock_ospid.return_value = 123
            mock_read.return_value = {"pid": "123", "start_time": "100000"}
            self.assertIsNone(PostmasterProcess.from_pidfile(''))

    @patch('psutil.Process.__init__')
    def test_from_pid(self, mock_init):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        self.assertEqual(PostmasterProcess.from_pid(123), None)
        mock_init.side_effect = None
        self.assertNotEqual(PostmasterProcess.from_pid(123), None)

    @patch('psutil.Process.__init__', Mock())
    @patch('psutil.Process.send_signal')
    @patch('psutil.Process.pid', Mock(return_value=123))
    @patch('os.name', 'posix')
    @patch('signal.SIGQUIT', 3, create=True)
    def test_signal_stop(self, mock_send_signal):
        proc = PostmasterProcess(-123)
        self.assertEqual(proc.signal_stop('immediate'), False)

        mock_send_signal.side_effect = [None, psutil.NoSuchProcess(123), psutil.AccessDenied()]
        proc = PostmasterProcess(123)
        self.assertEqual(proc.signal_stop('immediate'), None)
        self.assertEqual(proc.signal_stop('immediate'), True)
        self.assertEqual(proc.signal_stop('immediate'), False)

    @patch('psutil.Process.__init__', Mock())
    @patch('patroni.postgresql.postmaster.os')
    @patch('subprocess.call', Mock(side_effect=[0, OSError, 1]))
    @patch('psutil.Process.pid', Mock(return_value=123))
    @patch('psutil.Process.is_running', Mock(return_value=False))
    def test_signal_stop_nt(self, mock_os):
        mock_os.configure_mock(name="nt")
        proc = PostmasterProcess(-123)
        self.assertEqual(proc.signal_stop('immediate'), False)

        proc = PostmasterProcess(123)
        self.assertEqual(proc.signal_stop('immediate'), None)
        self.assertEqual(proc.signal_stop('immediate'), False)
        self.assertEqual(proc.signal_stop('immediate'), True)

    @patch('psutil.Process.__init__', Mock())
    @patch('psutil.wait_procs')
    def test_wait_for_user_backends_to_close(self, mock_wait):
        c1 = Mock()
        c1.cmdline = Mock(return_value=["postgres: startup process   "])
        c2 = Mock()
        c2.cmdline = Mock(return_value=["postgres: postgres postgres [local] idle"])
        c3 = Mock()
        c3.cmdline = Mock(side_effect=psutil.NoSuchProcess(123))
        with patch('psutil.Process.children', Mock(return_value=[c1, c2, c3])):
            proc = PostmasterProcess(123)
            self.assertIsNone(proc.wait_for_user_backends_to_close())
            mock_wait.assert_called_with([c2])

        with patch('psutil.Process.children', Mock(side_effect=psutil.NoSuchProcess(123))):
            proc = PostmasterProcess(123)
            self.assertIsNone(proc.wait_for_user_backends_to_close())

    @patch('subprocess.Popen')
    @patch('os.setsid', Mock(), create=True)
    @patch('multiprocessing.Process', MockProcess)
    @patch('multiprocessing.get_context', Mock(return_value=multiprocessing), create=True)
    @patch.object(PostmasterProcess, 'from_pid')
    @patch.object(PostmasterProcess, '_from_pidfile')
    def test_start(self, mock_frompidfile, mock_frompid, mock_popen):
        mock_frompidfile.return_value._is_postmaster_process.return_value = False
        mock_frompid.return_value = "proc 123"
        mock_popen.return_value.pid = 123
        self.assertEqual(PostmasterProcess.start('true', '/tmp', '/tmp/test.conf', []), "proc 123")
        mock_frompid.assert_called_with(123)

        mock_frompidfile.side_effect = psutil.NoSuchProcess(123)
        self.assertEqual(PostmasterProcess.start('true', '/tmp', '/tmp/test.conf', []), "proc 123")

        mock_popen.side_effect = Exception
        self.assertIsNone(PostmasterProcess.start('true', '/tmp', '/tmp/test.conf', []))

    @patch('psutil.Process.__init__', Mock(side_effect=psutil.NoSuchProcess(123)))
    def test_read_postmaster_pidfile(self):
        with patch.object(builtins, 'open', Mock(side_effect=IOError)):
            self.assertIsNone(PostmasterProcess.from_pidfile(''))
        with patch.object(builtins, 'open', mock_open(read_data='123\n')):
            self.assertIsNone(PostmasterProcess.from_pidfile(''))
