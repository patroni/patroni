import multiprocessing
import os
import unittest

from functools import partial
from pickle import UnpicklingError
from threading import Thread
from unittest.mock import call, MagicMock, Mock, mock_open, patch

import psutil

from patroni.postgresql.postmaster import pg_ctl_start, PostmasterProcess


class TestPostmasterProcess(unittest.TestCase):
    @patch('psutil.Process.__init__', Mock())
    def test_init(self):
        proc = PostmasterProcess(-123)
        self.assertTrue(proc.is_single_user)

    @patch('os.name', 'nt')
    @patch('psutil.Process.create_time')
    @patch('psutil.Process.__init__')
    @patch.object(PostmasterProcess, '_read_postmaster_pidfile')
    def test_from_pidfile(self, mock_read, mock_init, mock_create_time):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        mock_read.return_value = {}
        self.assertIsNone(PostmasterProcess.from_pidfile('', ''))
        mock_read.return_value = {"pid": "foo"}
        self.assertIsNone(PostmasterProcess.from_pidfile('', ''))
        mock_read.return_value = {"pid": "123"}
        self.assertIsNone(PostmasterProcess.from_pidfile('', ''))

        mock_init.side_effect = None
        with patch.object(psutil.Process, 'pid', 123), \
                patch.object(psutil.Process, 'ppid', return_value=124), \
                patch.object(psutil.Process, 'exe') as mock_exe, \
                patch.object(psutil.Process, 'cwd') as mock_cwd, \
                patch('os.getpid', return_value=125) as mock_ospid, \
                patch('os.getppid', return_value=126):
            mock_exe.return_value = 'postgres.exe'

            self.assertIsNone(PostmasterProcess.from_pidfile('', 'foo'))

            mock_create_time.return_value = 100000
            mock_read.return_value = {"pid": "123", "start_time": "200000"}
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))

            mock_read.return_value = {"pid": "123", "start_time": "foobar"}
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))

            mock_cwd.return_value = 'foo'
            self.assertIsNotNone(PostmasterProcess.from_pidfile('postgres', 'foo'))
            self.assertIsNone(PostmasterProcess.from_pidfile('postgres', 'bar'))
            with patch('os.path.realpath', Mock(side_effect=Exception)):
                self.assertIsNotNone(PostmasterProcess.from_pidfile('postgres', 'foo'))

            mock_exe.side_effect = mock_cwd.side_effect = Exception
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))

            mock_read.return_value = {"pid": "123", "start_time": "100000"}
            self.assertIsNotNone(PostmasterProcess.from_pidfile('', ''))

            mock_ospid.return_value = 123
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))

    @patch('psutil.Process.__init__')
    def test_from_pid(self, mock_init):
        mock_init.side_effect = psutil.NoSuchProcess(123)
        self.assertEqual(PostmasterProcess.from_pid(123), None)
        mock_init.side_effect = None
        self.assertNotEqual(PostmasterProcess.from_pid(123), None)

    @patch('psutil.Process.__init__', Mock())
    @patch('psutil.wait_procs', Mock())
    @patch('psutil.Process.suspend')
    @patch('psutil.Process.children')
    @patch('psutil.Process.kill')
    def test_signal_kill(self, mock_kill, mock_children, mock_suspend):
        proc = PostmasterProcess(123)

        # all processes successfully stopped
        mock_children.return_value = [Mock()]
        mock_children.return_value[0].kill.side_effect = psutil.NoSuchProcess(123)
        self.assertTrue(proc.signal_kill())

        # postmaster has gone before suspend
        mock_suspend.side_effect = psutil.NoSuchProcess(123)
        self.assertTrue(proc.signal_kill())

        # postmaster has gone before we got a list of children
        mock_suspend.side_effect = psutil.AccessDenied()
        mock_children.side_effect = psutil.NoSuchProcess(123)
        self.assertTrue(proc.signal_kill())

        # postmaster has gone after we got a list of children
        mock_children.side_effect = psutil.AccessDenied()
        mock_kill.side_effect = psutil.NoSuchProcess(123)
        self.assertTrue(proc.signal_kill())

        # failed to kill postmaster
        mock_kill.side_effect = psutil.AccessDenied()
        self.assertFalse(proc.signal_kill())

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
        c4 = Mock()
        c4.cmdline = Mock(return_value=['postgres: slotsync worker '])
        mock_wait.return_value = ([], [c2])
        with patch('psutil.Process.children', Mock(return_value=[c1, c2, c3, c4])):
            proc = PostmasterProcess(123)
            self.assertIsNone(proc.wait_for_user_backends_to_close(1))
            mock_wait.assert_called_with([c2], 1)

        mock_wait.return_value = ([c2], [])
        with patch('psutil.Process.children', Mock(return_value=[c1, c2, c3, c4])):
            proc = PostmasterProcess(123)
            self.assertTrue(proc.wait_for_user_backends_to_close(1))

        for children in ([], [c1, c3, c4]):
            for stop_timeout in (None, 1):
                with self.subTest(children=children, stop_timeout=stop_timeout):
                    mock_wait.reset_mock()
                    with patch('psutil.Process.children', Mock(return_value=children)):
                        self.assertTrue(proc.wait_for_user_backends_to_close(stop_timeout))
                    mock_wait.assert_not_called()

        with patch('psutil.Process.children', Mock(side_effect=psutil.NoSuchProcess(123))):
            proc = PostmasterProcess(123)
            self.assertIsNone(proc.wait_for_user_backends_to_close(None))

    @patch('os.setsid', Mock(), create=True)
    @patch('subprocess.Popen')
    def test_pg_ctl_start_without_handoff(self, popen):
        conn = Mock(poll=Mock(side_effect=[False] * 4))
        conn.recv.side_effect = [False, True, True, True]
        start = partial(pg_ctl_start, conn, ['postgres'], {})
        start()
        popen.assert_not_called()
        postmaster = popen.return_value
        popen.side_effect = [FileNotFoundError('postgres'), postmaster, postmaster]
        with patch('patroni.postgresql.postmaster.logger.exception') as log:
            start()
            log.assert_called_once_with('Failed to execute %s', ['postgres'])

        with patch('os.getppid', side_effect=[100, 1]):
            start()
        postmaster.kill.assert_called_once_with()
        postmaster.poll.side_effect = [None, 0]
        start()
        self.assertEqual(postmaster.poll.call_count, 2)
        postmaster.kill.assert_called_once_with()

    @patch('os.setsid', Mock(), create=True)
    @patch('subprocess.Popen')
    def test_pg_ctl_start_acknowledgment(self, popen):
        postmaster = popen.return_value = Mock(pid=123, poll=Mock(return_value=None))
        with self.assertLogs('patroni.postgresql.postmaster', level='ERROR') as logs:
            for reply in ('ack', 'disconnect', 'invalid'):
                with self.subTest(reply=reply):
                    parent, child = multiprocessing.Pipe()
                    helper = Thread(target=pg_ctl_start, args=(child, ['postgres'], {}), daemon=True)
                    helper.start()
                    try:
                        with parent:
                            parent.send(True)
                            self.assertTrue(parent.poll(1), 'helper did not report the postmaster PID')
                            self.assertEqual(parent.recv(), postmaster.pid)
                            if reply == 'ack':
                                parent.send(True)
                            elif reply == 'invalid':
                                parent.send_bytes(b'?')
                    finally:
                        helper.join(2)
                    self.assertFalse(helper.is_alive())
                    self.assertTrue(child.closed)
                    self.assertEqual(postmaster.kill.call_count, 0 if reply == 'ack' else 1)
                    postmaster.kill.reset_mock()
        self.assertEqual([record.exc_info[0] for record in logs.records], [UnpicklingError])

    @patch('multiprocessing.get_context')
    @patch('os.environ', {'PG_MALLOC_ARENA_MAX': ''})
    @patch.object(PostmasterProcess, 'from_pid')
    @patch('patroni.postgresql.postmaster.psutil.Process')
    @patch.object(PostmasterProcess, '_from_pidfile')
    def test_start(self, mock_frompidfile, mock_helper, mock_frompid, mock_context):
        mock_frompidfile.return_value._is_postmaster_process.return_value = False
        helper = mock_helper.return_value = Mock(pid=321, ppid=Mock(return_value=os.getpid()))
        postmaster = mock_frompid.return_value = Mock(pid=123, ppid=Mock(return_value=321))
        parent = MagicMock()
        mock_context.return_value.Pipe.return_value = parent, MagicMock()
        mock_context.return_value.Process.return_value.pid = 321
        parent.recv.return_value = 123
        handoff = Mock()
        parent.send = handoff.send
        start = partial(PostmasterProcess.start, 'true', '/tmp', '/tmp/test.conf', [])
        permit = [call.publish(helper), call.send(True)]
        acknowledged = permit + [call.publish(postmaster), call.send(True)]
        cases = (
            (321, [True, True], postmaster, acknowledged), (321, [True, False], None, acknowledged),
            (1, [True], None, permit), (321, [False], None, [call.publish(helper), call.send(False)]),
        )
        for ppid, decisions, result, messages in cases:
            postmaster.ppid.return_value = ppid
            handoff.reset_mock()
            handoff.publish.side_effect = decisions
            self.assertIs(start(handoff.publish), result, (ppid, decisions))
            self.assertEqual(handoff.mock_calls, messages, (ppid, decisions))
        mock_frompid.assert_called_with(postmaster.pid)

        mock_frompidfile.side_effect = psutil.NoSuchProcess(123)
        parent.recv.side_effect = EOFError
        self.assertIsNone(start())
        mock_helper.side_effect = psutil.AccessDenied(321)
        self.assertIsNone(start())

    @patch('psutil.Process.__init__', Mock(side_effect=psutil.NoSuchProcess(123)))
    def test_read_postmaster_pidfile(self):
        with patch('builtins.open', Mock(side_effect=IOError)):
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))
        with patch('builtins.open', mock_open(read_data='123\n')):
            self.assertIsNone(PostmasterProcess.from_pidfile('', ''))
