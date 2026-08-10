import subprocess
import unittest

from unittest.mock import Mock, patch

import psutil

from patroni.exceptions import PostgresException
from patroni.postgresql.cancellable import CancellableSubprocess
from patroni.thread_pool import PatroniThreadPoolExecutor


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

    @patch('patroni.thread_pool.get_executor', Mock(return_value=PatroniThreadPoolExecutor(max_workers=1)))
    @patch('patroni.postgresql.cancellable.psutil.Popen')
    def test_call_stream_cb(self, mock_popen):
        def _mock_stream(*chunks):
            return Mock(read1=Mock(side_effect=list(chunks) + [b'']), close=Mock())

        mock_popen.return_value = Mock(stdout=_mock_stream(b'out1\nout2\n', b'tail'),
                                       stderr=_mock_stream(b'err1\r\nerr2\n'))
        mock_popen.return_value.wait.return_value = 0

        lines = []

        def cb(name, line):
            lines.append((name, line))
            if line == b'out2':
                raise RuntimeError('boom')  # a bad callback must not stop streaming

        communicate = {}
        ret = self.c.call(['cmd'], communicate=communicate, text=True, stream_cb=cb)
        self.assertEqual(ret, 0)
        # callback errors are swallowed: every line is still delivered (per-stream order is deterministic),
        # the trailing stdout line has no newline and the stderr CRLF is consumed as a single separator
        self.assertEqual([line for name, line in lines if name == 'stdout'], [b'out1', b'out2', b'tail'])
        self.assertEqual([line for name, line in lines if name == 'stderr'], [b'err1', b'err2'])
        # the full output is still captured back into the communicate dict
        self.assertEqual(communicate['stdout'], b'out1\nout2\ntail')
        self.assertEqual(communicate['stderr'], b'err1\r\nerr2\n')
        # streaming forces binary pipes and DEVNULL stdin (no input)
        kwargs = mock_popen.call_args[1]
        self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
        self.assertEqual(kwargs['stdout'], subprocess.PIPE)
        self.assertEqual(kwargs['stderr'], subprocess.PIPE)
        self.assertNotIn('text', kwargs)

    @patch('patroni.postgresql.cancellable.psutil.Popen')
    def test_call_stream_cb_cr_terminated(self, mock_popen):
        lines = []

        class MockStream(object):

            def __init__(self, *chunks):
                self._chunks = list(chunks)
                self.lines_before_read = []
                self.close = Mock()

            def read1(self, _):
                self.lines_before_read.append(list(lines))
                return self._chunks.pop(0) if self._chunks else b''

        # pg_basebackup (PostgreSQL 10 and older) and pg_rewind (PostgreSQL 11 and older) terminate every
        # progress report with a bare CR, and a CRLF may be split between two reads
        stdout = MockStream(b'128/512 MB (25%) copied\r',
                            b'256/512 MB (50%) copied\r\n512/512 MB (100%) copied\r',
                            b'\nsyncing data to disk ...')
        mock_popen.return_value = Mock(stdout=stdout, stderr=None)
        mock_popen.return_value.wait.return_value = 0

        communicate = {}
        ret = self.c.call(['cmd'], communicate=communicate, stream_cb=lambda name, line: lines.append(line))
        self.assertEqual(ret, 0)
        # the CR-terminated report was delivered while the process was still writing, not at EOF
        self.assertEqual(stdout.lines_before_read[1], [b'128/512 MB (25%) copied'])
        self.assertEqual(lines, [b'128/512 MB (25%) copied', b'256/512 MB (50%) copied',
                                 b'512/512 MB (100%) copied', b'syncing data to disk ...'])
        self.assertEqual(communicate['stdout'], b'128/512 MB (25%) copied\r256/512 MB (50%) copied\r\n'
                                                b'512/512 MB (100%) copied\r\nsyncing data to disk ...')

    @patch('patroni.postgresql.cancellable.psutil.Popen')
    def test_call_stream_cb_ignored_with_input(self, mock_popen):
        # when there is input to send, stream_cb is ignored and communicate() is used instead
        mock_popen.return_value.wait.return_value = 0
        mock_popen.return_value.communicate.return_value = (b'o', b'e')

        called = []
        communicate = {'input': 'data'}
        ret = self.c.call(['cmd'], communicate=communicate,
                          stream_cb=lambda name, line: called.append((name, line)))
        self.assertEqual(ret, 0)
        self.assertEqual(called, [])
        mock_popen.return_value.communicate.assert_called_once_with(b'data\n')
        self.assertEqual(communicate['stdout'], b'o')
        self.assertEqual(communicate['stderr'], b'e')
        self.assertEqual(mock_popen.call_args[1]['stdin'], subprocess.PIPE)
