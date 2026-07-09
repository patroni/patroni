import sys
import unittest

from typing import List
from unittest.mock import Mock, patch

import psutil

from patroni.exceptions import PostgresException
from patroni.postgresql.cancellable import CancellableSubprocess


class TestCancellableSubprocess(unittest.TestCase):

    def setUp(self):
        self.c = CancellableSubprocess()

    def test_call(self):
        self.c.cancel()
        self.assertRaises(PostgresException, self.c.call)

    def test_call_with_stream_output(self):
        process = Mock()
        process.stdout.fileno.return_value = 3
        process.stderr.fileno.return_value = 4
        process.wait.return_value = 1

        def start_process(*args, **kwargs):
            self.c._process = process
            return True

        streamed = []
        communicate = {}
        stdout_data = b'out line 1\n\nout partial'
        stderr_data = b'  250/1000 kB (25%) copied\r  500/1000 kB (50%) copied\rerr partial'
        reads = [stdout_data, stderr_data, b'', BlockingIOError, b'']
        polls = [[(3, 1)], [(4, 1)], [(3, 1)], [(4, 1)], [(4, 1)]]
        with patch.object(CancellableSubprocess, '_start_process', side_effect=start_process), \
                patch('patroni.postgresql.cancellable.os.name', 'posix'), \
                patch('patroni.postgresql.cancellable.os.set_blocking') as mock_set_blocking, \
                patch('patroni.postgresql.cancellable.os.read', side_effect=reads), \
                patch('patroni.postgresql.cancellable.select') as mock_select:
            mock_select.poll.return_value.poll.side_effect = polls
            self.assertEqual(self.c.call(['cmd'], communicate=communicate, stream_output=streamed.append), 1)

        process.stdin.close.assert_called_once()
        self.assertEqual({call[0] for call in mock_set_blocking.call_args_list}, {(3, False), (4, False)})
        self.assertEqual({call[0][0] for call in mock_select.poll.return_value.unregister.call_args_list}, {3, 4})
        self.assertEqual(streamed, ['out line 1', '  250/1000 kB (25%) copied',
                                    '  500/1000 kB (50%) copied', 'out partial', 'err partial'])
        self.assertEqual(communicate, {'stdout': stdout_data, 'stderr': stderr_data})

    def test_call_reaps_process_when_stream_output_raises(self):
        processes: List[psutil.Process] = []

        def stream_output(line: str) -> None:
            assert self.c._process is not None
            processes.append(self.c._process)
            raise RuntimeError('stream callback failed')

        try:
            with self.assertRaisesRegex(RuntimeError, 'stream callback failed'):
                self.c.call([sys.executable, '-c', 'import time; print("ready", flush=True); time.sleep(30)'],
                            communicate={}, stream_output=stream_output)

            self.assertEqual(len(processes), 1)
            self.assertFalse(processes[0].is_running())
            self.assertIsNone(self.c._process)
        finally:
            if processes and processes[0].is_running():
                processes[0].kill()
                processes[0].wait()

    def test__stream_data_handles_separators_split_across_chunks(self):
        streamed: List[str] = []
        buf = bytearray()

        self.c._stream_data(b'hello\r', buf, streamed.append)
        self.c._stream_data(b'\nworld', buf, streamed.append)
        self.c._stream_data(b'\n', buf, streamed.append)

        self.assertEqual(streamed, ['hello', 'world'])
        self.assertEqual(buf, b'')

    def test__stream_data_keeps_large_incomplete_line_without_emitting(self):
        streamed: List[str] = []
        buf = bytearray()

        for _ in range(512):
            self.c._stream_data(b'x' * 8192, buf, streamed.append)

        self.assertEqual(streamed, [])
        self.assertEqual(len(buf), 4 * 1024 * 1024)

    def test_call_with_stream_output_fallback(self):
        process = Mock()
        process.communicate.return_value = (b'out', b'err')

        def start_process(*args, **kwargs):
            self.c._process = process
            return True

        with patch.object(CancellableSubprocess, '_start_process', side_effect=start_process):
            with patch('patroni.postgresql.cancellable.os.name', 'nt'):
                streamed = []
                communicate = {}
                self.c.call(['cmd'], communicate=communicate, stream_output=streamed.append)
                process.communicate.assert_called_once_with()
                self.assertEqual(streamed, ['out', 'err'])
                self.assertEqual(communicate, {'stdout': b'out', 'stderr': b'err'})

    def test_call_rejects_stream_output_without_communicate(self):
        with patch.object(CancellableSubprocess, '_start_process') as mock_start_process:
            with self.assertRaisesRegex(ValueError, 'requires a.*communicate.*dictionary'):
                self.c.call(['cmd'], stream_output=Mock())
            mock_start_process.assert_not_called()

    def test_call_rejects_stream_output_with_input(self):
        with patch.object(CancellableSubprocess, '_start_process') as mock_start_process:
            with self.assertRaisesRegex(ValueError, 'incompatible with.*input'):
                self.c.call(['cmd'], communicate={'input': 'x'}, stream_output=Mock())
            mock_start_process.assert_not_called()

    def test_call_rejects_stream_output_in_text_mode(self):
        text_options = ({'text': True}, {'universal_newlines': True},
                        {'encoding': 'utf-8'}, {'errors': 'replace'})
        for options in text_options:
            with self.subTest(options=options), \
                    patch.object(CancellableSubprocess, '_start_process') as mock_start_process:
                with self.assertRaisesRegex(ValueError, 'incompatible with text mode'):
                    self.c.call(['cmd'], communicate={}, stream_output=Mock(), **options)
                mock_start_process.assert_not_called()

    def test_call_with_input(self):
        process = Mock()
        process.communicate.return_value = (b'', b'')

        def start_process(*args, **kwargs):
            self.c._process = process
            return True

        communicate = {'input': 'x'}
        with patch.object(CancellableSubprocess, '_start_process', side_effect=start_process):
            self.c.call(['cmd'], communicate=communicate)

        process.communicate.assert_called_once_with(b'x\n')

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
