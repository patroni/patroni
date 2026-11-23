import unittest

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock, patch

from patroni.async_executor import AsyncExecutor, CriticalTask


class TestAsyncExecutor(unittest.TestCase):

    def setUp(self):
        self.a = AsyncExecutor(Mock(), Mock())

    def test_run(self):
        self.a.run(self.a.schedule('foo'), Mock(side_effect=Exception()))
        self.assertFalse(self.a.busy)

    def test_cancel(self):
        self.a.cancel()
        action = self.a.schedule('foo')
        self.a.cancel()
        callback = Mock()
        self.a.run(action, callback)
        callback.assert_not_called()

    @patch('patroni.thread_pool.get_executor')
    def test_cancel_queued_action(self, get_executor):
        submit = get_executor.return_value.submit
        old, replacement = Mock(), Mock()
        self.assertIsNone(self.a.try_run_async('restart', old))
        reservation = self.a.current_action
        run_old, *old_args = submit.call_args[0]
        with self.a, self.a.critical_task as task:
            task.cancel()
        self.a.cancel()
        self.assertIsNone(self.a.try_run_async('restart', replacement))
        run_new, *new_args = submit.call_args[0]
        self.a.cancel(reservation)
        with self.a.critical_task as task:
            self.assertFalse(task.is_cancelled)
            task.complete('replacement')
        run_old(*old_args)
        old.assert_not_called()
        self.assertEqual(self.a.scheduled_action, 'restart')
        self.assertEqual(task.result, 'replacement')
        run_new(*new_args)
        replacement.assert_called_once_with()
        self.assertFalse(self.a.busy)

    def test_cancel_running_action(self):
        started, stop, exiting = Event(), Event(), Event()
        action = self.a.schedule('restart')

        def work():
            started.set()
            if stop.wait(3):
                with self.a:
                    exiting.set()
                return True

        def cancel_process():
            stop.set()
            self.assertTrue(exiting.wait(3))
            self.assertEqual(self.a.schedule('reinitialize'), 'restart')

        self.a._cancellable.cancel.side_effect = cancel_process
        self.a._ha_wakeup.side_effect = lambda: self.a.schedule('reinitialize')
        with ThreadPoolExecutor(1) as pool:
            worker = pool.submit(self.a.run, action, work)
            self.assertTrue(started.wait(3))
            self.a.cancel()
            self.assertTrue(worker.result(3))
        self.a._cancellable.cancel.assert_called_once_with()
        self.assertEqual(self.a.scheduled_action, 'reinitialize')


class TestCriticalTask(unittest.TestCase):

    def test_completed_task(self):
        ct = CriticalTask()
        ct.complete(1)
        self.assertFalse(ct.cancel())
