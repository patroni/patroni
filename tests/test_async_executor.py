import unittest

from threading import Event, Thread
from unittest.mock import Mock, patch

from patroni.async_executor import AsyncExecutor, CriticalTask


class TestAsyncExecutor(unittest.TestCase):

    def setUp(self):
        self.a = AsyncExecutor(Mock(), Mock())

    def _start_task(self):
        started, finish = Event(), Event()
        self.addCleanup(finish.set)

        def func():
            started.set()
            finish.wait(5)

        thread = Thread(target=self.a.run, args=(func,), daemon=True)
        thread.start()
        self.assertTrue(started.wait(5))
        return thread, finish

    @patch('patroni.thread_pool.get_executor', Mock())
    def test_run_async(self):
        self.a.run_async(Mock(return_value=True))

    def test_run(self):
        self.a.run(Mock(side_effect=Exception()))

    def test_cancel(self):
        self.a.cancel()
        self.a.schedule('foo')
        self.a.cancel()
        self.a.run(Mock())

    def test_shutdown(self):
        self.a.shutdown()
        self.assertEqual(self.a.schedule('foo'), 'shutdown')
        func = Mock()
        self.assertIsNone(self.a.run(func))
        func.assert_not_called()

    def test_shutdown_waits_for_all_tasks(self):
        # a forced reinitialize cancels a pending restart, but the restart could still start afterwards
        self.a.schedule('restart')
        self.a.cancel()
        self.a.schedule('reinitialize')
        reinitialize, finish_reinitialize = self._start_task()
        restart, finish_restart = self._start_task()

        shutdown = Thread(target=self.a.shutdown, daemon=True)
        shutdown.start()
        finish_reinitialize.set()
        reinitialize.join(5)
        shutdown.join(0.2)
        self.assertTrue(shutdown.is_alive())

        finish_restart.set()
        restart.join(5)
        shutdown.join(5)
        self.assertFalse(shutdown.is_alive())


class TestCriticalTask(unittest.TestCase):

    def test_completed_task(self):
        ct = CriticalTask()
        ct.complete(1)
        self.assertFalse(ct.cancel())
