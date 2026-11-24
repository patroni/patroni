import logging
import os
import signal
import socket
import sys
import time
import unittest

from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer
from runpy import run_path
from threading import current_thread, Event, Thread, Timer
from types import SimpleNamespace
from unittest.mock import call, Mock, patch, PropertyMock

import etcd

import patroni.__main__ as patroni_main
import patroni.config as config

from patroni.__main__ import check_psycopg, main as _main, Patroni, ShutdownGuard
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.dcs import Cluster, ClusterConfig, Member
from patroni.dcs.etcd import AbstractEtcdClientWithFailover
from patroni.dcs.kubernetes import Kubernetes
from patroni.exceptions import DCSError
from patroni.log import PatroniLogger
from patroni.postgresql import Postgresql
from patroni.postgresql.config import ConfigHandler
from patroni.postgresql.misc import PostgresqlRole, PostgresqlState

from . import psycopg_connect, SleepException
from .test_etcd import etcd_read, etcd_write
from .test_postgresql import MockPostmaster


class GuardClosedEvent(Event):

    def wait(self, timeout=None):
        closed = super().wait(2 if timeout is None else timeout)
        if timeout is None and not closed:
            raise AssertionError('shutdown guard did not finish')
        return closed


class TestShutdownGuard(unittest.TestCase):

    def make_guard(self, is_requested=lambda: True, on_request=lambda: 0):
        guard = ShutdownGuard(is_requested=is_requested, on_request=on_request, on_timeout=Mock(),
                              force_stop=Mock(return_value=None))
        self.addCleanup(guard.close)
        return guard

    @unittest.skipUnless(os.name == 'posix', 'requires the POSIX wakeup fd')
    @patch.object(signal, 'set_wakeup_fd', side_effect=ValueError)
    def test_failed_guard_installation(self, fd):
        for owner, error in ((object(), RuntimeError), (None, ValueError)):
            guard = self.make_guard()
            arming = []
            with patch.object(patroni_main, '_guard', owner):
                self.assertRaises(error, guard.start, lambda armed: arming.append((armed, patroni_main._guard)))
                self.assertIs(patroni_main._guard, owner)
                self.assertEqual(fd.call_count, int(owner is None))
            self.assertEqual(arming, [(True, guard), (False, guard)] if owner is None else [])
            self.assertEqual((guard._reader.fileno(), guard._writer.fileno()), (-1, -1))
            guard.run()

    @unittest.skipUnless(hasattr(signal, 'pthread_sigmask'), 'requires POSIX signal masks')
    def test_fork_hooks(self):
        guard, calls = self.make_guard(), Mock()
        calls.detach.side_effect = guard.detach
        block = call.mask(signal.SIG_BLOCK, {signal.SIGTERM})
        unblock = call.mask(signal.SIG_UNBLOCK, {signal.SIGTERM})
        with patch.object(patroni_main, '_guard', guard), patch.object(guard, 'detach', calls.detach), \
                patch.object(signal, 'pthread_sigmask', calls.mask), patch.object(signal, 'signal', calls.handler), \
                patch.object(signal, 'set_wakeup_fd', calls.wakeup_fd):
            calls.mask.side_effect = [{signal.SIGTERM}, set(), None, set(), None]
            child = patroni_main._after_fork_in_child
            for after in (patroni_main._after_fork, child, child):
                patroni_main._before_fork()
                after()
            self.assertIsNone(patroni_main._guard)
            self.assertEqual((guard._reader.fileno(), guard._writer.fileno()), (-1, -1))
            with patch.object(patroni_main, 'current_thread', return_value=object()):
                patroni_main._before_fork()
                patroni_main._after_fork()
        reset = call.handler(signal.SIGTERM, signal.SIG_DFL)
        expected = [block, block, call.wakeup_fd(-1), reset, call.detach(), unblock, block, unblock]
        self.assertEqual(calls.mock_calls, expected)

    @patch('patroni.__main__.main')
    def test_module_entrypoint_uses_installed_hooks(self, main):
        with patch.object(os, 'register_at_fork', create=True) as register:
            run_path(patroni_main.__file__, run_name='__main__')
            register.assert_not_called()
            module = run_path(patroni_main.__file__, run_name='patroni.__main__')
            register.assert_called_once_with(before=module['_before_fork'], after_in_parent=module['_after_fork'],
                                             after_in_child=module['_after_fork_in_child'])
        legacy_os = SimpleNamespace(**vars(os))
        vars(legacy_os).pop('register_at_fork', None)
        with patch.dict(sys.modules, os=legacy_os):
            run_path(patroni_main.__file__, run_name='patroni.__main__')
        main.assert_called_once_with()

    def test_guard_drains_socket_without_takeover(self):
        calls = Mock()
        with patch.object(patroni_main, 'os', SimpleNamespace(name='nt', _exit=calls.exit)), \
                patch.object(signal, 'set_wakeup_fd', calls.arm), \
                patch.object(patroni_main, 'notify_systemd', calls.notify):
            for normal in (False, True):
                on_request = Mock(return_value=0)
                guard = self.make_guard(is_requested=Mock(return_value=normal), on_request=on_request)
                guard._force_stop = calls.fence
                if normal:
                    self.assertTrue(guard.claim_normal())
                    guard.notify()
                guard.extend_grace(60)
                guard._writer.send(bytes((signal.SIGINT,)))
                guard._writer.close()
                with guard._reader.dup() as reader:
                    guard.start()
                    guard.join(2)
                    self.assertFalse(guard.is_alive())
                    self.assertEqual(reader.recv(4096), b'')
                self.assertEqual(on_request.called, normal)
        self.assertEqual(calls.mock_calls, [])

    @patch('patroni.__main__.os._exit')
    def test_finish_respects_stop_owner(self, exit_process):
        for normal in (False, True):
            with self.subTest(normal=normal):
                guard = self.make_guard(is_requested=lambda: False, on_request=lambda: 60)
                guard._closed = GuardClosedEvent()
                if normal:
                    self.assertTrue(guard.claim_normal())
                guard._force_stop.side_effect = guard._closed.set
                guard.start()
                guard.finish()
                guard.close()
                guard.join(2)
                self.assertEqual(guard._force_stop.call_count, 0 if normal else 1)
        exit_process.assert_not_called()


def mock_import(*args, **kwargs):
    ret = Mock()
    ret.__version__ = '2.5.3.dev1 a b c' if args[0] == 'psycopg2' else '3.1.0'
    return ret


def mock_import2(*args, **kwargs):
    ret = Mock()
    ret.__version__ = '2.8.3.dev1 a b c'
    return ret


def mock_import3(*args, **kwargs):
    if args[0] == 'psycopg2':
        raise ImportError
    ret = Mock()
    ret.__version__ = '0.1.2'
    return ret


class MockFrozenImporter(object):

    toc = set(['patroni.dcs.etcd'])


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('patroni.psycopg.connect', psycopg_connect)
@patch('urllib3.PoolManager.request', Mock(side_effect=Exception))
@patch.object(ConfigHandler, 'append_pg_hba', Mock())
@patch.object(ConfigHandler, 'write_postgresql_conf', Mock())
@patch.object(ConfigHandler, 'write_recovery_conf', Mock())
@patch.object(Postgresql, 'is_running', Mock(return_value=MockPostmaster()))
@patch.object(Postgresql, 'call_nowait', Mock())
@patch.object(HTTPServer, '__init__', Mock())
@patch.object(AsyncExecutor, 'run', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
class TestPatroni(unittest.TestCase):

    @patch('sys.argv', ['patroni.py'])
    def test_no_config(self):
        self.assertRaises(SystemExit, _main)

    @patch('sys.argv', ['patroni.py', '--print', '--validate-config', 'postgres0.yml'])
    @patch('socket.socket.connect_ex', Mock(return_value=1))
    def test_validate_config(self):
        self.assertRaises(SystemExit, _main)
        with patch.object(config.Config, '__init__', Mock(return_value=None)), \
             patch.object(config.Config, 'local_configuration', PropertyMock(return_value={})):
            self.assertRaises(SystemExit, _main)

    @patch('pkgutil.iter_importers', Mock(return_value=[MockFrozenImporter()]))
    @patch('urllib3.PoolManager.request', Mock(side_effect=Exception))
    @patch('sys.frozen', Mock(return_value=True), create=True)
    @patch('patroni.api.PatroniThreadPoolExecutor', Mock())
    @patch('patroni.thread_pool.PatroniThreadPoolExecutor', Mock())
    @patch.object(HTTPServer, '__init__', Mock())
    @patch.object(etcd.Client, 'read', etcd_read)
    @patch.object(Thread, 'start', Mock())
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    @patch.object(Postgresql, '_get_gucs', Mock(return_value={'foo': True, 'bar': True}))
    def setUp(self):
        self._handlers = logging.getLogger().handlers[:]
        RestApiServer._BaseServer__is_shut_down = Mock()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
        conf = config.Config('postgres0.yml')
        self.p = Patroni(conf, PatroniLogger())
        self.addCleanup(self.p.shutdown_guard.close)

    def tearDown(self):
        logging.getLogger().handlers[:] = self._handlers

    def test_apply_dynamic_configuration(self):
        empty_cluster = Cluster.empty()
        self.p.config._dynamic_configuration = {}
        self.p.apply_dynamic_configuration(empty_cluster)
        self.assertEqual(self.p.config._dynamic_configuration['ttl'], 30)

        without_config = empty_cluster._asdict()
        del without_config['config']
        cluster = Cluster(
            config=ClusterConfig(version=1, modify_version=1, data={"ttl": 40}),
            **without_config
        )
        self.p.config._dynamic_configuration = {}
        self.p.apply_dynamic_configuration(cluster)
        self.assertEqual(self.p.config._dynamic_configuration['ttl'], 40)

    @patch('sys.argv', ['patroni.py', 'postgres0.yml'])
    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch('patroni.daemon.stack_size', Mock(side_effect=Exception))
    @patch.object(etcd.Client, 'delete', Mock())
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    @patch.object(Thread, 'join', Mock())
    @patch.object(Postgresql, '_get_gucs', Mock(return_value={'foo': True, 'bar': True}))
    @patch.object(Postgresql, '_wait_for_connection_close', Mock())
    @patch('patroni.postgresql.cancellable.psutil.Popen')
    @patch('patroni.__main__.os._exit')
    def test_patroni_patroni_main(self, exit_process, popen):
        postmaster = MockPostmaster()

        def interrupt(daemon):
            def wait(timeout):
                self.assertFalse(daemon.shutdown_guard._claim(patroni_main._ShutdownOwner.GUARD))
                postmaster.is_running.return_value = False

            postmaster.wait.side_effect = wait
            daemon.postgresql.config._config['before_stop'] = 'test-hook'
            daemon.shutdown_guard._closed = GuardClosedEvent()
            self.addCleanup(daemon.shutdown_guard.close)
            daemon.shutdown_guard.start()
            raise SleepException

        with patch.object(Postgresql, 'is_running', postmaster.is_running), patch('subprocess.call', return_value=1):
            with patch.object(Patroni, 'run', interrupt), \
                patch('patroni.daemon.__systemd_available', False), \
                patch.dict(os.environ, {'PATRONI_THREAD_STACK_SIZE': 'a',
                                        'PATRONI_THREAD_POOL_SIZE': 'a',
                                        'PATRONI_POSTGRESQL_DATA_DIR': 'data/test0',
                                        'NOTIFY_SOCKET': '/run/systemd/notify'}):
                self.assertRaises(SleepException, _main)
            popen.assert_called_once_with(['test-hook'])
            postmaster.signal_stop.assert_called_once()
            postmaster.wait.assert_called_once_with(timeout=None)
            exit_process.assert_not_called()
            postmaster.is_running.return_value = postmaster
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())), \
                    patch.dict(os.environ, {'PATRONI_POSTGRESQL_DATA_DIR': 'data/test0'}), \
                    patch('patroni.ha.Ha.is_paused', Mock(return_value=True)):
                _main()
            postmaster.signal_stop.assert_called_once()

    @patch('os.getpid')
    @patch('multiprocessing.Process')
    @patch('patroni.__main__.patroni_main', Mock())
    @patch('sys.argv', ['patroni.py', 'postgres0.yml'])
    def test_patroni_main(self, mock_process, mock_getpid):
        mock_getpid.return_value = 2
        _main()

        mock_getpid.return_value = 1

        def mock_signal(signo, handler):
            handler(signo, None)

        with patch('signal.signal', mock_signal), patch('os.kill') as mock_kill:
            with patch('os.waitpid', Mock(side_effect=[(1, 0), (0, 0)])), \
                 patch('patroni.__main__.logger') as mock_logger:
                _main()
                mock_kill.assert_called_with(mock_process.return_value.pid, signal.SIGTERM)
                if os.name != 'nt':
                    mock_logger.info.assert_called_with('Reaped pid=%s, exit status=%s', 1, 0)
            with patch('os.waitpid', Mock(side_effect=OSError)):
                _main()

        ref = {'passtochild': lambda signo, stack_frame: 0}

        def mock_sighup(signo, handler):
            if hasattr(signal, 'SIGHUP') and signo == signal.SIGHUP:
                ref['passtochild'] = handler

        def mock_join():
            ref['passtochild'](0, None)

        mock_process.return_value.join = mock_join
        with patch('signal.signal', mock_sighup), patch('os.kill', Mock()):
            self.assertIsNone(_main())

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.reload_local_configuration', Mock(return_value=True))
    @patch('patroni.ha.Ha.is_leader', Mock(return_value=True))
    @patch.object(Postgresql, 'state', PropertyMock(return_value=PostgresqlState.RUNNING))
    @patch.object(Postgresql, 'data_directory_empty', Mock(return_value=False))
    def test_run(self):
        self.p.postgresql.set_role(PostgresqlRole.REPLICA)
        self.p.sighup_handler()
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)
        self.p.api.start = Mock()
        self.p.logger.start = Mock()
        self.p.config._dynamic_configuration = {}
        self.assertRaises(SleepException, self.p.run)
        with patch('patroni.dcs.Cluster.is_unlocked', Mock(return_value=True)):
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.config.Config.reload_local_configuration', Mock(return_value=False)):
            self.p.sighup_handler()
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=True)):
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.postgresql.Postgresql.data_directory_empty', Mock(return_value=False)):
            self.assertRaises(SleepException, self.p.run)

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, self.p.sigterm_handler)
        self.p._on_sigterm = Mock()
        self.p._received_sigterm = False
        with self.p.postgresql._state_lock:
            if os.name == 'posix':
                os.kill(os.getpid(), signal.SIGTERM)
            else:
                self.p.sigterm_handler()
            self.assertTrue(self.p.received_sigterm)
        self.p.sigterm_handler()
        self.assertFalse(self.p.api_sigterm())
        self.p._on_sigterm.assert_called_once_with()
        self.assertRaises(SystemExit, self.p.check_shutdown)
        self.p._received_sigterm = False
        self.assertTrue(self.p.api_sigterm())
        self.assertEqual(self.p._on_sigterm.call_count, 2)

    @patch('patroni.postgresql.cancellable.psutil.Popen')
    @patch('patroni.__main__.os._exit', side_effect=SystemExit)
    def test_guard_takes_over_before_normal_stop(self, exit_process, popen):
        guard = self.p.shutdown_guard
        self.p.ha.set_is_leader(True)
        calls = Mock()
        self.p._received_sigterm = True
        self.p.ha._async_executor.cancel = lambda: self.assertRaises(SystemExit, guard.run)
        self.p.ha.while_not_sync_standby = calls.prepare_standby
        self.p.postgresql.stop, self.p.dcs.delete_leader = calls.normal_stop, calls.delete_leader
        self.p.postgresql.is_running = Mock(return_value=None)
        self.p.postgresql.shutdown_fence = calls.fence
        calls.fence.side_effect = [OSError, None]
        self.p.ha.watchdog.try_disable = calls.disarm
        calls.disarm.side_effect = OSError
        guard._on_request, guard._on_timeout = Mock(side_effect=OSError), calls.report

        def report():
            self.p.postgresql._do_before_stop('test-hook')
            raise OSError

        calls.report.side_effect = report
        calls.notify.side_effect = OSError
        guard._closed.wait = Mock(side_effect=[None])
        with patch('patroni.__main__.notify_systemd', calls.notify):
            self.p.ha.shutdown()
        popen.assert_not_called()
        guard._closed.wait.assert_called_once_with(0.1)
        self.assertEqual(calls.mock_calls, [call.notify('EXTEND_TIMEOUT_USEC=30000000'), call.report(),
                                            call.fence(None), call.fence(None), call.disarm()])
        exit_process.assert_called_once_with(1)

    def test_guard_with_busy_logger_and_watchdog(self):
        guard, watchdog = self.p.shutdown_guard, self.p.watchdog
        handler = self.p.logger._queue_handler
        queue = handler.queue
        watchdog.active = True
        watchdog.impl = Mock(is_running=True, can_be_disabled=False)
        with patch.object(logging.getLogger(), 'handlers', [handler]), ThreadPoolExecutor(1) as pool, queue.mutex, \
                patch('time.sleep', side_effect=AssertionError('unexpected shutdown retry')):
            pool.submit(guard._on_timeout).result(2)
            with watchdog.lock:
                pool.submit(guard._force_stop).result(2)
                self.assertTrue(watchdog.active)
            pool.submit(guard._force_stop).result(2)
        watchdog.impl.keepalive.assert_called_once_with()
        watchdog.impl.close.assert_called_once_with()
        self.assertTrue(queue.empty())
        guard._on_timeout()
        record = queue.get_nowait()
        self.assertEqual(record.levelno, logging.ERROR)
        self.assertIn('shutdown timeout', record.getMessage())
        queue.task_done()

    def test_shutdown_requested(self):
        wakeups = []
        self.p.dcs.wakeup = lambda: wakeups.append((current_thread(), self.p.received_sigterm))
        self.assertEqual(self.p.shutdown_requested(), max(self.p.dcs.loop_wait, 2 * self.p.config['retry_timeout']))
        self.assertEqual(wakeups, [(current_thread(), True)])

    @patch('patroni.__main__.time')
    @patch('patroni.__main__.os._exit', Mock(side_effect=SystemExit))
    def test_guard_waits_for_sync_standby_release(self, clock):
        guard = self.p.shutdown_guard
        deadline = 2 * self.p.dcs.loop_wait + 2 * self.p.config['retry_timeout'] + 2
        clock.monotonic.side_effect = [0, 0, deadline - 1, deadline, deadline, deadline + 3]
        self.p._received_sigterm = True
        self.p.ha.is_synchronous_mode = lambda: True
        calls = Mock()
        guard._force_stop = calls.fence
        self.p.postgresql.stop, self.p.postgresql.is_running = calls.stop, calls.is_running

        def wait_for_standby(stop):
            self.assertRaises(SystemExit, guard.run)
            return stop()

        self.p.ha.while_not_sync_standby = wait_for_standby
        with patch.object(guard, '_reader', autospec=True) as reader:
            def wakeup(_):
                if reader.recv.call_count == 1:
                    guard.extend_grace(3)
                    raise socket.timeout()
                return bytes((signal.SIGTERM,))

            reader.recv.side_effect = wakeup
            self.p.ha.shutdown()
        self.assertEqual(calls.mock_calls, [call.fence()])
        self.assertEqual(reader.settimeout.call_args_list, [call(1), call(3)])

    @unittest.skipUnless(os.name == 'posix', 'requires POSIX signal delivery')
    def test_sigterm_wakes_idle_cycle(self):
        self.p.api.start = self.p.logger.start = Mock()
        # Model a main thread that cannot run Patroni's handler: only CPython's wakeup fd can notify the guard.
        previous_handler = signal.signal(signal.SIGTERM, lambda *_: None)
        self.addCleanup(signal.signal, signal.SIGTERM, previous_handler)
        start = self.p.shutdown_guard.start

        def install(*args):
            self.assertIsNone(self.p._on_sigterm)
            start(*args)
            self.assertEqual(self.p._on_sigterm, self.p.shutdown_guard.notify)

        def wait_for_sigterm():
            self.p.dcs.event.clear()
            timer = Timer(0.02, os.kill, args=(os.getpid(), signal.SIGTERM))
            timer.start()
            self.addCleanup(timer.join)
            self.assertTrue(self.p.dcs.event.wait(1))
            self.assertTrue(self.p.received_sigterm)

        self.p._run_cycle = wait_for_sigterm
        with patch.object(self.p.shutdown_guard, 'start', install):
            self.p.run()

    def test_schedule_next_run(self):
        self.p.ha.cluster = Mock()
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.monotonic() - self.p.dcs.loop_wait - 1
        self.p.schedule_next_run()

    def test__filter_tags(self):
        tags = {'noloadbalance': False, 'clonefrom': False, 'nosync': False, 'smth': 'random'}
        self.assertEqual(self.p._filter_tags(tags), {'smth': 'random'})

        tags['clonefrom'] = True
        tags['smth'] = False
        self.assertEqual(self.p._filter_tags(tags), {'clonefrom': True, 'smth': False})

        tags = {'nofailover': False, 'failover_priority': 0}
        self.assertEqual(self.p._filter_tags(tags), tags)

        tags = {'nofailover': True, 'failover_priority': 1}
        self.assertEqual(self.p._filter_tags(tags), tags)

        tags = {'nosync': False, 'sync_priority': 0}
        self.assertEqual(self.p._filter_tags(tags), tags)

        tags = {'nosync': True, 'sync_priority': 1}
        self.assertEqual(self.p._filter_tags(tags), tags)

    def test_noloadbalance(self):
        self.p.tags['noloadbalance'] = True
        self.assertTrue(self.p.noloadbalance)

    def test_nofailover(self):
        for (nofailover, failover_priority, expected) in [
            # Without any tags, default is False
            (None, None, False),
            # Setting `nofailover: True` has precedence
            (True, 0, True),
            (True, 1, True),
            ('False', 1, True),  # because we use bool() for the value
            # Similarly, setting `nofailover: False` has precedence
            (False, 0, False),
            (False, 1, False),
            ('', 0, False),
            # Only when we have `nofailover: None` should we got based on priority
            (None, 0, True),
            (None, 1, False),
        ]:
            with self.subTest(nofailover=nofailover, failover_priority=failover_priority, expected=expected):
                self.p.tags['nofailover'] = nofailover
                self.p.tags['failover_priority'] = failover_priority
                self.assertEqual(self.p.nofailover, expected)

    def test_failover_priority(self):
        for (nofailover, failover_priority, expected) in [
            # Without any tags, default is 1
            (None, None, 1),
            # Setting `nofailover: True` has precedence (value 0)
            (True, 0, 0),
            (True, 1, 0),
            # Setting `nofailover: False` and `failover_priority: None` gives 1
            (False, None, 1),
            # Normal function of failover_priority
            (None, 0, 0),
            (None, 1, 1),
            (None, 2, 2),
        ]:
            with self.subTest(nofailover=nofailover, failover_priority=failover_priority, expected=expected):
                self.p.tags['nofailover'] = nofailover
                self.p.tags['failover_priority'] = failover_priority
                self.assertEqual(self.p.failover_priority, expected)

    def test_replicatefrom(self):
        self.assertIsNone(self.p.replicatefrom)
        self.p.tags['replicatefrom'] = 'foo'
        self.assertEqual(self.p.replicatefrom, 'foo')

    @patch('patroni.config.Config.reload_local_configuration', Mock(return_value=True))
    def test_reload_config(self):
        self.p.reload_config()
        self.p._get_tags = Mock(side_effect=Exception)
        self.p.reload_config(local=True)

    def test_reload_config_wakes_up_ha_for_watchdog(self):
        with patch.object(self.p, 'watchdog', Mock(needs_immediate_keepalive=True)), \
                patch.object(self.p.ha, 'wakeup') as mock_wakeup:
            self.p.reload_config()
        mock_wakeup.assert_called_once_with()

    def test_reload_config_checks_certificate_when_local_config_changed(self):
        """A renewed certificate must be detected even when the local config changed too."""
        self.p.api.reload_local_certificate = Mock(return_value=True)
        self.p.reload_config(sighup=True, local=True)
        self.p.api.reload_local_certificate.assert_called_once()

    def test_reload_config_updates_effective_role(self):
        """Test that reload_config updates _last_effective_role based on current role."""
        # When role is UNINITIALIZED, _last_effective_role should be None
        self.p.postgresql.set_role(PostgresqlRole.UNINITIALIZED)
        self.p.reload_config()
        self.assertIsNone(self.p._last_effective_role)

        # When role is REPLICA, _last_effective_role should be REPLICA
        self.p.postgresql.set_role(PostgresqlRole.REPLICA)
        self.p.reload_config()
        self.assertEqual(self.p._last_effective_role, PostgresqlRole.REPLICA)

        # When role is PRIMARY, _last_effective_role should be PRIMARY
        self.p.postgresql.set_role(PostgresqlRole.PRIMARY)
        self.p.reload_config()
        self.assertEqual(self.p._last_effective_role, PostgresqlRole.PRIMARY)

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=False))
    @patch.object(Postgresql, 'reload_config')
    def test_run_cycle_role_change_triggers_reload(self, mock_pg_reload):
        """Test that _run_cycle detects role changes and reloads PostgreSQL config."""
        self.p.ha.run_cycle = Mock(return_value='no action')
        self.p.ha.dcs.watch = Mock(return_value=True)

        # Start with _last_effective_role = None (simulating startup)
        self.p._last_effective_role = None
        self.p._last_effective_pg_config = self.p.config['postgresql']

        # Set role to REPLICA
        self.p.postgresql.set_role(PostgresqlRole.REPLICA)

        # Add role-specific parameters to config to ensure effective config differs
        self.p.config['postgresql']['parameters_replica'] = {'work_mem': '32MB'}

        # Run a cycle - should detect role change and reload
        mock_pg_reload.reset_mock()
        self.p._run_cycle()

        # Should have called postgresql.reload_config due to role change
        self.assertTrue(mock_pg_reload.called)
        self.assertEqual(self.p._last_effective_role, PostgresqlRole.REPLICA)

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=False))
    @patch.object(Postgresql, 'reload_config')
    def test_run_cycle_no_reload_when_role_unchanged(self, mock_pg_reload):
        """Test that _run_cycle does not reload when role hasn't changed."""
        self.p.ha.run_cycle = Mock(return_value='no action')
        self.p.ha.dcs.watch = Mock(return_value=True)

        # Set role to REPLICA and mark it as already processed
        self.p.postgresql.set_role(PostgresqlRole.REPLICA)
        self.p._last_effective_role = PostgresqlRole.REPLICA
        self.p._last_effective_pg_config = \
            self.p.config.build_effective_postgresql_configuration(PostgresqlRole.REPLICA)

        # Run a cycle - should NOT reload since role hasn't changed
        mock_pg_reload.reset_mock()
        self.p._run_cycle()

        # Should NOT have called postgresql.reload_config
        self.assertFalse(mock_pg_reload.called)

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=False))
    @patch.object(Postgresql, 'reload_config')
    def test_run_cycle_role_change_replica_to_primary(self, mock_pg_reload):
        """Test that _run_cycle handles role transition from REPLICA to PRIMARY."""
        self.p.ha.run_cycle = Mock(return_value='no action')
        self.p.ha.dcs.watch = Mock(return_value=True)

        # Start as REPLICA
        self.p.postgresql.set_role(PostgresqlRole.REPLICA)
        self.p._last_effective_role = PostgresqlRole.REPLICA
        self.p._last_effective_pg_config = \
            self.p.config.build_effective_postgresql_configuration(PostgresqlRole.REPLICA)

        # Add role-specific parameters to ensure configs differ
        self.p.config['postgresql']['parameters_primary'] = {'work_mem': '128MB'}
        self.p.config['postgresql']['parameters_replica'] = {'work_mem': '32MB'}

        # Transition to PRIMARY
        self.p.postgresql.set_role(PostgresqlRole.PRIMARY)

        # Run a cycle - should detect role change and reload
        mock_pg_reload.reset_mock()
        self.p._run_cycle()

        # Should have called postgresql.reload_config due to role change
        self.assertTrue(mock_pg_reload.called)
        self.assertEqual(self.p._last_effective_role, PostgresqlRole.PRIMARY)

    def test_nosync(self):
        self.p.tags['nosync'] = True
        self.assertTrue(self.p.nosync)
        self.p.tags['nosync'] = None
        self.assertFalse(self.p.nosync)

    def test_nostream(self):
        self.p.tags['nostream'] = 'True'
        self.assertTrue(self.p.nostream)
        self.p.tags['nostream'] = 'None'
        self.assertFalse(self.p.nostream)
        self.p.tags['nostream'] = 'foo'
        self.assertFalse(self.p.nostream)
        self.p.tags['nostream'] = ''
        self.assertFalse(self.p.nostream)

    def test_shutdown(self):
        self.p._api_started = True
        calls = Mock()
        self.p.api._executor = calls.pool
        self.p.ha.shutdown, self.p.logger.shutdown = calls.ha, calls.logger
        calls.pool.shutdown.side_effect = calls.ha.side_effect = RuntimeError
        with patch.object(HTTPServer, 'shutdown', calls.http):
            self.p.shutdown()
        self.assertEqual(calls.mock_calls, [call.http(), call.pool.shutdown(wait=True), call.ha(), call.logger()])

    def test_check_psycopg(self):
        with patch('builtins.__import__', Mock(side_effect=ImportError)):
            self.assertRaises(SystemExit, check_psycopg)
        with patch('builtins.__import__', mock_import):
            self.assertIsNone(check_psycopg())
        with patch('builtins.__import__', mock_import2):
            self.assertIsNone(check_psycopg())
        with patch('builtins.__import__', mock_import3):
            self.assertRaises(SystemExit, check_psycopg)

    def test_ensure_unique_name(self):
        # None/empty cluster implies unique name
        self.assertIsNone(self.p.ensure_unique_name(None))
        empty_cluster = Cluster.empty()
        self.assertIsNone(self.p.ensure_unique_name(empty_cluster))
        without_members = empty_cluster._asdict()
        del without_members['members']

        # Cluster with members with different names implies unique name
        okay_cluster = Cluster(
            members=[Member(version=1, name="distinct", session=1, data={})],
            **without_members
        )
        self.assertIsNone(self.p.ensure_unique_name(okay_cluster))

        # Cluster with a member with the same name that is running
        bad_cluster = Cluster(
            members=[Member(version=1, name="postgresql0", session=1, data={
                "api_url": "https://127.0.0.1:8008",
            })],
            **without_members
        )
        # If the api of the running node cannot be reached, this implies unique name
        with patch('urllib3.PoolManager.request', Mock(side_effect=ConnectionError)):
            self.assertIsNone(self.p.ensure_unique_name(bad_cluster))
        # Only if the api of the running node is reachable do we throw an error
        with patch('urllib3.PoolManager.request', Mock()):
            self.assertRaises(SystemExit, self.p.ensure_unique_name, bad_cluster)
        # With the Kubernetes DCS the check is skipped, the api_url in the Pod annotation may be stale
        with patch('urllib3.PoolManager.request', Mock()) as mock_request, \
                patch.object(self.p, 'dcs', Mock(spec=Kubernetes)):
            self.assertIsNone(self.p.ensure_unique_name(bad_cluster))
            mock_request.assert_not_called()

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(side_effect=[DCSError('foo'), DCSError('foo'), None]))
    def test_ensure_dcs_access(self):
        with patch('patroni.__main__.logger.warning') as mock_logger:
            result = self.p.ensure_dcs_access()
            self.assertEqual(result, None)
            self.assertEqual(mock_logger.call_count, 2)
