import logging
import os
import signal
import sys
import time
import unittest

from http.server import HTTPServer
from threading import Thread
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import etcd
import urllib3

import patroni.config as config

from patroni.__main__ import check_psycopg, main as _main, Patroni
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

from . import HangingHTTPServer, psycopg_connect, SleepException
from .test_etcd import etcd_read, etcd_write
from .test_postgresql import MockPostmaster


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
    def test_patroni_patroni_main(self):
        with patch('subprocess.call', Mock(return_value=1)):
            with patch.object(Patroni, 'run', Mock(side_effect=SleepException)), \
                patch('patroni.daemon.__systemd_available', False), \
                patch.dict(os.environ, {'PATRONI_THREAD_STACK_SIZE': 'a',
                                        'PATRONI_THREAD_POOL_SIZE': 'a',
                                        'PATRONI_POSTGRESQL_DATA_DIR': 'data/test0',
                                        'NOTIFY_SOCKET': '/run/systemd/notify'}):
                self.assertRaises(SleepException, _main)
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())), \
                    patch.dict(os.environ, {'PATRONI_POSTGRESQL_DATA_DIR': 'data/test0'}), \
                    patch('patroni.ha.Ha.is_paused', Mock(return_value=True)):
                _main()

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

    def test_api_sigterm(self):
        self.assertTrue(self.p.api_sigterm())
        self.assertFalse(self.p.api_sigterm())
        self.p.sigterm_handler()  # doesn't raise SystemExit, because SIGTERM was already received

    @patch('signal.alarm', create=True)
    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=False))
    @patch.object(Postgresql, 'reload_config', Mock())
    def test_graceful_sigterm(self, mock_alarm):
        self.p.enable_graceful_sigterm()
        self.p.api.start = Mock()
        self.p.logger.start = Mock()
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)

        def run_cycle():
            event = self.p.dcs.event
            event.clear()
            with patch('threading.Condition.__enter__', side_effect=AssertionError('SIGTERM entered a condition')):
                self.p.sigterm_handler()  # doesn't raise SystemExit anymore
            self.assertTrue(event.is_set())
            return 'no action'

        # the main loop exits after the HA cycle, during which SIGTERM was received, without waiting for the next one
        self.p.ha.run_cycle = Mock(side_effect=run_cycle)
        self.p.run()
        self.assertTrue(self.p.received_sigterm)
        self.p.ha.dcs.watch.assert_not_called()
        # the alarm is armed on SIGTERM and canceled when the main loop exits
        loop_wait = self.p.config['loop_wait']
        self.assertEqual(mock_alarm.call_args_list, [] if os.name == 'nt' else [((loop_wait,),), ((0,),)])

    def test_sigalrm_handler(self):
        self.p.sigalrm_handler()  # doesn't raise SystemExit before the main loop has started
        self.p.api.start = Mock()
        self.p.logger.start = Mock()
        # the main loop is stuck in the HA cycle when SIGALRM arrives
        self.p.ha.run_cycle = Mock(side_effect=self.p.sigalrm_handler)
        self.assertRaises(SystemExit, self.p.run)
        self.p.sigalrm_handler()  # doesn't raise SystemExit after the main loop has exited

    @unittest.skipIf(sys.thread_info.lock != 'semaphore', 'signals do not interrupt Lock.acquire()')
    def test_sigterm_wakes_up_ha_loop(self):
        self.p.enable_graceful_sigterm()
        self.p.dcs.event.clear()
        handler = signal.signal(signal.SIGALRM, self.p.sigterm_handler)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.1)
            start = time.time()
            self.assertTrue(self.p.dcs.watch(None, 10))
            self.assertLess(time.time() - start, 5)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, handler)
        self.assertTrue(self.p.received_sigterm)

    @unittest.skipIf(os.name == 'nt', "Windows not supported")
    def test_sigterm_interrupts_long_poll(self):
        self.p.enable_graceful_sigterm()
        self.p.dcs.event.clear()
        handler = signal.signal(signal.SIGALRM, self.p.sigterm_handler)
        http = self.p.dcs._client.http

        def watch(*args, **kwargs):
            signal.setitimer(signal.ITIMER_REAL, 0.1)
            # without the interruption the long-poll fails with ReadTimeoutError
            return http.urlopen('GET', server.url, retries=False, timeout=urllib3.Timeout(connect=5, read=10))

        try:
            with HangingHTTPServer() as server, \
                 patch.object(etcd.Client, 'watch', Mock(side_effect=watch)) as mock_watch:
                start = time.monotonic()
                self.assertTrue(self.p.dcs.watch(20729, 10))
                self.assertLess(time.monotonic() - start, 5)
                mock_watch.assert_called_once()
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, handler)
        self.assertTrue(self.p.received_sigterm)

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

    @patch.object(Thread, 'join', Mock())
    def test_shutdown(self):
        self.p.api.shutdown = Mock(side_effect=Exception)
        self.p.ha.shutdown = Mock(side_effect=Exception)
        self.p.shutdown()

    @patch.object(Thread, 'join', Mock())
    def test_main_thread_does_not_take_sigterm_lock(self):
        # SystemExit raised by sigalrm_handler() would leak the lock held by the main thread and deadlock shutdown()
        self.p._sigterm_lock = MagicMock()
        self.p._sigterm_lock.__enter__.side_effect = AssertionError('the main thread took _sigterm_lock')
        self.assertFalse(self.p.received_sigterm)
        self.p.api.shutdown = Mock()
        self.p.ha.shutdown = Mock()
        self.p.shutdown()
        self.assertTrue(self.p.received_sigterm)

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
