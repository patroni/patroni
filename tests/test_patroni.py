import etcd
import logging
import os
import signal
import time
import unittest

import patroni.config as config
from http.server import HTTPServer
from mock import Mock, PropertyMock, patch
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.dcs import Cluster, Member
from patroni.dcs.etcd import AbstractEtcdClientWithFailover
from patroni.exceptions import DCSError
from patroni.postgresql import Postgresql
from patroni.postgresql.config import ConfigHandler
from patroni.__main__ import check_psycopg, Patroni, main as _main
from threading import Thread

from . import psycopg_connect, SleepException
from .test_etcd import etcd_read, etcd_write
from .test_postgresql import MockPostmaster


def mock_import(*args, **kwargs):
    ret = Mock()
    ret.__version__ = '2.5.3.dev1 a b c' if args[0] == 'psycopg2' else '3.1.0'
    return ret


def mock_import2(*args, **kwargs):
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

    @patch('sys.argv', ['patroni.py', '--validate-config', 'postgres0.yml'])
    @patch('socket.socket.connect_ex', Mock(return_value=1))
    def test_validate_config(self):
        self.assertRaises(SystemExit, _main)
        with patch.object(config.Config, '__init__', Mock(return_value=None)):
            self.assertRaises(SystemExit, _main)

    @patch('pkgutil.iter_importers', Mock(return_value=[MockFrozenImporter()]))
    @patch('urllib3.PoolManager.request', Mock(side_effect=Exception))
    @patch('sys.frozen', Mock(return_value=True), create=True)
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
        self.p = Patroni(conf)

    def tearDown(self):
        logging.getLogger().handlers[:] = self._handlers

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(side_effect=[None, DCSError('foo'), None]))
    def test_load_dynamic_configuration(self):
        self.p.config._dynamic_configuration = {}
        self.p.load_dynamic_configuration()
        self.p.load_dynamic_configuration()

    @patch('sys.argv', ['patroni.py', 'postgres0.yml'])
    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(etcd.Client, 'delete', Mock())
    @patch.object(AbstractEtcdClientWithFailover, '_get_machines_list', Mock(return_value=['http://remotehost:2379']))
    @patch.object(Thread, 'join', Mock())
    @patch.object(Postgresql, '_get_gucs', Mock(return_value={'foo': True, 'bar': True}))
    def test_patroni_patroni_main(self):
        with patch('subprocess.call', Mock(return_value=1)):
            with patch.object(Patroni, 'run', Mock(side_effect=SleepException)):
                os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
                self.assertRaises(SleepException, _main)
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
                with patch('patroni.ha.Ha.is_paused', Mock(return_value=True)):
                    os.environ['PATRONI_POSTGRESQL_DATA_DIR'] = 'data/test0'
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

        with patch('signal.signal', mock_signal):
            with patch('os.waitpid', Mock(side_effect=[(1, 0), (0, 0)])):
                _main()
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
    @patch.object(Postgresql, 'state', PropertyMock(return_value='running'))
    @patch.object(Postgresql, 'data_directory_empty', Mock(return_value=False))
    def test_run(self):
        self.p.postgresql.set_role('replica')
        self.p.sighup_handler()
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)
        self.p.api.start = Mock()
        self.p.logger.start = Mock()
        self.p.config._dynamic_configuration = {}
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

    def test_schedule_next_run(self):
        self.p.ha.cluster = Mock()
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.dcs.loop_wait - 1
        self.p.schedule_next_run()

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
            # Similarly, setting `nofailover: False` has precedence
            (False, 0, False),
            (False, 1, False),
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

    def test_reload_config(self):
        self.p.reload_config()
        self.p._get_tags = Mock(side_effect=Exception)
        self.p.reload_config(local=True)

    def test_nosync(self):
        self.p.tags['nosync'] = True
        self.assertTrue(self.p.nosync)
        self.p.tags['nosync'] = None
        self.assertFalse(self.p.nosync)

    @patch.object(Thread, 'join', Mock())
    def test_shutdown(self):
        self.p.api.shutdown = Mock(side_effect=Exception)
        self.p.ha.shutdown = Mock(side_effect=Exception)
        self.p.shutdown()

    def test_check_psycopg(self):
        with patch('builtins.__import__', Mock(side_effect=ImportError)):
            self.assertRaises(SystemExit, check_psycopg)
        with patch('builtins.__import__', mock_import):
            self.assertIsNone(check_psycopg())
        with patch('builtins.__import__', mock_import2):
            self.assertRaises(SystemExit, check_psycopg)

    def test_ensure_unique_name(self):
        # None/empty cluster implies unique name
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=None)):
            self.assertIsNone(self.p.ensure_unique_name())
        empty_cluster = Cluster.empty()
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=empty_cluster)):
            self.assertIsNone(self.p.ensure_unique_name())
        without_members = empty_cluster._asdict()
        del without_members['members']

        # Cluster with members with different names implies unique name
        okay_cluster = Cluster(
            members=[Member(version=1, name="distinct", session=1, data={})],
            **without_members
        )
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=okay_cluster)):
            self.assertIsNone(self.p.ensure_unique_name())

        # Cluster with a member with the same name that is running
        bad_cluster = Cluster(
            members=[Member(version=1, name="postgresql0", session=1, data={
                "api_url": "https://127.0.0.1:8008",
            })],
            **without_members
        )
        with patch('patroni.dcs.AbstractDCS.get_cluster', Mock(return_value=bad_cluster)):
            # If the api of the running node cannot be reached, this implies unique name
            with patch('urllib3.PoolManager.request', Mock(side_effect=ConnectionError)):
                self.assertIsNone(self.p.ensure_unique_name())
            # Only if the api of the running node is reachable do we throw an error
            with patch('urllib3.PoolManager.request', Mock()):
                self.assertRaises(SystemExit, self.p.ensure_unique_name)
