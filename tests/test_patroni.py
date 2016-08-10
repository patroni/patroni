import etcd
import sys
import time
import unittest

from mock import Mock, patch
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.exceptions import DCSError
from patroni import Patroni, main as _main
from six.moves import BaseHTTPServer
from test_etcd import SleepException, etcd_read, etcd_write
from test_postgresql import Postgresql, psycopg2_connect


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch.object(Postgresql, 'write_pg_hba', Mock())
@patch.object(Postgresql, '_write_postgresql_conf', Mock())
@patch.object(Postgresql, 'write_recovery_conf', Mock())
@patch.object(Postgresql, 'is_running', Mock(return_value=True))
@patch.object(BaseHTTPServer.HTTPServer, '__init__', Mock())
@patch.object(AsyncExecutor, 'run', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
class TestPatroni(unittest.TestCase):

    @patch.object(etcd.Client, 'read', etcd_read)
    def setUp(self):
        RestApiServer._BaseServer__is_shut_down = Mock()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        with patch.object(etcd.Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            sys.argv = ['patroni.py', 'postgres0.yml']
            self.p = Patroni()

    @patch('patroni.dcs.AbstractDCS.get_cluster', Mock(side_effect=[None, DCSError('foo'), None]))
    def test_load_dynamic_configuration(self):
        self.p.config._dynamic_configuration = {}
        self.p.load_dynamic_configuration()
        self.p.load_dynamic_configuration()

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(etcd.Client, 'delete', Mock())
    @patch.object(etcd.Client, 'machines')
    def test_patroni_main(self, mock_machines):
        with patch('subprocess.call', Mock(return_value=1)):
            sys.argv = ['patroni.py', 'postgres0.yml']

            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            with patch.object(Patroni, 'run', Mock(side_effect=SleepException)):
                self.assertRaises(SleepException, _main)
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
                _main()

    @patch('patroni.config.Config.save_cache', Mock())
    @patch('patroni.config.Config.reload_local_configuration', Mock(return_value=True))
    def test_run(self):
        self.p.sighup_handler()
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)
        self.p.api.start = Mock()
        self.p.config._dynamic_configuration = {}
        self.assertRaises(SleepException, self.p.run)
        with patch('patroni.config.Config.set_dynamic_configuration', Mock(return_value=True)):
            self.assertRaises(SleepException, self.p.run)
        with patch('patroni.postgresql.Postgresql.data_directory_empty', Mock(return_value=False)):
            self.assertRaises(SleepException, self.p.run)

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, self.p.sigterm_handler)

    def test_schedule_next_run(self):
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.dcs.loop_wait - 1
        self.p.schedule_next_run()

    def test_noloadbalance(self):
        self.p.tags['noloadbalance'] = True
        self.assertTrue(self.p.noloadbalance)

    def test_nofailover(self):
        self.p.tags['nofailover'] = True
        self.assertTrue(self.p.nofailover)
        self.p.tags['nofailover'] = None
        self.assertFalse(self.p.nofailover)

    def test_replicatefrom(self):
        self.assertIsNone(self.p.replicatefrom)
        self.p.tags['replicatefrom'] = 'foo'
        self.assertEqual(self.p.replicatefrom, 'foo')

    def test_reload_config(self):
        self.p.reload_config()
        self.p.get_tags = Mock(side_effect=Exception)
        self.p.reload_config()
