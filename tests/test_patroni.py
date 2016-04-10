import etcd
import os
import sys
import time
import unittest
import yaml

from mock import Mock, patch
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.etcd import Etcd
from patroni import Patroni, main as _main
from patroni.zookeeper import ZooKeeper
from six.moves import BaseHTTPServer
from test_etcd import Client, SleepException, etcd_read, etcd_write
from test_postgresql import Postgresql, psycopg2_connect
from test_zookeeper import MockKazooClient


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch.object(Postgresql, 'write_pg_hba', Mock())
@patch.object(Postgresql, 'write_recovery_conf', Mock())
@patch.object(BaseHTTPServer.HTTPServer, '__init__', Mock())
@patch.object(AsyncExecutor, 'run', Mock())
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
class TestPatroni(unittest.TestCase):

    def setUp(self):
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            self.touched = False
            self.init_cancelled = False
            RestApiServer._BaseServer__is_shut_down = Mock()
            RestApiServer._BaseServer__shutdown_request = True
            RestApiServer.socket = 0
            with open('postgres0.yml', 'r') as f:
                config = yaml.load(f)
                self.p = Patroni(config)

    @patch('patroni.zookeeper.KazooClient', MockKazooClient())
    def test_get_dcs(self):
        self.assertIsInstance(self.p.get_dcs('', {'zookeeper': {'scope': '', 'hosts': ''}}), ZooKeeper)
        self.assertRaises(Exception, self.p.get_dcs, '', {})

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(Etcd, 'delete_leader', Mock())
    @patch.object(Client, 'machines')
    def test_patroni_main(self, mock_machines):
        _main()
        sys.argv = ['patroni.py', 'postgres0.yml']

        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        with patch.object(Patroni, 'run', Mock(side_effect=SleepException)):
            self.assertRaises(SleepException, _main)
        with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
            _main()
        sys.argv = ['patroni.py']
        # read the content of the yaml configuration file into the environment variable
        # in order to test how does patroni handle the configuration passed from the environment.
        with open('postgres0.yml', 'r') as f:
            os.environ[Patroni.PATRONI_CONFIG_VARIABLE] = f.read()
        with patch.object(Patroni, 'run', Mock(side_effect=SleepException())):
            self.assertRaises(SleepException, _main)
        del os.environ[Patroni.PATRONI_CONFIG_VARIABLE]

    def test_run(self):
        self.p.ha.dcs.watch = Mock(side_effect=SleepException)
        self.p.api.start = Mock()
        self.assertRaises(SleepException, self.p.run)

    def test_schedule_next_run(self):
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.nap_time - 1
        self.p.schedule_next_run()

    def test_nofailover(self):
        self.p.tags['nofailover'] = True
        self.assertTrue(self.p.nofailover)
        self.p.tags['nofailover'] = None
        self.assertFalse(self.p.nofailover)

    def test_replicatefrom(self):
        self.assertIsNone(self.p.replicatefrom)
        self.p.tags['replicatefrom'] = 'foo'
        self.assertEqual(self.p.replicatefrom, 'foo')
