import sys
import time
import unittest
import yaml

from mock import Mock, patch
from patroni.api import RestApiServer
from patroni.async_executor import AsyncExecutor
from patroni.etcd import Etcd
from patroni import Patroni, main
from patroni.zookeeper import ZooKeeper
from six.moves import BaseHTTPServer
from test_etcd import Client, SleepException, etcd_read, etcd_write
from test_postgresql import Postgresql, psycopg2_connect
from test_zookeeper import MockKazooClient


def time_sleep(*args):
    raise SleepException()


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch.object(Postgresql, 'write_pg_hba', Mock())
@patch.object(Postgresql, 'write_recovery_conf', Mock())
@patch.object(BaseHTTPServer.HTTPServer, '__init__', Mock())
@patch.object(AsyncExecutor, 'run', Mock())
class TestPatroni(unittest.TestCase):

    @patch.object(Client, 'machines')
    def setUp(self, mock_machines):
        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        self.touched = False
        self.init_cancelled = False
        RestApiServer._BaseServer__is_shut_down = Mock()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        with open('postgres0.yml', 'r') as f:
            config = yaml.load(f)
            self.p = Patroni(config)
            self.p.ha.dcs.client.write = etcd_write
            self.p.ha.dcs.client.read = etcd_read

    @patch('patroni.zookeeper.KazooClient', MockKazooClient())
    def test_get_dcs(self):
        self.assertIsInstance(self.p.get_dcs('', {'zookeeper': {'scope': '', 'hosts': ''}}), ZooKeeper)
        self.assertRaises(Exception, self.p.get_dcs, '', {})

    @patch('time.sleep', Mock(side_effect=SleepException()))
    @patch.object(Etcd, 'delete_leader', Mock())
    @patch.object(Client, 'machines')
    def test_patroni_main(self, mock_machines):
        main()
        sys.argv = ['patroni.py', 'postgres0.yml']

        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        with patch.object(Patroni, 'run', Mock(side_effect=SleepException())):
            self.assertRaises(SleepException, main)
        with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
            main()

    @patch('time.sleep', Mock(side_effect=SleepException()))
    def test_run(self):
        self.p.ha.dcs.watch = time_sleep
        self.assertRaises(SleepException, self.p.run)

        self.p.ha.state_handler.is_leader = Mock(return_value=False)
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
