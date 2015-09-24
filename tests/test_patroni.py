import datetime
import sys
import time
import unittest
import yaml

from mock import Mock, patch
from patroni.api import RestApiServer
from patroni.dcs import Cluster, Member
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
    @patch.object(Patroni, 'initialize', Mock())
    @patch.object(Etcd, 'delete_leader', Mock())
    @patch.object(Client, 'machines')
    def test_patroni_main(self, mock_machines):
        main()
        sys.argv = ['patroni.py', 'postgres0.yml']

        mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
        with patch.object(Patroni, 'touch_member', self.touch_member):
            with patch.object(Patroni, 'run', Mock(side_effect=SleepException())):
                self.assertRaises(SleepException, main)
            with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
                main()

    @patch('time.sleep', Mock(side_effect=SleepException()))
    def test_run(self):
        self.p.touch_member = self.touch_member
        self.p.ha.state_handler.sync_replication_slots = time_sleep
        self.p.ha.dcs.watch = time_sleep
        self.assertRaises(SleepException, self.p.run)

        self.p.ha.state_handler.is_leader = Mock(return_value=False)
        self.p.api.start = Mock()
        self.assertRaises(SleepException, self.p.run)

    def touch_member(self, ttl=None):
        if not self.touched:
            self.touched = True
            return False
        return True

    def test_touch_member(self):
        self.p.touch_member()
        now = datetime.datetime.utcnow()
        member = Member(0, self.p.postgresql.name, 'b', 'c', (now + datetime.timedelta(
            seconds=self.p.shutdown_member_ttl + 10)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'), None)
        self.p.ha.cluster = Cluster(True, member, 0, [member])
        self.p.touch_member()

    def test_patroni_initialize(self):
        self.p.touch_member = self.touch_member
        self.p.initialize()

    def test_schedule_next_run(self):
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.nap_time - 1
        self.p.schedule_next_run()
