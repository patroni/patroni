import datetime
import sys
import time
import unittest
import yaml

from mock import Mock, patch
from patroni.api import RestApiServer
from patroni.dcs import Cluster, Member, Leader
from patroni.etcd import Etcd
from patroni.exceptions import DCSError, PostgresException
from patroni import Patroni, main
from patroni.zookeeper import ZooKeeper
from six.moves import BaseHTTPServer
from test_etcd import Client, SleepException, etcd_read, etcd_write
from test_ha import true, false
from test_postgresql import Postgresql, psycopg2_connect
from test_zookeeper import MockKazooClient


def time_sleep(*args):
    raise SleepException()


def get_cluster(initialize, leader):
    return Cluster(initialize, leader, None, None)


def get_cluster_not_initialized_without_leader():
    return get_cluster(None, None)


def get_cluster_initialized_without_leader():
    return get_cluster(True, None)


def get_cluster_not_initialized_with_leader():
    return get_cluster(False, Leader(0, 0, 0,
                       Member(0, 'leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres',
                              None, None, 28)))


def get_cluster_initialized_with_leader():
    return get_cluster(True, Leader(0, 0, 0,
                       Member(0, 'leader', 'postgres://replicator:rep-pass@127.0.0.1:5435/postgres',
                              None, None, 28)))


def get_cluster_dcs_error():
    raise DCSError('')


@patch('time.sleep', Mock())
@patch('subprocess.call', Mock(return_value=0))
@patch('psycopg2.connect', psycopg2_connect)
@patch.object(Postgresql, 'write_pg_hba', Mock())
@patch.object(Postgresql, 'write_recovery_conf', Mock())
@patch.object(BaseHTTPServer.HTTPServer, '__init__', Mock())
class TestPatroni(unittest.TestCase):

    def setUp(self):
        self.touched = False
        self.init_cancelled = False
        RestApiServer._BaseServer__is_shut_down = Mock()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        with open('postgres0.yml', 'r') as f:
            config = yaml.load(f)
            with patch.object(Client, 'machines') as mock_machines:
                mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
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
    def test_patroni_main(self):
        main()
        sys.argv = ['patroni.py', 'postgres0.yml']

        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            with patch.object(Patroni, 'touch_member', self.touch_member):
                with patch.object(Patroni, 'run', Mock(side_effect=SleepException())):
                    self.assertRaises(SleepException, main)
                with patch.object(Patroni, 'run', Mock(side_effect=KeyboardInterrupt())):
                    main()

    @patch('time.sleep', Mock(side_effect=SleepException()))
    def test_patroni_run(self):
        self.p.touch_member = self.touch_member
        self.p.ha.state_handler.sync_replication_slots = time_sleep
        self.p.ha.dcs.watch = time_sleep
        self.assertRaises(SleepException, self.p.run)

        self.p.ha.state_handler.is_leader = false
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
        self.p.postgresql.data_directory_empty = true
        self.p.ha.dcs.initialize = true
        self.p.postgresql.initialize = true
        self.p.postgresql.start = true
        self.p.ha.dcs.get_cluster = get_cluster_not_initialized_without_leader
        self.p.initialize()

        self.p.ha.dcs.initialize = false
        self.p.ha.dcs.get_cluster = get_cluster_initialized_with_leader
        with patch('time.sleep', time_sleep):
            self.p.initialize()

            self.p.ha.dcs.get_cluster = get_cluster_initialized_without_leader
            self.assertRaises(SleepException, self.p.initialize)

            self.p.postgresql.data_directory_empty = false
            self.p.initialize()

            self.p.ha.dcs.get_cluster = get_cluster_not_initialized_with_leader
            self.p.postgresql.data_directory_empty = true
            self.p.initialize()

            self.p.ha.dcs.get_cluster = get_cluster_dcs_error
            self.assertRaises(SleepException, self.p.initialize)

    def test_schedule_next_run(self):
        self.p.ha.dcs.watch = Mock(return_value=True)
        self.p.schedule_next_run()
        self.p.next_run = time.time() - self.p.nap_time - 1
        self.p.schedule_next_run()

    def cancel_initialization(self):
        self.init_cancelled = True

    def test_cleanup_on_initialization(self):
        self.p.ha.dcs.get_cluster = get_cluster_not_initialized_without_leader
        self.p.touch_member = self.touch_member
        self.p.postgresql.data_directory_empty = true
        self.p.ha.dcs.initialize = true
        self.p.postgresql.initialize = true
        self.p.postgresql.start = false

        self.p.ha.dcs.cancel_initialization = self.cancel_initialization
        self.assertRaises(PostgresException, self.p.initialize)
        self.assertTrue(self.init_cancelled)
