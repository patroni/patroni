import datetime
import helpers.zookeeper
import psycopg2
import subprocess
import sys
import time
import unittest
import yaml

from helpers.api import RestApiServer
from helpers.dcs import Cluster, Member
from helpers.etcd import Etcd
from helpers.zookeeper import ZooKeeper
from mock import Mock, patch
from patroni import Patroni, main
from six.moves import BaseHTTPServer
from test_etcd import Client, etcd_read, etcd_write, etcd_watch
from test_ha import true, false
from test_postgresql import Postgresql, subprocess_call, psycopg2_connect
from test_zookeeper import MockKazooClient


def nop(*args, **kwargs):
    pass


class SleepException(Exception):
    pass


def time_sleep(*args):
    raise SleepException()


class Mock_BaseServer__is_shut_down:

    def set(self):
        pass

    def clear(self):
        pass


class TestPatroni(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestPatroni, self).__init__(method_name)

    def set_up(self):
        self.touched = False
        subprocess.call = subprocess_call
        psycopg2.connect = psycopg2_connect
        self.time_sleep = time.sleep
        time.sleep = nop
        self.write_pg_hba = Postgresql.write_pg_hba
        self.write_recovery_conf = Postgresql.write_recovery_conf
        Postgresql.write_pg_hba = nop
        Postgresql.write_recovery_conf = nop
        BaseHTTPServer.HTTPServer.__init__ = nop
        RestApiServer._BaseServer__is_shut_down = Mock_BaseServer__is_shut_down()
        RestApiServer._BaseServer__shutdown_request = True
        RestApiServer.socket = 0
        with open('postgres0.yml', 'r') as f:
            config = yaml.load(f)
            with patch.object(Client, 'machines') as mock_machines:
                mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
                self.p = Patroni(config)

    def tear_down(self):
        time.sleep = self.time_sleep
        Postgresql.write_pg_hba = self.write_pg_hba
        Postgresql.write_recovery_conf = self.write_recovery_conf

    def test_get_dcs(self):
        helpers.zookeeper.KazooClient = MockKazooClient
        self.assertIsInstance(self.p.get_dcs('', {'zookeeper': {'scope': '', 'hosts': ''}}), ZooKeeper)
        self.assertRaises(Exception, self.p.get_dcs, '', {})

    def test_patroni_main(self):
        main()
        sys.argv = ['patroni.py', 'postgres0.yml']
        time.sleep = time_sleep

        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://remotehost:2379'])
            Patroni.initialize = nop
            touch_member = Patroni.touch_member
            run = Patroni.run

            Patroni.touch_member = self.touch_member
            Patroni.run = time_sleep

            Etcd.delete_leader = nop

            self.assertRaises(SleepException, main)

            Patroni.run = run
            Patroni.touch_member = touch_member

    def test_patroni_run(self):
        time.sleep = time_sleep
        self.p.touch_member = self.touch_member
        self.p.ha.state_handler.sync_replication_slots = time_sleep
        self.p.ha.dcs.client.read = etcd_read
        self.p.ha.dcs.watch = time_sleep
        self.assertRaises(SleepException, self.p.run)
        self.p.ha.state_handler.is_leader = lambda: False
        self.p.api.start = nop
        self.assertRaises(SleepException, self.p.run)

    def touch_member(self, ttl=None):
        if not self.touched:
            self.touched = True
            return False
        return True

    def test_touch_member(self):
        self.p.ha.dcs.client.write = etcd_write
        self.p.touch_member()
        now = datetime.datetime.utcnow()
        member = Member(0, self.p.postgresql.name, 'b', 'c', (now + datetime.timedelta(
            seconds=self.p.shutdown_member_ttl + 10)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'), None)
        self.p.ha.cluster = Cluster(True, member, 0, [member])
        self.p.touch_member()

    def test_patroni_initialize(self):
        self.p.postgresql.should_use_s3_to_create_replica = false
        self.p.ha.dcs.client.write = etcd_write
        self.p.touch_member = self.touch_member
        self.p.postgresql.data_directory_empty = true
        self.p.ha.dcs.race = true
        self.p.initialize()

        self.p.ha.dcs.race = false
        time.sleep = time_sleep
        self.p.ha.dcs.client.read = etcd_read
        self.p.initialize()

        self.p.ha.dcs.current_leader = nop
        self.assertRaises(Exception, self.p.initialize)

        self.p.postgresql.data_directory_empty = false
        self.p.initialize()

    def test_schedule_next_run(self):
        self.p.next_run = time.time() - self.p.nap_time - 1
        self.p.schedule_next_run()
