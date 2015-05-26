import os
import psycopg2
import unittest
import requests
import sys
import time
import yaml

from governor import Governor, main, sigchld_handler, sigterm_handler
from test_ha import true, false
from test_postgresql import Postgresql, os_system, psycopg2_connect
from test_etcd import requests_get, requests_put, requests_delete

if sys.hexversion >= 0x03000000:
    import http.server as BaseHTTPServer
else:
    import BaseHTTPServer


def nop(*args, **kwargs):
    pass


def os_waitpid(a, b):
    return (0, 0)


def time_sleep(_):
    raise Exception()


class TestGovernor(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestGovernor, self).__init__(method_name)

    def set_up(self):
        self.touched = False
        os.system = os_system
        psycopg2.connect = psycopg2_connect
        requests.get = requests_get
        requests.put = requests_put
        requests.delete = requests_delete
        time.sleep = nop
        self.write_pg_hba = Postgresql.write_pg_hba
        self.write_recovery_conf = Postgresql.write_recovery_conf
        Postgresql.write_pg_hba = nop
        Postgresql.write_recovery_conf = nop
        BaseHTTPServer.HTTPServer.__init__ = nop

    def tear_down(self):
        Postgresql.write_pg_hba = self.write_pg_hba
        Postgresql.write_recovery_conf = self.write_recovery_conf

    def test_sigterm_handler(self):
        self.assertRaises(SystemExit, sigterm_handler, None, None)

    def test_governor_main(self):
        main()
        sys.argv = ['governor.py', 'postgres0.yml']
        time.sleep = time_sleep
        self.assertRaises(Exception, main)

    def touch_member(self):
        if not self.touched:
            self.touched = True
            return False
        return True

    def test_governor_initialize(self):
        with open('postgres0.yml', 'r') as f:
            config = yaml.load(f)
            g = Governor(config)
            g.postgresql.should_use_s3_to_create_replica = false
            g.etcd.base_client_url = 'http://remote'
            g.etcd.client_url
            g.postgresql.data_directory_empty = true
            g.etcd.race = true
            g.initialize()
            g.etcd.race = false
            g.initialize()
            g.postgresql.data_directory_empty = false
            g.touch_member = self.touch_member
            g.initialize()
            g.postgresql.data_directory_empty = true
            time.sleep = time_sleep
            g.postgresql.sync_from_leader = false
            self.assertRaises(Exception, g.initialize)

    def test_sigchld_handler(self):
        sigchld_handler(None, None)
        os.waitpid = os_waitpid
        sigchld_handler(None, None)
