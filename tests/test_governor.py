import os
import psycopg2
import unittest
import requests
import sys
import time
import yaml

from governor import Governor, main, sigchld_handler
from test_ha import true, false
from test_postgresql import Postgresql, os_system, psycopg2_connect
from test_etcd import requests_get, requests_put, requests_delete


def nop(*args, **kwargs):
    pass


def os_waitpid(a, b):
    return (0, 0)


class TestGovernor(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        self.tearDown = self.tear_down
        super(TestGovernor, self).__init__(method_name)

    def set_up(self):
        os.system = os_system
        psycopg2.connect = psycopg2_connect
        requests.get = requests_get
        requests.put = requests_put
        requests.delete = requests_delete
        time.sleep = nop
        Governor.run = nop
        self.write_pg_hba = Postgresql.write_pg_hba
        self.write_recovery_conf = Postgresql.write_recovery_conf
        Postgresql.write_pg_hba = nop
        Postgresql.write_recovery_conf = nop

    def tear_down(self):
        Postgresql.write_pg_hba = self.write_pg_hba
        Postgresql.write_recovery_conf = self.write_recovery_conf

    def test_governor_main(self):
        sys.argv = ['governor.py', 'postgres0.yml']
        main()

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
            g.initialize()

    def test_sigchld_handler(self):
        sigchld_handler(None, None)
        os.waitpid = os_waitpid
        sigchld_handler(None, None)
