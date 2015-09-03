import consul
import time
import unittest

from collections import namedtuple
from helpers.consul import Cluster, Consul, ConsulError, ConsulException, NotFound
from test_etcd import MockPostgresql, time_sleep_exception, SleepException


def nop(*args, **kwargs):
    pass


MockResponse = namedtuple('Response', 'status_code,headers,text')


def http_get(*args, **kwargs):
    return MockResponse(None, None, None)


def kv_get(self, key, **kwargs):
    if key == 'service/test/members/postgresql1':
        return 1, {'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7'}
    if key == 'service/test/':
        return None, None
    if key == 'service/good/':
        return ('6429',
                [{'CreateIndex': 1334, 'Flags': 0, 'Key': key + 'initialize', 'LockIndex': 0,
                  'ModifyIndex': 1334, 'Value': 'postgresql0'},
                 {'CreateIndex': 2621, 'Flags': 0, 'Key': key + 'leader', 'LockIndex': 1,
                  'ModifyIndex': 2621, 'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7', 'Value': 'postgresql1'},
                 {'CreateIndex': 6156, 'Flags': 0, 'Key': key+ 'members/postgresql0', 'LockIndex': 1,
                  'ModifyIndex': 6156, 'Session': '782e6da4-ed02-3aef-7963-99a90ed94b53',
                  'Value': 'postgres://replicator:rep-pass@127.0.0.1:5432/postgres?application_name=http://127.0.0.1:8008/patroni'},
                 {'CreateIndex': 2630, 'Flags': 0, 'Key': key + 'members/postgresql1', 'LockIndex': 1,
                  'ModifyIndex': 2630, 'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7',
                  'Value': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres?application_name=http://127.0.0.1:8009/patroni'},
                 {'CreateIndex': 1085, 'Flags': 0, 'Key': key + 'optime/leader', 'LockIndex': 0,
                  'ModifyIndex': 6429, 'Value': '4496294792'}])
    raise ConsulException


def kv_put(self, key, value, **kwargs):
    if key == 'service/good/leader':
        return False
    raise ConsulException


def kv_delete(self, key, *args, **kwargs):
    pass


def session_renew(self, session):
    raise NotFound


def session_create(self, *args, **kwargs):
    if kwargs.get('name', None) == 'test-postgresql1':
        return 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7'
    raise ConsulException


class TestConsul(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestConsul, self).__init__(method_name)

    def set_up(self):
        consul.Consul.KV.get = kv_get
        consul.Consul.KV.put = kv_put
        consul.Consul.KV.delete = kv_delete
        consul.Consul.Session.create = session_create
        consul.Consul.Session.renew = session_renew
        self.c = Consul('postgresql1', {'ttl': 30, 'scope': 'test', 'host': 'localhost:1'})
        self.c._base_path = '/service/good'
        self.c.get_cluster(1)

    def test_client_get(self):
        self.c.client.http.session.get = http_get
        self.c.client.http.get(nop, '', {'wait': '1s', 'index': 1})

    def test_referesh_session(self):
        self.c._session = '1'
        self.c._name = ''
        self.assertRaises(ConsulError, self.c.referesh_session)

    def test_create_session(self):
        time.sleep = time_sleep_exception
        self.c._session = None
        self.c._name = ''
        self.assertRaises(SleepException, self.c.create_session, True)
        self.c.create_session()

    def test_get_cluster(self):
        self.c._base_path = '/service/test'
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.c._base_path = '/service/fail'
        self.assertRaises(ConsulError, self.c.get_cluster)
        self.assertRaises(ConsulException, self.c.get_cluster, 1)
        self.c._base_path = '/service/good'
        self.c._session = 'fd4f44fe-2cac-bba5-a60b-304b51ff39b8'
        self.assertIsInstance(self.c.get_cluster(), Cluster)

    def test_touch_member(self):
        self.c.referesh_session = lambda: True
        self.c.touch_member('balbla')

    def test_take_leader(self):
        self.c.take_leader()

    def test_update_leader(self):
        self.c.update_leader(MockPostgresql())

    def test_delete_leader(self):
        self.c.delete_leader()

    def test_race(self):
        self.c.race('/initialize')

    def test_watch(self):
        self.c._name = ''
        self.c.get_cluster = nop
        self.c.watch(1)
        self.c.get_cluster = session_create
        self.assertRaises(SleepException, self.c.watch, 1)
