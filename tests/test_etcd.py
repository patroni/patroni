import datetime
import dns.resolver
import etcd
import json
import requests
import socket
import time
import unittest

from dns.exception import DNSException
from patroni.helpers.dcs import Cluster, DCSError, Member
from patroni.helpers.etcd import Client, Etcd
from mock import Mock, patch


class MockResponse:

    def __init__(self):
        self.status_code = 200
        self.content = '{}'
        self.ok = True

    def json(self):
        return json.loads(self.content)

    @property
    def data(self):
        return self.content

    @property
    def status(self):
        return self.status_code

    def getheader(*args):
        return ''


class MockPostgresql:
    name = ''

    def last_operation(self):
        return 0


def requests_get(url, **kwargs):
    members = '[{"id":14855829450254237642,"peerURLs":["http://localhost:2380","http://localhost:7001"],' +\
              '"name":"default","clientURLs":["http://localhost:2379","http://localhost:4001"]}]'
    response = MockResponse()
    if url.startswith('http://local'):
        raise requests.exceptions.RequestException()
    elif url.endswith('/members'):
        if url.startswith('http://error'):
            response.content = '[{}]'
        else:
            response.content = members
    elif url.startswith('http://exhibitor'):
        response.content = '{"servers":["127.0.0.1","127.0.0.2","127.0.0.3"],"port":2181}'
    else:
        response.status_code = 404
        response.ok = False
    return response


def etcd_write(key, value, **kwargs):
    if key == '/service/test/leader':
        if kwargs.get('prevValue', None) == 'foo' or not kwargs.get('prevExist', True):
            return True
    raise etcd.EtcdException


def etcd_delete(key, **kwargs):
    raise etcd.EtcdException


def etcd_read(key, **kwargs):
    if key == '/service/noleader':
        raise DCSError('noleader')
    elif key == '/service/nocluster':
        raise etcd.EtcdKeyNotFound

    response = {"action": "get", "node": {"key": "/service/batman5", "dir": True, "nodes": [
                {"key": "/service/batman5/initialize", "value": "postgresql0",
                 "modifiedIndex": 1582, "createdIndex": 1582},
                {"key": "/service/batman5/leader", "value": "postgresql1",
                 "expiration": "2015-05-15T09:11:00.037397538Z", "ttl": 21,
                 "modifiedIndex": 20728, "createdIndex": 20434},
                {"key": "/service/batman5/optime", "dir": True, "nodes": [
                    {"key": "/service/batman5/optime/leader", "value": "2164261704",
                     "modifiedIndex": 20729, "createdIndex": 20729}],
                 "modifiedIndex": 20437, "createdIndex": 20437},
                {"key": "/service/batman5/members", "dir": True, "nodes": [
                    {"key": "/service/batman5/members/postgresql1",
                     "value": "postgres://replicator:rep-pass@127.0.0.1:5434/postgres"
                        + "?application_name=http://127.0.0.1:8009/patroni",
                     "expiration": "2015-05-15T09:10:59.949384522Z", "ttl": 21,
                     "modifiedIndex": 20727, "createdIndex": 20727},
                    {"key": "/service/batman5/members/postgresql0",
                     "value": "postgres://replicator:rep-pass@127.0.0.1:5433/postgres"
                        + "?application_name=http://127.0.0.1:8008/patroni",
                     "expiration": "2015-05-15T09:11:09.611860899Z", "ttl": 30,
                     "modifiedIndex": 20730, "createdIndex": 20730}],
                 "modifiedIndex": 1581, "createdIndex": 1581}], "modifiedIndex": 1581, "createdIndex": 1581}}
    return etcd.EtcdResult(**response)


def time_sleep(_):
    pass


def time_sleep_exception(_):
    raise Exception()


class MockSRV:
    port = 2380
    target = '127.0.0.1'


def dns_query(name, type):
    if name == '_etcd-server._tcp.blabla':
        return []
    elif name == '_etcd-server._tcp.exception':
        raise DNSException()
    return [MockSRV()]


def socket_getaddrinfo(*args):
    if args[0] == 'ok':
        return [(2, 1, 6, '', ('127.0.0.1', 2379)), (2, 1, 6, '', ('127.0.0.1', 2379))]
    raise socket.error()


def http_request(method, url, **kwargs):
    if url == 'http://localhost:2379/':
        return MockResponse()
    raise socket.error


class TestMember(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(TestMember, self).__init__(method_name)

    def test_real_ttl(self):
        now = datetime.datetime.utcnow()
        member = Member(0, 'a', 'b', 'c', (now + datetime.timedelta(seconds=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'), None)
        self.assertLess(member.real_ttl(), 2)
        self.assertEquals(Member(0, 'a', 'b', 'c', '', None).real_ttl(), -1)


class TestClient(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestClient, self).__init__(method_name)

    def set_up(self):
        socket.getaddrinfo = socket_getaddrinfo
        requests.get = requests_get
        dns.resolver.query = dns_query
        with patch.object(etcd.Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://localhost:2379', 'http://localhost:4001'])
            self.client = Client({'discovery_srv': 'test'})
            self.client.http.request = http_request

    def test_api_execute(self):
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = ['http://localhost:2379']
        self.client.api_execute('/', 'GET')

    def test_get_srv_record(self):
        self.assertEquals(self.client.get_srv_record('blabla'), [])
        self.assertEquals(self.client.get_srv_record('exception'), [])

    def test__get_machines_cache_from_srv(self):
        self.client.get_srv_record = lambda e: [('localhost', 2380)]
        self.client._get_machines_cache_from_srv('blabla')

    def test__get_machines_cache_from_dns(self):
        self.client._get_machines_cache_from_dns('ok:2379')

    def test__load_machines_cache(self):
        self.client._config = {}
        self.assertRaises(Exception, self.client._load_machines_cache)
        self.client._config = {'discovery_srv': 'blabla'}
        self.assertRaises(etcd.EtcdException, self.client._load_machines_cache)


class TestEtcd(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestEtcd, self).__init__(method_name)

    def set_up(self):
        time.sleep = time_sleep
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://localhost:2379', 'http://localhost:4001'])
            self.etcd = Etcd('foo', {'ttl': 30, 'host': 'localhost:2379', 'scope': 'test'})
            self.etcd.client.write = etcd_write
            self.etcd.client.read = etcd_read

    def test_get_etcd_client(self):
        time.sleep = time_sleep_exception
        with patch.object(etcd.Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(side_effect=etcd.EtcdException)
            self.assertRaises(Exception, self.etcd.get_etcd_client, {'discovery_srv': 'test'})

    def test_get_cluster(self):
        self.assertIsInstance(self.etcd.get_cluster(), Cluster)
        self.etcd._base_path = '/service/nocluster'
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsNone(cluster.leader)

    def test_current_leader(self):
        self.assertIsInstance(self.etcd.current_leader(), Member)
        self.etcd._base_path = '/service/noleader'
        self.assertIsNone(self.etcd.current_leader())

    def test_touch_member(self):
        self.assertFalse(self.etcd.touch_member('', ''))

    def test_take_leader(self):
        self.assertFalse(self.etcd.take_leader())

    def test_update_leader(self):
        self.assertTrue(self.etcd.update_leader(MockPostgresql()))

    def test_race(self):
        self.assertFalse(self.etcd.race(''))

    def test_delete_leader(self):
        self.etcd.client.delete = etcd_delete
        self.assertFalse(self.etcd.delete_leader())
