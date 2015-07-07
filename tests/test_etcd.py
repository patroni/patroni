import datetime
import dns.resolver
import json
import requests
import socket
import time
import unittest

from dns.exception import DNSException
from helpers.dcs import Cluster, Member
from helpers.etcd import Client, Etcd, EtcdConnectionFailed, EtcdError


class MockResponse:

    def __init__(self):
        self.status_code = 200
        self.content = '{}'

    def json(self):
        return json.loads(self.content)


class MockPostgresql:
    name = ''

    def last_operation(self):
        return 0


def requests_get(url, **kwargs):
    members = '[{"id":14855829450254237642,"peerURLs":["http://localhost:2380","http://localhost:7001"],"name":"default","clientURLs":["http://localhost:2379","http://localhost:4001"]}]'
    response = MockResponse()
    if url.endswith('/v2/members'):
        response.content = '{"members": ' + members + '}'
        if url.startswith('http://error'):
            response.status_code = 404
    elif url.endswith('/members'):
        if url.startswith('http://error'):
            response.content = '[{}]'
        else:
            response.content = members
    elif url.endswith('/bad_response'):
        response.content = '{'
    elif url.startswith('http://local'):
        raise requests.exceptions.RequestException()
    elif url.startswith('http://remote') or url.startswith('http://127.0.0.1') or url.startswith('http://error'):
        response.content = '{"action":"get","node":{"key":"/service/batman5","dir":true,"nodes":[{"key":"/service/batman5/initialize","value":"postgresql0","modifiedIndex":1582,"createdIndex":1582},{"key":"/service/batman5/leader","value":"postgresql1","expiration":"2015-05-15T09:11:00.037397538Z","ttl":21,"modifiedIndex":20728,"createdIndex":20434},{"key":"/service/batman5/optime","dir":true,"nodes":[{"key":"/service/batman5/optime/leader","value":"2164261704","modifiedIndex":20729,"createdIndex":20729}],"modifiedIndex":20437,"createdIndex":20437},{"key":"/service/batman5/members","dir":true,"nodes":[{"key":"/service/batman5/members/postgresql1","value":"postgres://replicator:rep-pass@127.0.0.1:5434/postgres?application_name=http://127.0.0.1:8009/governor","expiration":"2015-05-15T09:10:59.949384522Z","ttl":21,"modifiedIndex":20727,"createdIndex":20727},{"key":"/service/batman5/members/postgresql0","value":"postgres://replicator:rep-pass@127.0.0.1:5433/postgres?application_name=http://127.0.0.1:8008/governor","expiration":"2015-05-15T09:11:09.611860899Z","ttl":30,"modifiedIndex":20730,"createdIndex":20730}],"modifiedIndex":1581,"createdIndex":1581}],"modifiedIndex":1581,"createdIndex":1581}}'
    elif url.startswith('http://other'):
        response.status_code = 404
    elif url.startswith('http://noleader'):
        response.content = '{"action":"get","node":{"key":"/service/batman5","dir":true,"nodes":[{"key":"/service/batman5/initialize","value":"postgresql0","modifiedIndex":1582,"createdIndex":1582},{"key":"/service/batman5/leader","value":"postgresql1","expiration":"2015-05-15T09:11:00.037397538Z","ttl":21,"modifiedIndex":20728,"createdIndex":20434},{"key":"/service/batman5/optime","dir":true,"nodes":[{"key":"/service/batman5/optime/leader","value":"2164261704","modifiedIndex":20729,"createdIndex":20729}],"modifiedIndex":20437,"createdIndex":20437},{"key":"/service/batman5/members","dir":true,"nodes":[{"key":"/service/batman5/members/postgresql0","value":"postgres://replicator:rep-pass@127.0.0.1:5433/postgres?application_name=http://127.0.0.1:8008/governor","expiration":"2015-05-15T09:11:09.611860899Z","ttl":30,"modifiedIndex":20730,"createdIndex":20730}],"modifiedIndex":1581,"createdIndex":1581}],"modifiedIndex":1581,"createdIndex":1581}}'
    return response


def requests_put(url, **kwargs):
    if url.startswith('http://local') or '/optime/leader' in url:
        raise requests.exceptions.RequestException()
    response = MockResponse()
    response.status_code = 201
    if url.startswith('http://other'):
        response.status_code = 404
    return response


def requests_delete(url, **kwargs):
    if url.startswith('http://local'):
        raise requests.exceptions.RequestException()
    response = MockResponse()
    response.status_code = 503 if url.startswith('http://error') else 204
    return response


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
        requests.put = requests_put
        requests.delete = requests_delete
        dns.resolver.query = dns_query
        self.client = Client({'discovery_srv': 'test'})

    def test__get(self):
        self.assertRaises(EtcdError, self.client._get, '/bad_response')

    def test_get_srv_record(self):
        self.assertEquals(Client.get_srv_record('blabla'), [])
        self.assertEquals(Client.get_srv_record('exception'), [])

    def test_get_client_urls_from_dns(self):
        self.assertEquals(Client.get_client_urls_from_dns('ok:2379'), ['http://127.0.0.1:2379/v2'])

    def test_load_members(self):
        self.client._base_uri = self.client._base_uri.replace('localhost', 'error_code')
        self.assertRaises(EtcdError, self.client.load_members)
        self.client._base_uri = 'http://error_code:2380'
        self.assertRaises(EtcdError, self.client.load_members)
        self.client._base_uri = None
        self.client._config = {}
        self.assertRaises(Exception, self.client.load_members)

    def test_get(self):
        self.client._base_uri = None
        self.assertRaises(EtcdConnectionFailed, self.client.get, '')
        self.client._members_cache = ['http://error_code:4001/v2']
        self.client.get('')

    def test_put(self):
        self.client._base_uri = None
        self.assertRaises(EtcdConnectionFailed, self.client.put, '')
        self.client._base_uri = 'http://localhost:4001/v2'
        self.client._members_cache = ['http://error_code:4001/v2']
        self.client.put('')

    def test_delete(self):
        self.client._base_uri = None
        self.assertRaises(EtcdConnectionFailed, self.client.delete, '')
        self.client._base_uri = 'http://localhost:4001/v2'
        self.client._members_cache = ['http://error_code:4001/v2']
        self.client.delete('')


class TestEtcd(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        self.setUp = self.set_up
        super(TestEtcd, self).__init__(method_name)

    def set_up(self):
        socket.getaddrinfo = socket_getaddrinfo
        requests.get = requests_get
        requests.put = requests_put
        requests.delete = requests_delete
        time.sleep = time_sleep
        self.etcd = Etcd('foo', {'ttl': 30, 'host': 'localhost:2379', 'scope': 'test'})

    def test_get_etcd_client(self):
        time.sleep = time_sleep_exception
        self.assertRaises(Exception, self.etcd.get_etcd_client, {'host': 'error:2379'})

    def test_get_client_path(self):
        self.assertRaises(Exception, self.etcd.get_client_path, '', 2)

    def test_put_client_path(self):
        self.assertRaises(EtcdError, self.etcd.put_client_path, '')

    def test_delete_client_path(self):
        self.assertFalse(self.etcd.delete_client_path(''))

    def test_get_cluster(self):
        self.assertRaises(EtcdError, self.etcd.get_cluster)
        self.etcd.client._base_uri = self.etcd.client._base_uri.replace('local', 'remote')
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.etcd.client._base_uri = self.etcd.client._base_uri.replace('remote', 'other')
        self.etcd.get_cluster()
        self.etcd.client._base_uri = self.etcd.client._base_uri.replace('other', 'noleader')
        self.etcd.get_cluster()

    def test_current_leader(self):
        self.assertIsNone(self.etcd.current_leader())

    def test_touch_member(self):
        self.assertFalse(self.etcd.touch_member('', ''))

    def test_take_leader(self):
        self.assertFalse(self.etcd.take_leader())

    def test_attempt_to_acquire_leader(self):
        self.assertFalse(self.etcd.attempt_to_acquire_leader())

    def test_update_leader(self):
        url = self.etcd.client._base_uri = self.etcd.client._base_uri.replace('local', 'remote')
        self.assertTrue(self.etcd.update_leader(MockPostgresql()))
        self.etcd.client._base_uri = url.replace('remote', 'other')
        self.assertFalse(self.etcd.update_leader(MockPostgresql()))

    def test_race(self):
        self.assertFalse(self.etcd.race(''))
