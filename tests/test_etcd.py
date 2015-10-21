import etcd
import json
import requests
import urllib3
import socket
import unittest

from dns.exception import DNSException
from mock import Mock, patch
from patroni.dcs import Cluster, DCSError, Leader
from patroni.etcd import Client, Etcd


class MockResponse:

    def __init__(self):
        self.status_code = 200
        self.content = '{}'
        self.ok = True

    def json(self):
        return json.loads(self.content)

    @property
    def data(self):
        if self.content == 'TimeoutError':
            raise urllib3.exceptions.TimeoutError
        if self.content == 'Exception':
            raise Exception
        return self.content

    @property
    def status(self):
        return self.status_code

    def getheader(*args):
        return ''


class MockPostgresql(Mock):

    def last_operation(self):
        return '0'


def requests_get(url, **kwargs):
    members = '[{"id":14855829450254237642,"peerURLs":["http://localhost:2380","http://localhost:7001"],' +\
              '"name":"default","clientURLs":["http://localhost:2379","http://localhost:4001"]}]'
    response = MockResponse()
    if url.startswith('http://local'):
        raise requests.exceptions.RequestException()
    elif ':8011/patroni' in url:
        response.content = '{"role": "replica", "xlog": {"replayed_location": 0}}'
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


def etcd_watch(key, index=None, timeout=None, recursive=None):
    if timeout == 2.0:
        raise urllib3.exceptions.TimeoutError
    elif timeout == 5.0:
        return etcd.EtcdResult('delete', {})
    elif timeout == 10.0:
        raise etcd.EtcdException
    elif index == 20729:
        return etcd.EtcdResult('set', {'value': 'postgresql1', 'modifiedIndex': index + 1})
    elif index == 20731:
        return etcd.EtcdResult('set', {'value': 'postgresql2', 'modifiedIndex': index + 1})


def etcd_write(key, value, **kwargs):
    if key == '/service/exists/leader':
        raise etcd.EtcdAlreadyExist
    if key == '/service/test/leader' or key == '/patroni/test/leader':
        if kwargs.get('prevValue', None) == 'foo' or not kwargs.get('prevExist', True):
            return True
    raise etcd.EtcdException


def etcd_read(key, **kwargs):
    if key == '/service/noleader/':
        raise DCSError('noleader')
    elif key == '/service/nocluster/':
        raise etcd.EtcdKeyNotFound

    response = {"action": "get", "node": {"key": "/service/batman5", "dir": True, "nodes": [
                {"key": "/service/batman5/failover", "value": "",
                 "modifiedIndex": 1582, "createdIndex": 1582},
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
                     "value": "postgres://replicator:rep-pass@127.0.0.1:5434/postgres" +
                        "?application_name=http://127.0.0.1:8009/patroni",
                     "expiration": "2015-05-15T09:10:59.949384522Z", "ttl": 21,
                     "modifiedIndex": 20727, "createdIndex": 20727},
                    {"key": "/service/batman5/members/postgresql0",
                     "value": "postgres://replicator:rep-pass@127.0.0.1:5433/postgres" +
                        "?application_name=http://127.0.0.1:8008/patroni",
                     "expiration": "2015-05-15T09:11:09.611860899Z", "ttl": 30,
                     "modifiedIndex": 20730, "createdIndex": 20730}],
                 "modifiedIndex": 1581, "createdIndex": 1581}], "modifiedIndex": 1581, "createdIndex": 1581}}
    return etcd.EtcdResult(**response)


class SleepException(Exception):
    pass


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
    raise socket.error


def http_request(method, url, **kwargs):
    if url == 'http://localhost:2379/':
        return MockResponse()
    raise socket.error


@patch('dns.resolver.query', dns_query)
@patch('socket.getaddrinfo', socket_getaddrinfo)
@patch('requests.get', requests_get)
class TestClient(unittest.TestCase):

    @patch('dns.resolver.query', dns_query)
    @patch('requests.get', requests_get)
    def setUp(self):
        with patch.object(etcd.Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://localhost:2379', 'http://localhost:4001'])
            self.client = Client({'discovery_srv': 'test'})
            self.client.http.request = http_request

    def test_api_execute(self):
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = ['http://localhost:2379']
        self.client.api_execute('/', 'GET')
        self.client._update_machines_cache = False
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = []
        self.assertRaises(etcd.EtcdConnectionFailed, self.client.api_execute, '/', 'GET')
        self.assertTrue(self.client._update_machines_cache)

    def test_get_srv_record(self):
        self.assertEquals(self.client.get_srv_record('blabla'), [])
        self.assertEquals(self.client.get_srv_record('exception'), [])

    def test__result_from_response(self):
        response = MockResponse()
        response.content = 'TimeoutError'
        self.assertRaises(urllib3.exceptions.TimeoutError, self.client._result_from_response, response)
        response.content = 'Exception'
        self.assertRaises(etcd.EtcdException, self.client._result_from_response, response)
        response.content = b'{}'
        self.assertRaises(etcd.EtcdException, self.client._result_from_response, response)

    def test__get_machines_cache_from_srv(self):
        self.client.get_srv_record = Mock(return_value=[('localhost', 2380)])
        self.client._get_machines_cache_from_srv('blabla')

    def test__get_machines_cache_from_dns(self):
        self.client._get_machines_cache_from_dns('error:2379')

    def test__load_machines_cache(self):
        self.client._config = {}
        self.assertRaises(Exception, self.client._load_machines_cache)
        self.client._config = {'discovery_srv': 'blabla'}
        self.assertRaises(etcd.EtcdException, self.client._load_machines_cache)


@patch('requests.get', requests_get)
class TestEtcd(unittest.TestCase):

    def setUp(self):
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://localhost:2379', 'http://localhost:4001'])
            self.etcd = Etcd('foo', {'namespace': '/patroni/', 'ttl': 30, 'host': 'localhost:2379', 'scope': 'test'})
            self.etcd.client.write = etcd_write
            self.etcd.client.read = etcd_read
            self.etcd.client.delete = Mock(side_effect=etcd.EtcdException())

    def test_base_path(self):
        self.assertEquals(self.etcd._base_path, '/patroni/test')

    @patch('dns.resolver.query', dns_query)
    def test_get_etcd_client(self):
        with patch.object(etcd.Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(side_effect=etcd.EtcdException)
            with patch('time.sleep', Mock(side_effect=SleepException())):
                self.assertRaises(SleepException, self.etcd.get_etcd_client, {'discovery_srv': 'test'})

    def test_get_cluster(self):
        self.assertIsInstance(self.etcd.get_cluster(), Cluster)
        self.etcd._base_path = '/service/nocluster'
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsNone(cluster.leader)

    def test_current_leader(self):
        self.assertIsInstance(self.etcd.current_leader(), Leader)
        self.etcd._base_path = '/service/noleader'
        self.assertIsNone(self.etcd.current_leader())

    def test_touch_member(self):
        self.assertFalse(self.etcd.touch_member('', ''))

    def test_take_leader(self):
        self.assertFalse(self.etcd.take_leader())

    def test_attempt_to_acquire_leader(self):
        self.etcd._base_path = '/service/exists'
        self.assertFalse(self.etcd.attempt_to_acquire_leader())
        self.etcd._base_path = '/service/failed'
        self.assertFalse(self.etcd.attempt_to_acquire_leader())

    def test_write_leader_optime(self):
        self.etcd.write_leader_optime('0')

    def test_update_leader(self):
        self.assertTrue(self.etcd.update_leader())

    def test_initialize(self):
        self.assertFalse(self.etcd.initialize())

    def test_cancel_initializion(self):
        self.assertFalse(self.etcd.cancel_initialization())

    def test_delete_leader(self):
        self.assertFalse(self.etcd.delete_leader())

    def test_watch(self):
        self.etcd.client.watch = etcd_watch
        self.etcd.watch(0)
        self.etcd.get_cluster()
        self.etcd.watch(1.5)
        self.etcd.watch(4.5)
        self.etcd.watch(9.5)
        self.etcd.watch(100)
