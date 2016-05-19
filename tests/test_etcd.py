import etcd
import json
import requests
import socket
import unittest

from dns.exception import DNSException
from mock import Mock, patch
from patroni.dcs.etcd import AbstractDCS, Client, Cluster, Etcd, EtcdError
from patroni.exceptions import DCSError
from urllib3.exceptions import ReadTimeoutError


class MockResponse(object):

    def __init__(self):
        self.status_code = 200
        self.content = '{}'
        self.ok = True
        self.text = ''

    def json(self):
        return json.loads(self.content)

    @property
    def data(self):
        return self.content.encode('utf-8')

    @property
    def status(self):
        return self.status_code

    @staticmethod
    def getheader(*args):
        return ''


def requests_get(url, **kwargs):
    members = '[{"id":14855829450254237642,"peerURLs":["http://localhost:2380","http://localhost:7001"],' +\
              '"name":"default","clientURLs":["http://localhost:2379","http://localhost:4001"]}]'
    response = MockResponse()
    if url.startswith('http://local'):
        raise requests.exceptions.RequestException()
    elif ':8011/patroni' in url:
        response.content = '{"role": "replica", "xlog": {"replayed_location": 0}, "tags": {}}'
    elif url.endswith('/members'):
        response.content = '[{}]' if url.startswith('http://error') else members
    elif url.startswith('http://exhibitor'):
        response.content = '{"servers":["127.0.0.1","127.0.0.2","127.0.0.3"],"port":2181}'
    else:
        response.status_code = 404
        response.ok = False
    return response


def etcd_watch(self, key, index=None, timeout=None, recursive=None):
    if timeout == 2.0:
        raise etcd.EtcdWatchTimedOut
    elif timeout == 5.0:
        return etcd.EtcdResult('delete', {})
    elif timeout == 10.0:
        raise etcd.EtcdException


def etcd_write(self, key, value, **kwargs):
    if key == '/service/exists/leader':
        raise etcd.EtcdAlreadyExist
    if key in ['/service/test/leader', '/patroni/test/leader'] and \
            (kwargs.get('prevValue') == 'foo' or not kwargs.get('prevExist', True)):
        return True
    raise etcd.EtcdException


def etcd_read(self, key, **kwargs):
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


class MockSRV(object):
    port = 2380
    target = '127.0.0.1'


def dns_query(name, _):
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
    if url == 'http://localhost:2379/timeout':
        raise ReadTimeoutError(None, None, None)
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
            self.client.http.request_encode_body = http_request

    def test_api_execute(self):
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = ['http://localhost:2379']
        self.assertRaises(etcd.EtcdWatchTimedOut, self.client.api_execute, '/timeout', 'POST', params={'wait': 'true'})
        self.client._update_machines_cache = False
        self.client.api_execute('/', 'POST', timeout=0)
        self.client._update_machines_cache = False
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = []
        self.assertRaises(etcd.EtcdConnectionFailed, self.client.api_execute, '/', 'GET')
        self.assertTrue(self.client._update_machines_cache)
        self.assertRaises(etcd.EtcdException, self.client.api_execute, '/', 'GET')
        self.assertRaises(etcd.EtcdException, self.client.api_execute, '/', '')
        self.assertRaises(ValueError, self.client.api_execute, '', '')

    def test_get_srv_record(self):
        self.assertEquals(self.client.get_srv_record('blabla'), [])
        self.assertEquals(self.client.get_srv_record('exception'), [])

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
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
@patch.object(etcd.Client, 'delete', Mock(side_effect=etcd.EtcdException))
class TestEtcd(unittest.TestCase):

    def setUp(self):
        with patch.object(Client, 'machines') as mock_machines:
            mock_machines.__get__ = Mock(return_value=['http://localhost:2379', 'http://localhost:4001'])
            self.etcd = Etcd('foo', {'namespace': '/patroni/', 'ttl': 30, 'host': 'localhost:2379', 'scope': 'test'})

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
        self.etcd._base_path = '/service/noleader'
        self.assertRaises(EtcdError, self.etcd.get_cluster)

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

    def test_delete_cluster(self):
        self.assertFalse(self.etcd.delete_cluster())

    @patch.object(etcd.Client, 'watch', etcd_watch)
    def test_watch(self):
        self.etcd.watch(0)
        self.etcd.get_cluster()
        self.etcd.watch(1.5)
        self.etcd.watch(4.5)
        with patch.object(AbstractDCS, 'watch', Mock()):
            self.etcd.watch(9.5)

    def test_other_exceptions(self):
        self.etcd.retry = Mock(side_effect=AttributeError('foo'))
        self.assertRaises(EtcdError, self.etcd.cancel_initialization)
