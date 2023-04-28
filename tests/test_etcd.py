import etcd
import urllib3.util.connection
import socket
import unittest

from dns.exception import DNSException
from mock import Mock, PropertyMock, patch
from patroni.dcs.etcd import AbstractDCS, EtcdClient, Cluster, Etcd, EtcdError, DnsCachingResolver
from patroni.exceptions import DCSError
from patroni.utils import Retry
from urllib3.exceptions import ReadTimeoutError

from . import SleepException, MockResponse, requests_get


def etcd_watch(self, key, index=None, timeout=None, recursive=None):
    if timeout == 2.0:
        raise etcd.EtcdWatchTimedOut
    elif timeout == 5.0:
        return etcd.EtcdResult('compareAndSwap', {})
    elif 5 < timeout <= 10.0:
        raise etcd.EtcdException
    elif timeout == 20.0:
        raise etcd.EtcdEventIndexCleared


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
                {"key": "/service/batman5/1", "dir": True, "nodes": [
                    {"key": "/service/batman5/1/initialize", "value": "2164261704",
                     "modifiedIndex": 20729, "createdIndex": 20729}],
                 "modifiedIndex": 20437, "createdIndex": 20437},
                {"key": "/service/batman5/config", "value": '{"synchronous_mode": 0, "failsafe_mode": true}',
                 "modifiedIndex": 1582, "createdIndex": 1582},
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
                {"key": "/service/batman5/sync", "value": '{"leader": "leader"}',
                 "modifiedIndex": 1582, "createdIndex": 1582},
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
                 "modifiedIndex": 1581, "createdIndex": 1581},
                {"key": "/service/batman5/failsafe", "value": '{', "modifiedIndex": 1582, "createdIndex": 1582},
                {"key": "/service/batman5/status", "value": '{"optime":2164261704,"slots":{"ls":12345}}',
                 "modifiedIndex": 1582, "createdIndex": 1582}], "modifiedIndex": 1581, "createdIndex": 1581}}
    if key == '/service/legacy/':
        response['node']['nodes'].pop()
    if key == '/service/broken/':
        response['node']['nodes'][-1]['value'] = '{'
    result = etcd.EtcdResult(**response)
    result.etcd_index = 0
    return result


def dns_query(name, _):
    if '-server' not in name or '-ssl' in name:
        return []
    if name == '_etcd-server._tcp.blabla':
        return []
    elif name == '_etcd-server._tcp.exception':
        raise DNSException()
    srv = Mock()
    srv.port = 2380
    srv.target.to_text.return_value = \
        'localhost' if name in ['_etcd-server._tcp.foobar', '_etcd-server-baz._tcp.foobar'] else '127.0.0.1'
    return [srv]


def socket_getaddrinfo(*args):
    if args[0] in ('ok', 'localhost', '127.0.0.1'):
        return [(socket.AF_INET, 1, 6, '', ('127.0.0.1', 0)), (socket.AF_INET6, 1, 6, '', ('::1', 0))]
    raise socket.gaierror


def http_request(method, url, **kwargs):
    if url == 'http://localhost:2379/timeout':
        raise ReadTimeoutError(None, None, None)
    ret = MockResponse()
    if url == 'http://localhost:2379/v2/machines':
        ret.content = 'http://localhost:2379,http://localhost:4001'
    elif url == 'http://localhost:4001/v2/machines':
        ret.content = ''
    elif url != 'http://localhost:2379/':
        raise socket.error
    return ret


class TestDnsCachingResolver(unittest.TestCase):

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch('socket.getaddrinfo', Mock(side_effect=socket.gaierror))
    def test_run(self):
        r = DnsCachingResolver()
        r._invoke_excepthook = Mock()
        self.assertIsNone(r.resolve_async('', 0))
        r.join()


@patch('dns.resolver.query', dns_query)
@patch('socket.getaddrinfo', socket_getaddrinfo)
@patch('patroni.dcs.etcd.requests_get', requests_get)
class TestClient(unittest.TestCase):

    @patch('dns.resolver.query', dns_query)
    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch('patroni.dcs.etcd.requests_get', requests_get)
    @patch.object(EtcdClient, '_get_machines_list',
                  Mock(return_value=['http://localhost:2379', 'http://localhost:4001']))
    def setUp(self):
        self.client = EtcdClient({'srv': 'test', 'retry_timeout': 3}, DnsCachingResolver())
        self.client.http.request = http_request
        self.client.http.request_encode_body = http_request

    def test_machines(self):
        self.client._base_uri = 'http://localhost:4002'
        self.client._machines_cache = ['http://localhost:4002', 'http://localhost:2379']
        self.assertIsNotNone(self.client.machines)
        self.client._base_uri = 'http://localhost:4001'
        self.client._machines_cache = ['http://localhost:4001']
        self.client._update_machines_cache = True
        machines = None
        try:
            machines = self.client.machines
            self.assertFail()
        except Exception:
            self.assertIsNone(machines)

    @patch.object(EtcdClient, '_get_machines_list',
                  Mock(return_value=['http://localhost:4001', 'http://localhost:2379']))
    def test_api_execute(self):
        self.client._base_uri = 'http://localhost:4001'
        self.assertRaises(etcd.EtcdException, self.client.api_execute, '/', 'POST', timeout=0)
        self.client._base_uri = 'http://localhost:4001'
        rtry = Retry(deadline=10, max_delay=1, max_tries=-1, retry_exceptions=(etcd.EtcdLeaderElectionInProgress,))
        rtry(self.client.api_execute, '/', 'POST', timeout=0, params={'retry': rtry})
        self.client._machines_cache_updated = 0
        self.client.api_execute('/', 'POST', timeout=0)
        self.client._machines_cache = [self.client._base_uri]
        self.assertRaises(etcd.EtcdWatchTimedOut, self.client.api_execute, '/timeout', 'POST', params={'wait': 'true'})
        self.assertRaises(etcd.EtcdWatchTimedOut, self.client.api_execute, '/timeout', 'POST', params={'wait': 'true'})

        with patch.object(EtcdClient, '_calculate_timeouts', Mock(side_effect=[(1, 1, 0), (1, 1, 0), (0, 1, 0)])),\
                patch.object(EtcdClient, '_load_machines_cache', Mock(side_effect=Exception)):
            self.client.http.request = Mock(side_effect=socket.error)
            self.assertRaises(etcd.EtcdException, rtry, self.client.api_execute, '/', 'GET', params={'retry': rtry})

        with patch.object(EtcdClient, '_calculate_timeouts', Mock(side_effect=[(1, 1, 0), (1, 1, 0), (0, 1, 0)])),\
                patch.object(EtcdClient, '_load_machines_cache', Mock(return_value=True)):
            self.assertRaises(etcd.EtcdException, rtry, self.client.api_execute, '/', 'GET', params={'retry': rtry})

        with patch.object(EtcdClient, '_do_http_request', Mock(side_effect=etcd.EtcdException)):
            self.client._read_timeout = 0.01
            self.assertRaises(etcd.EtcdException, self.client.api_execute, '/', 'GET')

    def test_get_srv_record(self):
        self.assertEqual(self.client.get_srv_record('_etcd-server._tcp.blabla'), [])
        self.assertEqual(self.client.get_srv_record('_etcd-server._tcp.exception'), [])

    def test__get_machines_cache_from_srv(self):
        self.client._get_machines_cache_from_srv('foobar')
        self.client._get_machines_cache_from_srv('foobar', 'baz')
        self.client.get_srv_record = Mock(return_value=[('localhost', 2380)])
        self.client._get_machines_cache_from_srv('blabla')

    def test__get_machines_cache_from_dns(self):
        self.client._get_machines_cache_from_dns('error', 2379)

    @patch.object(EtcdClient, '_get_machines_list', Mock(side_effect=etcd.EtcdConnectionFailed))
    def test__refresh_machines_cache(self):
        self.assertFalse(self.client._refresh_machines_cache())
        self.assertRaises(etcd.EtcdException, self.client._refresh_machines_cache, ['http://localhost:2379'])

    def test__load_machines_cache(self):
        self.client._config = {}
        self.assertRaises(Exception, self.client._load_machines_cache)
        self.client._config = {'srv': 'blabla'}
        self.assertRaises(etcd.EtcdException, self.client._load_machines_cache)

    @patch.object(socket.socket, 'connect')
    def test_create_connection_patched(self, mock_connect):
        self.assertRaises(socket.error, urllib3.util.connection.create_connection, ('fail', 2379))
        urllib3.util.connection.create_connection(('[localhost]', 2379))
        mock_connect.side_effect = socket.error
        self.assertRaises(socket.error, urllib3.util.connection.create_connection, ('[localhost]', 2379),
                          timeout=1, source_address=('localhost', 53333),
                          socket_options=[(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)])

    def test___del__(self):
        self.client.http.clear = Mock(side_effect=TypeError)
        del self.client


@patch('patroni.dcs.etcd.requests_get', requests_get)
@patch('socket.getaddrinfo', socket_getaddrinfo)
@patch.object(etcd.Client, 'write', etcd_write)
@patch.object(etcd.Client, 'read', etcd_read)
@patch.object(etcd.Client, 'delete', Mock(side_effect=etcd.EtcdException))
class TestEtcd(unittest.TestCase):

    @patch('socket.getaddrinfo', socket_getaddrinfo)
    @patch.object(EtcdClient, '_get_machines_list',
                  Mock(return_value=['http://localhost:2379', 'http://localhost:4001']))
    def setUp(self):
        self.etcd = Etcd({'namespace': '/patroni/', 'ttl': 30, 'retry_timeout': 10,
                          'host': 'localhost:2379', 'scope': 'test', 'name': 'foo'})

    def test_base_path(self):
        self.assertEqual(self.etcd._base_path, '/patroni/test')

    @patch('dns.resolver.query', dns_query)
    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(EtcdClient, '_get_machines_list', Mock(side_effect=etcd.EtcdConnectionFailed))
    def test_get_etcd_client(self):
        self.assertRaises(SleepException, self.etcd.get_etcd_client,
                          {'discovery_srv': 'test', 'retry_timeout': 10, 'cacert': '1', 'key': '1', 'cert': 1},
                          EtcdClient)
        self.assertRaises(SleepException, self.etcd.get_etcd_client,
                          {'url': 'https://test:2379', 'retry_timeout': 10}, EtcdClient)
        self.assertRaises(SleepException, self.etcd.get_etcd_client,
                          {'hosts': 'foo:4001,bar', 'retry_timeout': 10}, EtcdClient)
        with patch.object(EtcdClient, '_get_machines_list', Mock(return_value=[])):
            self.assertRaises(SleepException, self.etcd.get_etcd_client,
                              {'proxy': 'https://user:password@test:2379', 'retry_timeout': 10}, EtcdClient)

    def test_get_cluster(self):
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertFalse(cluster.is_synchronous_mode())
        self.etcd._base_path = '/service/legacy'
        self.assertIsInstance(self.etcd.get_cluster(), Cluster)
        self.etcd._base_path = '/service/broken'
        self.assertIsInstance(self.etcd.get_cluster(), Cluster)
        self.etcd._base_path = '/service/nocluster'
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsNone(cluster.leader)
        self.etcd._base_path = '/service/noleader'
        self.assertRaises(EtcdError, self.etcd.get_cluster)

    def test__get_citus_cluster(self):
        self.etcd._citus_group = '0'
        cluster = self.etcd.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsInstance(cluster.workers[1], Cluster)

    def test_touch_member(self):
        self.assertFalse(self.etcd.touch_member(''))

    def test_take_leader(self):
        self.assertFalse(self.etcd.take_leader())

    def test_attempt_to_acquire_leader(self):
        self.etcd._base_path = '/service/exists'
        self.assertFalse(self.etcd.attempt_to_acquire_leader())
        self.etcd._base_path = '/service/failed'
        self.assertFalse(self.etcd.attempt_to_acquire_leader())
        with patch.object(EtcdClient, 'write', Mock(side_effect=[etcd.EtcdConnectionFailed, Exception])):
            self.assertRaises(EtcdError, self.etcd.attempt_to_acquire_leader)
            self.assertRaises(EtcdError, self.etcd.attempt_to_acquire_leader)

    @patch.object(Cluster, 'min_version', PropertyMock(return_value=(2, 0)))
    def test_write_leader_optime(self):
        self.etcd.get_cluster()
        self.etcd.write_leader_optime('0')

    def test_update_leader(self):
        self.assertTrue(self.etcd.update_leader(None, failsafe={'foo': 'bar'}))
        with patch.object(etcd.Client, 'write',
                          Mock(side_effect=[etcd.EtcdConnectionFailed, etcd.EtcdClusterIdChanged, Exception])):
            self.assertRaises(EtcdError, self.etcd.update_leader, None)
            self.assertFalse(self.etcd.update_leader(None))
            self.assertRaises(EtcdError, self.etcd.update_leader, None)
        with patch.object(etcd.Client, 'write', Mock(side_effect=etcd.EtcdKeyNotFound)):
            self.assertFalse(self.etcd.update_leader(None))

    def test_initialize(self):
        self.assertFalse(self.etcd.initialize())

    def test_cancel_initializion(self):
        self.assertFalse(self.etcd.cancel_initialization())

    def test_delete_leader(self):
        self.assertFalse(self.etcd.delete_leader())

    def test_delete_cluster(self):
        self.assertFalse(self.etcd.delete_cluster())

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(etcd.Client, 'watch', etcd_watch)
    def test_watch(self):
        self.etcd.watch(None, 0)
        self.etcd.get_cluster()
        self.etcd.watch(20729, 1.5)
        with patch('time.sleep', Mock()):
            self.etcd.watch(20729, 4.5)
        with patch.object(AbstractDCS, 'watch', Mock()):
            self.assertTrue(self.etcd.watch(20729, 19.5))
            self.assertRaises(SleepException, self.etcd.watch, 20729, 9.5)

    def test_other_exceptions(self):
        self.etcd.retry = Mock(side_effect=AttributeError('foo'))
        self.assertRaises(EtcdError, self.etcd.cancel_initialization)

    def test_set_ttl(self):
        self.etcd.set_ttl(20)
        self.assertTrue(self.etcd.watch(None, 1))

    def test_sync_state(self):
        self.assertFalse(self.etcd.write_sync_state('leader', None))
        self.assertFalse(self.etcd.delete_sync_state())

    def test_set_history_value(self):
        self.assertFalse(self.etcd.set_history_value('{}'))

    def test_last_seen(self):
        self.assertIsNotNone(self.etcd.last_seen)
