import etcd
import json
import unittest
import urllib3

from mock import Mock, patch
from patroni.dcs.etcd import DnsCachingResolver
from patroni.dcs.etcd3 import PatroniEtcd3Client, Cluster, Etcd3Client, Etcd3Error, Etcd3ClientError, RetryFailedError,\
        InvalidAuthToken, Unavailable, Unknown, UnsupportedEtcdVersion, UserEmpty, AuthFailed, base64_encode, Etcd3
from threading import Thread

from . import SleepException, MockResponse


def mock_urlopen(self, method, url, **kwargs):
    ret = MockResponse()
    if method == 'GET' and url.endswith('/version'):
        ret.content = '{"etcdserver": "3.3.13", "etcdcluster": "3.3.0"}'
    elif method != 'POST':
        raise Exception('Unexpected request method: {0} {1} {2}'.format(method, url, kwargs))
    elif url.endswith('/cluster/member/list'):
        ret.content = '{"members":[{"clientURLs":["http://localhost:2379", "http://localhost:4001"]}]}'
    elif url.endswith('/auth/authenticate'):
        ret.content = '{"token":"authtoken"}'
    elif url.endswith('/lease/grant'):
        ret.content = '{"ID": "123"}'
    elif url.endswith('/lease/keepalive'):
        ret.content = '{"result":{"TTL":30}}'
    elif url.endswith('/kv/range'):
        ret.content = json.dumps({
            "header": {"revision": "1"},
            "kvs": [
                {"key": base64_encode('/patroni/test/1/initialize'),
                 "value": base64_encode('12345'), "mod_revision": '1'},
                {"key": base64_encode('/patroni/test/leader'),
                 "value": base64_encode('foo'), "lease": "bla", "mod_revision": '1'},
                {"key": base64_encode('/patroni/test/members/foo'),
                 "value": base64_encode('{}'), "lease": "123", "mod_revision": '1'},
                {"key": base64_encode('/patroni/test/members/bar'),
                 "value": base64_encode('{"version":"1.6.5"}'), "lease": "123", "mod_revision": '1'},
                {"key": base64_encode('/patroni/test/failover'), "value": base64_encode('{}'), "mod_revision": '1'},
                {"key": base64_encode('/patroni/test/failsafe'), "value": base64_encode('{'), "mod_revision": '1'}
            ]
        })
    elif url.endswith('/watch'):
        key = base64_encode('/patroni/test/config')
        ret.read_chunked = Mock(return_value=[json.dumps({
            'result': {'events': [
                {'kv': {'key': key, 'value': base64_encode('bar'), 'mod_revision': '2'}},
                {'kv': {'key': key, 'value': base64_encode('buzz'), 'mod_revision': '3'}},
                {'type': 'DELETE', 'kv': {'key': key, 'mod_revision': '4'}},
                {'kv': {'key': base64_encode('/patroni/test/optime/leader'),
                        'value': base64_encode('1234567'), 'mod_revision': '5'}},
            ]}
        })[:-1].encode('utf-8'), b'}{"error":{"grpc_code":14,"message":"","http_code":503}}'])
    elif url.endswith('/kv/put') or url.endswith('/kv/txn'):
        ret.status_code = 400
        ret.content = '{"code":5,"error":"etcdserver: requested lease not found"}'
    elif not url.endswith('/kv/deleterange'):
        raise Exception('Unexpected url: {0} {1} {2}'.format(method, url, kwargs))
    return ret


class TestEtcd3Client(unittest.TestCase):

    @patch.object(Thread, 'start', Mock())
    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_authenticate(self):
        etcd3 = Etcd3Client({'host': '127.0.0.1', 'port': 2379, 'use_proxies': True, 'retry_timeout': 10},
                            DnsCachingResolver())
        self.assertIsNotNone(etcd3._cluster_version)


class BaseTestEtcd3(unittest.TestCase):

    @patch.object(Thread, 'start', Mock())
    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def setUp(self):
        self.etcd3 = Etcd3({'namespace': '/patroni/', 'ttl': 30, 'retry_timeout': 10,
                            'host': 'localhost:2378', 'scope': 'test', 'name': 'foo',
                            'username': 'etcduser', 'password': 'etcdpassword'})
        self.client = self.etcd3._client
        self.kv_cache = self.client._kv_cache


class TestKVCache(BaseTestEtcd3):

    def test__do_watch(self):
        self.client.watchprefix = Mock(return_value=False)
        self.assertRaises(AttributeError, self.kv_cache._do_watch, '1')

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch('patroni.dcs.etcd3.KVCache._build_cache', Mock(side_effect=Exception))
    def test_run(self):
        self.assertRaises(SleepException, self.kv_cache.run)

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_kill_stream(self):
        self.assertRaises(Unavailable, self.kv_cache._do_watch, '1')
        self.kv_cache.kill_stream()
        with patch.object(MockResponse, 'connection', create=True) as mock_conn:
            self.kv_cache.kill_stream()
            mock_conn.sock.close.side_effect = Exception
            self.kv_cache.kill_stream()


class TestPatroniEtcd3Client(BaseTestEtcd3):

    @patch('patroni.dcs.etcd3.Etcd3Client.authenticate', Mock(side_effect=AuthFailed))
    def test__init__(self):
        self.assertRaises(SystemExit, self.setUp)

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test_call_rpc(self, mock_urlopen):
        request = {'key': base64_encode('/patroni/test/leader')}
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.content = '{"succeeded":true,"header":{"revision":"1"}}'
        self.client.call_rpc('/kv/txn', {'success': [{'request_delete_range': request}]})
        self.client.call_rpc('/kv/put', request)
        self.client.call_rpc('/kv/deleterange', request)

    @patch('time.time', Mock(side_effect=[1, 10.9, 100]))
    def test__wait_cache(self):
        with self.kv_cache.condition:
            self.assertRaises(RetryFailedError, self.client._wait_cache, 10)

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test__restart_watcher(self, mock_urlopen):
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.status_code = 400
        mock_urlopen.return_value.content = '{"code":9,"error":"etcdserver: authentication is not enabled"}'
        self.client.authenticate()

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test__handle_auth_errors(self, mock_urlopen):
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.content = '{"code":3,"error":"etcdserver: user name is empty"}'
        mock_urlopen.return_value.status_code = 403
        self.client._cluster_version = (3, 1, 5)
        self.assertRaises(UnsupportedEtcdVersion, self.client.deleteprefix, 'foo')
        self.client._cluster_version = (3, 3, 13)
        self.assertRaises(UserEmpty, self.client.deleteprefix, 'foo')
        mock_urlopen.return_value.content = '{"code":16,"error":"etcdserver: invalid auth token"}'
        self.assertRaises(InvalidAuthToken, self.client.deleteprefix, 'foo')
        with patch.object(PatroniEtcd3Client, 'authenticate', Mock(return_value=True)):
            self.assertRaises(InvalidAuthToken, self.client.deleteprefix, 'foo')
            self.client.username = None
            self.assertRaises(InvalidAuthToken, self.client.deleteprefix, 'foo')

    def test__handle_server_response(self):
        response = MockResponse()
        response.content = '{"code":0,"error":"'
        self.assertRaises(etcd.EtcdException, self.client._handle_server_response, response)
        response.status_code = 400
        self.assertRaises(Unknown, self.client._handle_server_response, response)
        response.content = '{"error":{"grpc_code":0,"message":"","http_code":400}}'
        try:
            self.client._handle_server_response(response)
        except Unknown as e:
            self.assertEqual(e.as_dict(), {'code': 2, 'codeText': 'OK', 'error': u'', 'status': 400})

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test__ensure_version_prefix(self, mock_urlopen):
        self.client.version_prefix = None
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.content = '{"etcdserver": "3.0.3", "etcdcluster": "3.0.0"}'
        self.assertRaises(UnsupportedEtcdVersion, self.client._ensure_version_prefix, '')
        mock_urlopen.return_value.content = '{"etcdserver": "3.0.4", "etcdcluster": "3.0.0"}'
        self.client._ensure_version_prefix('')
        self.assertEqual(self.client.version_prefix, '/v3alpha')
        mock_urlopen.return_value.content = '{"etcdserver": "3.4.4", "etcdcluster": "3.4.0"}'
        self.client._ensure_version_prefix('')
        self.assertEqual(self.client.version_prefix, '/v3')


@patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
class TestEtcd3(BaseTestEtcd3):

    @patch.object(Thread, 'start', Mock())
    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def setUp(self):
        super(TestEtcd3, self).setUp()
        self.assertRaises(AttributeError, self.kv_cache._build_cache)
        self.kv_cache._is_ready = True
        self.etcd3.get_cluster()

    def test_get_cluster(self):
        self.assertIsInstance(self.etcd3.get_cluster(), Cluster)
        self.client._kv_cache = None
        with patch.object(urllib3.PoolManager, 'urlopen') as mock_urlopen:
            mock_urlopen.return_value = MockResponse()
            mock_urlopen.return_value.content = json.dumps({
                "header": {"revision": "1"},
                "kvs": [
                    {"key": base64_encode('/patroni/test/status'),
                     "value": base64_encode('{"optime":1234567,"slots":{"ls":12345}}'), "mod_revision": '1'}
                ]
            })
            self.assertIsInstance(self.etcd3.get_cluster(), Cluster)
            mock_urlopen.return_value.content = json.dumps({
                "header": {"revision": "1"},
                "kvs": [
                    {"key": base64_encode('/patroni/test/status'), "value": base64_encode('{'), "mod_revision": '1'}
                ]
            })
            self.assertIsInstance(self.etcd3.get_cluster(), Cluster)
            mock_urlopen.side_effect = UnsupportedEtcdVersion('')
            self.assertRaises(UnsupportedEtcdVersion, self.etcd3.get_cluster)
            mock_urlopen.side_effect = SleepException()
            self.assertRaises(Etcd3Error, self.etcd3.get_cluster)

    def test__get_citus_cluster(self):
        self.etcd3._citus_group = '0'
        cluster = self.etcd3.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsInstance(cluster.workers[1], Cluster)

    def test_touch_member(self):
        self.etcd3.touch_member({})
        self.etcd3._lease = 'bla'
        self.etcd3.touch_member({})
        with patch.object(PatroniEtcd3Client, 'lease_grant', Mock(side_effect=Etcd3ClientError)):
            self.etcd3.touch_member({})

    def test__update_leader(self):
        self.etcd3._lease = None
        self.etcd3.update_leader('123', failsafe={'foo': 'bar'})
        self.etcd3._last_lease_refresh = 0
        self.etcd3.update_leader('124')
        with patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=True)),\
                patch('time.time', Mock(side_effect=[0, 100, 200, 300])):
            self.assertRaises(Etcd3Error, self.etcd3.update_leader, '126')
        self.etcd3._last_lease_refresh = 0
        with patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(side_effect=Unknown)):
            self.assertFalse(self.etcd3.update_leader('125'))

    def test_take_leader(self):
        self.assertFalse(self.etcd3.take_leader())

    def test_attempt_to_acquire_leader(self):
        self.assertFalse(self.etcd3.attempt_to_acquire_leader())
        with patch('time.time', Mock(side_effect=[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 200])):
            self.assertRaises(Etcd3Error, self.etcd3.attempt_to_acquire_leader)
        with patch('time.time', Mock(side_effect=[0, 100, 200, 300, 400])):
            self.assertRaises(Etcd3Error, self.etcd3.attempt_to_acquire_leader)
        with patch.object(PatroniEtcd3Client, 'put', Mock(return_value=False)):
            self.assertFalse(self.etcd3.attempt_to_acquire_leader())

    def test_set_ttl(self):
        self.etcd3.set_ttl(20)

    @patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=False))
    def test_refresh_lease(self):
        self.etcd3._last_lease_refresh = 0
        self.etcd3.refresh_lease()

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=False))
    @patch.object(PatroniEtcd3Client, 'lease_grant', Mock(side_effect=Etcd3ClientError))
    def test_create_lease(self):
        self.etcd3._lease = None
        self.etcd3._last_lease_refresh = 0
        self.assertRaises(SleepException, self.etcd3.create_lease)

    def test_set_failover_value(self):
        self.etcd3.set_failover_value('', 1)

    def test_set_config_value(self):
        self.etcd3.set_config_value('')

    def test_initialize(self):
        self.etcd3.initialize()

    def test_cancel_initialization(self):
        self.etcd3.cancel_initialization()

    def test_delete_leader(self):
        self.etcd3.delete_leader()

    def test_delete_cluster(self):
        self.etcd3.delete_cluster()

    def test_set_history_value(self):
        self.etcd3.set_history_value('')

    def test_set_sync_state_value(self):
        self.etcd3.set_sync_state_value('')

    def test_delete_sync_state(self):
        self.etcd3.delete_sync_state()

    def test_watch(self):
        self.etcd3.set_ttl(10)
        self.etcd3.watch(None, 0)
        self.etcd3.watch(None, 0)

    def test_set_socket_options(self):
        with patch('socket.SIO_KEEPALIVE_VALS', 1, create=True):
            self.etcd3.set_socket_options(Mock(), None)
