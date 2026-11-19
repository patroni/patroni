import json
import socket
import unittest

from threading import Thread, Timer
from unittest.mock import Mock, patch

import etcd
import urllib3

from patroni.dcs import get_dcs
from patroni.dcs.etcd import DnsCachingResolver
from patroni.dcs.etcd3 import AuthFailed, AuthOldRevision, base64_encode, Cluster, Etcd3, \
    Etcd3Client, Etcd3ClientError, Etcd3Error, InvalidAuthToken, PatroniEtcd3Client, \
    RetryFailedError, Unavailable, Unknown, UnsupportedEtcdVersion, UserEmpty
from patroni.interrupt import WatchInterrupted
from patroni.postgresql.mpp import get_mpp

from . import HangingHTTPServer, MockResponse, read_after_kill_stream, SleepException


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
            'result': {'header': {'cluster_id': '1', 'raft_term': 1}, 'events': [
                {'kv': {'key': key, 'value': base64_encode('bar'), 'mod_revision': '2'}},
                {'kv': {'key': key, 'value': base64_encode('buzz'), 'mod_revision': '3'}},
                {'type': 'DELETE', 'kv': {'key': key, 'mod_revision': '4'}},
                {'kv': {'key': base64_encode('/patroni/test/optime/leader'),
                        'value': base64_encode('1234567'), 'mod_revision': '5'}},
            ]}
        })[:-1].encode('utf-8'), b'}{"error":{"grpc_code":14,"message":"","http_code":503}}'])
    elif url.endswith('/kv/put') or url.endswith('/kv/txn'):
        if base64_encode('/patroni/test/sync') in kwargs['body']:
            ret.content = '{"header":{"revision":"1"},"succeeded":true}'
        else:
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

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test_get_members_retries_auth_errors(self, mock_urlopen):
        auth_requests = []
        member_list_requests = []

        def urlopen_side_effect(method, url, **kwargs):
            ret = MockResponse()
            if method == 'GET' and url.endswith('/version'):
                ret.content = '{"etcdserver": "3.6.10", "etcdcluster": "3.6.0"}'
            elif url.endswith('/auth/authenticate'):
                auth_requests.append(kwargs)
                ret.content = '{{"token":"authtoken{0}"}}'.format(len(auth_requests))
            elif url.endswith('/cluster/member/list'):
                member_list_requests.append(kwargs)
                if 1 < len(member_list_requests) < 4:
                    ret.status_code = 401
                    ret.content = '{"code":16,"error":"etcdserver: invalid auth token"}'
                else:
                    ret.content = '{"members":[{"clientURLs":["http://localhost:2379"]}]}'
            return ret

        mock_urlopen.side_effect = urlopen_side_effect
        client = Etcd3Client({'host': '127.0.0.1', 'port': 2379, 'retry_timeout': 10, 'patronictl': True,
                              'username': 'etcduser', 'password': 'etcdpassword'}, DnsCachingResolver())
        client._update_machines_cache = True

        kwargs = client._prepare_get_members(1)
        self.assertEqual(client._get_members('http://127.0.0.1:2379', **kwargs), ['http://localhost:2379'])
        self.assertEqual(len(auth_requests), 3)
        self.assertEqual(json.loads(auth_requests[-1]['body']), {'name': 'etcduser', 'password': 'etcdpassword'})
        self.assertNotIn('authorization', auth_requests[-1]['headers'])
        self.assertEqual(len(member_list_requests), 4)
        self.assertEqual(member_list_requests[-1]['headers']['authorization'], 'authtoken3')


class BaseTestEtcd3(unittest.TestCase):

    @patch.object(Thread, 'start', Mock())
    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def setUp(self):
        self.etcd3 = get_dcs({'namespace': '/patroni/', 'ttl': 30, 'retry_timeout': 10, 'name': 'foo', 'scope': 'test',
                              'etcd3': {'host': 'localhost:2378', 'username': 'etcduser', 'password': 'etcdpassword'}})
        self.assertIsInstance(self.etcd3, Etcd3)
        self.client = self.etcd3._client
        self.kv_cache = self.client._kv_cache


class TestKVCache(BaseTestEtcd3):

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    @patch.object(Etcd3Client, 'watchprefix', Mock(return_value=urllib3.response.HTTPResponse()))
    @patch.object(urllib3.response.HTTPResponse, 'read_chunked',
                  Mock(return_value=[b'{"result":{"canceled":true}}']))
    def test__build_cache(self):
        with patch('patroni.dcs.etcd3.logger') as mock_logger:
            self.kv_cache._build_cache()
            mock_logger.info.assert_called_once_with('Watch request canceled')
        with patch('patroni.dcs.etcd3.logger') as mock_logger, \
                patch.object(urllib3.response.HTTPResponse, 'read_chunked', Mock(side_effect=WatchInterrupted)):
            self.kv_cache._build_cache()
            mock_logger.debug.assert_called_once_with('Watch request interrupted')

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch('patroni.dcs.etcd3.KVCache._build_cache', Mock(side_effect=Exception))
    def test_run(self):
        self.assertRaises(SleepException, self.kv_cache.run)

    @patch.object(urllib3.response.HTTPResponse, 'read_chunked',
                  Mock(return_value=[b'{"error":{"grpc_code":14,"message":"","http_code":503}}']))
    @patch.object(Etcd3Client, 'watchprefix', Mock(return_value=urllib3.response.HTTPResponse()))
    def test__do_watch(self):
        self.assertRaises(Unavailable, self.kv_cache._do_watch, '1')

    def test_kill_stream(self):
        timeout = urllib3.Timeout(connect=5, read=10)
        with HangingHTTPServer(b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n') as server, \
                patch.object(Etcd3Client, 'watchprefix', Mock(side_effect=lambda *_: self.client.http.urlopen(
                    'POST', server.url, preload_content=False, retries=False, timeout=timeout))):
            Timer(0.1, self.kv_cache.kill_stream).start()
            self.assertRaises(WatchInterrupted, self.kv_cache._do_watch, '1')

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_kill_stream_before_watch(self):
        prefix = self.client.prefix

        def kill_while_listing(*args, **kwargs):
            self.kv_cache.kill_stream()
            return prefix(*args, **kwargs)

        with patch.object(self.client, 'watchprefix', side_effect=WatchInterrupted) as watch:
            with patch.object(self.kv_cache._condition, 'notify_all', side_effect=self.kv_cache.kill_stream):
                self.kv_cache._build_cache()
            watch.assert_not_called()
            with patch.object(self.client, 'prefix', kill_while_listing), \
                    patch.object(self.kv_cache._condition, 'notify_all') as notify:
                self.kv_cache._build_cache()
            notify.assert_not_called()
            watch.assert_not_called()
            self.kv_cache._build_cache()
            watch.assert_called_once()

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_kill_stream_while_publishing(self):
        cache = self.kv_cache
        previous = cache._object_cache
        with patch.object(cache, '_object_cache_lock') as lock, patch.object(cache._condition, 'notify_all') as notify:
            lock.__enter__.side_effect = cache.kill_stream
            cache._build_cache()
        notify.assert_not_called()
        self.assertIs(cache._object_cache, previous)

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_readers_wait_after_kill_stream(self):
        for cluster in read_after_kill_stream(self.kv_cache, self.etcd3.get_cluster):
            self.assertEqual(cluster.leader_name, 'foo')

    def test_snapshot_timeout(self):
        self.assertRaises(RetryFailedError, self.kv_cache.snapshot, 0)

    def test_watch_after_list_failover(self):
        first, responder = self.client.machines_cache
        old, fresh = MockResponse(), MockResponse()
        old.content = '{"header":{"revision":"100"}}'
        fresh.content = '{"header":{"revision":"4"}}'
        for selected in (responder, 'http://localhost:2381'):
            self.client._base_uri = first
            self.client._machines_cache = [first, responder]
            with self.subTest(selected=selected), \
                    patch('patroni.dcs.etcd3.current_thread', return_value=self.kv_cache), \
                    patch.object(self.client.http, 'urlopen', side_effect=[socket.timeout(), old, fresh]) as request, \
                    patch.object(self.client, '_get_machines_list', return_value=[selected]), \
                    patch.object(self.client, 'watchprefix', side_effect=WatchInterrupted) as watch, \
                    patch.object(self.kv_cache._condition, 'notify_all') as notify:
                self.kv_cache._build_cache()
                self.assertEqual(self.client._base_uri, selected)
                if selected != responder:
                    notify.assert_not_called()
                    watch.assert_not_called()
                    self.kv_cache._build_cache()
                watch.assert_called_once_with(self.etcd3.cluster_prefix, '100' if selected == responder else '4')
                endpoints = [first, responder] if selected == responder else [first, responder, selected]
                self.assertEqual([args[1] for args, _ in request.call_args_list],
                                 [endpoint + self.client.version_prefix + '/kv/range' for endpoint in endpoints])

    @patch.object(urllib3.PoolManager, 'urlopen', mock_urlopen)
    def test_is_ready(self):
        self.kv_cache._build_cache()
        with self.kv_cache._condition:
            self.kv_cache._is_ready = True
            self.client._raft_term = 2
            self.assertFalse(self.kv_cache.is_ready())


class TestPatroniEtcd3Client(BaseTestEtcd3):

    @patch('patroni.dcs.etcd3.Etcd3Client.authenticate', Mock(side_effect=AuthFailed))
    def test__init__(self):
        self.assertRaises(SystemExit, self.setUp)

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test_call_rpc(self, mock_urlopen):
        request = {'key': base64_encode('/patroni/test/leader')}
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.content = '{"succeeded":true,"header":{"revision":"1"}}'
        self.client.call_rpc('/kv/put', request)
        self.client.call_rpc('/kv/deleterange', request)

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test_txn(self, mock_urlopen):
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.content = '{"header":{"revision":"1"}}'
        self.client.txn({'target': 'MOD', 'mod_revision': '1'},
                        {'request_delete_range': {'key': base64_encode('/patroni/test/leader')}})

    @patch.object(urllib3.PoolManager, 'urlopen')
    def test__restart_watcher(self, mock_urlopen):
        mock_urlopen.return_value = MockResponse()
        mock_urlopen.return_value.status_code = 400
        mock_urlopen.return_value.content = '{"code":9,"error":"etcdserver: authentication is not enabled"}'
        self.client.authenticate()

    def test_start_watcher_registers_before_start(self):
        started = []
        with patch.object(Thread, 'start', lambda cache: started.append(self.client._kv_cache is cache)):
            self.client.start_watcher()
        self.assertEqual(started, [True])

    def test__restart_watcher_from_kv_cache(self):
        self.kv_cache.run = lambda: self.client.set_base_uri('http://localhost:2380')
        Thread.start(self.kv_cache)
        self.kv_cache.join()
        self.assertEqual(self.client._base_uri, 'http://localhost:2380')
        self.assertFalse(self.kv_cache._watch_interrupter.is_set())
        self.client.set_base_uri('http://localhost:2381')
        self.assertTrue(self.kv_cache._watch_interrupter.is_set())

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
            retry = self.etcd3._retry.copy()
            with patch('time.monotonic', Mock(side_effect=[0, 10, 20, 30, 40])):
                self.assertRaises(InvalidAuthToken, retry, self.client.deleteprefix, 'foo', retry=retry)
            with patch('time.monotonic', Mock(side_effect=[0, 10])):
                self.assertRaises(InvalidAuthToken, self.client.deleteprefix, 'foo')
            self.client.username = None
            self.client._reauthenticate = False
            retry = self.etcd3._retry.copy()
            self.assertRaises(InvalidAuthToken, retry, self.client.deleteprefix, 'foo', retry=retry)
            mock_urlopen.return_value.content = '{"code":3,"error":"etcdserver: revision of auth store is old"}'
            self.client._reauthenticate = False
            self.assertRaises(AuthOldRevision, retry, self.client.deleteprefix, 'foo', retry=retry)

    def test__handle_server_response(self):
        response = MockResponse()
        response.content = '{"code":0,"error":"'
        self.assertRaises(etcd.EtcdException, self.client._handle_server_response, response)
        response.status_code = 400
        self.assertRaises(Unknown, self.client._handle_server_response, response)
        response.content = '{"error":{"code":14,"message":"","http_code":400}}'
        self.assertRaises(socket.timeout, self.client._handle_server_response, response)
        response.content = '{"error":{"code":2,"message":"not a primary lessor","http_code":400}}'
        self.assertRaises(socket.timeout, self.client._handle_server_response, response)
        response.content = '{"error":{"grpc_code":0,"message":"","http_code":400}}'
        try:
            self.client._handle_server_response(response)
        except Etcd3ClientError as e:
            self.assertEqual(e.as_dict(), {'code': 0, 'codeText': 'OK', 'error': u'', 'status': 400})

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
#        self.assertRaises(AttributeError, self.kv_cache._build_cache)
        self.kv_cache._build_cache()
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
                     "value": base64_encode('{"optime":1234567,"slots":{"ls":12345},"retain_slots": ["foo"]}'),
                     "mod_revision": '1'}
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
        self.etcd3._mpp = get_mpp({'citus': {'group': 0, 'database': 'postgres'}})
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
        cluster = self.etcd3.get_cluster()
        self.etcd3._lease = None
        with patch.object(Etcd3Client, 'txn', Mock(return_value={'succeeded': True})):
            self.etcd3.update_leader(cluster, '123', failsafe={'foo': 'bar'})
        self.etcd3._last_lease_refresh = float('-inf')
        self.etcd3.update_leader(cluster, '124')
        with patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=True)), \
                patch('time.monotonic', Mock(side_effect=[0, 100, 200, 300])):
            self.assertRaises(Etcd3Error, self.etcd3.update_leader, cluster, '126')
        self.etcd3._lease = cluster.leader.session
        self.etcd3.update_leader(cluster, '124')
        self.etcd3._last_lease_refresh = float('-inf')
        with patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(side_effect=Unknown)):
            self.assertFalse(self.etcd3.update_leader(cluster, '125'))

    def test_take_leader(self):
        self.assertFalse(self.etcd3.take_leader())

    def test_attempt_to_acquire_leader(self):
        self.assertFalse(self.etcd3.attempt_to_acquire_leader())
        with patch('time.monotonic', Mock(side_effect=[0, 0, 0, 0, 0, 100, 200, 300])):
            self.assertFalse(self.etcd3.attempt_to_acquire_leader())
        with patch('time.monotonic', Mock(side_effect=[0, 100, 200, 300, 400])):
            self.assertRaises(Etcd3Error, self.etcd3.attempt_to_acquire_leader)
        with patch.object(PatroniEtcd3Client, 'put', Mock(return_value=False)):
            self.assertFalse(self.etcd3.attempt_to_acquire_leader())

    def test_set_ttl(self):
        self.etcd3.set_ttl(20)

    @patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=False))
    def test_refresh_lease(self):
        self.etcd3._last_lease_refresh = float('-inf')
        self.etcd3.refresh_lease()

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(PatroniEtcd3Client, 'lease_keepalive', Mock(return_value=False))
    @patch.object(PatroniEtcd3Client, 'lease_grant', Mock(side_effect=Etcd3ClientError))
    def test_create_lease(self):
        self.etcd3._lease = None
        self.etcd3._last_lease_refresh = float('-inf')
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
        leader = self.etcd3.get_cluster().leader
        self.etcd3.delete_leader(leader)
        self.etcd3._name = 'other'
        self.etcd3.delete_leader(leader)

    def test_delete_cluster(self):
        self.etcd3.delete_cluster()

    def test_set_history_value(self):
        self.etcd3.set_history_value('')

    def test_set_sync_state_value(self):
        self.etcd3.set_sync_state_value('', 1)

    def test_delete_sync_state(self):
        self.etcd3.delete_sync_state('1')

    def test_watch(self):
        self.etcd3.set_ttl(10)
        self.etcd3.watch(None, 0)
        self.etcd3.watch('5', 0)

    def test_set_socket_options(self):
        with patch('socket.SIO_KEEPALIVE_VALS', 1, create=True):
            self.etcd3.set_socket_options(Mock(), None)
