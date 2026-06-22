import unittest

from unittest.mock import Mock, MagicMock, patch, PropertyMock

from patroni.dcs import Cluster
from patroni.dcs.etcd3_grpc import Etcd3GrpcClient, Etcd3GrpcError, Etcd3_grpc, \
    catch_grpc_errors, _prefix_range_end
from patroni.postgresql.mpp import get_mpp
from patroni.utils import RetryFailedError

from . import SleepException


def mock_grpc_channel():
    """Create a mock gRPC secure channel."""
    channel = MagicMock()
    return channel


def mock_kv(key, value, mod_revision=1, lease=123):
    """Create a mock KV entry as returned by protobuf Range response."""
    kv = Mock()
    kv.key = key.encode('utf-8')
    kv.value = value.encode('utf-8')
    kv.mod_revision = mod_revision
    kv.lease = lease
    return kv


def make_range_response(kvs):
    resp = Mock()
    resp.kvs = [mock_kv(**kv) if isinstance(kv, dict) else kv for kv in kvs]
    return resp


def make_cluster_kvs():
    """Standard set of KVs for a test cluster."""
    return [
        mock_kv('/patroni/test/initialize', '12345', mod_revision=1, lease=0),
        mock_kv('/patroni/test/leader', 'foo', mod_revision=1, lease=123),
        mock_kv('/patroni/test/members/foo', '{}', mod_revision=1, lease=123),
        mock_kv('/patroni/test/members/bar', '{"version":"1.6.5"}', mod_revision=2, lease=456),
        mock_kv('/patroni/test/failover', '{}', mod_revision=1, lease=0),
        mock_kv('/patroni/test/failsafe', '{', mod_revision=1, lease=0),
    ]


class TestPrefixRangeEnd(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(_prefix_range_end('/patroni/test/'), b'/patroni/test0')

    def test_single_char(self):
        self.assertEqual(_prefix_range_end('a'), b'b')


class TestEtcd3GrpcClient(unittest.TestCase):

    def setUp(self):
        self.config = {
            'host': '127.0.0.1',
            'port': 2379,
            'cacert': None,
            'key': None,
            'cert': None,
        }

    def test_parse_endpoints_single(self):
        endpoints = Etcd3GrpcClient._parse_endpoints({'host': '10.0.0.1', 'port': 2379})
        self.assertEqual(endpoints, ['10.0.0.1:2379'])

    def test_parse_endpoints_hosts_string(self):
        endpoints = Etcd3GrpcClient._parse_endpoints({'hosts': '10.0.0.1,10.0.0.2', 'port': 2379})
        self.assertEqual(endpoints, ['10.0.0.1:2379', '10.0.0.2:2379'])

    def test_parse_endpoints_hosts_with_port(self):
        endpoints = Etcd3GrpcClient._parse_endpoints({'hosts': '10.0.0.1:2380,10.0.0.2:2380'})
        self.assertEqual(endpoints, ['10.0.0.1:2380', '10.0.0.2:2380'])

    def test_parse_endpoints_hosts_list(self):
        endpoints = Etcd3GrpcClient._parse_endpoints({'hosts': ['10.0.0.1', '10.0.0.2'], 'port': 2379})
        self.assertEqual(endpoints, ['10.0.0.1:2379', '10.0.0.2:2379'])

    @patch('grpc.ssl_channel_credentials')
    def test_build_credentials_no_certs(self, mock_ssl):
        mock_ssl.return_value = 'creds'
        result = Etcd3GrpcClient._build_credentials({'cacert': None, 'key': None, 'cert': None})
        mock_ssl.assert_called_once_with(root_certificates=None, private_key=None, certificate_chain=None)
        self.assertEqual(result, 'creds')

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_connect(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        mock_channel.assert_called_once_with('127.0.0.1:2379')
        self.assertIsNotNone(client._kv_stub)
        self.assertIsNotNone(client._lease_stub)

    @patch('grpc.secure_channel', return_value=mock_grpc_channel())
    @patch('grpc.ssl_channel_credentials', return_value='creds')
    def test_connect_tls(self, mock_ssl, mock_secure_channel):
        config = dict(self.config, cacert='/tmp/ca.crt')
        with patch('builtins.open', return_value=MagicMock(read=Mock(return_value=b'cert-data'))):
            client = Etcd3GrpcClient(config)
        client.connect()
        mock_secure_channel.assert_called_once_with('127.0.0.1:2379', 'creds')

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_close(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client.close()
        self.assertIsNone(client._channel)

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_rotate_endpoint(self, mock_channel):
        config = {'hosts': '10.0.0.1,10.0.0.2', 'port': 2379}
        client = Etcd3GrpcClient(config)
        client.connect()
        self.assertEqual(client._current_endpoint_idx, 0)
        client._rotate_endpoint()
        self.assertEqual(client._current_endpoint_idx, 1)
        client._rotate_endpoint()
        self.assertEqual(client._current_endpoint_idx, 0)

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_get_cluster(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client._kv_stub.Range = Mock(return_value=make_range_response([
            mock_kv('/patroni/test/leader', 'foo', mod_revision=5, lease=123),
        ]))
        nodes = client.get_cluster('/patroni/test/')
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]['key'], '/patroni/test/leader')
        self.assertEqual(nodes[0]['value'], 'foo')
        self.assertEqual(nodes[0]['mod_revision'], '5')
        self.assertEqual(nodes[0]['lease'], '123')

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_get_cluster_no_lease(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        client._kv_stub.Range = Mock(return_value=make_range_response([
            mock_kv('/patroni/test/config', '{}', mod_revision=1, lease=0),
        ]))
        nodes = client.get_cluster('/patroni/test/')
        self.assertIsNone(nodes[0]['lease'])

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_lease_grant(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.ID = 99999
        client._lease_stub.LeaseGrant = Mock(return_value=resp)
        lease_id = client.lease_grant(30)
        self.assertEqual(lease_id, 99999)

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_lease_keepalive(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.TTL = 30
        client._lease_stub.LeaseKeepAlive = Mock(return_value=iter([resp]))
        self.assertTrue(client.lease_keepalive(99999))

    @patch('grpc.insecure_channel', return_value=mock_grpc_channel())
    def test_lease_keepalive_expired(self, mock_channel):
        client = Etcd3GrpcClient(self.config)
        client.connect()
        resp = Mock()
        resp.TTL = 0
        client._lease_stub.LeaseKeepAlive = Mock(return_value=iter([resp]))
        self.assertFalse(client.lease_keepalive(99999))


class TestCatchGrpcErrors(unittest.TestCase):

    def test_catches_etcd3_grpc_error(self):
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self):
                raise Etcd3GrpcError('test error')

        self.assertFalse(FakeDCS().do_something())

    def test_catches_retry_failed_error(self):
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self):
                raise RetryFailedError('')

        self.assertFalse(FakeDCS().do_something())

    def test_passes_through_on_success(self):
        class FakeDCS:
            @catch_grpc_errors
            def do_something(self):
                return True

        self.assertTrue(FakeDCS().do_something())


@patch('grpc.insecure_channel', return_value=mock_grpc_channel())
class TestEtcd3Grpc(unittest.TestCase):

    def _make_dcs(self, mock_channel=None):
        with patch.object(Etcd3GrpcClient, 'lease_grant', return_value=123), \
                patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            dcs = Etcd3_grpc({'namespace': '/patroni/', 'ttl': 30, 'retry_timeout': 10,
                              'name': 'foo', 'scope': 'test',
                              'etcd3_grpc': {'host': '127.0.0.1', 'port': 2379}},
                             get_mpp({}))
        return dcs

    def test_init(self, mock_channel):
        dcs = self._make_dcs()
        self.assertEqual(dcs._ttl, 30)
        self.assertEqual(dcs._lease, 123)
        self.assertIsNotNone(dcs._client)

    def test_get_cluster(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertEqual(cluster.leader.name, 'foo')
        self.assertEqual(len(cluster.members), 2)

    def test_get_cluster_error(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', side_effect=Exception('connection refused')):
            self.assertRaises(Etcd3GrpcError, dcs.get_cluster)

    def test_touch_member(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()) as mock_put, \
                patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            self.assertTrue(dcs.touch_member({'conn_url': 'http://localhost:5432'}))
            mock_put.assert_called_once()

    def test_touch_member_no_lease(self, mock_channel):
        dcs = self._make_dcs()
        dcs._lease = None
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        # _refresh_lease will try to grant a new lease
        with patch.object(Etcd3GrpcClient, 'lease_grant', side_effect=Etcd3GrpcError('no lease')):
            self.assertFalse(dcs.touch_member({}))

    def test_touch_member_data_unchanged(self, mock_channel):
        dcs = self._make_dcs()
        # Member 'foo' exists with session '123' and data {}
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        with patch.object(Etcd3GrpcClient, 'put') as mock_put, \
                patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            # data matches what's stored — should skip the put
            self.assertTrue(dcs.touch_member({}))
            mock_put.assert_not_called()

    def test_take_leader(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()):
            self.assertTrue(dcs.take_leader())

    def test_attempt_to_acquire_leader_success(self, mock_channel):
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, 'txn', return_value=txn_resp), \
                patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            self.assertTrue(dcs.attempt_to_acquire_leader())

    def test_attempt_to_acquire_leader_fail(self, mock_channel):
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = False
        with patch.object(Etcd3GrpcClient, 'txn', return_value=txn_resp), \
                patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            self.assertFalse(dcs.attempt_to_acquire_leader())

    def test_attempt_to_acquire_leader_no_lease(self, mock_channel):
        dcs = self._make_dcs()
        dcs._lease = None
        with patch.object(Etcd3GrpcClient, 'lease_grant', side_effect=Etcd3GrpcError('err')):
            self.assertFalse(dcs.attempt_to_acquire_leader())

    def test_update_leader_same_session(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        # leader session is '123', our lease is 123 → str match
        with patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            self.assertTrue(dcs._update_leader(cluster.leader))

    def test_update_leader_different_session(self, mock_channel):
        dcs = self._make_dcs()
        dcs._lease = 999
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, 'txn', return_value=txn_resp), \
                patch.object(Etcd3GrpcClient, 'lease_keepalive', return_value=True):
            dcs._last_lease_refresh = 0
            self.assertTrue(dcs._update_leader(cluster.leader))

    def test_delete_leader(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            cluster = dcs.get_cluster()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, 'txn', return_value=txn_resp):
            self.assertTrue(dcs._delete_leader(cluster.leader))

    def test_set_failover_value(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()):
            self.assertTrue(dcs.set_failover_value('{}'))

    def test_set_config_value(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()):
            self.assertTrue(dcs.set_config_value('{}'))

    def test_initialize(self, mock_channel):
        dcs = self._make_dcs()
        txn_resp = Mock()
        txn_resp.succeeded = True
        with patch.object(Etcd3GrpcClient, 'txn', return_value=txn_resp):
            self.assertTrue(dcs.initialize())

    def test_initialize_not_new(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()):
            self.assertTrue(dcs.initialize(create_new=False, sysid='12345'))

    def test_cancel_initialization(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'delete', return_value=Mock()):
            self.assertTrue(dcs.cancel_initialization())

    def test_delete_cluster(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'delete_prefix', return_value=Mock()):
            self.assertTrue(dcs.delete_cluster())

    def test_set_history_value(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'put', return_value=Mock()):
            self.assertTrue(dcs.set_history_value('[]'))

    def test_set_sync_state_value(self, mock_channel):
        dcs = self._make_dcs()
        resp = Mock()
        resp.header.revision = 42
        with patch.object(Etcd3GrpcClient, 'put', return_value=resp):
            result = dcs.set_sync_state_value('{}')
            self.assertEqual(result, '42')

    def test_delete_sync_state(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'delete', return_value=Mock()):
            self.assertTrue(dcs.delete_sync_state())

    def test_set_ttl(self, mock_channel):
        dcs = self._make_dcs()
        self.assertEqual(dcs._ttl, 30)
        dcs.set_ttl(20)
        self.assertEqual(dcs._ttl, 20)
        self.assertIsNone(dcs._lease)

    def test_set_ttl_same_value(self, mock_channel):
        dcs = self._make_dcs()
        dcs.set_ttl(30)
        # lease should NOT be cleared when ttl doesn't change
        self.assertEqual(dcs._lease, 123)

    def test_set_retry_timeout(self, mock_channel):
        dcs = self._make_dcs()
        dcs.set_retry_timeout(20)
        self.assertEqual(dcs._retry.deadline, 20)

    @patch('time.sleep', Mock(side_effect=SleepException))
    def test_create_lease_retry(self, mock_channel):
        with patch.object(Etcd3GrpcClient, 'lease_grant', side_effect=Etcd3GrpcError('err')):
            with self.assertRaises(SleepException):
                Etcd3_grpc({'namespace': '/patroni/', 'ttl': 30, 'retry_timeout': 10,
                            'name': 'foo', 'scope': 'test',
                            'etcd3_grpc': {'host': '127.0.0.1', 'port': 2379,
                                           'cacert': None, 'key': None, 'cert': None}},
                           get_mpp({}))

    def test_watch(self, mock_channel):
        dcs = self._make_dcs()
        with patch.object(Etcd3GrpcClient, 'get_prefix', return_value=make_range_response(make_cluster_kvs())):
            dcs.get_cluster()
        # watch with timeout 0 just returns (no events to process)
        result = dcs.watch(None, 0)
        self.assertIn(result, (True, False))
