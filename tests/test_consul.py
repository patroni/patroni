import consul
import unittest

from consul import ConsulException, NotFound
from mock import Mock, PropertyMock, patch
from patroni.dcs.consul import AbstractDCS, Cluster, Consul, ConsulInternalError, \
        ConsulError, ConsulClient, HTTPClient, InvalidSessionTTL, InvalidSession, RetryFailedError
from . import SleepException


def kv_get(self, key, **kwargs):
    if key == 'service/test/members/postgresql1':
        return '1', {'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7'}
    if key == 'service/test/':
        return None, None
    if key == 'service/good/leader':
        return '1', None
    good_cls = ('6429',
                [{'CreateIndex': 1334, 'Flags': 0, 'Key': key + 'failover', 'LockIndex': 0,
                  'ModifyIndex': 1334, 'Value': b''},
                 {'CreateIndex': 1334, 'Flags': 0, 'Key': key + '1/initialize', 'LockIndex': 0,
                  'ModifyIndex': 1334, 'Value': b'postgresql0'},
                 {'CreateIndex': 1334, 'Flags': 0, 'Key': key + 'initialize', 'LockIndex': 0,
                  'ModifyIndex': 1334, 'Value': b'postgresql0'},
                 {'CreateIndex': 2621, 'Flags': 0, 'Key': key + 'leader', 'LockIndex': 1,
                  'ModifyIndex': 2621, 'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7', 'Value': b'postgresql1'},
                 {'CreateIndex': 6156, 'Flags': 0, 'Key': key + 'members/postgresql0', 'LockIndex': 1,
                  'ModifyIndex': 6156, 'Session': '782e6da4-ed02-3aef-7963-99a90ed94b53',
                  'Value': ('postgres://replicator:rep-pass@127.0.0.1:5432/postgres' +
                            '?application_name=http://127.0.0.1:8008/patroni').encode('utf-8')},
                 {'CreateIndex': 2630, 'Flags': 0, 'Key': key + 'members/postgresql1', 'LockIndex': 1,
                  'ModifyIndex': 2630, 'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7',
                  'Value': ('postgres://replicator:rep-pass@127.0.0.1:5433/postgres' +
                            '?application_name=http://127.0.0.1:8009/patroni').encode('utf-8')},
                 {'CreateIndex': 1085, 'Flags': 0, 'Key': key + 'optime/leader', 'LockIndex': 0,
                  'ModifyIndex': 6429, 'Value': b'4496294792'},
                 {'CreateIndex': 1085, 'Flags': 0, 'Key': key + 'sync', 'LockIndex': 0,
                  'ModifyIndex': 6429, 'Value': b'{"leader": "leader", "sync_standby": null}'},
                 {'CreateIndex': 1085, 'Flags': 0, 'Key': key + 'failsafe', 'LockIndex': 0,
                  'ModifyIndex': 6429, 'Value': b'{'},
                 {'CreateIndex': 1085, 'Flags': 0, 'Key': key + 'status', 'LockIndex': 0,
                  'ModifyIndex': 6429, 'Value': b'{"optime":4496294792, "slots":{"ls":12345}}'}])
    if key == 'service/good/':
        return good_cls
    if key == 'service/broken/':
        good_cls[1][-1]['Value'] = b'{'
        return good_cls
    if key == 'service/legacy/':
        good_cls[1].pop()
        return good_cls
    raise ConsulException


class TestHTTPClient(unittest.TestCase):

    def setUp(self):
        c = ConsulClient()
        self.client = c.http
        self.client.http.request = Mock()

    def test_get(self):
        self.client.get(Mock(), '')
        self.client.get(Mock(), '', {'wait': '1s', 'index': 1, 'token': 'foo'})
        self.client.http.request.return_value.status = 500
        self.client.http.request.return_value.data = b'Foo'
        self.assertRaises(ConsulInternalError, self.client.get, Mock(), '')
        self.client.http.request.return_value.data = b"Invalid Session TTL '3000000000', must be between [10s=24h0m0s]"
        self.assertRaises(InvalidSessionTTL, self.client.get, Mock(), '')
        self.client.http.request.return_value.data = b"invalid session '16492f43-c2d6-5307-432f-e32d6f7bcbd0'"
        self.assertRaises(InvalidSession, self.client.get, Mock(), '')

    def test_unknown_method(self):
        try:
            self.client.bla(Mock(), '')
            self.assertFail()
        except Exception as e:
            self.assertTrue(isinstance(e, AttributeError))

    def test_put(self):
        self.client.put(Mock(), '/v1/session/create')
        self.client.put(Mock(), '/v1/session/create', params=[], data='{"foo": "bar"}')


@patch.object(consul.Consul.KV, 'get', kv_get)
class TestConsul(unittest.TestCase):

    @patch.object(consul.Consul.Session, 'create', Mock(return_value='fd4f44fe-2cac-bba5-a60b-304b51ff39b7'))
    @patch.object(consul.Consul.Session, 'renew', Mock(side_effect=NotFound))
    @patch.object(consul.Consul.KV, 'get', kv_get)
    @patch.object(consul.Consul.KV, 'delete', Mock())
    def setUp(self):
        Consul({'ttl': 30, 'scope': 't', 'name': 'p', 'url': 'https://l:1', 'retry_timeout': 10,
                'verify': 'on', 'key': 'foo', 'cert': 'bar', 'cacert': 'buz', 'token': 'asd', 'dc': 'dc1',
                'register_service': True})
        Consul({'ttl': 30, 'scope': 't_', 'name': 'p', 'url': 'https://l:1', 'retry_timeout': 10,
                'verify': 'on', 'cert': 'bar', 'cacert': 'buz', 'register_service': True})
        self.c = Consul({'ttl': 30, 'scope': 'test', 'name': 'postgresql1', 'host': 'localhost:1', 'retry_timeout': 10,
                         'register_service': True, 'service_check_tls_server_name': True})
        self.c._base_path = 'service/good'
        self.c.get_cluster()

    @patch('time.sleep', Mock(side_effect=SleepException))
    @patch.object(consul.Consul.Session, 'create', Mock(side_effect=ConsulException))
    def test_create_session(self):
        self.c._session = None
        self.assertRaises(SleepException, self.c.create_session)

    @patch.object(consul.Consul.Session, 'renew', Mock(side_effect=NotFound))
    @patch.object(consul.Consul.Session, 'create', Mock(side_effect=[InvalidSessionTTL, ConsulException]))
    @patch.object(consul.Consul.Agent, 'self', Mock(return_value={'Config': {'SessionTTLMin': 0}}))
    @patch.object(HTTPClient, 'set_ttl', Mock(side_effect=ValueError))
    def test_referesh_session(self):
        self.c._session = '1'
        self.assertFalse(self.c.refresh_session())
        self.c._last_session_refresh = 0
        self.assertRaises(ConsulError, self.c.refresh_session)

    @patch.object(consul.Consul.KV, 'delete', Mock())
    def test_get_cluster(self):
        self.c._base_path = 'service/test'
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.c._base_path = 'service/fail'
        self.assertRaises(ConsulError, self.c.get_cluster)
        self.c._base_path = 'service/broken'
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.c._base_path = 'service/legacy'
        self.assertIsInstance(self.c.get_cluster(), Cluster)

    def test__get_citus_cluster(self):
        self.c._citus_group = '0'
        cluster = self.c.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertIsInstance(cluster.workers[1], Cluster)

    @patch.object(consul.Consul.KV, 'delete', Mock(side_effect=[ConsulException, True, True, True]))
    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=[True, ConsulException, InvalidSession]))
    def test_touch_member(self):
        self.c.refresh_session = Mock(return_value=False)
        with patch.object(Consul, 'update_service', Mock(side_effect=Exception)):
            self.c.touch_member({'conn_url': 'postgres://replicator:rep-pass@127.0.0.1:5433/postgres',
                                 'api_url': 'http://127.0.0.1:8009/patroni'})
        self.c._register_service = True
        self.c.refresh_session = Mock(return_value=True)
        for _ in range(0, 4):
            self.c.touch_member({'balbla': 'blabla'})
        self.c.refresh_session = Mock(side_effect=ConsulError('foo'))
        self.assertFalse(self.c.touch_member({'balbla': 'blabla'}))

    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=[InvalidSession, False, InvalidSession]))
    def test_take_leader(self):
        self.c.set_ttl(20)
        self.c._do_refresh_session = Mock()
        self.assertFalse(self.c.take_leader())
        with patch('time.time', Mock(side_effect=[0, 100])):
            self.assertRaises(ConsulError, self.c.take_leader)
        with patch('time.time', Mock(side_effect=[0, 0, 0, 0, 0, 0, 100])):
            self.assertRaises(ConsulError, self.c.take_leader)

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_set_failover_value(self):
        self.c.set_failover_value('')

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_set_config_value(self):
        self.c.set_config_value('')

    @patch.object(Cluster, 'min_version', PropertyMock(return_value=(2, 0)))
    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=ConsulException))
    def test_write_leader_optime(self):
        self.c.get_cluster()
        self.c.write_leader_optime('1')

    @patch.object(consul.Consul.Session, 'renew')
    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=ConsulException))
    def test_update_leader(self, mock_renew):
        self.c._session = 'fd4f44fe-2cac-bba5-a60b-304b51ff39b8'
        with patch.object(consul.Consul.KV, 'delete', Mock(return_value=True)):
            with patch.object(consul.Consul.KV, 'put', Mock(return_value=True)):
                self.assertTrue(self.c.update_leader(12345, failsafe={'foo': 'bar'}))
            with patch.object(consul.Consul.KV, 'put', Mock(side_effect=ConsulException)):
                self.assertFalse(self.c.update_leader(12345))
            with patch('time.time', Mock(side_effect=[0, 0, 0, 0, 0, 100, 200, 300])):
                self.assertRaises(ConsulError, self.c.update_leader, 12345)
        with patch('time.time', Mock(side_effect=[0, 100, 200, 300])):
            self.assertRaises(ConsulError, self.c.update_leader, 12345)
        with patch.object(consul.Consul.KV, 'delete', Mock(side_effect=ConsulException)):
            self.assertFalse(self.c.update_leader(12347))
        mock_renew.side_effect = RetryFailedError('')
        self.c._last_session_refresh = 0
        self.assertRaises(ConsulError, self.c.update_leader, 12346)
        mock_renew.side_effect = ConsulException
        self.assertFalse(self.c.update_leader(12347))

    @patch.object(consul.Consul.KV, 'delete', Mock(return_value=True))
    def test_delete_leader(self):
        self.c.delete_leader()

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_initialize(self):
        self.c.initialize()

    @patch.object(consul.Consul.KV, 'delete', Mock(return_value=True))
    def test_cancel_initialization(self):
        self.c.cancel_initialization()

    @patch.object(consul.Consul.KV, 'delete', Mock(return_value=True))
    def test_delete_cluster(self):
        self.c.delete_cluster()

    @patch.object(AbstractDCS, 'watch', Mock())
    def test_watch(self):
        self.c.watch(None, 1)
        self.c._name = ''
        self.c.watch(6429, 1)
        with patch.object(consul.Consul.KV, 'get', Mock(side_effect=ConsulException)):
            self.c.watch(6429, 1)

    def test_set_retry_timeout(self):
        self.c.set_retry_timeout(10)

    @patch.object(consul.Consul.KV, 'delete', Mock(return_value=True))
    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_sync_state(self):
        self.assertTrue(self.c.set_sync_state_value('{}'))
        self.assertTrue(self.c.delete_sync_state())

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_set_history_value(self):
        self.assertTrue(self.c.set_history_value('{}'))

    @patch.object(consul.Consul.Agent.Service, 'register', Mock(side_effect=(False, True, True, True)))
    @patch.object(consul.Consul.Agent.Service, 'deregister', Mock(return_value=True))
    def test_update_service(self):
        d = {'role': 'replica', 'api_url': 'http://a/t', 'conn_url': 'pg://c:1', 'state': 'running'}
        self.assertIsNone(self.c.update_service({}, {}))
        self.assertFalse(self.c.update_service({}, d))
        self.assertTrue(self.c.update_service(d, d))
        self.assertIsNone(self.c.update_service(d, d))
        d['state'] = 'stopped'
        self.assertTrue(self.c.update_service(d, d, force=True))
        d['state'] = 'unknown'
        self.assertIsNone(self.c.update_service({}, d))
        d['state'] = 'running'
        d['role'] = 'bla'
        self.assertIsNone(self.c.update_service({}, d))
        for role in ('master', 'primary'):
            d['role'] = role
            self.assertTrue(self.c.update_service({}, d))

    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=ConsulException))
    def test_reload_config(self):
        self.assertEqual([], self.c._service_tags)
        self.c.reload_config({'consul': {'token': 'foo', 'register_service': True, 'service_tags': ['foo']},
                              'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        self.assertEqual(["foo"], self.c._service_tags)

        self.c.refresh_session = Mock(return_value=False)

        d = {'role': 'replica', 'api_url': 'http://a/t', 'conn_url': 'pg://c:1', 'state': 'running'}

        # Changing register_service from True to False calls deregister()
        self.c.reload_config({'consul': {'register_service': False}, 'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.deregister') as mock_deregister:
            self.c.touch_member(d)
            mock_deregister.assert_called_once()

        self.assertEqual([], self.c._service_tags)

        # register_service staying False between reloads does not call deregister()
        self.c.reload_config({'consul': {'register_service': False}, 'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.deregister') as mock_deregister:
            self.c.touch_member(d)
            self.assertFalse(mock_deregister.called)

        # Changing register_service from False to True calls register()
        self.c.reload_config({'consul': {'register_service': True}, 'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.register') as mock_register:
            self.c.touch_member(d)
            mock_register.assert_called_once()

        # register_service staying True between reloads does not call register()
        self.c.reload_config({'consul': {'register_service': True}, 'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.register') as mock_register:
            self.c.touch_member(d)
            self.assertFalse(mock_deregister.called)

        # register_service staying True between reloads does calls register() if other service data has changed
        self.c.reload_config({'consul': {'register_service': True}, 'loop_wait': 10, 'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.register') as mock_register:
            self.c.touch_member(d)
            mock_register.assert_called_once()

        # register_service staying True between reloads does calls register() if service_tags have changed
        self.c.reload_config({'consul': {'register_service': True, 'service_tags': ['foo']}, 'loop_wait': 10,
                              'ttl': 30, 'retry_timeout': 10})
        with patch('consul.Consul.Agent.Service.register') as mock_register:
            self.c.touch_member(d)
            mock_register.assert_called_once()
