import consul
import unittest

from mock import Mock, patch
from patroni.dcs.consul import AbstractDCS, Cluster, Consul, ConsulError, ConsulException, HTTPClient, NotFound
from test_etcd import SleepException


def kv_get(self, key, **kwargs):
    if key == 'service/test/members/postgresql1':
        return '1', {'Session': 'fd4f44fe-2cac-bba5-a60b-304b51ff39b7'}
    if key == 'service/test/':
        return None, None
    if key == 'service/good/leader':
        return '1', None
    if key == 'service/good/':
        return ('6429',
                [{'CreateIndex': 1334, 'Flags': 0, 'Key': key + 'failover', 'LockIndex': 0,
                  'ModifyIndex': 1334, 'Value': b''},
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
                  'ModifyIndex': 6429, 'Value': b'4496294792'}])
    raise ConsulException


class TestHTTPClient(unittest.TestCase):

    def test_get(self):
        self.client = HTTPClient('127.0.0.1', '8500', 'http', False)
        self.client.session.get = Mock()
        self.client.get(Mock(), '')
        self.client.get(Mock(), '', {'wait': '1s', 'index': 1})


@patch.object(consul.Consul.KV, 'get', kv_get)
class TestConsul(unittest.TestCase):

    @patch.object(consul.Consul.Session, 'create', Mock(return_value='fd4f44fe-2cac-bba5-a60b-304b51ff39b7'))
    @patch.object(consul.Consul.Session, 'renew', Mock(side_effect=NotFound))
    @patch.object(consul.Consul.KV, 'get', kv_get)
    @patch.object(consul.Consul.KV, 'delete', Mock())
    def setUp(self):
        self.c = Consul('postgresql1', {'ttl': 30, 'scope': 'test', 'host': 'localhost:1'})
        self.c._base_path = '/service/good'
        self.c._load_cluster()

    @patch('time.sleep', Mock(side_effect=SleepException))
    def test_create_or_restore_session(self):
        self.c._session = None
        self.assertRaises(SleepException, self.c.create_or_restore_session)

    @patch.object(consul.Consul.Session, 'renew', Mock(side_effect=NotFound))
    @patch.object(consul.Consul.Session, 'create', Mock(side_effect=ConsulException))
    def test_referesh_session(self):
        self.c._session = '1'
        self.c._name = ''
        self.assertRaises(ConsulError, self.c.refresh_session)

    @patch.object(consul.Consul.KV, 'delete', Mock())
    def test_get_cluster(self):
        self.c._base_path = '/service/test'
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.assertIsInstance(self.c.get_cluster(), Cluster)
        self.c._base_path = '/service/fail'
        self.assertRaises(ConsulError, self.c.get_cluster)
        self.c._base_path = '/service/good'
        self.c._session = 'fd4f44fe-2cac-bba5-a60b-304b51ff39b8'
        self.assertIsInstance(self.c.get_cluster(), Cluster)

    @patch.object(consul.Consul.KV, 'delete', Mock(side_effect=[ConsulException, True, True]))
    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=[True, ConsulException]))
    def test_touch_member(self):
        self.c.refresh_session = Mock(return_value=True)
        self.c.touch_member('balbla')
        self.c.touch_member('balbla')
        self.c.touch_member('balbla')
        self.c.refresh_session = Mock(return_value=False)
        self.c.touch_member('balbla')

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=False))
    def test_take_leader(self):
        self.c.take_leader()

    @patch.object(consul.Consul.KV, 'put', Mock(return_value=True))
    def test_set_failover_value(self):
        self.c.set_failover_value('')

    @patch.object(consul.Consul.KV, 'put', Mock(side_effect=ConsulException))
    def test_write_leader_optime(self):
        self.c.write_leader_optime('')

    def test_update_leader(self):
        self.c.update_leader()

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
        self.c._name = ''
        self.c.watch(1)
        with patch.object(consul.Consul.KV, 'get', Mock(side_effect=ConsulException)):
            self.c.watch(1)
