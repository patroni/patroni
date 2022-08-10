import unittest
import urllib3

from mock import Mock, patch
from patroni.dcs.exhibitor import ExhibitorEnsembleProvider, Exhibitor
from patroni.dcs.zookeeper import ZooKeeperError

from . import SleepException, requests_get
from .test_zookeeper import MockKazooClient


@patch('patroni.dcs.exhibitor.requests_get', requests_get)
@patch('time.sleep', Mock(side_effect=SleepException))
class TestExhibitorEnsembleProvider(unittest.TestCase):

    def test_init(self):
        self.assertRaises(SleepException, ExhibitorEnsembleProvider, ['localhost'], 8181)

    def test_poll(self):
        self.assertFalse(ExhibitorEnsembleProvider(['exhibitor'], 8181).poll())


class TestExhibitor(unittest.TestCase):

    @patch('urllib3.PoolManager.request', Mock(return_value=urllib3.HTTPResponse(
        status=200, body=b'{"servers":["127.0.0.1","127.0.0.2","127.0.0.3"],"port":2181}')))
    @patch('patroni.dcs.zookeeper.PatroniKazooClient', MockKazooClient)
    def setUp(self):
        self.e = Exhibitor({'hosts': ['localhost', 'exhibitor'], 'port': 8181, 'scope': 'test',
                            'name': 'foo', 'ttl': 30, 'retry_timeout': 10})

    @patch.object(ExhibitorEnsembleProvider, 'poll', Mock(return_value=True))
    @patch.object(MockKazooClient, 'get_children', Mock(side_effect=Exception))
    def test_get_cluster(self):
        self.assertRaises(ZooKeeperError, self.e.get_cluster)
