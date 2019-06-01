import unittest

from mock import Mock, patch
from patroni.dcs.exhibitor import ExhibitorEnsembleProvider, Exhibitor
from patroni.dcs.zookeeper import ZooKeeperError

from . import SleepException, requests_get
from .test_zookeeper import MockKazooClient


@patch('requests.get', requests_get)
@patch('time.sleep', Mock(side_effect=SleepException))
class TestExhibitorEnsembleProvider(unittest.TestCase):

    def test_init(self):
        self.assertRaises(SleepException, ExhibitorEnsembleProvider, ['localhost'], 8181)

    def test_poll(self):
        self.assertFalse(ExhibitorEnsembleProvider(['exhibitor'], 8181).poll())


class TestExhibitor(unittest.TestCase):

    @patch('requests.get', requests_get)
    @patch('patroni.dcs.zookeeper.KazooClient', MockKazooClient)
    def setUp(self):
        self.e = Exhibitor({'hosts': ['localhost', 'exhibitor'], 'port': 8181, 'scope': 'test',
                            'name': 'foo', 'ttl': 30, 'retry_timeout': 10})

    @patch.object(ExhibitorEnsembleProvider, 'poll', Mock(return_value=True))
    def test_get_cluster(self):
        self.assertRaises(ZooKeeperError, self.e.get_cluster)
