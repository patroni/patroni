import unittest

from unittest.mock import Mock, patch

import urllib3

from patroni.dcs import get_dcs
from patroni.dcs.exhibitor import Exhibitor, ExhibitorEnsembleProvider
from patroni.dcs.zookeeper import ZooKeeperError

from . import requests_get, SleepException
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
        self.e = get_dcs({'exhibitor': {'hosts': ['localhost', 'exhibitor'], 'port': 8181},
                          'scope': 'test', 'name': 'foo', 'ttl': 30, 'retry_timeout': 10})
        self.assertIsInstance(self.e, Exhibitor)

    @patch.object(ExhibitorEnsembleProvider, 'poll', Mock(return_value=True))
    @patch.object(MockKazooClient, 'get_children', Mock(side_effect=Exception))
    def test_get_cluster(self):
        self.assertRaises(ZooKeeperError, self.e.get_cluster)
