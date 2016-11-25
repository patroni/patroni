import logging
import random
import requests
import time

from patroni.dcs.zookeeper import ZooKeeper
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class ExhibitorEnsembleProvider(object):

    TIMEOUT = 3.1

    def __init__(self, hosts, port, uri_path='/exhibitor/v1/cluster/list', poll_interval=300):
        self._exhibitor_port = port
        self._uri_path = uri_path
        self._poll_interval = poll_interval
        self._exhibitors = hosts
        self._master_exhibitors = hosts
        self._zookeeper_hosts = ''
        self._next_poll = None
        while not self.poll():
            logger.info('waiting on exhibitor')
            time.sleep(5)

    def poll(self):
        if self._next_poll and self._next_poll > time.time():
            return False

        json = self._query_exhibitors(self._exhibitors)
        if not json:
            json = self._query_exhibitors(self._master_exhibitors)

        if isinstance(json, dict) and 'servers' in json and 'port' in json:
            self._next_poll = time.time() + self._poll_interval
            zookeeper_hosts = ','.join([h + ':' + str(json['port']) for h in sorted(json['servers'])])
            if self._zookeeper_hosts != zookeeper_hosts:
                logger.info('ZooKeeper connection string has changed: %s => %s', self._zookeeper_hosts, zookeeper_hosts)
                self._zookeeper_hosts = zookeeper_hosts
                self._exhibitors = json['servers']
                return True
        return False

    def _query_exhibitors(self, exhibitors):
        random.shuffle(exhibitors)
        for host in exhibitors:
            uri = 'http://{0}:{1}{2}'.format(host, self._exhibitor_port, self._uri_path)
            try:
                response = requests.get(uri, timeout=self.TIMEOUT)
                return response.json()
            except RequestException:
                pass
        return None

    @property
    def zookeeper_hosts(self):
        return self._zookeeper_hosts


class Exhibitor(ZooKeeper):

    def __init__(self, config):
        interval = config.get('poll_interval', 300)
        self._ensemble_provider = ExhibitorEnsembleProvider(config['hosts'], config['port'], poll_interval=interval)
        config = config.copy()
        config['hosts'] = self._ensemble_provider.zookeeper_hosts
        super(Exhibitor, self).__init__(config)

    def _load_cluster(self):
        if self._ensemble_provider.poll():
            self._client.set_hosts(self._ensemble_provider.zookeeper_hosts)
        return super(Exhibitor, self)._load_cluster()
