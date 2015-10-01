import logging
import os
import sys
import time
import yaml

from patroni.api import RestApiServer
from patroni.etcd import Etcd
from patroni.ha import Ha
from patroni.postgresql import Postgresql
from patroni.utils import setup_signal_handlers, reap_children
from patroni.zookeeper import ZooKeeper

logger = logging.getLogger(__name__)


class Patroni:

    def __init__(self, config):
        self.nap_time = config['loop_wait']
        self.postgresql = Postgresql(config['postgresql'])
        self.dcs = self.get_dcs(self.postgresql.name, config)
        host, port = config['restapi']['listen'].split(':')
        self.api = RestApiServer(self, config['restapi'])
        self.ha = Ha(self)
        self.next_run = time.time()

    @staticmethod
    def get_dcs(name, config):
        if 'etcd' in config:
            return Etcd(name, config['etcd'])
        if 'zookeeper' in config:
            return ZooKeeper(name, config['zookeeper'])
        raise Exception('Can not find sutable configuration of distributed configuration store')

    def schedule_next_run(self):
        self.next_run += self.nap_time
        current_time = time.time()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
        elif self.dcs.watch(nap_time):
            self.next_run = time.time()

    def run(self):
        self.api.start()
        self.next_run = time.time()

        while True:
            logger.info(self.ha.run_cycle())
            reap_children()
            self.schedule_next_run()


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    logging.getLogger('requests').setLevel(logging.WARNING)
    setup_signal_handlers()

    if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
        print('Usage: {} config.yml'.format(sys.argv[0]))
        return

    with open(sys.argv[1], 'r') as f:
        config = yaml.load(f)

    patroni = Patroni(config)
    try:
        patroni.run()
    except KeyboardInterrupt:
        pass
    finally:
        patroni.api.shutdown()
        patroni.postgresql.stop()
        patroni.dcs.delete_leader()
