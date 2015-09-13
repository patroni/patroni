#!/usr/bin/env python
import logging
import os
import sys
import time
import yaml

from helpers.api import RestApiServer
from helpers.etcd import Etcd
from helpers.ha import Ha
from helpers.postgresql import Postgresql
from helpers.utils import setup_signal_handlers, sleep, reap_children
from helpers.zookeeper import ZooKeeper

logger = logging.getLogger(__name__)


class Patroni:

    def __init__(self, config):
        self.nap_time = config['loop_wait']
        self.postgresql = Postgresql(config['postgresql'])
        self.ha = Ha(self.postgresql, self.get_dcs(self.postgresql.name, config))
        host, port = config['restapi']['listen'].split(':')
        self.api = RestApiServer(self, config['restapi'])
        self.skydns2 = config.get('skydns2')
        self.next_run = time.time()
        self.shutdown_member_ttl = 300

    @staticmethod
    def get_dcs(name, config):
        if 'etcd' in config:
            assert config['etcd']['ttl'] > 2 * config['loop_wait']

            return Etcd(name, config['etcd'])
        if 'zookeeper' in config:
            return ZooKeeper(name, config['zookeeper'])
        raise Exception('Can not find sutable configuration of distributed configuration store')

    def touch_member(self, ttl=None):
        connection_string = self.postgresql.connection_string + '?application_name=' + self.api.connection_string
        if self.ha.cluster:
            for m in self.ha.cluster.members:
                # Do not update member TTL when it is far from being expired
                if m.name == self.postgresql.name and m.real_ttl() > self.shutdown_member_ttl:
                    return True
        return self.ha.dcs.touch_member(connection_string, ttl)

    def initialize(self):
        # wait for etcd to be available
        while not self.touch_member():
            logger.info('waiting on DCS')
            sleep(5)

        # is data directory empty?
        if self.postgresql.data_directory_empty():
            # racing to initialize
            if self.ha.dcs.race('/initialize'):
                self.postgresql.initialize()
                self.ha.dcs.take_leader()
                self.postgresql.start()
            else:
                while True:
                    leader = self.ha.dcs.current_leader()
                    if leader and self.postgresql.sync_from_leader(leader):
                        self.postgresql.write_recovery_conf(leader)
                        self.postgresql.start()
                        break
                    sleep(5)
        elif self.postgresql.is_running():
            self.postgresql.load_replication_slots()

    def schedule_next_run(self):
        self.next_run += self.nap_time
        current_time = time.time()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
        else:
            self.ha.dcs.sleep(nap_time)

    def run(self):
        self.api.start()
        self.next_run = time.time()

        while True:
            self.touch_member()
            logger.info(self.ha.run_cycle())
            try:
                if self.ha.state_handler.is_leader():
                    self.ha.cluster and self.ha.state_handler.create_replication_slots(self.ha.cluster)

                    # SkyDNS2 support: publish leader
                    if self.skydns2:
                        self.ha.dcs.client.set(self.skydns2['publish_leader'],
                            '{{"host": "{0}", "port": {1}}}'.format(*self.postgresql.connect_address), ttl=self.skydns2['ttl'])
                else:
                    self.ha.state_handler.drop_replication_slots()
            except:
                logger.exception('Exception when changing replication slots')
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
        patroni.initialize()
        patroni.run()
    except KeyboardInterrupt:
        pass
    finally:
        patroni.touch_member(patroni.shutdown_member_ttl)  # schedule member removal
        patroni.postgresql.stop()
        patroni.ha.dcs.delete_leader()


if __name__ == '__main__':
    main()
