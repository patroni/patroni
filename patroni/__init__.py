import logging
import os
import sys
import time
import yaml

from patroni.api import RestApiServer
from patroni.etcd import Etcd
from patroni.exceptions import DCSError
from patroni.ha import Ha
from patroni.postgresql import Postgresql
from patroni.utils import setup_signal_handlers, sleep, reap_children
from patroni.zookeeper import ZooKeeper

logger = logging.getLogger(__name__)


class Patroni:

    def __init__(self, config):
        self.nap_time = config['loop_wait']
        self.postgresql = Postgresql(config['postgresql'])
        self.ha = Ha(self.postgresql, self.get_dcs(self.postgresql.name, config))
        host, port = config['restapi']['listen'].split(':')
        self.api = RestApiServer(self, config['restapi'])
        self.next_run = time.time()
        self.shutdown_member_ttl = 300

    @staticmethod
    def get_dcs(name, config):
        if 'etcd' in config:
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

    def cleanup_on_failed_initialization(self):
        """ cleanup the DCS if initialization was not successfull """
        logger.info("removing initialize key after failed attempt to initialize the cluster")
        self.ha.dcs.cancel_initialization()
        self.touch_member(self.shutdown_member_ttl)
        self.postgresql.stop()
        self.postgresql.move_data_directory()

    def initialize(self):
        # wait for etcd to be available
        while not self.touch_member():
            logger.info('waiting on DCS')
            sleep(5)

        # is data directory empty?
        if self.postgresql.data_directory_empty():
            while True:
                try:
                    cluster = self.ha.dcs.get_cluster()
                    if not cluster.is_unlocked():  # the leader already exists
                        if not cluster.initialize:
                            self.ha.dcs.initialize()
                        self.postgresql.bootstrap(cluster.leader)
                        break
                    # racing to initialize
                    elif not cluster.initialize and self.ha.dcs.initialize():
                        try:
                            self.postgresql.bootstrap()
                        except:
                            # bail out and clean the initialize flag.
                            self.cleanup_on_failed_initialization()
                            raise
                        self.ha.dcs.take_leader()
                        break
                except DCSError:
                    logger.info('waiting on DCS')
                sleep(5)
        elif self.postgresql.is_running():
            self.postgresql.schedule_load_slots = True

    def schedule_next_run(self):
        self.next_run += self.nap_time
        current_time = time.time()
        nap_time = self.next_run - current_time
        if nap_time <= 0:
            self.next_run = current_time
        else:
            self.ha.dcs.watch(nap_time)

    def run(self):
        self.api.start()
        self.next_run = time.time()

        while True:
            self.touch_member()
            logger.info(self.ha.run_cycle())
            try:
                self.ha.cluster and self.ha.state_handler.sync_replication_slots(self.ha.cluster)
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
    patroni.initialize()
    try:
        patroni.run()
    except KeyboardInterrupt:
        pass
    finally:
        patroni.touch_member(patroni.shutdown_member_ttl)  # schedule member removal
        patroni.postgresql.stop()
        patroni.ha.dcs.delete_leader()
