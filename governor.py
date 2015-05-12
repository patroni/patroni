#!/usr/bin/env python

import logging
import os
import requests
import signal
import sys
import threading
import time
import yaml

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha
from helpers.statuspage import getHTTPServer


def sigterm_handler(signo, stack_frame):
    sys.exit()


class Governor:

    INSTANCE_METADATA_URL = "http://169.254.169.254/latest/meta-data/"

    def __init__(self, config):
        self.nap_time = config['loop_wait']
        self.etcd = Etcd(config['etcd'])
        aws_host_address = None
        if config.get('aws_use_host_address', False):
            # get host address of the AWS host via a call to
            # http://169.254.169.254/latest/meta-data/local-ipv4
            try:
                response = requests.get(Governor.INSTANCE_METADATA_URL + '/local-ipv4')
                if response.status_code == 200:
                    aws_host_address = response.content
            except:
                logging.exception('Error retrieiving IPv4 address from AWS instance')

        self.postgresql = Postgresql(config['postgresql'], aws_host_address)
        self.ha = Ha(self.postgresql, self.etcd)

    def initialize(self):
        # wait for etcd to be available
        while not self.etcd.touch_member(self.postgresql.name, self.postgresql.connection_string):
            logging.info('waiting on etcd')
            time.sleep(5)

        # is data directory empty?
        if self.postgresql.data_directory_empty():
            # racing to initialize
            if self.etcd.race('/initialize', self.postgresql.name):
                self.postgresql.initialize()
                self.etcd.take_leader(self.postgresql.name)
                self.postgresql.start()
                self.postgresql.create_replication_user()
            else:
                while True:
                    leader = self.etcd.current_leader()
                    if leader and self.postgresql.sync_from_leader(leader):
                        self.postgresql.write_recovery_conf(leader)
                        self.postgresql.start()
                        break
                    time.sleep(5)

    def run(self):
        while True:
            logging.info(self.ha.run_cycle())
            time.sleep(self.nap_time)


def main():
    if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
        print('Usage: {} config.yml'.format(sys.argv[0]))
        return

    with open(sys.argv[1], 'r') as f:
        config = yaml.load(f)

    governor = Governor(config)

    # Start the http_server to serve a simple healthcheck
    http_server = getHTTPServer(governor.postgresql, http_port=config.get('healtcheck_port', 8080), listen_address='0.0.0.0')
    http_thread = threading.Thread(target=http_server.serve_forever, args=())
    http_thread.daemon = True

    governor.initialize()
    http_thread.start()

    try:
        governor.run()
    finally:
        governor.postgresql.stop()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    signal.signal(signal.SIGTERM, sigterm_handler)
    main()
