#!/usr/bin/env python

import logging
import os
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


# handle SIGCHILD, since we are the equivalent of the INIT process
def sigchld_handler(signo, stack_frame):
    try:
        while True:
            ret = os.waitpid(-1, os.WNOHANG)
            if ret == (0, 0):
                break
    except OSError:
        pass


class Governor:

    def __init__(self, config):
        self.nap_time = config['loop_wait']
        self.etcd = Etcd(config['etcd'])
        self.postgresql = Postgresql(config['postgresql'])
        self.ha = Ha(self.postgresql, self.etcd)

    def touch_member(self):
        return self.etcd.touch_member(self.postgresql.name, self.postgresql.connection_string)

    def initialize(self):
        # wait for etcd to be available
        while not self.touch_member():
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
                self.postgresql.create_connection_users()
            else:
                while True:
                    leader = self.etcd.current_leader()
                    if leader and self.postgresql.sync_from_leader(leader):
                        self.postgresql.write_recovery_conf(leader)
                        self.postgresql.start()
                        break
                    time.sleep(5)
        elif self.postgresql.is_running():
            self.postgresql.load_replication_slots()

    def run(self):
        while True:
            self.touch_member()
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
    http_server = getHTTPServer(governor.postgresql, http_port=config.get(
        'healtcheck_port', 8008), listen_address='0.0.0.0')
    http_thread = threading.Thread(target=http_server.serve_forever, args=())
    http_thread.daemon = True

    governor.initialize()
    http_thread.start()

    try:
        governor.run()
    finally:
        governor.postgresql.stop()
        governor.etcd.delete_member(governor.postgresql.name)
        governor.etcd.delete_leader(governor.postgresql.name)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)
    main()
