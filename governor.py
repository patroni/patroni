#!/usr/bin/env python

import atexit
import logging
import sys
import time
import yaml

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"])
ha = Ha(postgresql, etcd)

# stop postgresql on script exit


def stop_postgresql():
    postgresql.stop()
atexit.register(stop_postgresql)

# wait for etcd to be available
while not etcd.touch_member(postgresql.name, postgresql.connection_string):
    logging.info("waiting on etcd")
    time.sleep(5)

# is data directory empty?
if postgresql.data_directory_empty():
    # racing to initialize
    if etcd.race("/initialize", postgresql.name):
        postgresql.initialize()
        etcd.take_leader(postgresql.name)
        postgresql.start()
        postgresql.create_replication_user()
    else:
        synced_from_leader = False
        while not synced_from_leader:
            leader = etcd.current_leader()
            if not leader:
                time.sleep(5)
                continue
            if postgresql.sync_from_leader(leader):
                postgresql.write_recovery_conf(leader)
                postgresql.start()
                synced_from_leader = True
            else:
                time.sleep(5)

while True:
    logging.info(ha.run_cycle())
    time.sleep(config["loop_wait"])
