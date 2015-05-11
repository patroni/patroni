#!/usr/bin/env python

import atexit
import logging
import sys
import time
import yaml

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha

INSTANCE_METADATA_URL = "http://169.254.169.254/latest/meta-data/"

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

if config.get('aws_use_host_address', False):
    # get host address of the AWS host via a call to
    # http://169.254.169.254/latest/meta-data/local-ipv4
    try:
        aws_host_address = urllib2.urlopen(INSTANCE_METADATA_URL+"/local-ipv4").read()
    except (urllib2.HTTPError, urllib2.URLError) as e:
        logging.error("Error retrieiving IPv4 address from AWS instance: {0}".format(e))
        aws_host_address = None
else:
    aws_host_address = None

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"], aws_host_address)
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
        postgresql.create_connection_users()
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
