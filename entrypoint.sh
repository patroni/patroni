#!/bin/bash
etcd --data-dir /tmp/etcd.data > /var/log/etcd.log 2> /var/log/etcd.err &
exec /patroni/patroni.py /patroni/postgres0.yml "$@"
