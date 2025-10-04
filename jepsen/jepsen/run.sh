#!/bin/bash

set -ex

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

for i in {1..3}; do
    ssh-keyscan -t rsa patroni$i >> /root/.ssh/known_hosts
    ssh-keyscan -t rsa etcd$i >> /root/.ssh/known_hosts
done

while true; do
    leader=$(ssh patroni1 "patronictl list -f json | jq -r '.[] | select(.Role==\"Leader\") | .Member'")
    [ -n "$leader" ] && break
    sleep 1
done

ssh "$leader" "psql -U postgres -c 'CREATE TABLE IF NOT EXISTS set (value integer primary key)'"

for member in $(ssh "$leader" "patronictl list -f json | jq -r '.[] | select(.Role!=\"Leader\") | .Member'"); do
    while ! ssh "$member" "psql -U postgres -tAc 'SELECT * FROM set'"; do
        sleep 1
    done
done

lein test
