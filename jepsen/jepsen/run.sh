#!/bin/bash

set -ex

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

for n in {1..3}; do
    ssh-keyscan -t rsa "patroni$n" >> /root/.ssh/known_hosts
    ssh-keyscan -t rsa "etcd$n" >> /root/.ssh/known_hosts
done

wait_timeout=60
for (( n=1; n <= wait_timeout; n++ )); do
    leader=$(ssh patroni1 "patronictl list -f json | jq -r '.[] | select(.Role==\"Leader\") | .Member'")
    [ -n "$leader" ] && break
    [ "$n" -eq $wait_timeout ] && exit 1
    sleep 1
done

ssh "$leader" "psql -U postgres -c 'CREATE TABLE IF NOT EXISTS set (value integer primary key)'"

for member in $(ssh "$leader" "patronictl list -f json | jq -r '.[] | select(.Role!=\"Leader\") | .Member'"); do
    for (( n=1; n <= wait_timeout; n++ )); do
        ssh "$member" "psql -U postgres -tAc 'SELECT * FROM set'" && break
        [ "$n" -eq $wait_timeout ] && exit 1
        sleep 1
    done
done

timeout 10800 lein test
