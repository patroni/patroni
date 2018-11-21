#!/bin/bash

# Dynamically add assigned UID/GID to /etc/passwd
export USER_ID=$(id -u)
export GROUP_ID=$(id -g)
if [ `id -u` -ge 10000 ]; then
    cat /etc/passwd | sed -e "s/^${USER_ID}:/builder:/" > /tmp/passwd
    echo "postgres:x:${USER_ID}:${GROUP_ID}:PostgreSQL Server:${HOME}:/bin/bash" >> /tmp/passwd
    cat /tmp/passwd > /etc/passwd
    rm /tmp/passwd
fi

cat > /home/postgres/patroni.yml <<__EOF__
bootstrap:
  dcs:
    postgresql:
      use_pg_rewind: true
  initdb:
  - auth-host: md5
  - auth-local: trust
  - encoding: UTF8
  - locale: en_US.UTF-8
  - data-checksums
  pg_hba:
  - host all all 0.0.0.0/0 md5
  - host replication ${PATRONI_REPLICATION_USERNAME} ${POD_IP}/16    md5
restapi:
  connect_address: '${POD_IP}:8008'
postgresql:
  connect_address: '${POD_IP}:5432'
  authentication:
    superuser:
      password: '${PATRONI_SUPERUSER_PASSWORD}'
    replication:
      password: '${PATRONI_REPLICATION_PASSWORD}'
  callbacks:
    on_start: /callback.py
    on_stop: /callback.py
    on_role_change: /callback.py
__EOF__

unset PATRONI_SUPERUSER_PASSWORD PATRONI_REPLICATION_PASSWORD
export KUBERNETES_NAMESPACE=$PATRONI_KUBERNETES_NAMESPACE
export POD_NAME=$PATRONI_NAME

exec /usr/bin/python3 /usr/local/bin/patroni /home/postgres/patroni.yml
