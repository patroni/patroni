#!/bin/bash

cat > /home/postgres/patroni.yml <<__EOF__
bootstrap:
  dcs:
    postgresql:
      use_pg_rewind: true
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

exec /usr/bin/python /usr/local/bin/patroni /home/postgres/patroni.yml
