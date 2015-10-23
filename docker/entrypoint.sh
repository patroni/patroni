#!/bin/bash

function usage()
{
    cat <<__EOF__
Usage: $0

Options:

    --etcd      ETCD    Provide an external etcd to connect to
    --name      NAME    Give the cluster a specific name
    --etcd-only         Do not run Patroni, run a standalone etcd

Examples:

    $0 --etcd=127.17.0.84:4001
    $0 --etcd-only
    $0
    $0 --name=true_scotsman
__EOF__
}

DOCKER_IP=$(hostname --ip-address)
PATRONI_SCOPE=${PATRONI_SCOPE:-batman}

optspec=":vh-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                etcd-only)
                    exec etcd --data-dir /tmp/etcd.data \
                        -advertise-client-urls=http://${DOCKER_IP}:4001 \
                        -listen-client-urls=http://0.0.0.0:4001 \
                        -listen-peer-urls=http://0.0.0.0:2380
                    exit 0
                    ;;
                cheat)
                    CHEAT=1
                    ;;
                name)
                    PATRONI_SCOPE="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                name=*)
                    PATRONI_SCOPE=${OPTARG#*=}
                    ;;
                etcd)
                    ETCD_CLUSTER="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                etcd=*)
                    ETCD_CLUSTER=${OPTARG#*=}
                    ;;
                help)
                    usage
                    exit 0
                    ;;
                *)
                    if [ "$OPTERR" = 1 ] && [ "${optspec:0:1}" != ":" ]; then
                        echo "Unknown option --${OPTARG}" >&2
                    fi
                    ;;
            esac;;
        *)
            if [ "$OPTERR" != 1 ] || [ "${optspec:0:1}" = ":" ]; then
                echo "Non-option argument: '-${OPTARG}'" >&2
                usage
                exit 1
            fi
            ;;
    esac
done

if [ -z ${ETCD_CLUSTER} ]
then
    etcd --data-dir /tmp/etcd.data \
        -advertise-client-urls=http://${DOCKER_IP}:4001 \
        -listen-client-urls=http://0.0.0.0:4001 \
        -listen-peer-urls=http://0.0.0.0:2380 > /var/log/etcd.log 2> /var/log/etcd.err &
    ETCD_CLUSTER="127.0.0.1:4001"
fi

cat > /patroni/postgres.yml <<__EOF__

ttl: &ttl 30
loop_wait: &loop_wait 10
scope: &scope '${PATRONI_SCOPE}'
namespace: 'patroni'
restapi:
  listen: 0.0.0.0:8008
  connect_address: ${DOCKER_IP}:8008
etcd:
  scope: *scope
  ttl: *ttl
  host: ${ETCD_CLUSTER}
postgresql:
  name: postgresql_${DOCKER_IP//./_} ## Replication slots do not allow dots in their name
  scope: *scope
  listen: 0.0.0.0:5432
  connect_address: ${DOCKER_IP}:5432
  data_dir: data/postgresql0
  maximum_lag_on_failover: 1048576 # 1 megabyte in bytes
  pg_hba:
  - host all all 0.0.0.0/0 md5
  - hostssl all all 0.0.0.0/0 md5
  - host replication replicator ${DOCKER_IP}/16    md5
  replication:
    username: replicator
    password: rep-pass
    network:  127.0.0.1/32
  superuser:
    password: zalando
  restore: patroni/scripts/restore.py
  admin:
    username: admin
    password: admin
  parameters:
    archive_mode: "on"
    wal_level: hot_standby
    archive_command: mkdir -p ../wal_archive && cp %p ../wal_archive/%f
    max_wal_senders: 20
    listen_addresses: 0.0.0.0
    wal_keep_segments: 8
    archive_timeout: 1800s
    max_replication_slots: 20
    hot_standby: "on"
__EOF__

cat /patroni/postgres.yml

if [ ! -z $CHEAT ]
then
    while :
    do
        sleep 60
    done
else
    exec python /patroni.py /patroni/postgres.yml
fi
