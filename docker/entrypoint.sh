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
                    (while sleep 1; do
                        confd -prefix=/service/$PATRONI_SCOPE -backend etcd -node 127.0.0.1:4001 \
                            -interval=10 >> /var/log/confd.log 2>> /var/log/confd.err
                    done) &
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

## We start an etcd
if [ -z ${ETCD_CLUSTER} ]
then
    etcd --data-dir /tmp/etcd.data \
        -advertise-client-urls=http://${DOCKER_IP}:4001 \
        -listen-client-urls=http://0.0.0.0:4001 \
        -listen-peer-urls=http://0.0.0.0:2380 > /var/log/etcd.log 2> /var/log/etcd.err &
    ETCD_CLUSTER="127.0.0.1:4001"
fi

export PATRONI_SCOPE
export PATRONI_NAME="${PATRONI_NAME:-${HOSTNAME}}"
export PATRONI_ETCD_HOST="$ETCD_CLUSTER"
export PATRONI_RESTAPI_CONNECT_ADDRESS="${DOCKER_IP}:8008"
export PATRONI_RESTAPI_LISTEN="0.0.0.0:8008"
export PATRONI_admin_PASSWORD="admin"
export PATRONI_admin_OPTIONS="createdb, createrole"
export PATRONI_POSTGRESQL_CONNECT_ADDRESS="${DOCKER_IP}:5432"
export PATRONI_POSTGRESQL_LISTEN="0.0.0.0:5432"
export PATRONI_POSTGRESQL_DATA_DIR="data/${PATRONI_SCOPE}"
export PATRONI_REPLICATION_USERNAME="replicator"
export PATRONI_REPLICATION_PASSWORD="abcd"
export PATRONI_SUPERUSER_USERNAME="postgres"
export PATRONI_SUPERUSER_PASSWORD="postgres"
export PATRONI_POSTGRESQL_PGPASS="$HOME/.pgpass"

cat > /patroni/postgres.yaml <<__EOF__
bootstrap:
  dcs:
    postgresql:
      use_pg_rewind: true

  pg_hba:
  - host all all 0.0.0.0/0 md5
  - host replication replicator ${DOCKER_IP}/16    md5
__EOF__

mkdir -p "$HOME/.config/patroni"
ln -s /patroni/postgres.yaml "$HOME/.config/patroni/patronictl.yaml"

if [ ! -z $CHEAT ]
then
    while :
    do
        sleep 60
    done
else
    exec python /patroni.py /patroni/postgres.yaml
fi
