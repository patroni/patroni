#!/bin/bash

function usage()
{
    cat <<__EOF__
Usage: $0

Options:

    --etcd      Do not run Patroni, run a standalone etcd
    --confd     Do not run Patroni, run a standalone confd
    --zookeeper Do not run Patroni, run a standalone zookeeper

Examples:

    $0 --etcd
    $0 --confd
    $0 --zookeeper
    $0
__EOF__
}

DOCKER_IP=$(hostname --ip-address)
PATRONI_SCOPE=${PATRONI_SCOPE:-batman}
ETCD_ARGS="--data-dir /tmp/etcd.data -advertise-client-urls=http://${DOCKER_IP}:2379 -listen-client-urls=http://0.0.0.0:2379"

optspec=":vh-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                confd)
                    haproxy -f /etc/haproxy/haproxy.cfg -p /var/run/haproxy.pid -D
                    CONFD="confd -prefix=${PATRONI_NAMESPACE:-/service}/$PATRONI_SCOPE -interval=10 -backend"
                    if [ ! -z ${PATRONI_ZOOKEEPER_HOSTS} ]; then
                        while ! /usr/share/zookeeper/bin/zkCli.sh -server ${PATRONI_ZOOKEEPER_HOSTS} ls /; do
                            sleep 1
                        done
                        exec $CONFD zookeeper -node ${PATRONI_ZOOKEEPER_HOSTS}
                    else
                        while ! curl -s ${PATRONI_ETCD_URL}/v2/members | jq -r '.members[0].clientURLs[0]' | grep -q http; do
                            sleep 1
                        done
                        exec $CONFD etcd -node $PATRONI_ETCD_URL
                    fi
                    ;;
                etcd)
                    exec etcd $ETCD_ARGS
                    ;;
                zookeeper)
                    exec /usr/share/zookeeper/bin/zkServer.sh start-foreground
                    ;;
                cheat)
                    CHEAT=1
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
if [[ -z ${PATRONI_ETCD_URL} && -z ${PATRONI_ZOOKEEPER_HOSTS} ]]; then
    etcd $ETCD_ARGS > /var/log/etcd.log 2> /var/log/etcd.err &
    export PATRONI_ETCD_URL="http://127.0.0.1:2379"
fi

export PATRONI_SCOPE
export PATRONI_NAME="${PATRONI_NAME:-${HOSTNAME}}"
export PATRONI_RESTAPI_CONNECT_ADDRESS="${DOCKER_IP}:8008"
export PATRONI_RESTAPI_LISTEN="0.0.0.0:8008"
export PATRONI_admin_PASSWORD="${PATRONI_admin_PASSWORD:=admin}"
export PATRONI_admin_OPTIONS="${PATRONI_admin_OPTIONS:-createdb, createrole}"
export PATRONI_POSTGRESQL_CONNECT_ADDRESS="${DOCKER_IP}:5432"
export PATRONI_POSTGRESQL_LISTEN="0.0.0.0:5432"
export PATRONI_POSTGRESQL_DATA_DIR="data/${PATRONI_SCOPE}"
export PATRONI_REPLICATION_USERNAME="${PATRONI_REPLICATION_USERNAME:-replicator}"
export PATRONI_REPLICATION_PASSWORD="${PATRONI_REPLICATION_PASSWORD:-abcd}"
export PATRONI_SUPERUSER_USERNAME="${PATRONI_SUPERUSER_USERNAME:-postgres}"
export PATRONI_SUPERUSER_PASSWORD="${PATRONI_SUPERUSER_PASSWORD:-postgres}"
export PATRONI_POSTGRESQL_PGPASS="$HOME/.pgpass"

cat > /patroni.yml <<__EOF__
bootstrap:
  dcs:
    postgresql:
      use_pg_rewind: true

  pg_hba:
  - host all all 0.0.0.0/0 md5
  - host replication replicator ${DOCKER_IP}/16    md5
__EOF__

mkdir -p "$HOME/.config/patroni"
[ -h "$HOME/.config/patroni/patronictl.yaml" ] || ln -s /patroni.yml "$HOME/.config/patroni/patronictl.yaml"

[ -z $CHEAT ] && exec python3 /patroni.py /patroni.yml

while true; do
    sleep 60
done
