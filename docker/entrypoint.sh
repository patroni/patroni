#!/bin/sh

if [ -f /a.tar.xz ]; then
    echo "decompressing image..."
    sudo tar xpJf /a.tar.xz -C / > /dev/null 2>&1
    sudo rm /a.tar.xz
    sudo ln -snf dash /bin/sh
fi

readonly PATRONI_SCOPE="${PATRONI_SCOPE:-batman}"
PATRONI_NAMESPACE="${PATRONI_NAMESPACE:-/service}"
readonly PATRONI_NAMESPACE="${PATRONI_NAMESPACE%/}"
DOCKER_IP=$(hostname --ip-address)
readonly DOCKER_IP

export DUMB_INIT_SETSID=0

case "$1" in
    haproxy)
        haproxy -f /etc/haproxy/haproxy.cfg -p /var/run/haproxy.pid -D
        set -- confd "-prefix=$PATRONI_NAMESPACE/$PATRONI_SCOPE" -interval=10 -backend
        if [ -n "$PATRONI_ZOOKEEPER_HOSTS" ]; then
            while ! /usr/share/zookeeper/bin/zkCli.sh -server "$PATRONI_ZOOKEEPER_HOSTS" ls /; do
                sleep 1
            done
            set -- "$@" zookeeper -node "$PATRONI_ZOOKEEPER_HOSTS"
        else
            while ! etcdctl member list 2> /dev/null; do
                sleep 1
            done
            set -- "$@" etcdv3
            while IFS='' read -r line; do
                set -- "$@" -node "$line"
            done <<-EOT
$(echo "$ETCDCTL_ENDPOINTS" | sed 's/,/\n/g')
EOT
        fi
        exec dumb-init "$@"
        ;;
    etcd)
        exec "$@" --auto-compaction-retention=1 -advertise-client-urls "http://$DOCKER_IP:2379"
        ;;
    zookeeper)
        exec /usr/share/zookeeper/bin/zkServer.sh start-foreground
        ;;
esac

## We start an etcd
if [ -z "$PATRONI_ETCD3_HOSTS" ] && [ -z "$PATRONI_ZOOKEEPER_HOSTS" ]; then
    export PATRONI_ETCD_URL="http://127.0.0.1:2379"
    etcd --data-dir /tmp/etcd.data -advertise-client-urls=$PATRONI_ETCD_URL -listen-client-urls=http://0.0.0.0:2379 > /var/log/etcd.log 2> /var/log/etcd.err &
fi

export PATRONI_SCOPE
export PATRONI_NAMESPACE
export PATRONI_NAME="${PATRONI_NAME:-$(hostname)}"
export PATRONI_RESTAPI_CONNECT_ADDRESS="$DOCKER_IP:8008"
export PATRONI_RESTAPI_LISTEN="0.0.0.0:8008"
export PATRONI_admin_PASSWORD="${PATRONI_admin_PASSWORD:-admin}"
export PATRONI_admin_OPTIONS="${PATRONI_admin_OPTIONS:-createdb, createrole}"
export PATRONI_POSTGRESQL_CONNECT_ADDRESS="$DOCKER_IP:5432"
export PATRONI_POSTGRESQL_LISTEN="0.0.0.0:5432"
export PATRONI_POSTGRESQL_DATA_DIR="${PATRONI_POSTGRESQL_DATA_DIR:-$PGDATA}"
export PATRONI_REPLICATION_USERNAME="${PATRONI_REPLICATION_USERNAME:-replicator}"
export PATRONI_REPLICATION_PASSWORD="${PATRONI_REPLICATION_PASSWORD:-replicate}"
export PATRONI_SUPERUSER_USERNAME="${PATRONI_SUPERUSER_USERNAME:-postgres}"
export PATRONI_SUPERUSER_PASSWORD="${PATRONI_SUPERUSER_PASSWORD:-postgres}"
export PATRONI_REPLICATION_SSLMODE="${PATRONI_REPLICATION_SSLMODE:-$PGSSLMODE}"
export PATRONI_REPLICATION_SSLKEY="${PATRONI_REPLICATION_SSLKEY:-$PGSSLKEY}"
export PATRONI_REPLICATION_SSLCERT="${PATRONI_REPLICATION_SSLCERT:-$PGSSLCERT}"
export PATRONI_REPLICATION_SSLROOTCERT="${PATRONI_REPLICATION_SSLROOTCERT:-$PGSSLROOTCERT}"
export PATRONI_SUPERUSER_SSLMODE="${PATRONI_SUPERUSER_SSLMODE:-$PGSSLMODE}"
export PATRONI_SUPERUSER_SSLKEY="${PATRONI_SUPERUSER_SSLKEY:-$PGSSLKEY}"
export PATRONI_SUPERUSER_SSLCERT="${PATRONI_SUPERUSER_SSLCERT:-$PGSSLCERT}"
export PATRONI_SUPERUSER_SSLROOTCERT="${PATRONI_SUPERUSER_SSLROOTCERT:-$PGSSLROOTCERT}"

exec dumb-init python3 /patroni.py postgres0.yml
