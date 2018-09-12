#!/bin/bash

DOCKER_IMAGE="registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT"
MEMBERS=3


function usage()
{
    cat <<__EOF__
Usage: $0 

Options:

    --image     IMAGE    The Docker image to use for the cluster
    --members   INT      The number of members for the cluster
    --name      NAME     The name of the new cluster

Examples:

    $0 --image ${DOCKER_IMAGE}
    $0
    $0 --image ${DOCKER_IMAGE} --members=2
__EOF__
}


optspec=":-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                help)
                    usage
                    exit 0
                    ;;
                name)
                    PATRONI_SCOPE="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                name=*)
                    PATRONI_SCOPE="${OPTARG#*=}"
                    ;;
                image)
                    DOCKER_IMAGE="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                image=*)
                    DOCKER_IMAGE="${OPTARG#*=}"
                    ;;
                members)
                    MEMBERS="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                members=*)
                    MEMBERS="${OPTARG#*=}"
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

if [ -z ${PATRONI_SCOPE} ]; then
    PATRONI_SCOPE=$(cat /dev/urandom | LC_ALL=C tr -dc 'a-z0-9' | head -c 8)
fi

function docker_run()
{
    local name=$1
    shift
    container=$(docker run -d --name=$name $*)
    container_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${container})
    echo "Started container ${name}, ip=${container_ip}"
}


ETCD_CONTAINER="${PATRONI_SCOPE}_etcd"
docker_run ${ETCD_CONTAINER} ${DOCKER_IMAGE} --etcd

DOCKER_ARGS="--link=${ETCD_CONTAINER}:${ETCD_CONTAINER} -e PATRONI_SCOPE=${PATRONI_SCOPE} -e PATRONI_ETCD_URL=http://${ETCD_CONTAINER}:2379"
PATRONI_ENV=$(sed 's/#.*//g' docker/patroni-secrets.env | sed -n 's/^PATRONI_.*$/-e &/p' | tr '\n' ' ')
PATRONI_VOLUME="-v $(dirname $(dirname $(realpath $0)))/patroni:/patroni"

for i in $(seq 1 "${MEMBERS}"); do
    container_name=postgres${i}
    docker_run "${PATRONI_SCOPE}_${container_name}" \
        $PATRONI_VOLUME \
        $DOCKER_ARGS \
        $PATRONI_ENV \
        -e PATRONI_NAME=${container_name} \
        ${DOCKER_IMAGE}
done

docker_run "${PATRONI_SCOPE}_haproxy" \
    -p=5000 -p=5001 \
    $DOCKER_ARGS \
    ${DOCKER_IMAGE} --confd
