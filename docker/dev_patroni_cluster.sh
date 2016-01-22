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

function random_name()
{
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | head -c 8
}

if [ -z ${PATRONI_SCOPE} ]
then
    PATRONI_SCOPE=$(random_name)
fi

etcd_container=$(docker run -P -d --name="${PATRONI_SCOPE}_etcd" "${DOCKER_IMAGE}" --etcd-only)
etcd_container_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${etcd_container})
echo "The etcd container is ${etcd_container}, ip=${etcd_container_ip}"

for i in $(seq 1 "${MEMBERS}")
do
    container_name=$(random_name)
    patroni_container=$(docker run -P -d --name="${PATRONI_SCOPE}_${container_name}" "${DOCKER_IMAGE}" --etcd="${etcd_container_ip}:4001" --name="${PATRONI_SCOPE}")
    patroni_container_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${patroni_container})
    echo "Started Patroni container ${patroni_container}, ip=${patroni_container_ip}"
done
