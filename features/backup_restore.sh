#!/bin/bash

set -x

while getopts ":-:" optchar; do
    [[ "${optchar}" == "-" ]] || continue
    case "${OPTARG}" in
        datadir=* )
            PGDATA=${OPTARG#*=}
            ;;
        sourcedir=* )
            SOURCE=${OPTARG#*=}
            ;;
    esac
done

[[ -z $PGDATA || -z $SOURCE ]] && exit 1

mkdir -p $(dirname $PGDATA)

exec cp -af $SOURCE $PGDATA
