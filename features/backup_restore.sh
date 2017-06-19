#!/bin/bash

PARSED=$(getopt --options v --longoptions datadir:,sourcedir: --name "$0" -- "$@" 2> /dev/null)
eval set -- "$PARSED"

while [[ $# -gt 0 ]]; do
    case $1 in
        --datadir )
            PGDATA=$2
            shift
            ;;
        --sourcedir )
            SOURCE=$2
            shift
            ;;
        * )
            ;;
    esac
    shift
done

[[ -z $PGDATA || -z $SOURCE ]] && exit 1

mkdir -p $(dirname $PGDATA)

exec cp -af $SOURCE $PGDATA
