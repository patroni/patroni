#!/bin/bash

PARSED=$(getopt --options v --longoptions datadir:,dbname:,walmethod: --name "$0" -- "$@" 2> /dev/null)
eval set -- "$PARSED"

while [[ $# -gt 0 ]]; do
    case $1 in
        --datadir )
            PGDATA=$2
            shift
            ;;
        --dbname )
            DBNAME=$2
            shift
            ;;
        --walmethod )
            WALMETHOD=$2
            shift
            ;;
        * )
            ;;
    esac
    shift
done

[[ -z $PGDATA || -z $DBNAME || -z $WALMETHOD ]] && exit 1

[[ $WALMETHOD != "none" ]] && WALMETHOD="-X $WALMETHOD" || WALMETHOD=""

exec pg_basebackup -D $PGDATA $WALMETHOD -c fast -d $DBNAME
