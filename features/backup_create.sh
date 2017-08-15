#!/bin/bash

while getopts ":-:" optchar; do
    [[ "${optchar}" == "-" ]] || continue
    case "${OPTARG}" in
        datadir=* )
            PGDATA=${OPTARG#*=}
            ;;
        dbname=* )
            DBNAME=${OPTARG#*=}
            ;;
        walmethod=* )
            WALMETHOD=${OPTARG#*=}
            ;;
    esac
done

[[ -z $PGDATA || -z $DBNAME || -z $WALMETHOD ]] && exit 1

[[ $WALMETHOD != "none" ]] && WALMETHOD="-X $WALMETHOD" || WALMETHOD=""

exec pg_basebackup -D $PGDATA $WALMETHOD -c fast -d $DBNAME
