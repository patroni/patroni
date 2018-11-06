#!/bin/bash
for i in "$@"
do
case $i in
    --scope=*)
    SCOPE="${i#*=}"
    ;;
    --datadir=*)
    DATADIR="${i#*=}"
    ;;
# Patroni switch not needed by pgbackrest
#    --role=*)
#    ROLE="${i#*=}"
#    ;;
#    --connstring=*)
#    CONNSTRING="${i#*=}"
#    ;;
    *)
            # unknown option
    ;;
esac
done

mkdir -p ${DATADIR}
chmod 0700 ${DATADIR}

/usr/bin/pgbackrest --stanza=${SCOPE} --db-path=${DATADIR} --delta restore
