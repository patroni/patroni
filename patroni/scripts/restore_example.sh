#!/bin/bash

optspec=":-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                help)
                    usage
                    exit 0
                    ;;
                scope)
                    PATRONI_SCOPE="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                scope=*)
                    PATRONI_SCOPE="${OPTARG#*=}"
                    ;;
                datadir)
                    PGDATA="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                datadir=*)
                    PGDATA="${OPTARG#*=}"
                    ;;
                backup_directory)
                    BACKUPDIR="${!OPTIND}"; OPTIND=$(( $OPTIND + 1 ))
                    ;;
                backup_directory=*)
                    BACKUPDIR="${OPTARG#*=}"
                    ;;
                *)
                    true
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

function usage()
{
    cat <<__EOT__
Usage: $0 --scope=SCOPE --datadir=DATADIR --backup_directory=BACKUPDIR
__EOT__
}

function die()
{
    echo "$1"
    exit 1
}

[[ -z ${PATRONI_SCOPE} ]] && usage && exit 1
[[ -z ${PGDATA} ]] && usage && exit 1
[[ -z ${BACKUPDIR} ]] && usage && exit 1

[[ -d "${PGDATA}" ]] && die "Will not overwrite existing PGDATA directory, stopping example restore"

LATESTBACKUP="$(ls -tr "${BACKUPDIR}/${PATRONI_SCOPE}/basebackup_"*.tar.gz | tail -n 1)"
[[ ! -f "${LATESTBACKUP}" ]] && die "Can not find the backup, stopping example restore"

mkdir -p "${PGDATA}"
tar xzfp "${LATESTBACKUP}" -C "${PGDATA}"
chmod 0700 "${PGDATA}"

echo "Restored ${LATESTBACKUP} into PGDATA=${PGDATA}"
