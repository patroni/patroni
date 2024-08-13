#!/usr/bin/env python

"""Implements ``patroni_barman recover`` sub-command.

Restore a Barman backup to the local node through ``pg-backup-api``.

This sub-command can be used both as a custom bootstrap method, and as a custom
create replica method. Check the output of ``--help`` to understand the
parameters supported by the sub-command. ``--datadir`` is a special parameter
and it is automatically filled by Patroni in both cases.

It requires that you have previously configured a Barman server, and that you
have ``pg-backup-api`` configured and running in the same host as Barman.

Refer to :class:`ExitCode` for possible exit codes of this sub-command.
"""
import logging
import time

from argparse import Namespace
from enum import IntEnum
from typing import TYPE_CHECKING

from .utils import OperationStatus, RetriesExceeded

if TYPE_CHECKING:  # pragma: no cover
    from .utils import PgBackupApi


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar RECOVERY_DONE: backup was successfully restored.
    :cvar RECOVERY_FAILED: recovery of the backup faced an issue.
    :cvar HTTP_ERROR: an error has occurred while communicating with
        ``pg-backup-api``
    """

    RECOVERY_DONE = 0
    RECOVERY_FAILED = 1
    HTTP_ERROR = 2


def _restore_backup(api: "PgBackupApi", barman_server: str, backup_id: str,
                    ssh_command: str, data_directory: str,
                    loop_wait: int) -> int:
    """Restore the configured Barman backup through ``pg-backup-api``.

    .. note::
        If requests to ``pg-backup-api`` fail recurrently or we face HTTP
        errors, then exit with :attr:`ExitCode.HTTP_ERROR`.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param barman_server: name of the Barman server which backup is to be
        restored.
    :param backup_id: ID of the backup from the Barman server.
    :param ssh_command: SSH command to connect from the Barman host to the
        target host.
    :param data_directory: path to the Postgres data directory where to restore
        the backup in.
    :param loop_wait: how long in seconds to wait before checking again the
        status of the recovery process. Higher values are useful for backups
        that are expected to take longer to restore.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    operation_id = None

    try:
        operation_id = api.create_recovery_operation(
            barman_server,
            backup_id,
            ssh_command,
            data_directory,
        )
    except RetriesExceeded as exc:
        logging.error("An issue was faced while trying to create a recovery "
                      "operation: %r", exc)
        return ExitCode.HTTP_ERROR

    logging.info("Created the recovery operation with ID %s", operation_id)

    status = None

    while True:
        try:
            status = api.get_operation_status(barman_server, operation_id)
        except RetriesExceeded:
            logging.error("Maximum number of retries exceeded, exiting.")
            return ExitCode.HTTP_ERROR

        if status != OperationStatus.IN_PROGRESS:
            break

        logging.info("Recovery operation %s is still in progress",
                     operation_id)
        time.sleep(loop_wait)

    if status == OperationStatus.DONE:
        logging.info("Recovery operation finished successfully.")
        return ExitCode.RECOVERY_DONE
    else:
        logging.error("Recovery operation failed.")
        return ExitCode.RECOVERY_FAILED


def run_barman_recover(api: "PgBackupApi", args: Namespace) -> int:
    """Run a remote ``barman recover`` through the ``pg-backup-api``.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param args: arguments received from the command-line of
        ``patroni_barman recover`` command.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    return _restore_backup(api, args.barman_server, args.backup_id,
                           args.ssh_command, args.data_directory,
                           args.loop_wait)
