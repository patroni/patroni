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
from argparse import Namespace
from enum import IntEnum
import logging
import sys
import time
from typing import Optional

from .utils import ApiNotOk, OperationStatus, PgBackupApi, RetriesExceeded


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar RECOVERY_DONE: backup was successfully restored.
    :cvar RECOVERY_FAILED: recovery of the backup faced an issue.
    :cvar API_NOT_OK: ``pg-backup-api`` status is not ``OK``.
    :cvar HTTP_ERROR: an error has occurred while communicating with
        ``pg-backup-api``
    """

    RECOVERY_DONE = 0
    RECOVERY_FAILED = 1
    API_NOT_OK = 2
    HTTP_ERROR = 3


class BarmanRecover:
    """Facilities for performing a remote ``barman recover`` operation.

    You should instantiate this class, which will take care of configuring the
    operation accordingly. When you want to start the operation, you should
    call :meth:`restore_backup`. At any point of interaction with this class,
    you may face a :func:`sys.exit` call. Refer to :class:`ExitCode` for a view
    on the possible exit codes.

    :ivar barman_server: name of the Barman server which backup is to be
        restored.
    :ivar backup_id: ID of the backup from the Barman server.
    :ivar ssh_command: SSH command to connect from the Barman host to the
        local host.
    :ivar data_directory: path to the Postgres data directory where to
        restore the backup at.
    :ivar loop_wait: how long to wait before checking again the status of the
        recovery process. Higher values are useful for backups that are
        expected to take long to restore.
    """

    def __init__(self, barman_server: str, backup_id: str,
                 ssh_command: str, data_directory: str, loop_wait: int,
                 api_url: str, cert_file: Optional[str],
                 key_file: Optional[str], retry_wait: int,
                 max_retries: int) -> None:
        """Create a new instance of :class:`BarmanRecover`.

        :param barman_server: name of the Barman server which backup is to be
            restored.
        :param backup_id: ID of the backup from the Barman server.
        :param ssh_command: SSH command to connect from the Barman host to the
            local host.
        :param data_directory: path to the Postgres data directory where to
            restore the backup in.
        :param loop_wait: how long to wait before checking again the status of
            the recovery process. Higher values are useful for backups that are
            expected to take longer to restore.
        :param api_url: base URL to reach the ``pg-backup-api``.
        :param cert_file: certificate to authenticate against the
            ``pg-backup-api``, if required.
        :param key_file: certificate key to authenticate against the
            ``pg-backup-api``, if required.
        :param retry_wait: how long in seconds to wait before retrying a failed request to
            the ``pg-backup-api``.
        :param max_retries: maximum number of retries when ``pg-backup-api``
            returns malformed responses.
        """
        self.barman_server = barman_server
        self.backup_id = backup_id
        self.ssh_command = ssh_command
        self.data_directory = data_directory
        self.loop_wait = loop_wait

        try:
            self._api = PgBackupApi(api_url, cert_file, key_file, retry_wait,
                                    max_retries)
        except ApiNotOk as exc:
            logging.error("pg-backup-api is not working: %r", exc)
            sys.exit(ExitCode.API_NOT_OK)

    def restore_backup(self) -> bool:
        """Restore the configured Barman backup through ``pg-backup-api``.

        .. note::
            If requests to ``pg-backup-api`` fail recurrently or we face HTTP
            errors, then exit with :attr:`ExitCode.HTTP_ERROR`.

        :returns: ``True`` if it was successfully recovered, ``False``
            otherwise.
        """
        operation_id = None

        try:
            operation_id = self._api.create_recovery_operation(
                self.barman_server, self.backup_id, self.ssh_command,
                self.data_directory,
            )
        except RetriesExceeded as exc:
            logging.error("An issue was faced while trying to create a "
                          "recovery operation: %r", exc)
            sys.exit(ExitCode.HTTP_ERROR)

        logging.info("Created the recovery operation with ID %s", operation_id)

        status = None

        while True:
            try:
                status = self._api.get_operation_status(self.barman_server,
                                                        operation_id)
            except RetriesExceeded:
                logging.error("Maximum number of retries exceeded, "
                              "exiting.")
                sys.exit(ExitCode.HTTP_ERROR)

            if status != OperationStatus.IN_PROGRESS:
                break

            logging.info("Recovery operation %s is still in progress",
                         operation_id)
            time.sleep(self.loop_wait)

        return status == OperationStatus.DONE


def run_barman_recover(args: Namespace) -> None:
    """Run a remote ``barman recover`` through the ``pg-backup-api``.

    :param args: arguments received from the command-line of
        ``patroni_barman recover`` command.
    """
    barman_recover = BarmanRecover(args.barman_server, args.backup_id,
                                   args.ssh_command, args.data_directory,
                                   args.loop_wait, args.api_url,
                                   args.cert_file, args.key_file,
                                   args.retry_wait, args.max_retries)

    successful = barman_recover.restore_backup()

    if successful:
        logging.info("Recovery operation finished successfully.")
        sys.exit(ExitCode.RECOVERY_DONE)
    else:
        logging.error("Recovery operation failed.")
        sys.exit(ExitCode.RECOVERY_FAILED)
