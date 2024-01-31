#!/usr/bin/env python

"""Implements ``patroni_barman config-switch`` sub-command.

Apply a Barman configuration model through ``pg-backup-api``.

This sub-command is specially useful as a ``on_role_change`` callback to change
Barman configuration in response to failovers and switchovers. Check the output
of ``--help`` to understand the parameters supported by the sub-command.

It requires that you have previously configured a Barman server and Barman
config models, and that you have ``pg-backup-api`` configured and running in
the same host as Barman.

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

    :cvar CONFIG_SWITCH_DONE: config switch was successfully performed.
    :cvar CONFIG_SWITCH_FAILED: config switch faced an issue.
    :cvar API_NOT_OK: ``pg-backup-api`` status is not ``OK``.
    :cvar HTTP_ERROR: an error has occurred while communicating with
        ``pg-backup-api``
    :cvar INVALID_ARGS: an invalid set of arguments has been given to the
        operation.
    """

    CONFIG_SWITCH_DONE = 0
    CONFIG_SWITCH_SKIPPED = 1
    CONFIG_SWITCH_FAILED = 2
    API_NOT_OK = 3
    HTTP_ERROR = 4
    INVALID_ARGS = 5


class BarmanConfigSwitch:
    """Facilities for performing a remote ``barman config-switch`` operation.

    You should instantiate this class, which will take care of configuring the
    operation accordingly. When you want to start the operation, you should
    call :meth:`switch_config`. At any point of interaction with this class,
    you may face a :func:`sys.exit` call. Refer to :class:`ExitCode` for a view
    on the possible exit codes.

    :ivar barman_server: name of the Barman server which config is to be
        switched.
    :ivar barman_model: name of the Barman model to be applied to the server, if
        any.
    :ivar reset: ``True`` if you would like to unapply the currently active
        model for the server, if any.
    """

    def __init__(self, barman_server: str, barman_model: Optional[str],
                 reset: Optional[bool], api_url: str, cert_file: Optional[str],
                 key_file: Optional[str], retry_wait: int,
                 max_retries: int) -> None:
        """Create a new instance of :class:`BarmanConfigSwitch`.

        :param barman_server: name of the Barman server which config is to be
            switched.
        :param barman_model: name of the Barman model to be applied to the
            server, if any.
        :param reset: ``True`` if you would like to unapply the currently active
            model for the server, if any.
        :param api_url: base URL to reach the ``pg-backup-api``.
        :param cert_file: certificate to authenticate against the
            ``pg-backup-api``, if required.
        :param key_file: certificate key to authenticate against the
            ``pg-backup-api``, if required.
        :param retry_wait: how long in seconds to wait before retrying a failed
            request to the ``pg-backup-api``.
        :param max_retries: maximum number of retries when ``pg-backup-api``
            returns malformed responses.
        """
        self.barman_server = barman_server
        self.barman_model = barman_model
        self.reset = reset

        if all([barman_model, reset]) or not any([barman_model, reset]):
            logging.error("One, and only one among ' barman-model' ('%s') and "
                          "'reset' ('%s') should be given", barman_model, reset)
            sys.exit(ExitCode.INVALID_ARGS)

        try:
            self._api = PgBackupApi(api_url, cert_file, key_file, retry_wait,
                                    max_retries)
        except ApiNotOk as exc:
            logging.error("pg-backup-api is not working: %r", exc)
            sys.exit(ExitCode.API_NOT_OK)

    def switch_config(self) -> bool:
        """Switch configuration of Barman server through ``pg-backup-api``.

        .. note::
            If requests to ``pg-backup-api`` fail recurrently or we face HTTP
            errors, then exit with :attr:`ExitCode.HTTP_ERROR`.

        :returns: ``True`` if config was successfully switched, ``False``
            otherwise.
        """
        operation_id = None

        try:
            operation_id = self._api.create_config_switch_operation(
                self.barman_server,
                self.barman_model,
                self.reset,
            )
        except RetriesExceeded as exc:
            logging.error("An issue was faced while trying to create a "
                          "config switch operation: %r", exc)
            sys.exit(ExitCode.HTTP_ERROR)

        logging.info("Created the config switch operation with ID %s",
                     operation_id)

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

            logging.info("Config switch operation %s is still in progress",
                         operation_id)
            time.sleep(5)

        return status == OperationStatus.DONE


def _should_skip_switch(args: Namespace) -> bool:
    """Check if we should skip the config switch operation.

    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.

    :returns: if the operation should be skipped.
    """
    ret = False

    if args.switch_when == "promoted" and args.role not in ["master",
                                                            "primary",
                                                            "promoted"]:
        ret = True
    elif args.switch_when == "demoted" and args.role not in ["replica",
                                                             "demoted"]:
        ret = True

    return ret


def run_barman_config_switch(args: Namespace) -> None:
    """Run a remote ``barman config-switch`` through the ``pg-backup-api``.

    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.
    """
    if _should_skip_switch(args):
        logging.info("Config switch operation was skipped (role=%s, "
                     "switch_when=%s).", args.role, args.switch_when)
        sys.exit(ExitCode.CONFIG_SWITCH_SKIPPED)

    barman_config_switch = BarmanConfigSwitch(args.barman_server,
                                              args.barman_model, args.reset,
                                              args.api_url, args.cert_file,
                                              args.key_file, args.retry_wait,
                                              args.max_retries)

    successful = barman_config_switch.switch_config()

    if successful:
        logging.info("Config switch operation finished successfully.")
        sys.exit(ExitCode.CONFIG_SWITCH_DONE)
    else:
        logging.error("Config switch operation failed.")
        sys.exit(ExitCode.CONFIG_SWITCH_FAILED)
