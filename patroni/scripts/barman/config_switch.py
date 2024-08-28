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
import logging
import time

from argparse import Namespace
from enum import IntEnum
from typing import Optional, TYPE_CHECKING

from .utils import OperationStatus, RetriesExceeded

if TYPE_CHECKING:  # pragma: no cover
    from .utils import PgBackupApi


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar CONFIG_SWITCH_DONE: config switch was successfully performed.
    :cvar CONFIG_SWITCH_SKIPPED: if the execution was skipped because of not
        matching user expectations.
    :cvar CONFIG_SWITCH_FAILED: config switch faced an issue.
    :cvar HTTP_ERROR: an error has occurred while communicating with
        ``pg-backup-api``
    :cvar INVALID_ARGS: an invalid set of arguments has been given to the
        operation.
    """

    CONFIG_SWITCH_DONE = 0
    CONFIG_SWITCH_SKIPPED = 1
    CONFIG_SWITCH_FAILED = 2
    HTTP_ERROR = 3
    INVALID_ARGS = 4


def _should_skip_switch(args: Namespace) -> bool:
    """Check if we should skip the config switch operation.

    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.

    :returns: if the operation should be skipped.
    """
    if args.switch_when == "promoted":
        return args.role not in {"primary", "promoted"}
    if args.switch_when == "demoted":
        return args.role not in {"replica", "demoted"}
    return False


def _switch_config(api: "PgBackupApi", barman_server: str,
                   barman_model: Optional[str], reset: Optional[bool]) -> int:
    """Switch configuration of Barman server through ``pg-backup-api``.

    .. note::
        If requests to ``pg-backup-api`` fail recurrently or we face HTTP
        errors, then exit with :attr:`ExitCode.HTTP_ERROR`.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param barman_server: name of the Barman server which config is to be
        switched.
    :param barman_model: name of the Barman model to be applied to the server,
        if any.
    :param reset: ``True`` if you would like to unapply the currently active
        model for the server, if any.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    operation_id = None

    try:
        operation_id = api.create_config_switch_operation(
            barman_server,
            barman_model,
            reset,
        )
    except RetriesExceeded as exc:
        logging.error("An issue was faced while trying to create a config "
                      "switch operation: %r", exc)
        return ExitCode.HTTP_ERROR

    logging.info("Created the config switch operation with ID %s",
                 operation_id)

    status = None

    while True:
        try:
            status = api.get_operation_status(barman_server, operation_id)
        except RetriesExceeded:
            logging.error("Maximum number of retries exceeded, exiting.")
            return ExitCode.HTTP_ERROR

        if status != OperationStatus.IN_PROGRESS:
            break

        logging.info("Config switch operation %s is still in progress",
                     operation_id)
        time.sleep(5)

    if status == OperationStatus.DONE:
        logging.info("Config switch operation finished successfully.")
        return ExitCode.CONFIG_SWITCH_DONE
    else:
        logging.error("Config switch operation failed.")
        return ExitCode.CONFIG_SWITCH_FAILED


def run_barman_config_switch(api: "PgBackupApi", args: Namespace) -> int:
    """Run a remote ``barman config-switch`` through the ``pg-backup-api``.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    if _should_skip_switch(args):
        logging.info("Config switch operation was skipped (role=%s, "
                     "switch_when=%s).", args.role, args.switch_when)
        return ExitCode.CONFIG_SWITCH_SKIPPED

    if not bool(args.barman_model) ^ bool(args.reset):
        logging.error("One, and only one among 'barman_model' ('%s') and "
                      "'reset' ('%s') should be given", args.barman_model, args.reset)
        return ExitCode.INVALID_ARGS

    return _switch_config(api, args.barman_server, args.barman_model, args.reset)
