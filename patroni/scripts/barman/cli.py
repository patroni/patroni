#!/usr/bin/env python

"""Perform operations on Barman through ``pg-backup-api``.

The actual operations are implemented by separate modules. This module only
builds the CLI that makes an interface with the actual commands.

.. note::
    See :class:ExitCode` for possible exit codes of this main script.
"""

import logging
import sys

from argparse import ArgumentParser
from enum import IntEnum

from .config_switch import run_barman_config_switch
from .recover import run_barman_recover
from .utils import ApiNotOk, PgBackupApi, set_up_logging


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar NO_COMMAND: if no sub-command of ``patroni_barman`` application has
        been selected by the user.
    :cvar API_NOT_OK: ``pg-backup-api`` status is not ``OK``.
    """

    NO_COMMAND = -1
    API_NOT_OK = -2


def main() -> None:
    """Entry point of ``patroni_barman`` application.

    Implements the parser for the application and for its sub-commands.

    The script exit code may be one of:

    * :attr:`ExitCode.NO_COMMAND`: if no sub-command was specified in the
        ``patroni_barman`` call;
    * :attr:`ExitCode.API_NOT_OK`: if ``pg-backup-api`` is not correctly up and
        running;
    * Value returned by :func:`~patroni.scripts.barman.config_switch.run_barman_config_switch`,
        if running ``patroni_barman config-switch``;
    * Value returned by :func:`~patroni.scripts.barman.recover.run_barman_recover`,
        if running ``patroni_barman recover``.

    The called sub-command is expected to exit execution once finished using
    its own set of exit codes.
    """
    parser = ArgumentParser(
        description=(
            "Wrapper application for pg-backup-api. Communicate with the API "
            "running at the given URL to perform remote Barman operations."
        ),
    )
    parser.add_argument(
        "--api-url",
        type=str,
        required=True,
        help="URL to reach the pg-backup-api, e.g. 'http://localhost:7480'",
        dest="api_url",
    )
    parser.add_argument(
        "--cert-file",
        type=str,
        required=False,
        help="Certificate to authenticate against the API, if required.",
        dest="cert_file",
    )
    parser.add_argument(
        "--key-file",
        type=str,
        required=False,
        help="Certificate key to authenticate against the API, if required.",
        dest="key_file",
    )
    parser.add_argument(
        "--retry-wait",
        type=int,
        required=False,
        default=2,
        help="How long in seconds to wait before retrying a failed "
             "pg-backup-api request (default: '%(default)s')",
        dest="retry_wait",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        required=False,
        default=5,
        help="Maximum number of retries when receiving malformed responses "
             "from the pg-backup-api (default: '%(default)s')",
        dest="max_retries",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        required=False,
        help="File where to log messages produced by this application, if any.",
        dest="log_file",
    )

    subparsers = parser.add_subparsers(title="Sub-commands")

    recover_parser = subparsers.add_parser(
        "recover",
        help="Remote 'barman recover'",
        description="Restore a Barman backup of a given Barman server"
    )
    recover_parser.add_argument(
        "--barman-server",
        type=str,
        required=True,
        help="Name of the Barman server from which to restore the backup.",
        dest="barman_server",
    )
    recover_parser.add_argument(
        "--backup-id",
        type=str,
        required=False,
        default="latest",
        help="ID of the Barman backup to be restored. You can use any value "
             "supported by 'barman recover' command "
             "(default: '%(default)s')",
        dest="backup_id",
    )
    recover_parser.add_argument(
        "--ssh-command",
        type=str,
        required=True,
        help="Value to be passed as '--remote-ssh-command' to 'barman recover'.",
        dest="ssh_command",
    )
    recover_parser.add_argument(
        "--data-directory",
        "--datadir",
        type=str,
        required=True,
        help="Destination path where to restore the barman backup in the "
             "local host.",
        dest="data_directory",
    )
    recover_parser.add_argument(
        "--loop-wait",
        type=int,
        required=False,
        default=10,
        help="How long to wait before checking again the status of the "
             "recovery process, in seconds. Use higher values if your "
             "recovery is expected to take long (default: '%(default)s')",
        dest="loop_wait",
    )
    recover_parser.set_defaults(func=run_barman_recover)

    config_switch_parser = subparsers.add_parser(
        "config-switch",
        help="Remote 'barman config-switch'",
        description="Switch the configuration of a given Barman server. "
                    "Intended to be used as a 'on_role_change' callback."
    )
    config_switch_parser.add_argument(
        "action",
        type=str,
        choices=["on_role_change"],
        help="Name of the callback (automatically filled by Patroni)",
    )
    config_switch_parser.add_argument(
        "role",
        type=str,
        choices=["primary", "promoted", "standby_leader", "replica", "demoted"],
        help="Name of the new role of this node (automatically filled by "
             "Patroni)",
    )
    config_switch_parser.add_argument(
        "cluster",
        type=str,
        help="Name of the Patroni cluster involved in the callback "
             "(automatically filled by Patroni)",
    )
    config_switch_parser.add_argument(
        "--barman-server",
        type=str,
        required=True,
        help="Name of the Barman server which config is to be switched.",
        dest="barman_server",
    )
    group = config_switch_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--barman-model",
        type=str,
        help="Name of the Barman config model to be applied to the server.",
        dest="barman_model",
    )
    group.add_argument(
        "--reset",
        action="store_true",
        help="Unapply the currently active model for the server, if any.",
        dest="reset",
    )
    config_switch_parser.add_argument(
        "--switch-when",
        type=str,
        required=True,
        default="promoted",
        choices=["promoted", "demoted", "always"],
        help="Controls under which circumstances the 'on_role_change' callback "
             "should actually switch config in Barman. 'promoted' means the "
             "'role' is either 'primary' or 'promoted'. 'demoted' "
             "means the 'role' is either 'replica' or 'demoted' "
             "(default: '%(default)s')",
        dest="switch_when",
    )
    config_switch_parser.set_defaults(func=run_barman_config_switch)

    args, _ = parser.parse_known_args()

    set_up_logging(args.log_file)

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(ExitCode.NO_COMMAND)

    api = None

    try:
        api = PgBackupApi(args.api_url, args.cert_file, args.key_file,
                          args.retry_wait, args.max_retries)
    except ApiNotOk as exc:
        logging.error("pg-backup-api is not working: %r", exc)
        sys.exit(ExitCode.API_NOT_OK)

    sys.exit(args.func(api, args))


if __name__ == "__main__":
    main()
