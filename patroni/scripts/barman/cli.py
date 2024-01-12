#!/usr/bin/env python

"""Perform operations on Barman through ``pg-backup-api``.

The actual operations are implemented by separate modules. This module only
builds the CLI that makes an interface with the actual commands.

.. note::
    Will exit with ``-1`` if no sub-command has been selected.
"""

from argparse import ArgumentParser
import sys

from .recover import run_barman_recover
from .utils import set_up_logging


def main() -> None:
    """Entry point of ``patroni_barman`` application.

    Implements the parser for the application and for its sub-commands.

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
        help="How long to wait before retrying a failed pg-backup-api  request "
             "(default: '%(default)s')",
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
    args, _ = parser.parse_known_args()

    set_up_logging(args.log_file)

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(-1)

    args.func(args)


if __name__ == "__main__":
    main()
