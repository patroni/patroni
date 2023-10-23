#!/usr/bin/python3

"""Restore a Barman backup to the local node through ``pg-backup-api``.

This script can be used both as a custom bootstrap method, and as a custom
create replica method. Check the output of ``--help`` to understand the
parameters supported by the script. ``--datadir`` is a special parameter and it
is automatically filled by Patroni in both cases.

It requires that you have previously configured a Barman server, and that you
have ``pg-backup-api`` configured and running in the same host as Barman.

Refer to :class:`ExitCode` for possible exit codes of this script.
"""
from argparse import ArgumentParser
from enum import IntEnum
import json
import logging
import ssl
import sys
import time
from typing import Any, Optional, TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


if TYPE_CHECKING:
    from http.client import HTTPResponse


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar RECOVERY_DONE: backup was successfully restored.
    :cvar RECOVERY_FAILED: recovery of the backup faced an issue.
    :cvar API_NOT_OK: ``pg-backup-api`` status is not ``OK``.
    :cvar BARMAN_SERVER_DOES_NOT_EXIST: there is no such Barman server
        configured in the Barman host.
    :cvar HTTP_REQUEST_ERROR: an error has occurred during a request to the
        ``pg-backup-api``.
    :cvar HTTP_RESPONSE_MALFORMED: ``pg-backup-api`` returned a bogus response.
    """

    RECOVERY_DONE = 0
    RECOVERY_FAILED = 1
    API_NOT_OK = 2
    BARMAN_SERVER_DOES_NOT_EXIST = 3
    HTTP_REQUEST_ERROR = 4
    HTTP_RESPONSE_MALFORMED = 5


class BarmanRecover:
    """Facilities for performing a remote ``barman recover`` operation.

    You should instantiate this class, which will take care of configuring the
    operation accordingly. When you want to start the operation, you should
    call :meth:`restore_backup`. At any point of interaction with this class,
    you may face a :func:`sys.exit` call. Refer to :class:`ExitCode` for a view
    on the possible exit codes.

    :ivar api_url: base URL to reach the ``pg-backup-api``.
    :ivar cert_file: certificate to authenticate against the
        ``pg-backup-api``, if required.
    :ivar key_file: certificate key to authenticate against the
        ``pg-backup-api``, if required.
    :ivar barman_server: name of the Barman server which backup is to be
        restored.
    :ivar backup_id: ID of the backup from the Barman server.
    :ivar ssh_command: SSH command to connect from the Barman host to the
        local host.
    :ivar data_directory: path to the Postgres data directory where to
        restore the backup at.
    """

    def __init__(self, api_url: str, barman_server: str, backup_id: str,
                 ssh_command: str, data_directory: str,
                 cert_file: Optional[str] = None,
                 key_file: Optional[str] = None) -> None:
        """Create a new instance of :class:`BarmanRecover`.

        Make sure the ``pg-backup-api`` is reachable and running fine.

        :param api_url: base URL to reach the ``pg-backup-api``.
        :param barman_server: name of the Barman server which backup is to be
            restored.
        :param backup_id: ID of the backup from the Barman server.
        :param ssh_command: SSH command to connect from the Barman host to the
            local host.
        :param data_directory: path to the Postgres data directory where to
            restore the backup at.
        :param cert_file: certificate to authenticate against the
            ``pg-backup-api``, if required.
        :param key_file: certificate key to authenticate against the
            ``pg-backup-api``, if required.
        """
        self.api_url = api_url
        self.cert_file = cert_file
        self.key_file = key_file
        self.barman_server = barman_server
        self.backup_id = backup_id
        self.ssh_command = ssh_command
        self.data_directory = data_directory
        self._ensure_api_ok()

    def _build_full_url(self, url_path: str) -> str:
        """Build the full URL by concatenating *url_path* with the base URL.

        :param url_path: path to be accessed in the ``pg-backup-api``.

        :returns: the full URL after concatenating.
        """
        return f"{self.api_url}/{url_path}"

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create an SSL context, if required.

        :returns: the SSL context, if we have a client certificate file,
            otherwise ``None``.
        """
        if self.cert_file is None:
            return None

        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(self.cert_file, self.key_file)

        return ctx

    @staticmethod
    def _deserialize_response(response: 'HTTPResponse') -> Any:
        """Retrieve body from *response* as a deserialized JSON object.

        :param response: response from which JSON body will be deserialized.

        :returns: the deserialized JSON body.
        """
        return json.loads(response.read().decode("utf-8"))

    @staticmethod
    def _serialize_request(body: Any) -> Any:
        """Serialize a request body.

        :param body: content of the request body to be serialized.

        :returns: the serialized request body.
        """
        return json.dumps(body).encode("utf-8")

    def _get_request(self, url_path: str) -> Any:
        """Perform a ``GET`` request to *url_path*.

        .. note::
            If a :exc:`HTTPError` or :exc:`URLError` is faced while performing
            the request, then exit with :attr:`ExitCode.HTTP_REQUEST_ERROR`

        :param url_path: URL to perform the ``GET`` request against.

        :returns: the deserialized response body.
        """
        response = None

        try:
            response = urlopen(self._build_full_url(url_path),
                               context=self._create_ssl_context())
        except (HTTPError, URLError) as exc:
            logging.critical("An error ocurred while performing an HTTP GET "
                             f"request: {str(exc)}")
            sys.exit(ExitCode.HTTP_REQUEST_ERROR)

        return self._deserialize_response(response)

    def _post_request(self, url_path: str, body: Any) -> Any:
        """Perform a ``POST`` request to *url_path* serializing *body* as JSON.

        .. note::
            If a :exc:`HTTPError` or :exc:`URLError` is faced while performing
            the request, then exit with :attr:`ExitCode.HTTP_REQUEST_ERROR`

        :param url_path: URL to perform the ``POST`` request against.
        :param body: the body to be serialized as JSON and sent in the request.

        :returns: the deserialized response body.
        """
        body = self._serialize_request(body)
        request = Request(self._build_full_url(url_path))
        request.add_header("Content-Type", "application/json")
        request.add_header("Content-Length", str(len(body)))

        response = None

        try:
            response = urlopen(request, body,
                               context=self._create_ssl_context())
        except (HTTPError, URLError) as exc:
            logging.critical("An error occurred while performing an HTTP POST "
                             f"request: {str(exc)}")
            sys.exit(ExitCode.HTTP_REQUEST_ERROR)

        return self._deserialize_response(response)

    def _ensure_api_ok(self):
        """Ensure ``pg-backup-api`` is reachable and ``OK``.

        .. note::
            If ``pg-backup-api`` status is not ``OK``, then exit with
            :attr:`ExitCode.API_NOT_OK`.
        """
        response = self._get_request("status")

        if response != "OK":
            logging.critical(f"pg-backup-api is not working: {response}")
            sys.exit(ExitCode.API_NOT_OK)

    def restore_backup(self) -> bool:
        """Restore the configured Barman backup through ``pg-backup-api``.

        .. note::
            If recover API request returns a malformed response, then exit with
            :attr:`ExitCode.HTTP_RESPONSE_MALFORMED`.

        :returns: ``True`` if it was successfully recovered, ``False``
            otherwise.
        """
        response = None

        try:
            response = self._post_request(
                f"servers/{self.barman_server}/operations",
                {
                    "operation_type": "recover",
                    "backup_id": self.backup_id,
                    "remote_ssh_command": self.ssh_command,
                    "destination_directory": self.data_directory,
                },
            )
            
            operation_id = response["operation_id"]
        except KeyError:
            logging.critical("Recived a malformed response from the API: "
                             f"{response}")
            sys.exit(ExitCode.HTTP_RESPONSE_MALFORMED)

        logging.info(f"Created the recovery operation with ID {operation_id}")

        status = None

        while True:
            try:
                response = self._get_request(
                    f"servers/{self.barman_server}/operations/{operation_id}",
                )
                
                status = response["status"]
            except KeyError:
                logging.critical("Recived a malformed response from the API: "
                                 f"{response}")
                sys.exit(ExitCode.HTTP_RESPONSE_MALFORMED)

            if status != "IN_PROGRESS":
                break

            logging.info(f"Recovery operation still in progress")
            time.sleep(10)

        return status == "DONE"


def set_up_logging(log_file: Optional[str] = None) -> None:
    """Set up logging to file, if *log_file* is given, otherwise to console.

    :param log_file: file where to log messages, if any.
    """
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")


def main() -> None:
    """Entry point of this script.

    Parse the command-line arguments and recover a Barman backup through
    ``pg-backup-api`` to the local host.
    """
    parser = ArgumentParser(
        epilog=(
            "Wrapper script for ``pg-backup-api``. Communicate with the API "
            "running  at ``--api-url`` to restore a ``--backup-id`` Barman "
            "backup of the server ``--barman-server``."
        ),
    )
    parser.add_argument(
        "--api-url",
        type=str,
        required=True,
        help="URL to reach the ``pg-backup-api``, e.g. "
             "``http://localhost:7480``",
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
        "--barman-server",
        type=str,
        required=True,
        help="Name of the Barman server from which to restore the backup.",
        dest="barman_server",
    )
    parser.add_argument(
        "--backup-id",
        type=str,
        required=False,
        default="latest",
        help="ID of the Barman backup to be restored. You can use any value "
             "supported by ``barman recover`` command "
             "(default: ``%(default)s``)",
        dest="backup_id",
    )
    parser.add_argument(
        "--ssh-command",
        type=str,
        required=True,
        help="Value to be passed as ``--remote-ssh-command`` to "
             "``barman recover``.",
        dest="ssh_command",
    )
    parser.add_argument(
        "--data-directory",
        "--datadir",
        type=str,
        required=True,
        help="Destination path where to restore the barman backup in the "
             "local host.",
        dest="data_directory",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        required=False,
        help="File where to log messages produced by this script, if any.",
        dest="log_file",
    )
    args, _ = parser.parse_known_args()

    set_up_logging(args.log_file)

    barman_recover = BarmanRecover(args.api_url, args.barman_server,
                                   args.backup_id, args.ssh_command,
                                   args.data_directory, args.cert_file,
                                   args.key_file)

    successful = barman_recover.restore_backup()

    if successful:
        logging.info("Recovery operation finished successfully.")
        sys.exit(ExitCode.RECOVERY_DONE)
    else:
        logging.critical("Recovery operation failed.")
        sys.exit(ExitCode.RECOVERY_FAILED)


if __name__ == "__main__":
    main()
