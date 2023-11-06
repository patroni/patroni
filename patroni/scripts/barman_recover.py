#!/usr/bin/env python

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
import sys
import time
from typing import Any, Callable, Optional, Tuple, Type, Union
from urllib.parse import urljoin
from urllib3 import PoolManager
from urllib3.exceptions import MaxRetryError
from urllib3.response import HTTPResponse


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar RECOVERY_DONE: backup was successfully restored.
    :cvar RECOVERY_FAILED: recovery of the backup faced an issue.
    :cvar API_NOT_OK: ``pg-backup-api`` status is not ``OK``.
    :cvar HTTP_REQUEST_ERROR: an error has occurred during a request to the
        ``pg-backup-api``.
    :cvar HTTP_RESPONSE_MALFORMED: ``pg-backup-api`` returned a bogus response.
    """

    RECOVERY_DONE = 0
    RECOVERY_FAILED = 1
    API_NOT_OK = 2
    HTTP_REQUEST_ERROR = 3
    HTTP_RESPONSE_MALFORMED = 4


class RetriesExceeded(Exception):
    """Maximum number of retries exceeded."""


def retry(exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]]) \
        -> Any:
    """Retry an operation n times if expected *exceptions* are faced.

    .. note::
        Should be used as a decorator of a class' method as it expects the
        first argument to be a class instance.

        The class which method is going to be decorated should contain a couple
        attributes:

        * ``max_retries``: maximum retry attempts before failing;
        * ``retry_wait``: how long to wait before retrying.

    :param exceptions: exceptions that could trigger a retry attempt.

    :raises:
        :exc:`RetriesExceeded`: if the maximum number of attempts has been
            exhausted.
    """
    def decorator(func: Callable[..., Any]) -> Any:
        def inner_func(instance: object, *args: Any, **kwargs: Any) -> Any:
            times: int = getattr(instance, "max_retries")
            retry_wait: int = getattr(instance, "retry_wait")
            method_name = f"{instance.__class__.__name__}.{func.__name__}"

            attempt = 1

            while attempt <= times:
                try:
                    return func(instance, *args, **kwargs)
                except exceptions as exc:
                    logging.warning("Attempt %d of %d on method %s failed "
                                    "with %r.",
                                    attempt, times, method_name, exc)
                    attempt += 1

                time.sleep(retry_wait)

            raise RetriesExceeded("Maximum number of retries exceeded for "
                                  f"method {method_name}.")
        return inner_func
    return decorator


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
    :ivar loop_wait: how long to wait before checking again the status of the
        recovery process. Higher values are useful for backups that are
        expected to take long to restore.
    :ivar retry_wait: how long to wait before retrying a failed request to the
        ``pg-backup-api``.
    :ivar max_retries: maximum number of retries when ``pg-backup-api`` returns
        malformed responses.
    :ivar http: a HTTP pool manager for performing web requests.
    """

    def __init__(self, api_url: str, barman_server: str, backup_id: str,
                 ssh_command: str, data_directory: str, loop_wait: int,
                 retry_wait: int, max_retries: int,
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
        :param loop_wait: how long to wait before checking again the status of
            the recovery process. Higher values are useful for backups that are
            expected to take long to restore.
        :param retry_wait: how long to wait before retrying a failed request to
            the ``pg-backup-api``.
        :param max_retries: maximum number of retries when ``pg-backup-api``
            returns malformed responses.
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
        self.loop_wait = loop_wait
        self.retry_wait = retry_wait
        self.max_retries = max_retries
        self.http = PoolManager(cert_file=cert_file, key_file=key_file)
        self._ensure_api_ok()

    def _build_full_url(self, url_path: str) -> str:
        """Build the full URL by concatenating *url_path* with the base URL.

        :param url_path: path to be accessed in the ``pg-backup-api``.

        :returns: the full URL after concatenating.
        """
        return urljoin(self.api_url, url_path)

    @staticmethod
    def _deserialize_response(response: HTTPResponse) -> Any:
        """Retrieve body from *response* as a deserialized JSON object.

        :param response: response from which JSON body will be deserialized.

        :returns: the deserialized JSON body.
        """
        return json.loads(response.data.decode("utf-8"))

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
            If a :exc:`MaxRetryError` is faced while performing the request,
            then exit with :attr:`ExitCode.HTTP_REQUEST_ERROR`

        :param url_path: URL to perform the ``GET`` request against.

        :returns: the deserialized response body.
        """
        response = None

        try:
            response = self.http.request("GET", self._build_full_url(url_path))
        except MaxRetryError as exc:
            logging.critical("An error occurred while performing an HTTP GET "
                             "request: %r", exc)
            sys.exit(ExitCode.HTTP_REQUEST_ERROR)

        return self._deserialize_response(response)

    def _post_request(self, url_path: str, body: Any) -> Any:
        """Perform a ``POST`` request to *url_path* serializing *body* as JSON.

        .. note::
            If a :exc:`MaxRetryError` is faced while performing the request,
            then exit with :attr:`ExitCode.HTTP_REQUEST_ERROR`

        :param url_path: URL to perform the ``POST`` request against.
        :param body: the body to be serialized as JSON and sent in the request.

        :returns: the deserialized response body.
        """
        body = self._serialize_request(body)

        response = None

        try:
            response = self.http.request("POST",
                                         self._build_full_url(url_path),
                                         body=body,
                                         headers={
                                             "Content-Type": "application/json"
                                         })
        except MaxRetryError as exc:
            logging.critical("An error occurred while performing an HTTP POST "
                             "request: %r", exc)
            sys.exit(ExitCode.HTTP_REQUEST_ERROR)

        return self._deserialize_response(response)

    def _ensure_api_ok(self) -> None:
        """Ensure ``pg-backup-api`` is reachable and ``OK``.

        .. note::
            If ``pg-backup-api`` status is not ``OK``, then exit with
            :attr:`ExitCode.API_NOT_OK`.
        """
        response = self._get_request("status")

        if response != "OK":
            logging.critical("pg-backup-api is not working: %s", response)
            sys.exit(ExitCode.API_NOT_OK)

    @retry(KeyError)
    def _create_recovery_operation(self) -> str:
        """Create a recovery operation on the ``pg-backup-api``.

        :returns: the ID of the recovery operation that has been created.
        """
        response = self._post_request(
            f"servers/{self.barman_server}/operations",
            {
                "type": "recovery",
                "backup_id": self.backup_id,
                "remote_ssh_command": self.ssh_command,
                "destination_directory": self.data_directory,
            },
        )

        return response["operation_id"]

    @retry(KeyError)
    def _get_recovery_operation_status(self, operation_id: str) -> str:
        """Get status of the recovery operation *operation_id*.

        :param operation_id: ID of the recovery operation to be checked.

        :returns: the status of the recovery operation.
        """
        response = self._get_request(
            f"servers/{self.barman_server}/operations/{operation_id}",
        )

        return response["status"]

    def restore_backup(self) -> bool:
        """Restore the configured Barman backup through ``pg-backup-api``.

        .. note::
            If recovery API request returns a malformed response, then exit with
            :attr:`ExitCode.HTTP_RESPONSE_MALFORMED`.

        :returns: ``True`` if it was successfully recovered, ``False``
            otherwise.
        """
        operation_id = None

        try:
            operation_id = self._create_recovery_operation()
        except RetriesExceeded:
            logging.critical("Maximum number of retries exceeded, exiting.")
            sys.exit(ExitCode.HTTP_RESPONSE_MALFORMED)

        logging.info("Created the recovery operation with ID %s", operation_id)

        status = None

        while True:
            try:
                status = self._get_recovery_operation_status(operation_id)
            except RetriesExceeded:
                logging.critical("Maximum number of retries exceeded, "
                                 "exiting.")
                sys.exit(ExitCode.HTTP_RESPONSE_MALFORMED)

            if status != "IN_PROGRESS":
                break

            logging.info("Recovery operation %s is still in progress",
                         operation_id)
            time.sleep(self.loop_wait)

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
    parser.add_argument(
        "--loop-wait",
        type=int,
        required=False,
        default=10,
        help="How long to wait before checking again the status of the "
             "recovery process, in seconds. Use higher values if your "
             "recovery is expected to take long (default: ``%(default)s``)",
        dest="loop_wait",
    )
    parser.add_argument(
        "--retry-wait",
        type=int,
        required=False,
        default=2,
        help="How long to wait before retrying a failed ``pg-backup-api`` "
             "request (default: ``%(default)s``)",
        dest="retry_wait",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        required=False,
        default=5,
        help="Maximum number of retries when receiving malformed responses "
             "from the ``pg-backup-api`` (default: ``%(default)s``)",
        dest="max_retries",
    )
    args, _ = parser.parse_known_args()

    set_up_logging(args.log_file)

    barman_recover = BarmanRecover(args.api_url, args.barman_server,
                                   args.backup_id, args.ssh_command,
                                   args.data_directory, args.loop_wait,
                                   args.retry_wait, args.max_retries,
                                   args.cert_file, args.key_file)

    successful = barman_recover.restore_backup()

    if successful:
        logging.info("Recovery operation finished successfully.")
        sys.exit(ExitCode.RECOVERY_DONE)
    else:
        logging.critical("Recovery operation failed.")
        sys.exit(ExitCode.RECOVERY_FAILED)


if __name__ == "__main__":
    main()
