#!/usr/bin/env python

"""Utilitary stuff to be used by Barman related scripts."""

import json
import logging
import time

from enum import IntEnum
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union
from urllib.parse import urljoin

from urllib3 import PoolManager
from urllib3.exceptions import MaxRetryError
from urllib3.response import HTTPResponse


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
        * ``retry_wait``: how long in seconds to wait before retrying.

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


def set_up_logging(log_file: Optional[str] = None) -> None:
    """Set up logging to file, if *log_file* is given, otherwise to console.

    :param log_file: file where to log messages, if any.
    """
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")


class OperationStatus(IntEnum):
    """Possible status of ``pg-backup-api`` operations.

    :cvar IN_PROGRESS: the operation is still ongoing.
    :cvar FAILED: the operation failed.
    :cvar DONE: the operation finished successfully.
    """

    IN_PROGRESS = 0
    FAILED = 1
    DONE = 2


class ApiNotOk(Exception):
    """The ``pg-backup-api`` is not currently up and running."""


class PgBackupApi:
    """Facilities for communicating with the ``pg-backup-api``.

    :ivar api_url: base URL to reach the ``pg-backup-api``.
    :ivar cert_file: certificate to authenticate against the ``pg-backup-api``,
        if required.
    :ivar key_file: certificate key to authenticate against the
        ``pg-backup-api``, if required.
    :ivar retry_wait: how long in seconds to wait before retrying a failed
        request to the ``pg-backup-api``.
    :ivar max_retries: maximum number of retries when ``pg-backup-api`` returns
        malformed responses.
    :ivar http: a HTTP pool manager for performing web requests.
    """

    def __init__(self, api_url: str, cert_file: Optional[str],
                 key_file: Optional[str], retry_wait: int,
                 max_retries: int) -> None:
        """Create a new instance of :class:`BarmanRecover`.

        Make sure the ``pg-backup-api`` is reachable and running fine.

        .. note::
            When using any method which send requests to the API, be aware that
            they might raise :exc:`RetriesExceeded` upon HTTP request errors.

            Similarly, when instantiating this class you may face an
            :exc:`ApiNotOk`, if the API is down or returns a bogus status.

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
        self.api_url = api_url
        self.cert_file = cert_file
        self.key_file = key_file
        self.retry_wait = retry_wait
        self.max_retries = max_retries
        self._http = PoolManager(cert_file=cert_file, key_file=key_file)
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

        :param url_path: URL to perform the ``GET`` request against.

        :returns: the deserialized response body.

        :raises:
            :exc:`RetriesExceeded`: raised from the corresponding :mod:`urllib3`
                exception.
        """
        url = self._build_full_url(url_path)
        response = None

        try:
            response = self._http.request("GET", url)
        except MaxRetryError as exc:
            msg = f"Failed to perform a GET request to {url}"
            raise RetriesExceeded(msg) from exc

        return self._deserialize_response(response)

    def _post_request(self, url_path: str, body: Any) -> Any:
        """Perform a ``POST`` request to *url_path* serializing *body* as JSON.

        :param url_path: URL to perform the ``POST`` request against.
        :param body: the body to be serialized as JSON and sent in the request.

        :returns: the deserialized response body.

        :raises:
            :exc:`RetriesExceeded`: raised from the corresponding :mod:`urllib3`
                exception.
        """
        body = self._serialize_request(body)

        url = self._build_full_url(url_path)
        response = None

        try:
            response = self._http.request("POST",
                                          url,
                                          body=body,
                                          headers={
                                              "Content-Type": "application/json"
                                          })
        except MaxRetryError as exc:
            msg = f"Failed to perform a POST request to {url} with {body}"
            raise RetriesExceeded(msg) from exc

        return self._deserialize_response(response)

    def _ensure_api_ok(self) -> None:
        """Ensure ``pg-backup-api`` is reachable and ``OK``.

        :raises:
            :exc:`ApiNotOk`: if ``pg-backup-api`` status is not ``OK``.
        """
        response = self._get_request("status")

        if response != "OK":
            msg = (
                "pg-backup-api is currently not up and running at "
                f"{self.api_url}: {response}"
            )

            raise ApiNotOk(msg)

    @retry(KeyError)
    def get_operation_status(self, barman_server: str,
                             operation_id: str) -> OperationStatus:
        """Get status of the operation which ID is *operation_id*.

        :param barman_server: name of the Barman server related with the
            operation.
        :param operation_id: ID of the operation to be checked.

        :returns: the status of the operation.
        """
        response = self._get_request(
            f"servers/{barman_server}/operations/{operation_id}",
        )

        status = response["status"]
        return OperationStatus[status]

    @retry(KeyError)
    def create_recovery_operation(self, barman_server: str, backup_id: str,
                                  ssh_command: str, data_directory: str) -> str:
        """Create a recovery operation on the ``pg-backup-api``.

        :param barman_server: name of the Barman server which backup is to be
            restored.
        :param backup_id: ID of the backup from the Barman server.
        :param ssh_command: SSH command to connect from the Barman host to the
            target host.
        :param data_directory: path to the Postgres data directory where to
            restore the backup at.

        :returns: the ID of the recovery operation that has been created.
        """
        response = self._post_request(
            f"servers/{barman_server}/operations",
            {
                "type": "recovery",
                "backup_id": backup_id,
                "remote_ssh_command": ssh_command,
                "destination_directory": data_directory,
            },
        )

        return response["operation_id"]

    @retry(KeyError)
    def create_config_switch_operation(self, barman_server: str,
                                       barman_model: Optional[str],
                                       reset: Optional[bool]) -> str:
        """Create a config switch operation on the ``pg-backup-api``.

        :param barman_server: name of the Barman server which config is to be
            switched.
        :param barman_model: name of the Barman model to be applied to the
            server, if any.
        :param reset: ``True`` if you would like to unapply the currently active
            model for the server, if any.

        :returns: the ID of the config switch operation that has been created.
        """
        body: Dict[str, Any] = {"type": "config_switch"}

        if barman_model:
            body["model_name"] = barman_model
        elif reset:
            body["reset"] = reset

        response = self._post_request(
            f"servers/{barman_server}/operations",
            body,
        )

        return response["operation_id"]
