"""Implement Patroni's REST API.

Exposes a REST API of patroni operations functions, such as status, performance and management to web clients.

Much of what can be achieved with the command line tool patronictl can be done via the API. Patroni CLI and daemon
utilises the API to perform these functions.
"""

import base64
import datetime
import hmac
import json
import logging
import os
import socket
import sys
import time
import traceback

from http.server import BaseHTTPRequestHandler, HTTPServer
from ipaddress import ip_address, ip_network, IPv4Network, IPv6Network
from socketserver import ThreadingMixIn
from threading import Thread
from typing import Any, Callable, cast, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import parse_qs, urlparse

import dateutil.parser

from . import global_config, psycopg
from .__main__ import Patroni
from .dcs import Cluster
from .exceptions import PostgresConnectionException, PostgresException
from .postgresql.misc import postgres_version_to_int, PostgresqlRole, PostgresqlState
from .utils import cluster_as_json, deep_compare, enable_keepalive, parse_bool, \
    parse_int, patch_config, Retry, RetryFailedError, split_host_port, tzutc, uri

logger = logging.getLogger(__name__)


def check_access(*args: Any, **kwargs: Any) -> Callable[..., Any]:
    """Check the source ip, authorization header, or client certificates.

    .. note::
        The actual logic to check access is implemented through :func:`RestApiServer.check_access`.

        Optionally it is possible to skip source ip check by specifying ``allowlist_check_members=False``.

    :returns: a decorator that executes *func* only if :func:`RestApiServer.check_access` returns ``True``.

    :Example:

        >>> class FooServer:
        ...   def check_access(self, *args, **kwargs):
        ...     print(f'In FooServer: {args[0].__class__.__name__}')
        ...     return True
        ...

        >>> class Foo:
        ...   server = FooServer()
        ...   @check_access
        ...   def do_PUT_foo(self):
        ...      print('In do_PUT_foo')
        ...   @check_access(allowlist_check_members=False)
        ...   def do_POST_bar(self):
        ...      print('In do_POST_bar')

        >>> f = Foo()
        >>> f.do_PUT_foo()
        In FooServer: Foo
        In do_PUT_foo
    """
    allowlist_check_members = kwargs.get('allowlist_check_members', True)

    def inner_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(self: 'RestApiHandler', *args: Any, **kwargs: Any) -> Any:
            if self.server.check_access(self, allowlist_check_members=allowlist_check_members):
                return func(self, *args, **kwargs)

        return wrapper

    # A hacky way to have decorators that work with and without parameters.
    if len(args) == 1 and callable(args[0]):
        # The first parameter is a function, it means decorator is used as "@check_access"
        return inner_decorator(args[0])
    else:
        # @check_access(allowlist_check_members=False) case
        return inner_decorator


class RestApiHandler(BaseHTTPRequestHandler):
    """Define how to handle each of the requests that are made against the REST API server."""

    # Comment from pyi stub file. These unions can cause typing errors with IDEs, e.g. PyCharm
    #
    # Those are technically of types, respectively:
    # * _RequestType = Union[socket.socket, Tuple[bytes, socket.socket]]
    # * _AddressType = Tuple[str, int]
    # But there are some concerns that having unions here would cause
    # too much inconvenience to people using it (see
    # https://github.com/python/typeshed/pull/384#issuecomment-234649696)

    def __init__(self, request: Any,
                 client_address: Any,
                 server: Union['RestApiServer', HTTPServer]) -> None:
        """Create a :class:`RestApiHandler` instance.

        .. note::
            Currently not different from its superclass :func:`__init__`, and only used so ``pyright`` can understand
            the type of ``server`` attribute.

        :param request: client request to be processed.
        :param client_address: address of the client connection.
        :param server: HTTP server that received the request.
        """
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(server, RestApiServer)
        super(RestApiHandler, self).__init__(request, client_address, server)
        self.server: 'RestApiServer' = server  # pyright: ignore [reportIncompatibleVariableOverride]
        self.__start_time: float = 0.0
        self.path_query: Dict[str, List[str]] = {}

    def version_string(self) -> str:
        """Override the default version string to return the server header as specified in the configuration.

        If the server header is not set, then it returns the default version string of the HTTP server.

        :return: ``Server`` version string, which is either the server header or the default version string
            from the :class:`BaseHTTPRequestHandler`.
        """
        return self.server.server_header or super().version_string()

    def _write_status_code_only(self, status_code: int) -> None:
        """Write a response that is composed only of the HTTP status.

        The response is written with these values separated by space:

            * HTTP protocol version;
            * *status_code*;
            * description of *status_code*.

        .. note::
            This is usually useful for replying to requests from software like HAProxy.

        :param status_code: HTTP status code.

        :Example:

            * ``_write_status_code_only(200)`` would write a response like ``HTTP/1.0 200 OK``.
        """
        message = self.responses[status_code][0]
        self.wfile.write('{0} {1} {2}\r\n\r\n'.format(self.protocol_version, status_code, message).encode('utf-8'))
        self.log_request(status_code)

    def write_response(self, status_code: int, body: str, content_type: str = 'text/html',
                       headers: Optional[Dict[str, str]] = None) -> None:
        """Write an HTTP response.

        .. note::
            Besides ``Content-Type`` header, and the HTTP headers passed through *headers*, this function will also
            write the HTTP headers defined through ``restapi.http_extra_headers`` and ``restapi.https_extra_headers``
            from Patroni configuration.

        :param status_code: response HTTP status code.
        :param body: response body.
        :param content_type: value for ``Content-Type`` HTTP header.
        :param headers: dictionary of additional HTTP headers to set for the response. Each key is the header name, and
            the corresponding value is the value for the header in the response.
        """
        # TODO: try-catch ConnectionResetError: [Errno 104] Connection reset by peer and log it in DEBUG level
        self.send_response(status_code)
        headers = headers or {}
        if content_type:
            headers['Content-Type'] = content_type
        for name, value in headers.items():
            self.send_header(name, value)
        for name, value in (self.server.http_extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def _write_json_response(self, status_code: int, response: Any) -> None:
        """Write an HTTP response with a JSON content type.

        Call :func:`write_response` with ``content_type`` as ``application/json``.

        :param status_code: response HTTP status code.
        :param response: value to be dumped as a JSON string and to be used as the response body.
        """
        self.write_response(status_code, json.dumps(response, default=str), content_type='application/json')

    def _write_status_response(self, status_code: int, response: Dict[str, Any]) -> None:
        """Write an HTTP response with Patroni/Postgres status in JSON format.

        Modifies *response* before sending it to the client. Defines the ``patroni`` key, which is a
        dictionary that contains the mandatory keys:

            * ``version``: Patroni version, e.g. ``3.0.2``;
            * ``scope``: value of ``scope`` setting from Patroni configuration.

        May also add the following optional keys, depending on the status of this Patroni/PostgreSQL node:

            * ``tags``: tags that were set through Patroni configuration merged with dynamically applied tags;
            * ``database_system_identifier``: ``Database system identifier`` from ``pg_controldata`` output;
            * ``pending_restart``: ``True`` if PostgreSQL is pending to be restarted;
            * ``pending_restart_reason``: dictionary where each key is the parameter that caused "pending restart" flag
                to be set and the value is a dictionary with the old and the new value.
            * ``scheduled_restart``: a dictionary with a single key ``schedule``, which is the timestamp for the
                scheduled restart;
            * ``watchdog_failed``: ``True`` if watchdog device is unhealthy;
            * ``logger_queue_size``: log queue length if it is longer than expected;
            * ``logger_records_lost``: number of log records that have been lost while the log queue was full.

        :param status_code: response HTTP status code.
        :param response: represents the status of the PostgreSQL node, and is used as a basis for the HTTP response.
            This dictionary is built through :func:`get_postgresql_status`.
        """
        patroni = self.server.patroni
        tags = patroni.ha.get_effective_tags()
        if tags:
            response['tags'] = tags
        if patroni.postgresql.sysid:
            response['database_system_identifier'] = patroni.postgresql.sysid
        if patroni.postgresql.pending_restart_reason:
            response['pending_restart'] = True
            response['pending_restart_reason'] = dict(patroni.postgresql.pending_restart_reason)
        response['patroni'] = {
            'version': patroni.version,
            'scope': patroni.postgresql.scope,
            'name': patroni.postgresql.name
        }
        if patroni.scheduled_restart:
            response['scheduled_restart'] = patroni.scheduled_restart.copy()
            del response['scheduled_restart']['postmaster_start_time']
            response['scheduled_restart']['schedule'] = (response['scheduled_restart']['schedule']).isoformat()
        if not patroni.ha.watchdog.is_healthy:
            response['watchdog_failed'] = True
        qsize = patroni.logger.queue_size
        if qsize > patroni.logger.NORMAL_LOG_QUEUE_SIZE:
            response['logger_queue_size'] = qsize
            lost = patroni.logger.records_lost
            if lost:
                response['logger_records_lost'] = lost
        self._write_json_response(status_code, response)

    def do_GET(self, write_status_code_only: bool = False) -> None:
        """Process all GET requests which can not be routed to other methods.

        Is used for handling all health-checks requests. E.g. "GET /(primary|replica|sync|async|etc...)".

        The (optional) query parameters and the HTTP response status depend on the requested path:

            * ``/``, ``primary``, or ``read-write``:

                * HTTP status ``200``: if a primary with the leader lock.

            * ``/standby-leader``:

                * HTTP status ``200``: if holds the leader lock in a standby cluster.

            * ``/leader``:

                * HTTP status ``200``: if holds the leader lock.

            * ``/replica``:

                * Query parameters:

                    * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                        represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                        ``10MB``).
                    * Any custom parameter: will attempt to match them against node tags.

                * HTTP status ``200``: if up and running as a standby and without ``noloadbalance`` tag.

            * ``/read-only``:

                * HTTP status ``200``: if up and running and without ``noloadbalance`` tag.

            * ``/quorum``:

                * HTTP status ``200``: if up and running as a quorum synchronous standby.

            * ``/read-only-quorum``:

                * HTTP status ``200``: if up and running as a quorum synchronous standby or primary.

            * ``/synchronous`` or ``/sync``:

                * HTTP status ``200``: if up and running as a synchronous standby.

            * ``/read-only-sync``:

                * HTTP status ``200``: if up and running as a synchronous standby or primary.

            * ``/asynchronous``:

                * Query parameters:

                    * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                        represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                        ``10MB``).

                * HTTP status ``200``: if up and running as an asynchronous standby.

            * ``/health``:

                * HTTP status ``200``: if up and running.

        .. note::
            If not able to honor the query parameter, or not able to match the condition described for HTTP status
            ``200`` in each path above, then HTTP status will be ``503``.

        .. note::
            Independently of the requested path, if *write_status_code_only* is ``False``, then it always write an HTTP
            response through :func:`_write_status_response`, with the node status.

        :param write_status_code_only: indicates that instead of a normal HTTP response we should
                                       send only the HTTP Status Code and close the connection.
                                       Useful when health-checks are executed by HAProxy.
        """
        path = '/primary' if self.path == '/' else self.path
        response = self.get_postgresql_status()
        latest_end_lsn = response.pop('latest_end_lsn', 0)

        patroni = self.server.patroni
        cluster = patroni.dcs.cluster
        config = global_config.from_cluster(cluster)

        leader_optime = max(cluster and cluster.status.last_lsn or 0, latest_end_lsn)
        replayed_location = response.get('xlog', {}).get('replayed_location', 0)
        max_replica_lag = parse_int(self.path_query.get('lag', [sys.maxsize])[0], 'B')
        if max_replica_lag is None:
            max_replica_lag = sys.maxsize
        is_lagging = leader_optime and leader_optime > replayed_location + max_replica_lag

        replica_status_code = 200 if not patroni.noloadbalance and not is_lagging and \
            response.get('role') == PostgresqlRole.REPLICA and response.get('state') == PostgresqlState.RUNNING else 503

        if not cluster and response.get('pause'):
            leader_status_code = 200 if response.get('role') in (PostgresqlRole.PRIMARY,
                                                                 PostgresqlRole.STANDBY_LEADER) else 503
            primary_status_code = 200 if response.get('role') == PostgresqlRole.PRIMARY else 503
            standby_leader_status_code = 200 if response.get('role') == PostgresqlRole.STANDBY_LEADER else 503
        elif patroni.ha.is_leader():
            leader_status_code = 200
            if config.is_standby_cluster:
                primary_status_code = replica_status_code = 503
                standby_leader_status_code =\
                    200 if response.get('role') in (PostgresqlRole.REPLICA, PostgresqlRole.STANDBY_LEADER) else 503
            else:
                primary_status_code = 200
                standby_leader_status_code = 503
        else:
            leader_status_code = primary_status_code = standby_leader_status_code = 503

        status_code = 503

        ignore_tags = False
        if 'standby_leader' in path or 'standby-leader' in path:
            status_code = standby_leader_status_code
            ignore_tags = True
        elif 'leader' in path:
            status_code = leader_status_code
            ignore_tags = True
        elif 'master' in path or 'primary' in path or 'read-write' in path:
            status_code = primary_status_code
            ignore_tags = True
        elif 'replica' in path:
            status_code = replica_status_code
        elif 'read-only' in path and 'sync' not in path and 'quorum' not in path:
            status_code = 200 if 200 in (primary_status_code, standby_leader_status_code) else replica_status_code
        elif 'health' in path:
            status_code = 200 if response.get('state') == PostgresqlState.RUNNING else 503
        elif cluster:  # dcs is available
            is_quorum = response.get('quorum_standby')
            is_synchronous = response.get('sync_standby')
            if path in ('/sync', '/synchronous') and is_synchronous:
                status_code = replica_status_code
            elif path == '/quorum' and is_quorum:
                status_code = replica_status_code
            elif path in ('/async', '/asynchronous') and not is_synchronous and not is_quorum:
                status_code = replica_status_code
            elif path == '/read-only-quorum':
                if 200 in (primary_status_code, standby_leader_status_code):
                    status_code = 200
                elif is_quorum:
                    status_code = replica_status_code
            elif path in ('/read-only-sync', '/read-only-synchronous'):
                if 200 in (primary_status_code, standby_leader_status_code):
                    status_code = 200
                elif is_synchronous:
                    status_code = replica_status_code

        # check for user defined tags in query params
        if not ignore_tags and status_code == 200:
            qs_tag_prefix = "tag_"
            for qs_key, qs_value in self.path_query.items():
                if not qs_key.startswith(qs_tag_prefix):
                    continue
                qs_key = qs_key[len(qs_tag_prefix):]
                qs_value = qs_value[0]
                instance_tag_value = patroni.tags.get(qs_key)
                # tag not registered for instance
                if instance_tag_value is None:
                    status_code = 503
                    break
                if not isinstance(instance_tag_value, str):
                    instance_tag_value = str(instance_tag_value).lower()
                if instance_tag_value != qs_value:
                    status_code = 503
                    break

        if write_status_code_only:  # when haproxy sends OPTIONS request it reads only status code and nothing more
            self._write_status_code_only(status_code)
        else:
            self._write_status_response(status_code, response)

    def do_OPTIONS(self) -> None:
        """Handle an ``OPTIONS`` request.

        Write a simple HTTP response that represents the current PostgreSQL status. Send only ``200 OK`` or
        ``503 Service Unavailable`` as a response and nothing more, particularly no headers.
        """
        self.do_GET(write_status_code_only=True)

    def do_HEAD(self) -> None:
        """Handle a ``HEAD`` request.

        Write a simple HTTP response that represents the current PostgreSQL status. Send only ``200 OK`` or
        ``503 Service Unavailable`` as a response and nothing more, particularly no headers.
        """
        self.do_GET(write_status_code_only=True)

    def do_GET_liveness(self) -> None:
        """Handle a ``GET`` request to ``/liveness`` path.

        Write a simple HTTP response with HTTP status:

            * ``200``:

                * If the cluster is in maintenance mode; or
                * If Patroni heartbeat loop is properly running;

            * ``503``:

                * if Patroni heartbeat loop last run was more than ``ttl`` setting ago on the primary (or twice the
                    value of ``ttl`` on a replica).

        """
        patroni: Patroni = self.server.patroni
        is_primary = patroni.postgresql.role == PostgresqlRole.PRIMARY and patroni.postgresql.is_running()
        # We can tolerate Patroni problems longer on the replica.
        # On the primary the liveness probe most likely will start failing only after the leader key expired.
        # It should not be a big problem because replicas will see that the primary is still alive via REST API call.
        liveness_threshold = patroni.dcs.ttl * (1 if is_primary else 2)

        # In maintenance mode (pause) we are fine if heartbeat loop stuck.
        status_code = 200 if patroni.ha.is_paused() or patroni.next_run + liveness_threshold > time.time() else 503
        self._write_status_code_only(status_code)

    def _readiness(self) -> Optional[str]:
        """Check if readiness conditions are met.

        :returns: None if node can be considered ready or diagnostic message if not."""

        patroni = self.server.patroni
        if patroni.ha.is_leader():
            # We only become leader after bootstrap or once up as a standby, so we are definitely ready.
            return

        # When postgres is not running we are not ready.
        if patroni.postgresql.state != PostgresqlState.RUNNING:
            return 'PostgreSQL is not running'

        postgres = self.get_postgresql_status(True)
        latest_end_lsn = postgres.get('latest_end_lsn', 0)

        if postgres.get('replication_state') != 'streaming':
            return 'PostgreSQL replication state is not streaming'

        cluster = patroni.dcs.cluster

        if not cluster and not latest_end_lsn:
            if patroni.ha.failsafe_is_active():
                return
            return 'DCS is not accessible'

        leader_optime = max(cluster and cluster.status.last_lsn or 0, latest_end_lsn)

        mode = 'write' if self.path_query.get('mode', [None])[0] == 'write' else 'apply'
        location = 'received_location' if mode == 'write' else 'replayed_location'
        lag = leader_optime - postgres.get('xlog', {}).get(location, 0)

        max_replica_lag = parse_int(self.path_query.get('lag', [None])[0], 'B')
        if max_replica_lag is None:
            max_replica_lag = global_config.maximum_lag_on_failover

        if lag > max_replica_lag:
            return f'Replication {mode} lag {lag} exceeds maximum allowable {max_replica_lag}'

    def do_GET_readiness(self) -> None:
        """Handle a ``GET`` request to ``/readiness`` path.

            * Query parameters:

                * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                    represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                    ``10MB``).
                * ``mode``: allowed values ``write``, ``apply``. Base replication lag off of received WAL or
                    replayed WAL. Defaults to ``apply``.

        Write a simple HTTP response which HTTP status can be:

            * ``200``:

                * If this Patroni node considers itself the leader; or
                * If PostgreSQL is running, replicating and not lagging;

            * ``503``: if none of the previous conditions apply.

        """
        failure_reason = self._readiness()

        if failure_reason:
            logger.debug("Readiness check failure: %s", failure_reason)

        self._write_status_code_only(200 if not failure_reason else 503)

    def do_GET_patroni(self) -> None:
        """Handle a ``GET`` request to ``/patroni`` path.

        Write an HTTP response through :func:`_write_status_response`, with HTTP status ``200`` and the status of
        Postgres.
        """
        response = self.get_postgresql_status(True)
        response.pop('latest_end_lsn', None)
        self._write_status_response(200, response)

    def do_GET_cluster(self) -> None:
        """Handle a ``GET`` request to ``/cluster`` path.

        Write an HTTP response with JSON content based on the output of :func:`~patroni.utils.cluster_as_json`, with
        HTTP status ``200`` and the JSON representation of the cluster topology.
        """
        cluster = self.server.patroni.dcs.get_cluster()

        response = cluster_as_json(cluster)
        response['scope'] = self.server.patroni.postgresql.scope
        self._write_json_response(200, response)

    def do_GET_history(self) -> None:
        """Handle a ``GET`` request to ``/history`` path.

        Write an HTTP response with a JSON content representing the history of events in the cluster, with HTTP status
        ``200``.

        The response contains a :class:`list` of failover/switchover events. Each item is a :class:`list` with the
        following items:

            * Timeline when the event occurred (class:`int`);
            * LSN at which the event occurred (class:`int`);
            * The reason for the event (class:`str`);
            * Timestamp when the new timeline was created (class:`str`);
            * Name of the involved Patroni node (class:`str`).

        """
        cluster = self.server.patroni.dcs.cluster or self.server.patroni.dcs.get_cluster()
        self._write_json_response(200, cluster.history and cluster.history.lines or [])

    def do_GET_config(self) -> None:
        """Handle a ``GET`` request to ``/config`` path.

        Write an HTTP response with a JSON content representing the Patroni configuration that is stored in the DCS,
        with HTTP status ``200``.

        If the cluster information is not available in the DCS, then it will respond with no body and HTTP status
        ``502`` instead.
        """
        cluster = self.server.patroni.dcs.cluster or self.server.patroni.dcs.get_cluster()
        if cluster.config and cluster.config.modify_version:
            self._write_json_response(200, cluster.config.data)
        else:
            self.send_error(502)

    def do_GET_metrics(self) -> None:
        """Handle a ``GET`` request to ``/metrics`` path.

        Write an HTTP response with plain text content in the format used by Prometheus, with HTTP status ``200``.

        The response contains the following items:

            * ``patroni_version``: Patroni version without periods, e.g. ``030002`` for Patroni ``3.0.2``;
            * ``patroni_postgres_running``: ``1`` if PostgreSQL is running, else ``0``;
            * ``patroni_postmaster_start_time``: epoch timestamp since Postmaster was started;
            * ``patroni_primary``: ``1`` if this node holds the leader lock, else ``0``;
            * ``patroni_xlog_location``: ``pg_wal_lsn_diff(pg_current_wal_flush_lsn(), '0/0')`` if leader, else ``0``;
            * ``patroni_standby_leader``: ``1`` if standby leader node, else ``0``;
            * ``patroni_replica``: ``1`` if a replica, else ``0``;
            * ``patroni_sync_standby``: ``1`` if a sync replica, else ``0``;
            * ``patroni_quorum_standby``: ``1`` if a quorum sync replica, else ``0``;
            * ``patroni_xlog_received_location``: ``pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')``;
            * ``patroni_xlog_replayed_location``: ``pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0)``;
            * ``patroni_xlog_replayed_timestamp``: ``pg_last_xact_replay_timestamp``;
            * ``patroni_xlog_paused``: ``pg_is_wal_replay_paused()``;
            * ``patroni_postgres_server_version``: Postgres version without periods, e.g. ``150002`` for Postgres
              ``15.2``;
            * ``patroni_cluster_unlocked``: ``1`` if no one holds the leader lock, else ``0``;
            * ``patroni_failsafe_mode_is_active``: ``1`` if ``failsafe_mode`` is currently active, else ``0``;
            * ``patroni_postgres_timeline``: PostgreSQL timeline based on current WAL file name;
            * ``patroni_dcs_last_seen``: epoch timestamp when DCS was last contacted successfully;
            * ``patroni_pending_restart``: ``1`` if this PostgreSQL node is pending a restart, else ``0``;
            * ``patroni_is_paused``: ``1`` if Patroni is in maintenance node, else ``0``.

        For PostgreSQL v9.6+ the response will also have the following:

            * ``patroni_postgres_streaming``: 1 if Postgres is streaming from another node, else ``0``;
            * ``patroni_postgres_in_archive_recovery``: ``1`` if Postgres isn't streaming and
              there is ``restore_command`` available, else ``0``.
        """
        postgres = self.get_postgresql_status(True)
        patroni = self.server.patroni
        epoch = datetime.datetime(1970, 1, 1, tzinfo=tzutc)

        metrics: List[str] = []

        labels = f'{{scope="{patroni.postgresql.scope}",name="{patroni.postgresql.name}"}}'
        metrics.append("# HELP patroni_version Patroni semver without periods.")
        metrics.append("# TYPE patroni_version gauge")
        padded_semver = ''.join([x.zfill(2) for x in patroni.version.split('.')])  # 2.0.2 => 020002
        metrics.append("patroni_version{0} {1}".format(labels, padded_semver))

        metrics.append("# HELP patroni_postgres_running Value is 1 if Postgres is running, 0 otherwise.")
        metrics.append("# TYPE patroni_postgres_running gauge")
        metrics.append("patroni_postgres_running{0} {1}".format(
            labels, int(postgres['state'] == PostgresqlState.RUNNING)))

        metrics.append("# HELP patroni_postmaster_start_time Epoch seconds since Postgres started.")
        metrics.append("# TYPE patroni_postmaster_start_time gauge")
        postmaster_start_time = postgres.get('postmaster_start_time')
        postmaster_start_time = (postmaster_start_time - epoch).total_seconds() if postmaster_start_time else 0
        metrics.append("patroni_postmaster_start_time{0} {1}".format(labels, postmaster_start_time))

        metrics.append("# HELP patroni_primary Value is 1 if this node is the leader, 0 otherwise.")
        metrics.append("# TYPE patroni_primary gauge")
        metrics.append("patroni_primary{0} {1}".format(labels, int(postgres['role'] == PostgresqlRole.PRIMARY)))

        metrics.append("# HELP patroni_xlog_location Current location of the Postgres"
                       " transaction log, 0 if this node is not the leader.")
        metrics.append("# TYPE patroni_xlog_location counter")
        metrics.append("patroni_xlog_location{0} {1}".format(labels, postgres.get('xlog', {}).get('location', 0)))

        metrics.append("# HELP patroni_standby_leader Value is 1 if this node is the standby_leader, 0 otherwise.")
        metrics.append("# TYPE patroni_standby_leader gauge")
        metrics.append("patroni_standby_leader{0} {1}".format(labels,
                                                              int(postgres['role'] == PostgresqlRole.STANDBY_LEADER)))

        metrics.append("# HELP patroni_replica Value is 1 if this node is a replica, 0 otherwise.")
        metrics.append("# TYPE patroni_replica gauge")
        metrics.append("patroni_replica{0} {1}".format(labels, int(postgres['role'] == PostgresqlRole.REPLICA)))

        metrics.append("# HELP patroni_sync_standby Value is 1 if this node is a sync standby, 0 otherwise.")
        metrics.append("# TYPE patroni_sync_standby gauge")
        metrics.append("patroni_sync_standby{0} {1}".format(labels, int(postgres.get('sync_standby', False))))

        metrics.append("# HELP patroni_quorum_standby Value is 1 if this node is a quorum standby, 0 otherwise.")
        metrics.append("# TYPE patroni_quorum_standby gauge")
        metrics.append("patroni_quorum_standby{0} {1}".format(labels, int(postgres.get('quorum_standby', False))))

        metrics.append("# HELP patroni_xlog_received_location Current location of the received"
                       " Postgres transaction log, 0 if this node is not a replica.")
        metrics.append("# TYPE patroni_xlog_received_location counter")
        metrics.append("patroni_xlog_received_location{0} {1}"
                       .format(labels, postgres.get('xlog', {}).get('received_location', 0)))

        metrics.append("# HELP patroni_xlog_replayed_location Current location of the replayed"
                       " Postgres transaction log, 0 if this node is not a replica.")
        metrics.append("# TYPE patroni_xlog_replayed_location counter")
        metrics.append("patroni_xlog_replayed_location{0} {1}"
                       .format(labels, postgres.get('xlog', {}).get('replayed_location', 0)))

        metrics.append("# HELP patroni_xlog_replayed_timestamp Current timestamp of the replayed"
                       " Postgres transaction log, 0 if null.")
        metrics.append("# TYPE patroni_xlog_replayed_timestamp gauge")
        replayed_timestamp = postgres.get('xlog', {}).get('replayed_timestamp')
        replayed_timestamp = (replayed_timestamp - epoch).total_seconds() if replayed_timestamp else 0
        metrics.append("patroni_xlog_replayed_timestamp{0} {1}".format(labels, replayed_timestamp))

        metrics.append("# HELP patroni_xlog_paused Value is 1 if the Postgres xlog is paused, 0 otherwise.")
        metrics.append("# TYPE patroni_xlog_paused gauge")
        metrics.append("patroni_xlog_paused{0} {1}"
                       .format(labels, int(postgres.get('xlog', {}).get('paused', False) is True)))

        if postgres.get('server_version', 0) >= 90600:
            metrics.append("# HELP patroni_postgres_streaming Value is 1 if Postgres is streaming, 0 otherwise.")
            metrics.append("# TYPE patroni_postgres_streaming gauge")
            metrics.append("patroni_postgres_streaming{0} {1}"
                           .format(labels, int(postgres.get('replication_state') == 'streaming')))

            metrics.append("# HELP patroni_postgres_in_archive_recovery Value is 1"
                           " if Postgres is replicating from archive, 0 otherwise.")
            metrics.append("# TYPE patroni_postgres_in_archive_recovery gauge")
            metrics.append("patroni_postgres_in_archive_recovery{0} {1}"
                           .format(labels, int(postgres.get('replication_state') == 'in archive recovery')))

        metrics.append("# HELP patroni_postgres_server_version Version of Postgres (if running), 0 otherwise.")
        metrics.append("# TYPE patroni_postgres_server_version gauge")
        metrics.append("patroni_postgres_server_version {0} {1}".format(labels, postgres.get('server_version', 0)))

        metrics.append("# HELP patroni_cluster_unlocked Value is 1 if the cluster is unlocked, 0 if locked.")
        metrics.append("# TYPE patroni_cluster_unlocked gauge")
        metrics.append("patroni_cluster_unlocked{0} {1}".format(labels, int(postgres.get('cluster_unlocked', 0))))

        metrics.append("# HELP patroni_failsafe_mode_is_active Value is 1 if failsafe mode is active, 0 if inactive.")
        metrics.append("# TYPE patroni_failsafe_mode_is_active gauge")
        metrics.append("patroni_failsafe_mode_is_active{0} {1}"
                       .format(labels, int(postgres.get('failsafe_mode_is_active', 0))))

        metrics.append("# HELP patroni_postgres_timeline Postgres timeline of this node (if running), 0 otherwise.")
        metrics.append("# TYPE patroni_postgres_timeline counter")
        metrics.append("patroni_postgres_timeline{0} {1}".format(labels, postgres.get('timeline') or 0))

        metrics.append("# HELP patroni_dcs_last_seen Epoch timestamp when DCS was last contacted successfully"
                       " by Patroni.")
        metrics.append("# TYPE patroni_dcs_last_seen gauge")
        metrics.append("patroni_dcs_last_seen{0} {1}".format(labels, postgres.get('dcs_last_seen', 0)))

        metrics.append("# HELP patroni_pending_restart Value is 1 if the node needs a restart, 0 otherwise.")
        metrics.append("# TYPE patroni_pending_restart gauge")
        metrics.append("patroni_pending_restart{0} {1}"
                       .format(labels, int(bool(patroni.postgresql.pending_restart_reason))))

        metrics.append("# HELP patroni_is_paused Value is 1 if auto failover is disabled, 0 otherwise.")
        metrics.append("# TYPE patroni_is_paused gauge")
        metrics.append("patroni_is_paused{0} {1}".format(labels, int(postgres.get('pause', 0))))

        metrics.append("# HELP patroni_postgres_state Numeric representation of Postgres state.")
        # Generate description of all state values for metrics documentation
        state_descriptions = [f"{state.index}={state.name.lower()}" for state in PostgresqlState]
        metrics.append(f"# Values: {', '.join(state_descriptions)}")
        metrics.append("# TYPE patroni_postgres_state gauge")
        current_state = postgres['state']
        state_value = current_state.index if isinstance(current_state, PostgresqlState) else -1
        metrics.append(f"patroni_postgres_state{labels} {state_value}")

        self.write_response(200, '\n'.join(metrics) + '\n', content_type='text/plain')

    def _read_json_content(self, body_is_optional: bool = False) -> Optional[Dict[Any, Any]]:
        """Read JSON from HTTP request body.

        .. note::
            Retrieves the request body based on `content-length` HTTP header. The body is expected to be a JSON
            string with that length.

            If request body is expected but `content-length` HTTP header is absent, then write an HTTP response
            with HTTP status ``411``.

            If request body is expected but contains nothing, or if an exception is faced, then write an HTTP
            response with HTTP status ``400``.

        :param body_is_optional: if ``False`` then the request must contain a body. If ``True``, then the request may or
            may not contain a body.

        :returns: deserialized JSON string from request body, if present. If body is absent, but *body_is_optional* is
            ``True``, then return an empty dictionary. Returns ``None`` otherwise.
        """
        if 'content-length' not in self.headers:
            return self.send_error(411) if not body_is_optional else {}
        try:
            content_length = int(self.headers.get('content-length') or 0)
            if content_length == 0 and body_is_optional:
                return {}
            request = json.loads(self.rfile.read(content_length).decode('utf-8'))
            if isinstance(request, dict) and (request or body_is_optional):
                return cast(Dict[str, Any], request)
        except Exception:
            logger.exception('Bad request')
        self.send_error(400)

    @check_access
    def do_PATCH_config(self) -> None:
        """Handle a ``PATCH`` request to ``/config`` path.

        Updates the Patroni configuration based on the JSON request body, then writes a response with the new
        configuration, with HTTP status ``200``.

        .. note::
            If the configuration has been previously wiped out from DCS, then write a response with
            HTTP status ``503``.

            If applying a configuration value fails, then write a response with HTTP status ``409``.
        """
        request = self._read_json_content()
        if request:
            cluster = self.server.patroni.dcs.get_cluster()
            if not (cluster.config and cluster.config.modify_version):
                return self.send_error(503)
            data = cluster.config.data.copy()
            if patch_config(data, request):
                value = json.dumps(data, separators=(',', ':'))
                if not self.server.patroni.dcs.set_config_value(value, cluster.config.version):
                    return self.send_error(409)
            self.server.patroni.ha.wakeup()
            self._write_json_response(200, data)

    @check_access
    def do_PUT_config(self) -> None:
        """Handle a ``PUT`` request to ``/config`` path.

        Overwrites the Patroni configuration based on the JSON request body, then writes a response with the new
        configuration, with HTTP status ``200``.

        .. note::
            If applying the new configuration fails, then write a response with HTTP status ``502``.
        """
        request = self._read_json_content()
        if request:
            cluster = self.server.patroni.dcs.get_cluster()
            if not (cluster.config and deep_compare(request, cluster.config.data)):
                value = json.dumps(request, separators=(',', ':'))
                if not self.server.patroni.dcs.set_config_value(value):
                    return self.send_error(502)
            self._write_json_response(200, request)

    @check_access
    def do_POST_reload(self) -> None:
        """Handle a ``POST`` request to ``/reload`` path.

        Schedules a reload to Patroni and writes a response with HTTP status ``202``.
        """
        self.server.patroni.sighup_handler()
        self.write_response(202, 'reload scheduled')

    def do_GET_failsafe(self) -> None:
        """Handle a ``GET`` request to ``/failsafe`` path.

        Writes a response with a JSON string body containing all nodes that are known to Patroni at a given point
        in time, with HTTP status ``200``. The JSON contains a dictionary, each key is the name of the Patroni node,
        and the corresponding value is the URI to access `/patroni` path of its REST API.

        .. note::
            If ``failsafe_mode`` is not enabled, then write a response with HTTP status ``502``.
        """
        failsafe = self.server.patroni.dcs.failsafe
        if isinstance(failsafe, dict):
            self._write_json_response(200, failsafe)
        else:
            self.send_error(502)

    @check_access(allowlist_check_members=False)
    def do_POST_failsafe(self) -> None:
        """Handle a ``POST`` request to ``/failsafe`` path.

        Writes a response with HTTP status ``200`` if this node is a Standby, or with HTTP status ``500`` if this is
        the primary. In addition to that it returns absolute value of received/replayed LSN in the ``lsn`` header.

        .. note::
            If ``failsafe_mode`` is not enabled, then write a response with HTTP status ``502``.
        """
        if self.server.patroni.ha.is_failsafe_mode():
            request = self._read_json_content()
            if request:
                ret = self.server.patroni.ha.update_failsafe(request)
                headers = {'lsn': str(ret)} if isinstance(ret, int) else {}
                message = ret if isinstance(ret, str) else 'Accepted'
                code = 200 if message == 'Accepted' else 500
                self.write_response(code, message, headers=headers)
        else:
            self.send_error(502)

    @check_access
    def do_POST_sigterm(self) -> None:
        """Handle a ``POST`` request to ``/sigterm`` path.

        Schedule a shutdown and write a response with HTTP status ``202``.

        .. note::
            Only for behave testing on Windows.
        """
        if os.name == 'nt' and os.getenv('BEHAVE_DEBUG'):
            self.server.patroni.api_sigterm()
        self.write_response(202, 'shutdown scheduled')

    @staticmethod
    def parse_schedule(schedule: str,
                       action: str) -> Tuple[Union[int, None], Union[str, None], Union[datetime.datetime, None]]:
        """Parse the given *schedule* and validate it.

        :param schedule: a string representing a timestamp, e.g. ``2023-04-14T20:27:00+00:00``.
        :param action: the action to be scheduled (``restart``, ``switchover``, or ``failover``).

        :returns: a tuple composed of 3 items:

            * Suggested HTTP status code for a response:

                * ``None``: if no issue was faced while parsing, leaving it up to the caller to decide the status; or
                * ``400``: if no timezone information could be found in *schedule*; or
                * ``422``: if *schedule* is invalid -- in the past or not parsable.

            * An error message, if any error is faced, otherwise ``None``;
            * Parsed *schedule*, if able to parse, otherwise ``None``.

        """
        error = None
        scheduled_at = None
        try:
            scheduled_at = dateutil.parser.parse(schedule)
            if scheduled_at.tzinfo is None:
                error = 'Timezone information is mandatory for the scheduled {0}'.format(action)
                status_code = 400
            elif scheduled_at < datetime.datetime.now(tzutc):
                error = 'Cannot schedule {0} in the past'.format(action)
                status_code = 422
            else:
                status_code = None
        except (ValueError, TypeError):
            logger.exception('Invalid scheduled %s time: %s', action, schedule)
            error = 'Unable to parse scheduled timestamp. It should be in an unambiguous format, e.g. ISO 8601'
            status_code = 422
        return status_code, error, scheduled_at

    @check_access
    def do_POST_restart(self) -> None:
        """Handle a ``POST`` request to ``/restart`` path.

        Used to restart postgres (or schedule a restart), mainly by ``patronictl restart``.

        The request body should be a JSON dictionary, and it can contain the following keys:

            * ``schedule``: timestamp at which the restart should occur;
            * ``role``: restart only nodes which role is ``role``. Can be either:

                * ``primary`; or
                * ``replica``.

            * ``postgres_version``: restart only nodes which PostgreSQL version is less than ``postgres_version``, e.g.
                ``15.2``;
            * ``timeout``: if restart takes longer than ``timeout`` return an error and fail over to a replica;
            * ``restart_pending``: if we should restart only when have ``pending restart`` flag;

        Response HTTP status codes:

            * ``200``: if successfully performed an immediate restart; or
            * ``202``: if successfully scheduled a restart for later; or
            * ``500``: if the cluster is in maintenance mode; or
            * ``400``: if

                * ``role`` value is invalid; or
                * ``postgres_version`` value is invalid; or
                * ``timeout`` is not a number, or lesser than ``0``; or
                * request contains an unknown key; or
                * exception is faced while performing an immediate restart.

            * ``409``: if another restart was already previously scheduled; or
            * ``503``: if any issue was found while performing an immediate restart; or
            * HTTP status returned by :func:`parse_schedule`, if any error was observed while parsing the schedule.

        .. note::
            If it's not able to parse the request body, then the request is silently discarded.
        """
        status_code = 500
        data = PostgresqlState.RESTART_FAILED
        request = self._read_json_content(body_is_optional=True)
        cluster = self.server.patroni.dcs.get_cluster()
        if request is None:
            # failed to parse the json
            return
        if request:
            logger.debug("received restart request: {0}".format(request))

        if global_config.from_cluster(cluster).is_paused and 'schedule' in request:
            self.write_response(status_code, "Can't schedule restart in the paused state")
            return

        for k in request:
            if k == 'schedule':
                (_, data, request[k]) = self.parse_schedule(request[k], "restart")
                if _:
                    status_code = _
                    break
            elif k == 'role':
                if request[k] not in (PostgresqlRole.PRIMARY, PostgresqlRole.STANDBY_LEADER, PostgresqlRole.REPLICA):
                    status_code = 400
                    data = "PostgreSQL role should be either primary, standby_leader, or replica"
                    break
            elif k == 'postgres_version':
                try:
                    postgres_version_to_int(request[k])
                except PostgresException as e:
                    status_code = 400
                    data = e.value
                    break
            elif k == 'timeout':
                request[k] = parse_int(request[k], 's')
                if request[k] is None or request[k] <= 0:
                    status_code = 400
                    data = "Timeout should be a positive number of seconds"
                    break
            elif k != 'restart_pending':
                status_code = 400
                data = "Unknown filter for the scheduled restart: {0}".format(k)
                break
        else:
            if 'schedule' not in request:
                try:
                    status, data = self.server.patroni.ha.restart(request)
                    status_code = 200 if status else 503
                except Exception:
                    logger.exception('Exception during restart')
                    status_code = 400
            else:
                if self.server.patroni.ha.schedule_future_restart(request):
                    data = "Restart scheduled"
                    status_code = 202
                else:
                    data = "Another restart is already scheduled"
                    status_code = 409
        # pyright thinks ``data`` can be ``None`` because ``parse_schedule`` call may return ``None``. However, if
        # that's the case, ``data`` will be overwritten when the ``for`` loop ends
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(data, str)
        self.write_response(status_code, data)

    @check_access
    def do_DELETE_restart(self) -> None:
        """Handle a ``DELETE`` request to ``/restart`` path.

        Used to remove a scheduled restart of PostgreSQL.

        Response HTTP status codes:

            * ``200``: if a scheduled restart was removed; or
            * ``404``: if no scheduled restart could be found.
        """
        if self.server.patroni.ha.delete_future_restart():
            data = "scheduled restart deleted"
            code = 200
        else:
            data = "no restarts are scheduled"
            code = 404
        self.write_response(code, data)

    @check_access
    def do_DELETE_switchover(self) -> None:
        """Handle a ``DELETE`` request to ``/switchover`` path.

        Used to remove a scheduled switchover in the cluster.

        It writes a response, and the HTTP status code can be:

            * ``200``: if a scheduled switchover was removed; or
            * ``404``: if no scheduled switchover could be found; or
            * ``409``: if not able to update the switchover info in the DCS.
        """
        failover = self.server.patroni.dcs.get_cluster().failover
        if failover and failover.scheduled_at:
            if not self.server.patroni.dcs.manual_failover('', '', version=failover.version):
                return self.send_error(409)
            else:
                data = "scheduled switchover deleted"
                code = 200
        else:
            data = "no switchover is scheduled"
            code = 404
        self.write_response(code, data)

    @check_access
    def do_POST_reinitialize(self) -> None:
        """Handle a ``POST`` request to ``/reinitialize`` path.

        The request body may contain a JSON dictionary with the following key:

            * ``force``: ``True`` if we want to cancel an already running task in order to reinit a replica.
            * ``from_leader``: ``True`` if we want to reinit a replica and get basebackup from the leader node.

        Response HTTP status codes:

            * ``200``: if the reinit operation has started; or
            * ``503``: if any error is returned by :func:`~patroni.ha.Ha.reinitialize`.
        """
        request = self._read_json_content(body_is_optional=True)

        if request:
            logger.debug('received reinitialize request: %s', request)

        force = isinstance(request, dict) and parse_bool(request.get('force')) or False
        from_leader = isinstance(request, dict) and parse_bool(request.get('from_leader')) or False

        data = self.server.patroni.ha.reinitialize(force, from_leader)
        if data is None:
            status_code = 200
            data = 'reinitialize started'
        else:
            status_code = 503
        self.write_response(status_code, data)

    def poll_failover_result(self, leader: Optional[str], candidate: Optional[str], action: str) -> Tuple[int, str]:
        """Poll failover/switchover operation until it finishes or times out.

        :param leader: name of the current Patroni leader.
        :param candidate: name of the Patroni node to be promoted.
        :param action: the action that is ongoing (``switchover`` or ``failover``).

        :returns: a tuple composed of 2 items:

            * Response HTTP status codes:

                * ``200``: if the operation succeeded; or
                * ``503``: if the operation failed or timed out.

            * A status message about the operation.

        """
        timeout = max(10, self.server.patroni.dcs.loop_wait)
        for _ in range(0, timeout * 2):
            time.sleep(1)
            try:
                cluster = self.server.patroni.dcs.get_cluster()
                if not cluster.is_unlocked() and cluster.leader and cluster.leader.name != leader:
                    if not candidate or candidate == cluster.leader.name:
                        return 200, 'Successfully {0}ed over to "{1}"'.format(action[:-4], cluster.leader.name)
                    else:
                        return 200, '{0}ed over to "{1}" instead of "{2}"'.format(action[:-4].title(),
                                                                                  cluster.leader.name, candidate)
                if not cluster.failover:
                    return 503, action.title() + ' failed'
            except Exception as e:
                logger.debug('Exception occurred during polling %s result: %s', action, e)
        return 503, action.title() + ' status unknown'

    def is_failover_possible(self, cluster: Cluster, leader: Optional[str], candidate: Optional[str],
                             action: str) -> Optional[str]:
        """Checks whether there are nodes that could take over after demoting the primary.

        :param cluster: the Patroni cluster.
        :param leader: name of the current Patroni leader.
        :param candidate: name of the Patroni node to be promoted.
        :param action: the action to be performed (``switchover`` or ``failover``).

        :returns: a string with the error message or ``None`` if good nodes are found.
        """
        config = global_config.from_cluster(cluster)
        if leader and (not cluster.leader or cluster.leader.name != leader):
            return 'leader name does not match'
        if candidate:
            if action == 'switchover' and config.is_synchronous_mode\
                    and not config.is_quorum_commit_mode and not cluster.sync.matches(candidate):
                return 'candidate name does not match with sync_standby'
            members = [m for m in cluster.members if m.name == candidate]
            if not members:
                return 'candidate does not exists'
        elif config.is_synchronous_mode and not config.is_quorum_commit_mode:
            members = [m for m in cluster.members if cluster.sync.matches(m.name)]
            if not members:
                return action + ' is not possible: can not find sync_standby'
        else:
            members = [m for m in cluster.members if not cluster.leader or m.name != cluster.leader.name and m.api_url]
            if not members:
                return action + ' is not possible: cluster does not have members except leader'
        for st in self.server.patroni.ha.fetch_nodes_statuses(members):
            if st.failover_limitation() is None:
                return None
        return action + ' is not possible: no good candidates have been found'

    @check_access
    def do_POST_failover(self, action: str = 'failover') -> None:
        """Handle a ``POST`` request to ``/failover`` path.

        Handles manual failovers/switchovers, mainly from ``patronictl``.

        The request body should be a JSON dictionary, and it can contain the following keys:

            * ``leader``: name of the current leader in the cluster;
            * ``candidate``: name of the Patroni node to be promoted;
            * ``scheduled_at``: a string representing the timestamp when to execute the switchover/failover, e.g.
                ``2023-04-14T20:27:00+00:00``.

        Response HTTP status codes:

            * ``202``: if operation has been scheduled;
            * ``412``: if operation is not possible;
            * ``503``: if unable to register the operation to the DCS;
            * HTTP status returned by :func:`parse_schedule`, if any error was observed while parsing the schedule;
            * HTTP status returned by :func:`poll_failover_result` if the operation has been processed immediately;
            * ``400``: if none of the above applies.

        .. note::
            If unable to parse the request body, then the request is silently discarded.

        :param action: the action to be performed (``switchover`` or ``failover``).
        """
        request = self._read_json_content()
        (status_code, data) = (400, '')
        if not request:
            return

        leader = request.get('leader')
        candidate = request.get('candidate') or request.get('member')
        scheduled_at = request.get('scheduled_at')
        cluster = self.server.patroni.dcs.get_cluster()
        config = global_config.from_cluster(cluster)

        logger.info("received %s request with leader=%s candidate=%s scheduled_at=%s",
                    action, leader, candidate, scheduled_at)

        if action == 'failover' and not candidate:
            data = 'Failover could be performed only to a specific candidate'
        elif action == 'switchover' and not leader:
            data = 'Switchover could be performed only from a specific leader'

        if not data and scheduled_at:
            if action == 'failover':
                data = "Failover can't be scheduled"
            elif config.is_paused:
                data = "Can't schedule switchover in the paused state"
            else:
                (status_code, data, scheduled_at) = self.parse_schedule(scheduled_at, action)

        if not data and config.is_paused and not candidate:
            data = 'Switchover is possible only to a specific candidate in a paused state'

        if action == 'failover' and leader:
            logger.warning('received failover request with leader specified - performing switchover instead')
            action = 'switchover'

        if not data and leader == candidate:
            data = 'Switchover target and source are the same'

        if not data and not scheduled_at:
            data = self.is_failover_possible(cluster, leader, candidate, action)
            if data:
                status_code = 412

        if not data:
            if self.server.patroni.dcs.manual_failover(leader, candidate, scheduled_at=scheduled_at):
                self.server.patroni.ha.wakeup()
                if scheduled_at:
                    data = action.title() + ' scheduled'
                    status_code = 202
                else:
                    status_code, data = self.poll_failover_result(cluster.leader and cluster.leader.name,
                                                                  candidate, action)
            else:
                data = 'failed to write failover key into DCS'
                status_code = 503
        # pyright thinks ``status_code`` can be ``None`` because ``parse_schedule`` call may return ``None``. However,
        # if that's the case, ``status_code`` will be overwritten somewhere between ``parse_schedule`` and
        # ``write_response`` calls.
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(status_code, int)
        self.write_response(status_code, data)

    def do_POST_switchover(self) -> None:
        """Handle a ``POST`` request to ``/switchover`` path.

        Calls :func:`do_POST_failover` with ``switchover`` option.
        """
        self.do_POST_failover(action='switchover')

    @check_access
    def do_POST_citus(self) -> None:
        """Handle a ``POST`` request to ``/citus`` path.

        .. note::
            We keep this entrypoint for backward compatibility and simply dispatch the request to :meth:`do_POST_mpp`.
        """
        self.do_POST_mpp()

    def do_POST_mpp(self) -> None:
        """Handle a ``POST`` request to ``/mpp`` path.

        Call :func:`~patroni.postgresql.mpp.AbstractMPPHandler.handle_event` to handle the request,
        then write a response with HTTP status code ``200``.

        .. note::
            If unable to parse the request body, then the request is silently discarded.
        """
        request = self._read_json_content()
        if not request:
            return

        patroni = self.server.patroni
        if patroni.postgresql.mpp_handler.is_coordinator() and patroni.ha.is_leader():
            cluster = patroni.dcs.get_cluster()
            patroni.postgresql.mpp_handler.handle_event(cluster, request)
        self.write_response(200, 'OK')

    def parse_request(self) -> bool:
        """Override :func:`parse_request` to enrich basic functionality of :class:`~http.server.BaseHTTPRequestHandler`.

        Original class can only invoke :func:`do_GET`, :func:`do_POST`, :func:`do_PUT`, etc method implementations if
        they are defined.

        But we would like to have at least some simple routing mechanism, i.e.:

            * ``GET /uri1/part2`` request should invoke :func:`do_GET_uri1()`
            * ``POST /other`` should invoke :func:`do_POST_other()`

        If the :func:`do_<REQUEST_METHOD>_<first_part_url>` method does not exist we'll fall back to original behavior.

        :returns: ``True`` for success, ``False`` for failure; on failure, any relevant error response has already been
                  sent back.

        """
        ret = BaseHTTPRequestHandler.parse_request(self)
        if ret:
            urlpath = urlparse(self.path)
            self.path = urlpath.path
            self.path_query = parse_qs(urlpath.query) or {}
            mname = self.path.lstrip('/').split('/')[0]
            mname = self.command + ('_' + mname if mname else '')
            if hasattr(self, 'do_' + mname):
                self.command = mname
        return ret

    def query(self, sql: str, *params: Any, retry: bool = False) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        :param sql: the SQL statement to be run.
        :param params: positional arguments to call :func:`RestApiServer.query` with.
        :param retry: whether the query should be retried upon failure or given up immediately.

        :returns: a list of rows that were fetched from the database.
        """
        if not retry:
            return self.server.query(sql, *params)
        return Retry(delay=1, retry_exceptions=PostgresConnectionException)(self.server.query, sql, *params)

    def get_postgresql_status(self, retry: bool = False) -> Dict[str, Any]:
        """Builds an object representing a status of "postgres".

        Some of the values are collected by executing a query and other are taken from the state stored in memory.

        :param retry: whether the query should be retried if failed or give up immediately

        :returns: a dict with the status of Postgres/Patroni. The keys are:

            * ``state``: one of :class:`~patroni.postgresql.misc.PostgresqlState` or ``unknown``;
            * ``postmaster_start_time``: ``pg_postmaster_start_time()``;
            * ``role``: :class:`~patroni.postgresql.misc.PostgresqlRole.REPLICA` or
                :class:`~patroni.postgresql.misc.PostgresqlRole.PRIMARY` based on ``pg_is_in_recovery()`` output;
            * ``server_version``: Postgres version without periods, e.g. ``150002`` for Postgres ``15.2``;
            * ``latest_end_lsn``: latest_end_lsn value from ``pg_stat_get_wal_receiver()``, only on replica nodes;
            * ``xlog``: dictionary. Its structure depends on ``role``:

                * If :class:`~patroni.postgresql.misc.PostgresqlRole.PRIMARY`:

                    * ``location``: ``pg_current_wal_flush_lsn()``

                * If :class:`~patroni.postgresql.misc.PostgresqlRole.REPLICA`:

                    * ``received_location``: ``pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')``;
                    * ``replayed_location``: ``pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0)``;
                    * ``replayed_timestamp``: ``pg_last_xact_replay_timestamp``;
                    * ``paused``: ``pg_is_wal_replay_paused()``;

            * ``sync_standby``: ``True`` if replication mode is synchronous and this is a sync standby;
            * ``quorum_standby``: ``True`` if replication mode is quorum and this is a quorum standby;
            * ``timeline``: PostgreSQL primary node timeline;
            * ``replication``: :class:`list` of :class:`dict` entries, one for each replication connection. Each entry
                contains the following keys:

                * ``application_name``: ``pg_stat_activity.application_name``;
                * ``client_addr``: ``pg_stat_activity.client_addr``;
                * ``state``: ``pg_stat_replication.state``;
                * ``sync_priority``: ``pg_stat_replication.sync_priority``;
                * ``sync_state``: ``pg_stat_replication.sync_state``;
                * ``usename``: ``pg_stat_activity.usename``.

            * ``pause``: ``True`` if cluster is in maintenance mode;
            * ``cluster_unlocked``: ``True`` if cluster has no node holding the leader lock;
            * ``failsafe_mode_is_active``: ``True`` if DCS failsafe mode is currently active;
            * ``dcs_last_seen``: epoch timestamp DCS was last reached by Patroni.

        """
        postgresql = self.server.patroni.postgresql
        cluster = self.server.patroni.dcs.cluster
        config = global_config.from_cluster(cluster)
        try:

            if postgresql.state not in (PostgresqlState.RUNNING, PostgresqlState.RESTARTING,
                                        PostgresqlState.STARTING):
                raise RetryFailedError('')
            replication_state = ("pg_catalog.pg_{0}_{1}_diff(wr.latest_end_lsn, '0/0')::bigint, wr.status"
                                 if postgresql.major_version >= 90600 else "NULL, NULL") + ", " +\
                ("pg_catalog.current_setting('restore_command')" if postgresql.major_version >= 120000 else "NULL") +\
                ", " + ("pg_catalog.pg_wal_lsn_diff(wr.written_lsn, '0/0')::bigint"
                        if postgresql.major_version >= 130000 else "NULL")
            stmt = ("SELECT " + postgresql.POSTMASTER_START_TIME + ", " + postgresql.TL_LSN + ","
                    " pg_catalog.pg_last_xact_replay_timestamp(), " + replication_state + ","
                    " (SELECT pg_catalog.array_to_json(pg_catalog.array_agg(pg_catalog.row_to_json(ri))) "
                    "FROM (SELECT (SELECT rolname FROM pg_catalog.pg_authid WHERE oid = usesysid) AS usename,"
                    " application_name, client_addr, w.state, sync_state, sync_priority"
                    " FROM pg_catalog.pg_stat_get_wal_senders() w, pg_catalog.pg_stat_get_activity(pid)) AS ri)") +\
                (" FROM pg_catalog.pg_stat_get_wal_receiver() AS wr" if postgresql.major_version >= 90600 else "")

            row = self.query(stmt.format(postgresql.wal_name, postgresql.lsn_name,
                                         postgresql.wal_flush), retry=retry)[0]
            result = {
                'state': postgresql.state,
                'postmaster_start_time': row[0],
                'role': PostgresqlRole.REPLICA if row[1] == 0 else PostgresqlRole.PRIMARY,
                'server_version': postgresql.server_version,
                'xlog': ({
                    'received_location': row[10] or row[4] or row[3],
                    'replayed_location': row[3],
                    'replayed_timestamp': row[6],
                    'paused': row[5]} if row[1] == 0 else {
                    'location': row[2]
                })
            }

            if result['role'] == PostgresqlRole.REPLICA and config.is_standby_cluster:
                result['role'] = postgresql.role

            if result['role'] == PostgresqlRole.REPLICA and config.is_synchronous_mode\
                    and cluster and cluster.sync.matches(postgresql.name):
                result['quorum_standby' if global_config.is_quorum_commit_mode else 'sync_standby'] = True

            if row[1] > 0:
                result['timeline'] = row[1]
            else:
                leader_timeline = None\
                    if not cluster or cluster.is_unlocked() or not cluster.leader else cluster.leader.timeline
                result['timeline'] = postgresql.replica_cached_timeline(leader_timeline)

            if row[7]:
                result['latest_end_lsn'] = row[7]

            replication_state = postgresql.replication_state_from_parameters(row[1] > 0, row[8], row[9])
            if replication_state:
                result['replication_state'] = replication_state

            if row[11]:
                result['replication'] = row[11]

        except (psycopg.Error, RetryFailedError, PostgresConnectionException):
            state = postgresql.state
            if state == PostgresqlState.RUNNING:
                logger.exception('get_postgresql_status')
                state = 'unknown'
            result: Dict[str, Any] = {'state': state, 'role': postgresql.role}

        if config.is_paused:
            result['pause'] = True
        if not cluster or cluster.is_unlocked():
            result['cluster_unlocked'] = True
        if self.server.patroni.ha.failsafe_is_active():
            result['failsafe_mode_is_active'] = True
        result['dcs_last_seen'] = self.server.patroni.dcs.last_seen
        return result

    def handle_one_request(self) -> None:
        """Parse and dispatch a request to the appropriate ``do_*`` method.

        .. note::
            This is only used to keep track of latency when logging messages through :func:`log_message`.
        """
        self.__start_time = time.time()
        BaseHTTPRequestHandler.handle_one_request(self)

    def log_message(self, format: str, *args: Any) -> None:
        """Log a custom ``debug`` message.

        Additionally, to *format*, the log entry contains the client IP address and the current latency of the request.

        :param format: printf-style format string message to be logged.
        :param args: arguments to be applied as inputs to *format*.
        """
        latency = 1000.0 * (time.time() - self.__start_time)
        logger.debug("API thread: %s - - %s latency: %0.3f ms", self.client_address[0], format % args, latency)


class RestApiServer(ThreadingMixIn, HTTPServer, Thread):
    """Patroni REST API server.

    An asynchronous thread-based HTTP server.
    """

    # On 3.7+ the `ThreadingMixIn` gathers all non-daemon worker threads in order to join on them at server close.
    daemon_threads = True  # Make worker threads "fire and forget" to prevent a memory leak.

    def __init__(self, patroni: Patroni, config: Dict[str, Any]) -> None:
        """Establish patroni configuration for the REST API daemon.

        Create a :class:`RestApiServer` instance.

        :param patroni: Patroni daemon process.
        :param config: ``restapi`` section of Patroni configuration.
        """
        self.connection_string: str
        self.__auth_key = None
        self.__allowlist_include_members: Optional[bool] = None
        self.__allowlist: Tuple[Union[IPv4Network, IPv6Network], ...] = ()
        self.http_extra_headers: Dict[str, str] = {}
        self.patroni = patroni
        self.__listen = None
        self.request_queue_size = int(config.get('request_queue_size', 5))
        self.__ssl_options: Dict[str, Any] = {}
        self.__ssl_serial_number = None
        self._received_new_cert = False
        self.reload_config(config)
        self.daemon = True

    def construct_server_tokens(self, token_config: str) -> str:
        """Construct the value for the ``Server`` HTTP header based on *server_tokens*.

        :param server_tokens: the value of ``restapi.server_tokens`` configuration option.

        :returns: a string to be used as the value of ``Server`` HTTP header.
        """
        token = token_config.lower()
        logger.debug('restapi.server_tokens is set to "%s".', token_config)

        # If 'original' is set, we do not modify the Server header.
        # This is useful for compatibility with existing setups that expect the original header.
        if token == 'original':
            return ""

        # If 'productonly', or 'minimal' is set, we construct the header accordingly.
        if token == 'productonly':  # Show only the product name, without versions.
            return 'Patroni'
        elif token == 'minimal':    # Show only the product name and version, without PostgreSQL version.
            return f'Patroni/{self.patroni.version}'
        else:
            # Token is not valid (one of 'original', 'productonly', 'minimal') so report a warning and
            # return an empty string.
            logger.warning('restapi.server_tokens is set to "%s". Patroni will not modify the Server header. '
                           'Valid values are: "Minimal", "ProductOnly".', token_config)
            return ""

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        .. note::
            Prefer to use own connection to postgres and fallback to ``heartbeat`` when own isn't available.

        :param sql: the SQL statement to be run.
        :param params: positional arguments to be used as parameters for *sql*.

        :returns: a list of rows that were fetched from the database.

        :raises:
            :class:`psycopg.Error`: if had issues while executing *sql*.
            :class:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.
        """
        # We first try to get a heartbeat connection because it is always required for the main thread.
        try:
            heartbeat_connection = self.patroni.postgresql.connection_pool.get('heartbeat')
            heartbeat_connection.get()  # try to open psycopg connection to postgres
        except psycopg.Error as exc:
            raise PostgresConnectionException('connection problems') from exc

        try:
            connection = self.patroni.postgresql.connection_pool.get('restapi')
            connection.get()  # try to open psycopg connection to postgres
        except psycopg.Error:
            logger.debug('restapi connection to postgres is not available')
            connection = heartbeat_connection

        return connection.query(sql, *params)

    @staticmethod
    def _set_fd_cloexec(fd: socket.socket) -> None:
        """Set ``FD_CLOEXEC`` for *fd*.

        It is used to avoid inheriting the REST API port when forking its process.

        .. note::
            Only takes effect on non-Windows environments.

        :param fd: socket file descriptor.
        """
        if os.name != 'nt':
            import fcntl
            flags = fcntl.fcntl(fd, fcntl.F_GETFD)
            fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    def check_basic_auth_key(self, key: str) -> bool:
        """Check if *key* matches the password configured for the REST API.

        :param key: the password received through the Basic authorization header of an HTTP request.

        :returns: ``True`` if *key* matches the password configured for the REST API.
        """
        # pyright -- ``__auth_key`` was already checked through the caller method (:func:`check_auth_header`).
        if TYPE_CHECKING:  # pragma: no cover
            assert self.__auth_key is not None
        return hmac.compare_digest(self.__auth_key, key.encode('utf-8'))

    def check_auth_header(self, auth_header: Optional[str]) -> Optional[str]:
        """Validate HTTP Basic authorization header, if present.

        :param auth_header: value of ``Authorization`` HTTP header, if present, else ``None``.

        :returns: an error message if any issue is found, ``None`` otherwise.
        """
        if self.__auth_key:
            if auth_header is None:
                return 'no auth header received'
            if not auth_header.startswith('Basic ') or not self.check_basic_auth_key(auth_header[6:]):
                return 'not authenticated'

    @staticmethod
    def __resolve_ips(host: str, port: int) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve *host* + *port* to one or more IP networks.

        :param host: hostname to be checked.
        :param port: port to be checked.

        :yields: *host* + *port* resolved to IP networks.
        """
        try:
            for _, _, _, _, sa in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP):
                yield ip_network(sa[0], False)
        except Exception as e:
            logger.error('Failed to resolve %s: %r', host, e)

    def __members_ips(self) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve each Patroni node ``restapi.connect_address`` to IP networks.

        .. note::
            Only yields object if ``restapi.allowlist_include_members`` setting is enabled.

        :yields: each node ``restapi.connect_address`` resolved to an IP network.
        """
        cluster = self.patroni.dcs.cluster
        if self.__allowlist_include_members and cluster:
            for cluster in [cluster] + list(cluster.workers.values()):
                for member in cluster.members:
                    if member.api_url:
                        try:
                            r = urlparse(member.api_url)
                            if r.hostname:
                                port = r.port or (443 if r.scheme == 'https' else 80)
                                for ip in self.__resolve_ips(r.hostname, port):
                                    yield ip
                        except Exception as e:
                            logger.debug('Failed to parse url %s: %r', member.api_url, e)

    def check_access(self, rh: RestApiHandler, allowlist_check_members: bool = True) -> Optional[bool]:
        """Ensure client has enough privileges to perform a given request.

        Write a response back to the client if any issue is observed, and the HTTP status may be:

            * ``401``: if ``Authorization`` header is missing or contain an invalid password;
            * ``403``: if:

                * ``restapi.allowlist`` was configured, but client IP is not in the allowed list; or
                * ``restapi.allowlist_include_members`` is enabled, but client IP is not in the members list; or
                * a client certificate is expected by the server, but is missing in the request.

        :param rh: the request which access should be checked.
        :param allowlist_check_members: whether we should check the source ip against existing cluster members.

        :returns: ``True`` if client access verification succeeded, otherwise ``None``.
        """
        allowlist_check_members = allowlist_check_members and bool(self.__allowlist_include_members)
        if self.__allowlist or allowlist_check_members:
            incoming_ip = ip_address(rh.client_address[0])

            members_ips = tuple(self.__members_ips()) if allowlist_check_members else ()

            if not any(incoming_ip in net for net in self.__allowlist + members_ips):
                return rh.write_response(403, 'Access is denied')

        if not hasattr(rh.request, 'getpeercert') or not rh.request.getpeercert():  # valid client cert isn't present
            if self.__protocol == 'https' and self.__ssl_options.get('verify_client') in ('required', 'optional'):
                return rh.write_response(403, 'client certificate required')

        reason = self.check_auth_header(rh.headers.get('Authorization'))
        if reason:
            headers = {'WWW-Authenticate': 'Basic realm="' + self.patroni.__class__.__name__ + '"'}
            return rh.write_response(401, reason, headers=headers)
        return True

    @staticmethod
    def __has_dual_stack() -> bool:
        """Check if the system has support for dual stack sockets.

        :returns: ``True`` if it has support for dual stack sockets.
        """
        if hasattr(socket, 'AF_INET6') and hasattr(socket, 'IPPROTO_IPV6') and hasattr(socket, 'IPV6_V6ONLY'):
            sock = None
            try:
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
                return True
            except socket.error as e:
                logger.debug('Error when working with ipv6 socket: %s', e)
            finally:
                if sock:
                    sock.close()
        return False

    def __httpserver_init(self, host: str, port: int) -> None:
        """Start REST API HTTP server.

        .. note::
            If system has no support for dual stack sockets, then IPv4 is preferred over IPv6.

        :param host: host to bind REST API to.
        :param port: port to bind REST API to.
        """
        dual_stack = self.__has_dual_stack()
        hostname = host
        if hostname in ('', '*'):
            hostname = None

        # Filter out unexpected results when python is compiled with --disable-ipv6 and running on IPv6 system.
        info = [(a[0], a[4][0], a[4][1])
                for a in socket.getaddrinfo(hostname, port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
                if isinstance(a[4][0], str) and isinstance(a[4][1], int)]
        # in case dual stack is not supported we want IPv4 to be preferred over IPv6
        info.sort(key=lambda x: x[0] == socket.AF_INET, reverse=not dual_stack)

        self.address_family = info[0][0]
        try:
            HTTPServer.__init__(self, (info[0][1], info[0][2]), RestApiHandler)
        except socket.error:
            logger.error(
                "Couldn't start a service on '%s:%s', please check your `restapi.listen` configuration", hostname, port)
            raise

    def __initialize(self, listen: str, ssl_options: Dict[str, Any]) -> None:
        """Configure and start REST API HTTP server.

        .. note::
            This method can be called upon first initialization, and also when reloading Patroni. When reloading
            Patroni, it restarts the HTTP server thread.

        :param listen: IP and port to bind REST API to. It should be a string in the format ``host:port``, where
            ``host`` can be a hostname or IP address. It is the value of ``restapi.listen`` setting.
        :param ssl_options: dictionary that may contain the following keys, depending on what has been configured in
            ``restapi`` section:

            * ``certfile``: path to PEM certificate. If given, will start in HTTPS mode;
            * ``keyfile``: path to key of ``certfile``;
            * ``keyfile_password``: password for decrypting ``keyfile``;
            * ``cafile``: path to CA file to validate client certificates;
            * ``ciphers``: permitted cipher suites;
            * ``verify_client``: value can be one among:

                * ``none``: do not check client certificates;
                * ``optional``: check client certificate only for unsafe REST API endpoints;
                * ``required``: check client certificate for all REST API endpoints.

        :raises:
            :class:`ValueError`: if any issue is faced while parsing *listen*.
        """
        try:
            host, port = split_host_port(listen, None)
        except Exception:
            raise ValueError('Invalid "restapi" config: expected <HOST>:<PORT> for "listen", but got "{0}"'
                             .format(listen))

        reloading_config = self.__listen is not None  # changing config in runtime
        if reloading_config:
            self.shutdown()
            # Rely on ThreadingMixIn.server_close() to have all requests terminate before we continue
            self.server_close()

        self.__listen = listen
        self.__ssl_options = ssl_options
        self._received_new_cert = False  # reset to False after reload_config()

        self.__httpserver_init(host, port)
        Thread.__init__(self, target=self.serve_forever)
        self._set_fd_cloexec(self.socket)

        # wrap socket with ssl if 'certfile' is defined in a config.yaml
        # Sometime it's also needed to pass reference to a 'keyfile'.
        self.__protocol = 'https' if ssl_options.get('certfile') else 'http'
        if self.__protocol == 'https':
            import ssl
            ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=ssl_options.get('cafile'))
            if ssl_options.get('ciphers'):
                ctx.set_ciphers(ssl_options['ciphers'])
            ctx.load_cert_chain(certfile=ssl_options['certfile'], keyfile=ssl_options.get('keyfile'),
                                password=ssl_options.get('keyfile_password'))
            verify_client = ssl_options.get('verify_client')
            if verify_client:
                modes = {'none': ssl.CERT_NONE, 'optional': ssl.CERT_OPTIONAL, 'required': ssl.CERT_REQUIRED}
                if verify_client in modes:
                    ctx.verify_mode = modes[verify_client]
                else:
                    logger.error('Bad value in the "restapi.verify_client": %s', verify_client)
            self.__ssl_serial_number = self.get_certificate_serial_number()
            self.socket = ctx.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)
        if reloading_config:
            self.start()

    def process_request_thread(self, request: Union[socket.socket, Tuple[bytes, socket.socket]],
                               client_address: Tuple[str, int]) -> None:
        """Process a request to the REST API.

        Wrapper for :func:`~socketserver.ThreadingMixIn.process_request_thread` that additionally:

            * Enable TCP keepalive
            * Perform SSL handshake (if an SSL socket).

        :param request: socket to handle the client request.
        :param client_address: tuple containing the client IP and port.
        """
        if isinstance(request, socket.socket):
            enable_keepalive(request, 10, 3)
        if hasattr(request, 'context'):  # SSLSocket
            from ssl import SSLSocket
            if isinstance(request, SSLSocket):  # pyright
                request.do_handshake()
        super(RestApiServer, self).process_request_thread(request, client_address)

    def shutdown_request(self, request: Union[socket.socket, Tuple[bytes, socket.socket]]) -> None:
        """Shut down a request to the REST API.

        Wrapper for :func:`http.server.HTTPServer.shutdown_request` that additionally:

            * Perform SSL shutdown handshake (if a SSL socket).

        :param request: socket to handle the client request.
        """
        if hasattr(request, 'context'):  # SSLSocket
            try:
                from ssl import SSLSocket
                if isinstance(request, SSLSocket):  # pyright
                    request.unwrap()
            except Exception as e:
                logger.debug('Failed to shutdown SSL connection: %r', e)
        super(RestApiServer, self).shutdown_request(request)

    def get_certificate_serial_number(self) -> Optional[str]:
        """Get serial number of the certificate used by the REST API.

        :returns: serial number of the certificate configured through ``restapi.certfile`` setting.
        """
        certfile: Optional[str] = self.__ssl_options.get('certfile')
        if certfile:
            import ssl
            try:
                crt = cast(Dict[str, Any], ssl._ssl._test_decode_cert(certfile))  # pyright: ignore
                return crt.get('serialNumber')
            except ssl.SSLError as e:
                logger.error('Failed to get serial number from certificate %s: %r', self.__ssl_options['certfile'], e)

    def reload_local_certificate(self) -> Optional[bool]:
        """Reload the SSL certificate used by the REST API.

        :return: ``True`` if a different certificate has been configured through ``restapi.certfile` setting, ``None``
            otherwise.
        """
        if self.__protocol == 'https':
            on_disk_cert_serial_number = self.get_certificate_serial_number()
            if on_disk_cert_serial_number != self.__ssl_serial_number:
                self._received_new_cert = True
                self.__ssl_serial_number = on_disk_cert_serial_number
                return True

    def _build_allowlist(self, value: Optional[List[str]]) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve each entry in *value* to an IP network object.

        :param value: list of IPs and/or networks contained in ``restapi.allowlist`` setting. Each item can be a host,
            an IP, or a network in CIDR format.

        :yields: *host* + *port* resolved to IP networks.
        """
        if isinstance(value, list):
            for v in value:
                if '/' in v:  # netmask
                    try:
                        yield ip_network(v, False)
                    except Exception as e:
                        logger.error('Invalid value "%s" in the allowlist: %r', v, e)
                else:  # ip or hostname, try to resolve it
                    for ip in self.__resolve_ips(v, 8080):
                        yield ip

    def reload_config(self, config: Dict[str, Any]) -> None:
        """Reload REST API configuration.

        :param config: dictionary representing values under the ``restapi`` configuration section.

        :raises:
            :class:`ValueError`: if ``listen`` key is not present in *config*.
        """
        if 'listen' not in config:  # changing config in runtime
            raise ValueError('Can not find "restapi.listen" config')

        self.__allowlist = tuple(self._build_allowlist(config.get('allowlist')))
        self.__allowlist_include_members = config.get('allowlist_include_members')

        ssl_options = {n: config[n] for n in ('certfile', 'keyfile', 'keyfile_password',
                                              'cafile', 'ciphers') if n in config}

        self.http_extra_headers = config.get('http_extra_headers') or {}
        self.http_extra_headers.update((config.get('https_extra_headers') or {}) if ssl_options.get('certfile') else {})

        if isinstance(config.get('verify_client'), str):
            ssl_options['verify_client'] = config['verify_client'].lower()

        if self.__listen != config['listen'] or self.__ssl_options != ssl_options or self._received_new_cert:
            self.__initialize(config['listen'], ssl_options)

        self.__auth_key = base64.b64encode(config['auth'].encode('utf-8')) if 'auth' in config else None
        # pyright -- ``__listen`` is initially created as ``None``, but right after that it is replaced with a string
        # through :func:`__initialize`.
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(self.__listen, str)
        self.connection_string = uri(self.__protocol, config.get('connect_address') or self.__listen, 'patroni')

        # Define the Server header response using the server_tokens option.
        self.server_header = self.construct_server_tokens(config.get('server_tokens', 'original'))

    def handle_error(self, request: Union[socket.socket, Tuple[bytes, socket.socket]],
                     client_address: Tuple[str, int]) -> None:
        """Handle any exception that is thrown while handling a request to the REST API.

        Logs ``WARNING`` messages with the client information, and the stack trace of the faced exception.

        :param request: the request that faced an exception.
        :param client_address: a tuple composed of the IP and port of the client connection.
        """
        logger.warning('Exception happened during processing of request from %s:%s',
                       client_address[0], client_address[1])
        logger.warning(traceback.format_exc())
