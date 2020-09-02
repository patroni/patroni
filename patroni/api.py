import base64
import json
import logging
import psycopg2
import time
import traceback
import dateutil.parser
import datetime
import os
import six
import socket
import sys

from six.moves.BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from six.moves.socketserver import ThreadingMixIn
from six.moves.urllib_parse import urlparse, parse_qs
from threading import Thread

from .exceptions import PostgresConnectionException, PostgresException
from .postgresql.misc import postgres_version_to_int
from .utils import deep_compare, enable_keepalive, parse_bool, patch_config, Retry, \
    RetryFailedError, parse_int, split_host_port, tzutc, uri, cluster_as_json

logger = logging.getLogger(__name__)


class RestApiHandler(BaseHTTPRequestHandler):

    def _write_status_code_only(self, status_code):
        message = self.responses[status_code][0]
        self.wfile.write('{0} {1} {2}\r\n\r\n'.format(self.protocol_version, status_code, message).encode('utf-8'))
        self.log_request(status_code)

    def _write_response(self, status_code, body, content_type='text/html', headers=None):
        self.send_response(status_code)
        headers = headers or {}
        if content_type:
            headers['Content-Type'] = content_type
        for name, value in headers.items():
            self.send_header(name, value)
        for name, value in self.server.http_extra_headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def _write_json_response(self, status_code, response):
        self._write_response(status_code, json.dumps(response), content_type='application/json')

    def check_auth(func):
        """Decorator function to check authorization header or client certificates

        Usage example:
        @check_auth
        def do_PUT_foo():
            pass
        """

        def wrapper(self, *args, **kwargs):
            if self.server.check_auth(self):
                return func(self, *args, **kwargs)

        return wrapper

    def _write_status_response(self, status_code, response):
        patroni = self.server.patroni
        tags = patroni.ha.get_effective_tags()
        if tags:
            response['tags'] = tags
        if patroni.postgresql.sysid:
            response['database_system_identifier'] = patroni.postgresql.sysid
        if patroni.postgresql.pending_restart:
            response['pending_restart'] = True
        response['patroni'] = {'version': patroni.version, 'scope': patroni.postgresql.scope}
        if patroni.scheduled_restart and isinstance(patroni.scheduled_restart, dict):
            response['scheduled_restart'] = patroni.scheduled_restart.copy()
            del response['scheduled_restart']['postmaster_start_time']
            response['scheduled_restart']['schedule'] = (response['scheduled_restart']['schedule']).isoformat()
        if not patroni.ha.watchdog.is_healthy:
            response['watchdog_failed'] = True
        if patroni.ha.is_paused():
            response['pause'] = True
        qsize = patroni.logger.queue_size
        if qsize > patroni.logger.NORMAL_LOG_QUEUE_SIZE:
            response['logger_queue_size'] = qsize
            lost = patroni.logger.records_lost
            if lost:
                response['logger_records_lost'] = lost
        self._write_json_response(status_code, response)

    def do_GET(self, write_status_code_only=False):
        """Default method for processing all GET requests which can not be routed to other methods"""

        path = '/master' if self.path == '/' else self.path
        response = self.get_postgresql_status()

        patroni = self.server.patroni
        cluster = patroni.dcs.cluster

        leader_optime = cluster and cluster.last_leader_operation or 0
        replayed_location = response.get('xlog', {}).get('replayed_location', 0)
        max_replica_lag = parse_int(self.path_query.get('lag', [sys.maxsize])[0], 'B')
        if max_replica_lag is None:
            max_replica_lag = sys.maxsize
        is_lagging = leader_optime and leader_optime > replayed_location + max_replica_lag

        replica_status_code = 200 if not patroni.noloadbalance and not is_lagging and \
            response.get('role') == 'replica' and response.get('state') == 'running' else 503

        if not cluster and patroni.ha.is_paused():
            primary_status_code = 200 if response.get('role') == 'master' else 503
            standby_leader_status_code = 200 if response.get('role') == 'standby_leader' else 503
        elif patroni.ha.is_leader():
            if patroni.ha.is_standby_cluster():
                primary_status_code = replica_status_code = 503
                standby_leader_status_code = 200 if response.get('role') in ('replica', 'standby_leader') else 503
            else:
                primary_status_code = 200
                standby_leader_status_code = 503
        else:
            primary_status_code = standby_leader_status_code = 503

        status_code = 503

        if 'standby_leader' in path or 'standby-leader' in path:
            status_code = standby_leader_status_code
        elif 'master' in path or 'leader' in path or 'primary' in path or 'read-write' in path:
            status_code = primary_status_code
        elif 'replica' in path:
            status_code = replica_status_code
        elif 'read-only' in path:
            status_code = 200 if 200 in (primary_status_code, standby_leader_status_code) else replica_status_code
        elif 'health' in path:
            status_code = 200 if response.get('state') == 'running' else 503
        elif cluster:  # dcs is available
            is_synchronous = cluster.is_synchronous_mode() and cluster.sync \
                    and patroni.postgresql.name in cluster.sync.members
            if path in ('/sync', '/synchronous') and is_synchronous:
                status_code = replica_status_code
            elif path in ('/async', '/asynchronous') and not is_synchronous:
                status_code = replica_status_code

        if write_status_code_only:  # when haproxy sends OPTIONS request it reads only status code and nothing more
            self._write_status_code_only(status_code)
        else:
            self._write_status_response(status_code, response)

    def do_OPTIONS(self):
        self.do_GET(write_status_code_only=True)

    def do_GET_liveness(self):
        self._write_status_code_only(200)

    def do_GET_readiness(self):
        patroni = self.server.patroni
        if patroni.ha.is_leader():
            status_code = 200
        elif patroni.postgresql.state == 'running':
            status_code = 200 if patroni.dcs.cluster else 503
        else:
            status_code = 503
        self._write_status_code_only(status_code)

    def do_GET_patroni(self):
        response = self.get_postgresql_status(True)
        self._write_status_response(200, response)

    def do_GET_cluster(self):
        cluster = self.server.patroni.dcs.get_cluster(True)
        self._write_json_response(200, cluster_as_json(cluster))

    def do_GET_history(self):
        cluster = self.server.patroni.dcs.cluster or self.server.patroni.dcs.get_cluster()
        self._write_json_response(200, cluster.history and cluster.history.lines or [])

    def do_GET_config(self):
        cluster = self.server.patroni.dcs.cluster or self.server.patroni.dcs.get_cluster()
        if cluster.config:
            self._write_json_response(200, cluster.config.data)
        else:
            self.send_error(502)

    def _read_json_content(self, body_is_optional=False):
        if 'content-length' not in self.headers:
            return self.send_error(411) if not body_is_optional else {}
        try:
            content_length = int(self.headers.get('content-length'))
            if content_length == 0 and body_is_optional:
                return {}
            request = json.loads(self.rfile.read(content_length).decode('utf-8'))
            if isinstance(request, dict) and (request or body_is_optional):
                return request
        except Exception:
            logger.exception('Bad request')
        self.send_error(400)

    @check_auth
    def do_PATCH_config(self):
        request = self._read_json_content()
        if request:
            cluster = self.server.patroni.dcs.get_cluster()
            if not (cluster.config and cluster.config.modify_index):
                return self.send_error(503)
            data = cluster.config.data.copy()
            if patch_config(data, request):
                value = json.dumps(data, separators=(',', ':'))
                if not self.server.patroni.dcs.set_config_value(value, cluster.config.index):
                    return self.send_error(409)
            self.server.patroni.ha.wakeup()
            self._write_json_response(200, data)

    @check_auth
    def do_PUT_config(self):
        request = self._read_json_content()
        if request:
            cluster = self.server.patroni.dcs.get_cluster()
            if not deep_compare(request, cluster.config.data):
                value = json.dumps(request, separators=(',', ':'))
                if not self.server.patroni.dcs.set_config_value(value):
                    return self.send_error(502)
            self._write_json_response(200, request)

    @check_auth
    def do_POST_reload(self):
        self.server.patroni.sighup_handler()
        self._write_response(202, 'reload scheduled')

    @staticmethod
    def parse_schedule(schedule, action):
        """ parses the given schedule and validates at """
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
        return (status_code, error, scheduled_at)

    @check_auth
    def do_POST_restart(self):
        status_code = 500
        data = 'restart failed'
        request = self._read_json_content(body_is_optional=True)
        cluster = self.server.patroni.dcs.get_cluster()
        if request is None:
            # failed to parse the json
            return
        if request:
            logger.debug("received restart request: {0}".format(request))

        if cluster.is_paused() and 'schedule' in request:
            self._write_response(status_code, "Can't schedule restart in the paused state")
            return

        for k in request:
            if k == 'schedule':
                (_, data, request[k]) = self.parse_schedule(request[k], "restart")
                if _:
                    status_code = _
                    break
            elif k == 'role':
                if request[k] not in ('master', 'replica'):
                    status_code = 400
                    data = "PostgreSQL role should be either master or replica"
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
        self._write_response(status_code, data)

    @check_auth
    def do_DELETE_restart(self):
        if self.server.patroni.ha.delete_future_restart():
            data = "scheduled restart deleted"
            code = 200
        else:
            data = "no restarts are scheduled"
            code = 404
        self._write_response(code, data)

    @check_auth
    def do_DELETE_switchover(self):
        failover = self.server.patroni.dcs.get_cluster().failover
        if failover and failover.scheduled_at:
            if not self.server.patroni.dcs.manual_failover('', '', index=failover.index):
                return self.send_error(409)
            else:
                data = "scheduled switchover deleted"
                code = 200
        else:
            data = "no switchover is scheduled"
            code = 404
        self._write_response(code, data)

    @check_auth
    def do_POST_reinitialize(self):
        request = self._read_json_content(body_is_optional=True)

        if request:
            logger.debug('received reinitialize request: %s', request)

        force = isinstance(request, dict) and parse_bool(request.get('force')) or False

        data = self.server.patroni.ha.reinitialize(force)
        if data is None:
            status_code = 200
            data = 'reinitialize started'
        else:
            status_code = 503
        self._write_response(status_code, data)

    def poll_failover_result(self, leader, candidate, action):
        timeout = max(10, self.server.patroni.dcs.loop_wait)
        for _ in range(0, timeout*2):
            time.sleep(1)
            try:
                cluster = self.server.patroni.dcs.get_cluster()
                if not cluster.is_unlocked() and cluster.leader.name != leader:
                    if not candidate or candidate == cluster.leader.name:
                        return 200, 'Successfully {0}ed over to "{1}"'.format(action[:-4], cluster.leader.name)
                    else:
                        return 200, '{0}ed over to "{1}" instead of "{2}"'.format(action[:-4].title(),
                                                                                  cluster.leader.name, candidate)
                if not cluster.failover:
                    return 503, action.title() + ' failed'
            except Exception as e:
                logger.debug('Exception occured during polling %s result: %s', action, e)
        return 503, action.title() + ' status unknown'

    def is_failover_possible(self, cluster, leader, candidate, action):
        if leader and (not cluster.leader or cluster.leader.name != leader):
            return 'leader name does not match'
        if candidate:
            if action == 'switchover' and cluster.is_synchronous_mode() and candidate not in cluster.sync.members:
                return 'candidate name does not match with sync_standby'
            members = [m for m in cluster.members if m.name == candidate]
            if not members:
                return 'candidate does not exists'
        elif cluster.is_synchronous_mode():
            members = [m for m in cluster.members if m.name in cluster.sync.members]
            if not members:
                return action + ' is not possible: can not find sync_standby'
        else:
            members = [m for m in cluster.members if m.name != cluster.leader.name and m.api_url]
            if not members:
                return action + ' is not possible: cluster does not have members except leader'
        for st in self.server.patroni.ha.fetch_nodes_statuses(members):
            if st.failover_limitation() is None:
                return None
        return action + ' is not possible: no good candidates have been found'

    @check_auth
    def do_POST_failover(self, action='failover'):
        request = self._read_json_content()
        (status_code, data) = (400, '')
        if not request:
            return

        leader = request.get('leader')
        candidate = request.get('candidate') or request.get('member')
        scheduled_at = request.get('scheduled_at')
        cluster = self.server.patroni.dcs.get_cluster()

        logger.info("received %s request with leader=%s candidate=%s scheduled_at=%s",
                    action, leader, candidate, scheduled_at)

        if action == 'failover' and not candidate:
            data = 'Failover could be performed only to a specific candidate'
        elif action == 'switchover' and not leader:
            data = 'Switchover could be performed only from a specific leader'

        if not data and scheduled_at:
            if not leader:
                data = 'Scheduled {0} is possible only from a specific leader'.format(action)
            if not data and cluster.is_paused():
                data = "Can't schedule {0} in the paused state".format(action)
            if not data:
                (status_code, data, scheduled_at) = self.parse_schedule(scheduled_at, action)

        if not data and cluster.is_paused() and not candidate:
            data = action.title() + ' is possible only to a specific candidate in a paused state'

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
                data = 'failed to write {0} key into DCS'.format(action)
                status_code = 503
        self._write_response(status_code, data)

    def do_POST_switchover(self):
        self.do_POST_failover(action='switchover')

    def parse_request(self):
        """Override parse_request method to enrich basic functionality of `BaseHTTPRequestHandler` class

        Original class can only invoke do_GET, do_POST, do_PUT, etc method implementations if they are defined.
        But we would like to have at least some simple routing mechanism, i.e.:
        GET /uri1/part2 request should invoke `do_GET_uri1()`
        POST /other should invoke `do_POST_other()`

        If the `do_<REQUEST_METHOD>_<first_part_url>` method does not exists we'll fallback to original behavior."""

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

    def query(self, sql, *params, **kwargs):
        if not kwargs.get('retry', False):
            return self.server.query(sql, *params)
        retry = Retry(delay=1, retry_exceptions=PostgresConnectionException)
        return retry(self.server.query, sql, *params)

    def get_postgresql_status(self, retry=False):
        postgresql = self.server.patroni.postgresql
        try:
            cluster = self.server.patroni.dcs.cluster

            if postgresql.state not in ('running', 'restarting', 'starting'):
                raise RetryFailedError('')
            stmt = ("SELECT " + postgresql.POSTMASTER_START_TIME + ", " + postgresql.TL_LSN + ","
                    " pg_catalog.to_char(pg_catalog.pg_last_xact_replay_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),"
                    " pg_catalog.array_to_json(pg_catalog.array_agg(pg_catalog.row_to_json(ri))) "
                    "FROM (SELECT (SELECT rolname FROM pg_authid WHERE oid = usesysid) AS usename,"
                    " application_name, client_addr, w.state, sync_state, sync_priority"
                    " FROM pg_catalog.pg_stat_get_wal_senders() w, pg_catalog.pg_stat_get_activity(pid)) AS ri")

            row = self.query(stmt.format(postgresql.wal_name, postgresql.lsn_name), retry=retry)[0]

            result = {
                'state': postgresql.state,
                'postmaster_start_time': row[0],
                'role': 'replica' if row[1] == 0 else 'master',
                'server_version': postgresql.server_version,
                'cluster_unlocked': bool(not cluster or cluster.is_unlocked()),
                'xlog': ({
                    'received_location': row[4] or row[3],
                    'replayed_location': row[3],
                    'replayed_timestamp': row[6],
                    'paused': row[5]} if row[1] == 0 else {
                    'location': row[2]
                })
            }

            if result['role'] == 'replica' and self.server.patroni.ha.is_standby_cluster():
                result['role'] = postgresql.role

            if row[1] > 0:
                result['timeline'] = row[1]
            else:
                leader_timeline = None if not cluster or cluster.is_unlocked() else cluster.leader.timeline
                result['timeline'] = postgresql.replica_cached_timeline(leader_timeline)

            if row[7]:
                result['replication'] = row[7]

            return result
        except (psycopg2.Error, RetryFailedError, PostgresConnectionException):
            state = postgresql.state
            if state == 'running':
                logger.exception('get_postgresql_status')
                state = 'unknown'
            return {'state': state, 'role': postgresql.role}

    def handle_one_request(self):
        self.__start_time = time.time()
        BaseHTTPRequestHandler.handle_one_request(self)

    def log_message(self, fmt, *args):
        latency = 1000.0 * (time.time() - self.__start_time)
        logger.debug("API thread: %s - - %s latency: %0.3f ms", self.client_address[0], fmt % args, latency)


class RestApiServer(ThreadingMixIn, HTTPServer, Thread):
    # On 3.7+ the `ThreadingMixIn` gathers all non-daemon worker threads in order to join on them at server close.
    daemon_threads = True  # Make worker threads "fire and forget" to prevent a memory leak.

    def __init__(self, patroni, config):
        self.patroni = patroni
        self.__listen = None
        self.__ssl_options = None
        self.http_extra_headers = {}
        self.reload_config(config)
        self.daemon = True

    def query(self, sql, *params):
        cursor = None
        try:
            with self.patroni.postgresql.connection().cursor() as cursor:
                cursor.execute(sql, params)
                return [r for r in cursor]
        except psycopg2.Error as e:
            if cursor and cursor.connection.closed == 0:
                raise e
            raise PostgresConnectionException('connection problems')

    @staticmethod
    def _set_fd_cloexec(fd):
        if os.name != 'nt':
            import fcntl
            flags = fcntl.fcntl(fd, fcntl.F_GETFD)
            fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    def check_basic_auth_key(self, key):
        return self.__auth_key == key

    def check_auth_header(self, auth_header):
        if self.__auth_key:
            if auth_header is None:
                return 'no auth header received'
            if not auth_header.startswith('Basic ') or not self.check_basic_auth_key(auth_header[6:]):
                return 'not authenticated'

    def check_auth(self, rh):
        if not hasattr(rh.request, 'getpeercert') or not rh.request.getpeercert():  # valid client cert isn't present
            if self.__protocol == 'https' and self.__ssl_options.get('verify_client') in ('required', 'optional'):
                return rh._write_response(403, 'client certificate required')

        reason = self.check_auth_header(rh.headers.get('Authorization'))
        if reason:
            headers = {'WWW-Authenticate': 'Basic realm="' + self.patroni.__class__.__name__ + '"'}
            return rh._write_response(401, reason, headers=headers)
        return True

    @staticmethod
    def __has_dual_stack():
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

    def __httpserver_init(self, host, port):
        dual_stack = self.__has_dual_stack()
        if host in ('', '*'):
            host = None

        info = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
        # in case dual stack is not supported we want IPv4 to be preferred over IPv6
        info.sort(key=lambda x: x[0] == socket.AF_INET, reverse=not dual_stack)

        self.address_family = info[0][0]
        try:
            HTTPServer.__init__(self, info[0][-1][:2], RestApiHandler)
        except socket.error:
            logger.error(
                    "Couldn't start a service on '%s:%s', please check your `restapi.listen` configuration", host, port)
            raise

    def __initialize(self, listen, ssl_options):
        try:
            host, port = split_host_port(listen, None)
        except Exception:
            raise ValueError('Invalid "restapi" config: expected <HOST>:<PORT> for "listen", but got "{0}"'
                             .format(listen))

        reloading_config = self.__listen is not None  # changing config in runtime
        if reloading_config:
            self.shutdown()

        self.__listen = listen
        self.__ssl_options = ssl_options

        self.__httpserver_init(host, port)
        Thread.__init__(self, target=self.serve_forever)
        self._set_fd_cloexec(self.socket)

        # wrap socket with ssl if 'certfile' is defined in a config.yaml
        # Sometime it's also needed to pass reference to a 'keyfile'.
        self.__protocol = 'https' if ssl_options.get('certfile') else 'http'
        if self.__protocol == 'https':
            import ssl
            ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=ssl_options.get('cafile'))
            ctx.load_cert_chain(certfile=ssl_options['certfile'], keyfile=ssl_options.get('keyfile'))
            verify_client = ssl_options.get('verify_client')
            if verify_client:
                modes = {'none': ssl.CERT_NONE, 'optional': ssl.CERT_OPTIONAL, 'required': ssl.CERT_REQUIRED}
                if verify_client in modes:
                    ctx.verify_mode = modes[verify_client]
                else:
                    logger.error('Bad value in the "restapi.verify_client": %s', verify_client)
            self.socket = ctx.wrap_socket(self.socket, server_side=True)
        if reloading_config:
            self.start()

    def process_request_thread(self, request, client_address):
        if isinstance(request, tuple):
            sock, newsock = request
            try:
                request = sock.context.wrap_socket(newsock, do_handshake_on_connect=sock.do_handshake_on_connect,
                                                   suppress_ragged_eofs=sock.suppress_ragged_eofs, server_side=True)
            except socket.error:
                return
        super(RestApiServer, self).process_request_thread(request, client_address)

    def get_request(self):
        sock = self.socket
        newsock, addr = socket.socket.accept(sock)
        enable_keepalive(newsock, 10, 3)
        if hasattr(sock, 'context'):  # SSLSocket, we want to do the deferred handshake from a thread
            newsock = (sock, newsock)
        return newsock, addr

    def reload_config(self, config):
        if 'listen' not in config:  # changing config in runtime
            raise ValueError('Can not find "restapi.listen" config')

        ssl_options = {n: config[n] for n in ('certfile', 'keyfile', 'cafile') if n in config}

        self.http_extra_headers = config.get('http_extra_headers') or {}
        self.http_extra_headers.update((config.get('https_extra_headers') or {}) if ssl_options.get('certfile') else {})

        if isinstance(config.get('verify_client'), six.string_types):
            ssl_options['verify_client'] = config['verify_client'].lower()

        if self.__listen != config['listen'] or self.__ssl_options != ssl_options:
            self.__initialize(config['listen'], ssl_options)

        self.__auth_key = base64.b64encode(config['auth'].encode('utf-8')).decode('utf-8') if 'auth' in config else None
        self.connection_string = uri(self.__protocol, config.get('connect_address') or self.__listen, 'patroni')

    @staticmethod
    def handle_error(request, client_address):
        address, port = client_address
        logger.warning('Exception happened during processing of request from {}:{}'.format(address, port))
        logger.warning(traceback.format_exc())
