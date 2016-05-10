import base64
import fcntl
import json
import logging
import psycopg2
import socket
import time
import dateutil
import datetime
import pytz

from patroni.exceptions import PostgresConnectionException
from patroni.utils import Retry, RetryFailedError
from six.moves.BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from six.moves.socketserver import ThreadingMixIn
from threading import Thread

logger = logging.getLogger(__name__)


def check_auth(func):
    """Decorator function to check authorization header.

    Usage example:
    @check_auth
    def do_PUT_foo():
        pass
    """
    def wrapper(handler):
        if handler.check_auth_header():
            return func(handler)
    return wrapper


class RestApiHandler(BaseHTTPRequestHandler):

    def _write_response(self, status_code, body, headers=None):
        self.send_response(status_code)
        if body is not None:
            headers = headers or {}
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'text/html'
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body.encode('utf-8'))

    def send_auth_request(self, body):
        headers = {'WWW-Authenticate': 'Basic realm="' + self.server.patroni.__class__.__name__ + '"'}
        self._write_response(401, body, headers)

    def finish(self):
        try:
            if not self.wfile.closed:
                self.wfile.flush()
                self.wfile.close()
        except socket.error:
            pass
        self.rfile.close()

    def check_auth_header(self):
        auth_header = self.headers.get('Authorization')
        status = self.server.check_auth_header(auth_header)
        return not status or self.send_auth_request(status)

    def _write_status_response(self, status_code, response, options=False):
        if options:
            body = None
        else:
            patroni = self.server.patroni
            response.update({'tags': patroni.tags} if patroni.tags else {})
            if patroni.postgresql.sysid:
                response['database_system_identifier'] = patroni.postgresql.sysid
            response['patroni'] = {'version': patroni.version, 'scope': patroni.postgresql.scope}
            body = json.dumps(response)
        self._write_response(status_code, body, {'Content-Type': 'application/json'})

    def do_GET(self, options=False):
        """Default method for processing all GET requests which can not be routed to other methods"""

        path = '/master' if self.path == '/' else self.path
        response = self.get_postgresql_status()

        patroni = self.server.patroni
        cluster = patroni.dcs.cluster
        if cluster:  # dcs available
            if cluster.leader and cluster.leader.name == patroni.postgresql.name:  # is_leader
                status_code = 200 if 'master' in path else 503
            elif 'role' not in response:
                status_code = 503
            elif response['role'] == 'master':  # running as master but without leader lock!!!!
                status_code = 503
            elif response['role'] in path:  # response['role'] != 'master'
                status_code = 503 if patroni.noloadbalance else 200
            else:
                status_code = 503
        elif 'role' in response and response['role'] in path:
            status_code = 503 if response['role'] != 'master' and patroni.noloadbalance else 200
        elif patroni.ha.restart_scheduled() and patroni.postgresql.role == 'master' and 'master' in path:
            # exceptional case for master node when the postgres is being restarted via API
            status_code = 200
        else:
            status_code = 503
        self._write_status_response(status_code, response, options)

    def do_OPTIONS(self):
        self.do_GET(options=True)

    def do_GET_patroni(self):
        response = self.get_postgresql_status(True)
        self._write_status_response(200, response)

    @check_auth
    def do_POST_restart(self):
        status_code = 500
        data = 'restart failed'
        try:
            status, data = self.server.patroni.ha.restart()
            status_code = 200 if status else 503
        except Exception:
            logger.exception('Exception during restart')
        self._write_response(status_code, data)

    @check_auth
    def do_POST_reinitialize(self):
        ha = self.server.patroni.ha
        cluster = ha.dcs.get_cluster()
        if cluster.is_unlocked():
            status_code = 503
            data = 'Cluster has no leader, can not reinitialize'
        elif cluster.leader.name == ha.state_handler.name:
            status_code = 503
            data = 'I am the leader, can not reinitialize'
        else:
            action = ha.schedule_reinitialize()
            if action is not None:
                status_code = 503
                data = action + ' already in progress'
            else:
                status_code = 200
                data = 'reinitialize scheduled'
        self._write_response(status_code, data)

    def poll_failover_result(self, leader, candidate):
        for _ in range(0, 15):
            time.sleep(1)
            try:
                cluster = self.server.patroni.dcs.get_cluster()
                if cluster.leader and cluster.leader.name != leader:
                    if not candidate or candidate == cluster.leader.name:
                        return 200, 'Successfully failed over to "{0}"'.format(cluster.leader.name)
                    else:
                        return 200, 'Failed over to "{0}" instead of "{1}"'.format(cluster.leader.name, candidate)
                if not cluster.failover:
                    return 503, 'Failover failed'
            except Exception as e:
                logger.debug('Exception occured during polling failover result: %s', e)
        return 503, 'Failover status unknown'

    def is_failover_possible(self, cluster, leader, candidate):
        if leader and not cluster.leader or cluster.leader.name != leader:
            return 'leader name does not match'
        if candidate:
            members = [m for m in cluster.members if m.name == candidate]
            if not members:
                return 'candidate does not exists'
        else:
            members = [m for m in cluster.members if m.name != cluster.leader.name and m.api_url]
            if not members:
                return 'failover is not possible: cluster does not have members except leader'
        for _, reachable, _, _, tags in self.server.patroni.ha.fetch_nodes_statuses(members):
            if reachable and not tags.get('nofailover', False):
                return None
        return 'failover is not possible: no good candidates have been found'

    @check_auth
    def do_POST_failover(self):
        content_length = int(self.headers.get('content-length', 0))
        try:
            request = json.loads(self.rfile.read(content_length).decode('utf-8'))
        except ValueError:
            request = {}
        leader = request.get('leader')
        candidate = request.get('candidate') or request.get('member')
        scheduled_at = request.get('scheduled_at')
        cluster = self.server.patroni.ha.dcs.get_cluster()
        status_code = 500

        logger.info("received failover request with leader=%s candidate=%s scheduled_at=%s",
                    leader, candidate, scheduled_at)

        data = ''
        if leader or candidate:
            if scheduled_at:
                try:
                    scheduled_at = dateutil.parser.parse(scheduled_at)
                    if scheduled_at.tzinfo is None:
                        data = 'Timezone information is mandatory for scheduled_at'
                        status_code = 400
                    elif scheduled_at < datetime.datetime.now(pytz.utc):
                        data = 'Cannot schedule failover in the past'
                        status_code = 422
                    elif self.server.patroni.dcs.manual_failover(leader, candidate, scheduled_at=scheduled_at):
                        self.server.patroni.dcs.event.set()
                        data = 'Failover scheduled'
                        status_code = 200
                    else:
                        data = 'failed to write failover key into DCS'
                        status_code = 503
                except (ValueError, TypeError):
                    logger.exception('Invalid scheduled failover time: %s', request['scheduled_at'])
                    data = 'Unable to parse scheduled timestamp. It should be in an unambiguous format, e.g. ISO 8601'
                    status_code = 422
            else:
                data = self.is_failover_possible(cluster, leader, candidate)
                if not data:
                    if self.server.patroni.dcs.manual_failover(leader, candidate):
                        self.server.patroni.dcs.event.set()
                        status_code, data = self.poll_failover_result(cluster.leader and cluster.leader.name, candidate)
                    else:
                        data = 'failed to write failover key into DCS'
                        status_code = 503
        else:
            status_code = 400
            data = 'No values given for required parameters leader and candidate'
        self._write_response(status_code, data)

    def parse_request(self):
        """Override parse_request method to enrich basic functionality of `BaseHTTPRequestHandler` class

        Original class can only invoke do_GET, do_POST, do_PUT, etc method implementations if they are defined.
        But we would like to have at least some simple routing mechanism, i.e.:
        GET /uri1/part2 request should invoke `do_GET_uri1()`
        POST /other should invoke `do_POST_other()`

        If the `do_<REQUEST_METHOD>_<first_part_url>` method does not exists we'll fallback to original behavior."""

        ret = BaseHTTPRequestHandler.parse_request(self)
        if ret:
            mname = self.path.lstrip('/').split('/')[0]
            mname = self.command + ('_' + mname if mname else '')
            if hasattr(self, 'do_' + mname):
                self.command = mname
        return ret

    def handle_one_request(self):
        try:
            BaseHTTPRequestHandler.handle_one_request(self)
        except socket.error:
            pass

    def query(self, sql, *params, **kwargs):
        if not kwargs.get('retry', False):
            return self.server.query(sql, *params)
        retry = Retry(delay=1, retry_exceptions=PostgresConnectionException)
        return retry(self.server.query, sql, *params)

    def get_postgresql_status(self, retry=False):
        try:
            row = self.query("""SELECT to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
                                       pg_is_in_recovery(),
                                       CASE WHEN pg_is_in_recovery()
                                            THEN 0
                                            ELSE pg_xlog_location_diff(pg_current_xlog_location(), '0/0')::bigint
                                       END,
                                       pg_xlog_location_diff(pg_last_xlog_receive_location(), '0/0')::bigint,
                                       pg_xlog_location_diff(pg_last_xlog_replay_location(), '0/0')::bigint,
                                       to_char(pg_last_xact_replay_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
                                       pg_is_in_recovery() AND pg_is_xlog_replay_paused()""", retry=retry)[0]
            return {
                'state': self.server.patroni.postgresql.state,
                'postmaster_start_time': row[0],
                'role': 'replica' if row[1] else 'master',
                'server_version': self.server.patroni.postgresql.server_version,
                'xlog': ({
                    'received_location': row[3],
                    'replayed_location': row[4],
                    'replayed_timestamp': row[5],
                    'paused': row[6]} if row[1] else {
                    'location': row[2]
                })
            }
        except (psycopg2.Error, RetryFailedError, PostgresConnectionException):
            state = self.server.patroni.postgresql.state
            if state == 'running':
                logger.exception('get_postgresql_status')
                state = 'unknown'
            return {'state': state}

    def log_message(self, fmt, *args):
        logger.debug("API thread: %s - - [%s] %s", self.client_address[0], self.log_date_time_string(), fmt % args)


class RestApiServer(ThreadingMixIn, HTTPServer, Thread):

    def __init__(self, patroni, config):
        self._auth_key = base64.b64encode(config['auth'].encode('utf-8')).decode('utf-8') if 'auth' in config else None
        host, port = config['listen'].split(':')
        HTTPServer.__init__(self, (host, int(port)), RestApiHandler)
        Thread.__init__(self, target=self.serve_forever)
        self._set_fd_cloexec(self.socket)

        protocol = 'http'

        # wrap socket with ssl if 'certfile' is defined in a config.yaml
        # Sometime it's also needed to pass reference to a 'keyfile'.
        options = {option: config[option] for option in ['certfile', 'keyfile'] if option in config}
        if options.get('certfile'):
            import ssl
            self.socket = ssl.wrap_socket(self.socket, server_side=True, **options)
            protocol = 'https'

        self.connection_string = '{0}://{1}/patroni'.format(protocol, config.get('connect_address', config['listen']))

        self.patroni = patroni
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
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    def check_basic_auth_key(self, key):
        return self._auth_key == key

    def check_auth_header(self, auth_header):
        if self._auth_key:
            if auth_header is None:
                return 'no auth header received'
            if not auth_header.startswith('Basic ') or not self.check_basic_auth_key(auth_header[6:]):
                return 'not authenticated'
