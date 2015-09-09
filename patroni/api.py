import fcntl
import json
import logging
import psycopg2

from six.moves.BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from six.moves.socketserver import ThreadingMixIn
from threading import Thread

logger = logging.getLogger(__name__)


class RestApiHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        response = self.get_postgresql_status()

        path = '/master' if self.path == '/' else self.path
        status_code = 200 if response['running'] and 'role' in response and response['role'] in path else 503

        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def get_postgresql_status(self):
        try:
            row = self.server.query("""SELECT to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
                                              pg_is_in_recovery(),
                                              CASE WHEN pg_is_in_recovery()
                                                   THEN null
                                                   ELSE pg_current_xlog_location() END,
                                              pg_last_xlog_receive_location(),
                                              pg_last_xlog_replay_location(),
                                              pg_is_in_recovery() AND pg_is_xlog_replay_paused()""")[0]
            return {
                'running': True,
                'postmaster_start_time': row[0],
                'role': 'slave' if row[1] else 'master',
                'xlog': ({
                    'received_location': row[3],
                    'replayed_location': row[4],
                    'paused': row[5]} if row[1] else {
                    'location': row[2]
                })
            }
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.exception('get_postgresql_status')
            return {'running': self.server.patroni.postgresql.is_running()}


class RestApiServer(ThreadingMixIn, HTTPServer, Thread):

    def __init__(self, patroni, config):
        host, port = config['listen'].split(':')
        HTTPServer.__init__(self, (host, int(port)), RestApiHandler)
        Thread.__init__(self, target=self.serve_forever)
        self._set_fd_cloexec(self.socket)

        protocol = 'http'

        # wrap socket with ssl if 'certfile' is defined in a config.yaml
        # Sometime it's also needed to pass reference to a 'keyfile'.
        options = {option: config[option] for option in ['certfile', 'keyfile'] if option in config}
        if options.get('certfile', None):
            import ssl
            self.socket = ssl.wrap_socket(self.socket, server_side=True, **options)
            protocol = 'https'

        self.connection_string = '{}://{}/patroni'.format(protocol, config.get('connect_address', config['listen']))

        self.patroni = patroni
        self.daemon = True

    def query(self, sql, *params):
        cursor = self.patroni.postgresql.connection().cursor()
        cursor.execute(sql, params)
        ret = [r for r in cursor]
        cursor.close()
        return ret

    @staticmethod
    def _set_fd_cloexec(fd):
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
