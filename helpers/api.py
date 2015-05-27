import json
import logging
import psycopg2
import sys

from threading import Thread

if sys.hexversion >= 0x03000000:
    from http.server import BaseHTTPRequestHandler, HTTPServer
else:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer


logger = logging.getLogger(__name__)


class RestApiHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            response = self.get_postgresql_status()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logging.exception('get_postgresql_status')
            response = {'running': False}

        path = '/master' if self.path == '/' else self.path
        status_code = 200 if response['running'] and response['role'] in path else 503

        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def get_postgresql_status(self):
        if not self.server.governor.postgresql.is_running():
            return {'running': False}
        cursor = self.server._cursor()
        cursor.execute("""SELECT to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
                                 pg_is_in_recovery(),
                                 CASE WHEN pg_is_in_recovery()
                                      THEN null
                                      ELSE pg_current_xlog_location() END,
                                 pg_last_xlog_receive_location(),
                                 pg_last_xlog_replay_location(),
                                 pg_is_in_recovery() AND pg_is_xlog_replay_paused()""")
        row = cursor.fetchone()
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


class RestApiServer(HTTPServer, Thread):

    def __init__(self, governor, config):
        self.connection_string = 'http://{}/governor'.format(config.get('connect_address', None) or config['listen'])
        host, port = config['listen'].split(':')
        HTTPServer.__init__(self, (host, int(port)), RestApiHandler)
        Thread.__init__(self, target=self.serve_forever)
        self.governor = governor
        self._cursor_holder = None
        self.daemon = True

    def _cursor(self):
        if not self._cursor_holder or self._cursor_holder.closed:
            self._cursor_holder = self.governor.postgresql.connection().cursor()
        return self._cursor_holder
