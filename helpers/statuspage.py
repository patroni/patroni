#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys

if sys.hexversion >= 0x03000000:
    from http.server import BaseHTTPRequestHandler, HTTPServer
else:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer


class StatusPage(BaseHTTPRequestHandler):

    def do_GET(self):
        content_type = 'text/plain'
        if self.path == '/pg_master':
            if not self.pg_is_in_recovery():
                response, content = 200, 'I am currently a master'
            else:
                response, content = 503, 'I am not a master'
        elif self.path == '/pg_slave':
            if self.pg_is_in_recovery():
                response, content = 200, 'I am currently a slave'
            else:
                response, content = 503, 'I am not a slave'
        elif self.path == '/pg_status':
            response, content = 200, self.pg_status()
            content_type = 'application/json'
        else:
            response, content = 404, 'Page not found'

        self.send_response(response)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(content)

    def pg_is_in_recovery(self):
        cursor = self.server.postgresql.cursor()
        cursor.execute('SELECT pg_is_in_recovery()')
        res = cursor.fetchone()
        return res[0]

    def pg_status(self):
        cursor = self.server.postgresql.cursor()
        cursor.execute("""
            SELECT pg_is_in_recovery(),
                   to_char(pg_last_xact_replay_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
                   extract(epoch from now() - pg_last_xact_replay_timestamp()),
                   inet_server_addr(),
                   inet_server_port(),
                   to_char(pg_postmaster_start_time(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
                    """)
        res = cursor.fetchone()
        status = {'role': ('master' if not res[0] else 'slave'), 'recovery': {'last_transaction_timestamp': res[1]},
                  'server': {'hostaddr': res[3], 'port': res[4], 'start_time': res[5]}}

        return json.dumps(status)


def getHTTPServer(postgresql, http_port=8081, listen_address='0.0.0.0'):
    server = HTTPServer((listen_address, http_port), StatusPage)
    server.postgresql = postgresql

    return server


if __name__ == '__main__':
    import sys
    import logging

    logging.basicConfig(format='%(levelname)-6s %(asctime)s - %(message)s', level=logging.DEBUG)
    logging.debug('Starting as a standalone application')

    # Create a dummy configuration to be able to use the Postgresql class
    from postgresql import Postgresql
    postgres_config = {
        'name': 'dummy',
        'listen': 'localhost:5432',
        'data_dir': 'nonsense',
        'replication': {'username': None, 'password': None},
        'superuser': None,
        'admin': None,
    }
    aws_host_address = None
    if len(sys.argv) > 1:
        postgres_config['listen'] = sys.argv[1]
    postgresql = Postgresql(postgres_config, aws_host_address)

    http_port = 8081
    if len(sys.argv) > 2:
        http_port = int(sys.argv[2])

    getHTTPServer(postgresql, http_port, '0.0.0.0').serve_forever()
