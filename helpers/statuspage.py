#!/usr/bin/env python
# -*- coding: utf-8 -*-

from BaseHTTPServer import BaseHTTPRequestHandler


class StatusPage(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            if self.path == '/pg_master':
                response = (200 if postgresql.is_leader else 503)
                self.send_response(response)
            elif self.path == '/pg_slave':
                response = (503 if postgresql.is_leader else 200)
                self.send_response(response)
            elif self.path == '/pg_status':
                self.send_response(200)
                self.end_headers()
                self.wfile.write(postgresql.status())
            else:
                self.send_response(404)
        except:
            self.send_response(500)


def getHTTPServer(postgresql, http_port=8081, listen_address='0.0.0.0'):
    server = HTTPServer((listen_address, http_port), StatusPage)
    server.postgresql = postgresql

    return server


if __name__ == '__main__':
    import sys
    import logging
    from BaseHTTPServer import HTTPServer

    logging.basicConfig(format='%(levelname)-6s %(asctime)s - %(message)s', level=logging.DEBUG)
    logging.debug('Starting as a standalone application')

    # # Create a dummy configuration to be able to use the Postgresql class
    from postgresql import Postgresql
    postgres_config = {
        'name': 'dummy',
        'listen': 'localhost:5432',
        'data_dir': None,
        'replication': {'username': None, 'password': None},
    }
    aws_host_address = None
    if len(sys.argv) > 1:
        postgres_config['listen'] = sys.argv[1]
    postgresql = Postgresql(postgres_config, aws_host_address)

    getHTTPServer(postgresql, 8081, '0.0.0.0').serve_forever()
    logging.debug('Abc')
