import base64
import fcntl
import json
import logging
import psycopg2
import logging
import socket
import shlex
import time
import subprocess

from six.moves.BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from six.moves.socketserver import ThreadingMixIn
from threading import Thread

class HostMonitor():

    # Inspired by https://gist.github.com/kirpit/1306188
    def __init__(self, command):
        self.command = shlex.split(command)


    # Allows us to run a subprocess with a timeout
    def run(self, parameters=None, timeout=5, **kwargs):
        if parameters is None:
            parameters = []

        def target(**kwargs):
            try:
                self.process = subprocess.Popen(self.command+parameters, **kwargs)
                self.output, self.error = self.process.communicate()
                self.status = self.process.returncode
            except (Exception) as e:
                logging.exception('Error when calling Host Monitor')
                self.error = str(e)
                self.output = None
                self.status = -1

        kwargs.setdefault('stdout', subprocess.PIPE)
        kwargs.setdefault('stderr', subprocess.PIPE)

        thread = Thread(target=target, kwargs=kwargs)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.process.terminate()
            thread.join()

        return self.status, self.output, self.error

a = HostMonitor('echo')
status, stdout, stderr = a.run(parameters=['bier'])
print(status)
print(stdout)
print(stderr)
