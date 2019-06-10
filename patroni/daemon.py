import abc
import os
import signal
import six
import sys

from threading import Lock


@six.add_metaclass(abc.ABCMeta)
class AbstractPatroniDaemon(object):

    def __init__(self):
        from patroni.config import Config
        from patroni.log import PatroniLogger

        self.setup_signal_handlers()

        self.logger = PatroniLogger()
        self.config = Config()
        self.logger.reload_config(self.config.get('log', {}))

    def sighup_handler(self, *args):
        self._received_sighup = True

    def sigterm_handler(self, *args):
        with self._sigterm_lock:
            if not self._received_sigterm:
                self._received_sigterm = True
                sys.exit()

    def setup_signal_handlers(self):
        self._received_sighup = False
        self._sigterm_lock = Lock()
        self._received_sigterm = False
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @property
    def received_sigterm(self):
        with self._sigterm_lock:
            return self._received_sigterm

    def reload_config(self):
        self.logger.reload_config(self.config.get('log', {}))

    @abc.abstractmethod
    def _run_cycle(self):
        """_run_cycle"""

    def run(self):
        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                if self.config.reload_local_configuration():
                    self.reload_config()

            self._run_cycle()

    @abc.abstractmethod
    def shutdown(self):
        """shutdown"""


def abstract_main(cls):
    controller = cls()
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
