import abc
import os
import signal
import six
import sys

from threading import Lock


@six.add_metaclass(abc.ABCMeta)
class AbstractPatroniDaemon(object):

    def __init__(self, config):
        from patroni.log import PatroniLogger

        self.setup_signal_handlers()

        self.logger = PatroniLogger()
        self.config = config
        AbstractPatroniDaemon.reload_config(self, local=True)

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

    def reload_config(self, sighup=False, local=False):
        if local:
            self.logger.reload_config(self.config.get('log', {}))

    @abc.abstractmethod
    def _run_cycle(self):
        """_run_cycle"""

    def run(self):
        self.logger.start()
        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                self.reload_config(True, self.config.reload_local_configuration())

            self._run_cycle()

    @abc.abstractmethod
    def _shutdown(self):
        """_shutdown"""

    def shutdown(self):
        with self._sigterm_lock:
            self._received_sigterm = True
        self._shutdown()
        self.logger.shutdown()


def abstract_main(cls, validator=None):
    import argparse

    from .config import Config, ConfigParseError
    from .version import __version__

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))
    if validator:
        parser.add_argument('--validate-config', action='store_true', help='Run config validator and exit')
    parser.add_argument('configfile', nargs='?', default='',
                        help='Patroni may also read the configuration from the {0} environment variable'
                        .format(Config.PATRONI_CONFIG_VARIABLE))
    args = parser.parse_args()
    try:
        if validator and args.validate_config:
            Config(args.configfile, validator=validator)
            sys.exit()

        config = Config(args.configfile)
    except ConfigParseError as e:
        if e.value:
            print(e.value)
        parser.print_help()
        sys.exit(1)

    controller = cls(config)
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
