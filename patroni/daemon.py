"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
from __future__ import print_function

import abc
import os
import signal
import sys

from threading import Lock
from typing import Any, Optional, Type

from .config import Config
from .validator import Schema


class AbstractPatroniDaemon(abc.ABC):
    """A Patroni daemon process.

    .. note::

        When inheriting from :class:`AbstractPatroniDaemon` you are expected to define the methods :func:`_run_cycle`
        to determine what it should do in each execution cycle, and :func:`_shutdown` to determine what it should do
        when shutting down.

    :ivar logger: log handler used by this daemon.
    :ivar config: configuration options for this daemon.
    """

    def __init__(self, config: Config) -> None:
        """Set up signal handlers, logging handler and configuration.

        :param config: configuration options for this daemon.
        """
        from patroni.log import PatroniLogger

        self.setup_signal_handlers()

        self.logger = PatroniLogger()
        self.config = config
        AbstractPatroniDaemon.reload_config(self, local=True)

    def sighup_handler(self, *_: Any) -> None:
        """Handle SIGHUP signals.

        Flag the daemon as "SIGHUP received".
        """
        self._received_sighup = True

    def api_sigterm(self) -> bool:
        """Guarantee only a single SIGTERM is being processed.

        Flag the daemon as "SIGTERM received" with a lock-based approach.

        :returns: ``True`` if the daemon was flagged as "SIGTERM received".
        """
        ret = False
        with self._sigterm_lock:
            if not self._received_sigterm:
                self._received_sigterm = True
                ret = True
        return ret

    def sigterm_handler(self, *_: Any) -> None:
        """Handle SIGTERM signals.

        Terminate the daemon process through :func:`api_sigterm`.
        """
        if self.api_sigterm():
            sys.exit()

    def setup_signal_handlers(self) -> None:
        """Set up daemon signal handlers.

        Set up SIGHUP and SIGTERM signal handlers.

        .. note::

            SIGHUP is only handled in non-Windows environments.
        """
        self._received_sighup = False
        self._sigterm_lock = Lock()
        self._received_sigterm = False
        if os.name != 'nt':
            signal.signal(signal.SIGHUP, self.sighup_handler)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    @property
    def received_sigterm(self) -> bool:
        """If daemon was signaled with SIGTERM."""
        with self._sigterm_lock:
            return self._received_sigterm

    def reload_config(self, sighup: Optional[bool] = False, local: Optional[bool] = False) -> None:
        """Reload configuration.

        :param sighup: if it is related to a SIGHUP signal.
                       The sighup parameter could be used in the method overridden in a child class.
        :param local: will be ``True`` if there are changes in the local configuration file.
        """
        if local:
            self.logger.reload_config(self.config.get('log', {}))

    @abc.abstractmethod
    def _run_cycle(self) -> None:
        """Define what the daemon should do in each execution cycle.

        Keep being called in the daemon's main loop until the daemon is eventually terminated.
        """

    def run(self) -> None:
        """Run the daemon process.

        Start the logger thread and keep running execution cycles until a SIGTERM is eventually received. Also reload
        configuration uppon receiving SIGHUP.
        """
        self.logger.start()
        while not self.received_sigterm:
            if self._received_sighup:
                self._received_sighup = False
                self.reload_config(True, self.config.reload_local_configuration())

            self._run_cycle()

    @abc.abstractmethod
    def _shutdown(self) -> None:
        """Define what the daemon should do when shutting down."""

    def shutdown(self) -> None:
        """Shut the daemon down when a SIGTERM is received.

        Shut down the daemon process and the logger thread.
        """
        with self._sigterm_lock:
            self._received_sigterm = True
        self._shutdown()
        self.logger.shutdown()


def abstract_main(cls: Type[AbstractPatroniDaemon], validator: Optional[Schema] = None) -> None:
    """Create the main entry point of a given daemon process.

    Expose a basic argument parser, parse the command-line arguments, and run the given daemon process.

    :param cls: a class that should inherit from :class:`AbstractPatroniDaemon`.
    :param validator: used to validate the daemon configuration schema, if requested by the user through
        ``--validate-config`` CLI option.
    """
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
    validate_config = validator and args.validate_config
    try:
        if validate_config:
            Config(args.configfile, validator=validator)
            sys.exit()

        config = Config(args.configfile)
    except ConfigParseError as e:
        if e.value:
            print(e.value, file=sys.stderr)
        if not validate_config:
            parser.print_help()
        sys.exit(1)

    controller = cls(config)
    try:
        controller.run()
    except KeyboardInterrupt:
        pass
    finally:
        controller.shutdown()
