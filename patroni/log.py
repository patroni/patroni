"""Patroni logging facilities.

Daemon processes will use a 2-step logging handler. Whenever a log message is issued it is initially enqueued in-memory
and is later asynchronously flushed by a thread to the final destination.
"""
import logging
import os
import sys

from copy import deepcopy
from logging.handlers import RotatingFileHandler
from patroni.utils import deep_compare
from queue import Queue, Full
from threading import Lock, Thread

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

_LOGGER = logging.getLogger(__name__)


def debug_exception(self: logging.Logger, msg: object, *args: Any, **kwargs: Any) -> None:
    """Add full stack trace info to debug log messages and partial to others.

    Handle :func:`~self.exception` calls for *self*.

    .. note::
        * If *self* log level is set to ``DEBUG``, then issue a ``DEBUG`` message with the complete stack trace;
        * If *self* log level is ``INFO`` or higher, then issue an ``ERROR`` message with only the last line of
            the stack trace.

    :param self: logger for which :func:`~self.exception` will be processed.
    :param msg: the message related to the exception to be logged.
    :param args: positional arguments to be passed to :func:`~self.debug` or :func:`~self.error`.
    :param kwargs: keyword arguments to be passed to :func:`~self.debug` or :func:`~self.error`.
    """
    kwargs.pop("exc_info", False)
    if self.isEnabledFor(logging.DEBUG):
        self.debug(msg, *args, exc_info=True, **kwargs)
    else:
        msg = "{0}, DETAIL: '{1}'".format(msg, sys.exc_info()[1])
        self.error(msg, *args, exc_info=False, **kwargs)


def error_exception(self: logging.Logger, msg: object, *args: Any, **kwargs: Any) -> None:
    """Add full stack trace info to error messages.

    Handle :func:`~self.exception` calls for *self*.

    .. note::
        * By default issue an ``ERROR`` message with the complete stack trace. If you do not want to show the complete
          stack trace, call with ``exc_info=False``.

    :param self: logger for which :func:`~self.exception` will be processed.
    :param msg: the message related to the exception to be logged.
    :param args: positional arguments to be passed to :func:`~self.error`.
    :param kwargs: keyword arguments to be passed to :func:`~self.error`.
    """
    exc_info = kwargs.pop("exc_info", True)
    self.error(msg, *args, exc_info=exc_info, **kwargs)


class QueueHandler(logging.Handler):
    """Queue-based logging handler.

    :ivar queue: queue to hold log messages that are pending to be flushed to the final destination.
    """

    def __init__(self) -> None:
        """Queue initialised and initial records_lost established."""
        super().__init__()
        self.queue: Queue[Union[logging.LogRecord, None]] = Queue()
        self._records_lost = 0

    def _put_record(self, record: logging.LogRecord) -> None:
        """Asynchronously enqueue a log record.

        :param record: the record to be logged.
        """
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        self.queue.put_nowait(record)

    def _try_to_report_lost_records(self) -> None:
        """Report the number of log messages that have been lost and reset the counter.

        .. note::
            It will issue an ``WARNING`` message in the logs with the number of lost log messages.
        """
        if self._records_lost:
            try:
                record = _LOGGER.makeRecord(_LOGGER.name, logging.WARNING, __file__, 0,
                                            'QueueHandler has lost %s log records',
                                            (self._records_lost,), None, 'emit')
                self._put_record(record)
                self._records_lost = 0
            except Exception:
                pass

    def emit(self, record: logging.LogRecord) -> None:
        """Handle each log record that is emitted.

        Call :func:`_put_record` to enqueue the emitted log record.

        Also check if we have previously lost any log record, and if so, log a ``WARNING`` message.

        :param record: the record that was emitted.
        """
        try:
            self._put_record(record)
            self._try_to_report_lost_records()
        except Exception:
            self._records_lost += 1

    @property
    def records_lost(self) -> int:
        """Number of log messages that have been lost while the queue was full."""
        return self._records_lost


class ProxyHandler(logging.Handler):
    """Handle log records in place of pending log handlers.

    .. note::
        This is used to handle log messages while the logger thread has not started yet, in which case the queue-based
        handler is not yet started.

    :ivar patroni_logger: the logger thread.
    """

    def __init__(self, patroni_logger: 'PatroniLogger') -> None:
        """Create a new :class:`ProxyHandler` instance.

        :param patroni_logger: the logger thread.
        """
        super().__init__()
        self.patroni_logger = patroni_logger

    def emit(self, record: logging.LogRecord) -> None:
        """Emit each log record that is handled.

        Will push the log record down to :func:`~logging.Handler.handle` method of the currently configured log handler.

        :param record: the record that was emitted.
        """
        if self.patroni_logger.log_handler is not None:
            self.patroni_logger.log_handler.handle(record)


class PatroniLogger(Thread):
    """Logging thread for the Patroni daemon process.

    It is a 2-step logging approach. Any time a log message is issued it is initially enqueued in-memory, and then
    asynchronously flushed to the final destination by the logging thread.

    .. seealso::
        :class:`QueueHandler`: object used for enqueueing messages in-memory.

    :cvar DEFAULT_LEVEL: default logging level (``INFO``).
    :cvar DEFAULT_TRACEBACK_LEVEL: default traceback logging level (``ERROR``).
    :cvar DEFAULT_FORMAT: default format of log messages (``%(asctime)s %(levelname)s: %(message)s``).
    :cvar NORMAL_LOG_QUEUE_SIZE: expected number of log messages per HA loop when operating under a normal situation.
    :cvar DEFAULT_MAX_QUEUE_SIZE: default maximum queue size for holding a backlog of log messages that are pending
        to be flushed.
    :cvar LOGGING_BROKEN_EXIT_CODE: exit code to be used if it detects(``5``).

    :ivar log_handler: log handler that is currently being used by the thread.
    :ivar log_handler_lock: lock used to modify ``log_handler``.
    """

    DEFAULT_LEVEL = 'INFO'
    DEFAULT_TRACEBACK_LEVEL = 'ERROR'
    DEFAULT_FORMAT = '%(asctime)s %(levelname)s: %(message)s'

    NORMAL_LOG_QUEUE_SIZE = 2  # When everything goes normal Patroni writes only 2 messages per HA loop
    DEFAULT_MAX_QUEUE_SIZE = 1000
    LOGGING_BROKEN_EXIT_CODE = 5

    def __init__(self) -> None:
        """Prepare logging queue and proxy handlers as they become ready during daemon startup.

        .. note::
            While Patroni is starting up it keeps ``DEBUG`` log level, and writes log messages through a proxy handler.
            Once the logger thread is finally started, it switches from that proxy handler to the queue based logger,
            and applies the configured log settings. The switching is used to avoid that the logger thread prevents
            Patroni from shutting down if any issue occurs in the meantime until the thread is properly started.
        """
        super(PatroniLogger, self).__init__()
        self._queue_handler = QueueHandler()
        self._root_logger = logging.getLogger()
        self._config: Optional[Dict[str, Any]] = None
        self.log_handler = None
        self.log_handler_lock = Lock()
        self._old_handlers: List[logging.Handler] = []
        # initially set log level to ``DEBUG`` while the logger thread has not started running yet. The daemon process
        # will later adjust all log related settings with what was provided through the user configuration file.
        self.reload_config({'level': 'DEBUG'})
        # We will switch to the QueueHandler only when thread was started.
        # This is necessary to protect from the cases when Patroni constructor
        # failed and PatroniLogger thread remain running and prevent shutdown.
        self._proxy_handler = ProxyHandler(self)
        self._root_logger.addHandler(self._proxy_handler)

    def update_loggers(self, config: Dict[str, Any]) -> None:
        """Configure custom loggers' log levels.

        .. note::
            It creates logger objects that are not defined yet in the log manager.

        :param config: :class:`dict` object with custom loggers configuration, is set either from:

                       * ``log.loggers`` section of Patroni configuration; or

                       * from the method that is trying to make sure that the node name
                         isn't duplicated (to silence annoying ``urllib3`` WARNING's).

        :Example:

            .. code-block:: python

                update_loggers({'urllib3.connectionpool': 'WARNING'})
        """
        loggers = deepcopy(config)
        for name, logger in self._root_logger.manager.loggerDict.items():
            # ``Placeholder`` is a node in the log manager for which no logger has been defined. We are interested only
            # in the ones that were defined
            if not isinstance(logger, logging.PlaceHolder):
                # if this logger is present in *config*, use the configured level, otherwise
                # use ``logging.NOTSET``, which means it will inherit the level
                # from any parent node up to the root for which log level is defined.
                level = loggers.pop(name, logging.NOTSET)
                logger.setLevel(level)

        # define loggers that do not exist yet and set level as configured in the *config*
        for name, level in loggers.items():
            logger = self._root_logger.manager.getLogger(name)
            logger.setLevel(level)

    def reload_config(self, config: Dict[str, Any]) -> None:
        """Apply log related configuration.

        .. note::
            It is also able to deal with runtime configuration changes.

        :param config: ``log`` section from Patroni configuration.
        """
        if self._config is None or not deep_compare(self._config, config):
            with self._queue_handler.queue.mutex:
                self._queue_handler.queue.maxsize = config.get('max_queue_size', self.DEFAULT_MAX_QUEUE_SIZE)

            self._root_logger.setLevel(config.get('level', PatroniLogger.DEFAULT_LEVEL))
            if config.get('traceback_level', PatroniLogger.DEFAULT_TRACEBACK_LEVEL).lower() == 'debug':
                # show stack traces only if ``log.traceback_level`` is ``DEBUG``
                logging.Logger.exception = debug_exception
            else:
                # show stack traces as ``ERROR`` log messages
                logging.Logger.exception = error_exception

            new_handler = None
            if 'dir' in config:
                if not isinstance(self.log_handler, RotatingFileHandler):
                    new_handler = RotatingFileHandler(os.path.join(config['dir'], __name__))
                handler = new_handler or self.log_handler
                if TYPE_CHECKING:  # pragma: no cover
                    assert isinstance(handler, RotatingFileHandler)
                handler.maxBytes = int(config.get('file_size', 25000000))  # pyright: ignore [reportGeneralTypeIssues]
                handler.backupCount = int(config.get('file_num', 4))
            else:
                if self.log_handler is None or isinstance(self.log_handler, RotatingFileHandler):
                    new_handler = logging.StreamHandler()
                handler = new_handler or self.log_handler

            oldlogformat = (self._config or {}).get('format', PatroniLogger.DEFAULT_FORMAT)
            logformat = config.get('format', PatroniLogger.DEFAULT_FORMAT)

            olddateformat = (self._config or {}).get('dateformat') or None
            dateformat = config.get('dateformat') or None  # Convert empty string to `None`

            if (oldlogformat != logformat or olddateformat != dateformat or new_handler) and handler:
                handler.setFormatter(logging.Formatter(logformat, dateformat))

            if new_handler:
                with self.log_handler_lock:
                    if self.log_handler:
                        self._old_handlers.append(self.log_handler)
                    self.log_handler = new_handler

            self._config = config.copy()
            self.update_loggers(config.get('loggers') or {})

    def _close_old_handlers(self) -> None:
        """Close old log handlers.

        .. note::
            It is used to remove different handlers that were configured previous to a reload in the configuration,
            e.g. if we are switching from :class:`~logging.handlers.RotatingFileHandler` to
            class:`~logging.StreamHandler` and vice-versa.
        """
        while True:
            with self.log_handler_lock:
                if not self._old_handlers:
                    break
                handler = self._old_handlers.pop()
            try:
                handler.close()
            except Exception:
                _LOGGER.exception('Failed to close the old log handler %s', handler)

    def run(self) -> None:
        """Run logger's thread main loop.

        Keep consuming log queue until requested to quit through ``None`` special log record.
        """
        # switch to QueueHandler only when the thread was started
        with self.log_handler_lock:
            self._root_logger.addHandler(self._queue_handler)
            self._root_logger.removeHandler(self._proxy_handler)

        prev_record = None

        while True:
            self._close_old_handlers()
            if TYPE_CHECKING:  # pragma: no cover
                assert self.log_handler is not None

            record = self._queue_handler.queue.get(True)
            # special message that indicates Patroni is shutting down
            if record is None:
                break

            if self._root_logger.level == logging.INFO:
                # messages like ``Lock owner: postgresql0; I am postgresql1`` will be shown only when stream doesn't
                # look normal. This is used to reduce chattiness of Patroni logs.
                if record.msg.startswith('Lock owner: '):
                    prev_record, record = record, None
                else:
                    if prev_record and prev_record.thread == record.thread:
                        if not (record.msg.startswith('no action. ') or record.msg.startswith('PAUSE: no action')):
                            self.log_handler.handle(prev_record)
                        prev_record = None

            if record:
                self.log_handler.handle(record)

            self._queue_handler.queue.task_done()

    def shutdown(self) -> None:
        """Shut down the logger thread."""
        try:
            # ``None`` is a special message indicating to queue handler that it should quit its main loop.
            self._queue_handler.queue.put_nowait(None)
        except Full:  # Queue is full.
            # It seems that logging is not working, exiting with non-standard exit-code is the best we can do.
            sys.exit(self.LOGGING_BROKEN_EXIT_CODE)
        self.join()
        logging.shutdown()

    @property
    def queue_size(self) -> int:
        """Number of log records in the queue."""
        return self._queue_handler.queue.qsize()

    @property
    def records_lost(self) -> int:
        """Number of logging records that have been lost while the queue was full."""
        return self._queue_handler.records_lost
