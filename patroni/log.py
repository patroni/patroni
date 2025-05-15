"""Patroni logging facilities.

Daemon processes will use a 2-step logging handler. Whenever a log message is issued it is initially enqueued in-memory
and is later asynchronously flushed by a thread to the final destination.
"""
import logging
import os
import sys

from copy import deepcopy
from io import TextIOWrapper
from logging.handlers import RotatingFileHandler
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, cast, Dict, List, Optional, TYPE_CHECKING, Union

from .file_perm import pg_perm
from .utils import deep_compare, parse_int

type_logformat = Union[List[Union[str, Dict[str, Any], Any]], str, Any]

_LOGGER = logging.getLogger(__name__)


class PatroniFileHandler(RotatingFileHandler):
    """Wrapper of :class:`RotatingFileHandler` to handle permissions of log files. """

    def __init__(self, filename: str, mode: Optional[int]) -> None:
        """Create a new :class:`PatroniFileHandler` instance.

        :param filename: basename for log files.
        :param mode: permissions for log files.
        """
        self.set_log_file_mode(mode)
        super(PatroniFileHandler, self).__init__(filename)

    def set_log_file_mode(self, mode: Optional[int]) -> None:
        """Set mode for Patroni log files.

        :param mode: permissions for log files.

        .. note::
            If *mode* is not specified, we calculate it from the `umask` value.
        """
        self._log_file_mode = 0o666 & ~pg_perm.orig_umask if mode is None else mode

    def _open(self) -> TextIOWrapper:
        """Open a new log file and assign permissions.

        :returns: the resulting stream.
        """
        ret = super(PatroniFileHandler, self)._open()
        os.chmod(self.baseFilename, self._log_file_mode)
        return ret


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


def _type(value: Any) -> str:
    """Get type of the *value*.

    :param value: any arbitrary value.
    :returns: a string with a type name.
    """
    return value.__class__.__name__


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

    :cvar DEFAULT_TYPE: default type of log format (``plain``).
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

    DEFAULT_TYPE = 'plain'
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

    def _is_config_changed(self, config: Dict[str, Any]) -> bool:
        """Checks if the given config is different from the current one.

        :param config: ``log`` section from Patroni configuration.

        :returns: ``True`` if the config is changed, ``False`` otherwise.
        """
        old_config = self._config or {}

        oldlogtype = old_config.get('type', PatroniLogger.DEFAULT_TYPE)
        logtype = config.get('type', PatroniLogger.DEFAULT_TYPE)

        oldlogformat: type_logformat = old_config.get('format', PatroniLogger.DEFAULT_FORMAT)
        logformat: type_logformat = config.get('format', PatroniLogger.DEFAULT_FORMAT)

        olddateformat = old_config.get('dateformat') or None
        dateformat = config.get('dateformat') or None  # Convert empty string to `None`

        old_static_fields = old_config.get('static_fields', {})
        static_fields = config.get('static_fields', {})

        old_log_config = {
            'type': oldlogtype,
            'format': oldlogformat,
            'dateformat': olddateformat,
            'static_fields': old_static_fields
        }

        log_config = {
            'type': logtype,
            'format': logformat,
            'dateformat': dateformat,
            'static_fields': static_fields
        }

        return not deep_compare(old_log_config, log_config)

    def _get_plain_formatter(self, logformat: type_logformat, dateformat: Optional[str]) -> logging.Formatter:
        """Returns a logging formatter with the specified format and date format.

        .. note::
            If the log format isn't a string, prints a warning message and uses the default log format instead.

        :param logformat: The format of the log messages.
        :param dateformat: The format of the timestamp in the log messages.

        :returns: A logging formatter object that can be used to format log records.
        """

        if not isinstance(logformat, str):
            _LOGGER.warning('Expected log format to be a string when log type is plain, but got "%s"', _type(logformat))
            logformat = PatroniLogger.DEFAULT_FORMAT

        return logging.Formatter(logformat, dateformat)

    def _get_json_formatter(self, logformat: type_logformat, dateformat: Optional[str],
                            static_fields: Dict[str, Any]) -> logging.Formatter:
        """Returns a logging formatter that outputs JSON formatted messages.

        .. note::
            If :mod:`pythonjsonlogger` library is not installed, prints an error message and returns
            a plain log formatter instead.

        :param logformat: Specifies the log fields and their key names in the JSON log message.
        :param dateformat: The format of the timestamp in the log messages.
        :param static_fields: A dictionary of static fields that are added to every log message.

        :returns: A logging formatter object that can be used to format log records as JSON strings.
        """

        if isinstance(logformat, str):
            jsonformat = logformat
            rename_fields = {}
        elif isinstance(logformat, list):
            logformat = cast(List[Any], logformat)
            log_fields: List[str] = []
            rename_fields: Dict[str, str] = {}

            for field in logformat:
                if isinstance(field, str):
                    log_fields.append(field)
                elif isinstance(field, dict):
                    field = cast(Dict[str, Any], field)
                    for original_field, renamed_field in field.items():
                        if isinstance(renamed_field, str):
                            log_fields.append(original_field)
                            rename_fields[original_field] = renamed_field
                        else:
                            _LOGGER.warning(
                                'Expected renamed log field to be a string, but got "%s"',
                                _type(renamed_field)
                            )

                else:
                    _LOGGER.warning(
                        'Expected each item of log format to be a string or dictionary, but got "%s"',
                        _type(field)
                    )

            if len(log_fields) > 0:
                jsonformat = ' '.join([f'%({field})s' for field in log_fields])
            else:
                jsonformat = PatroniLogger.DEFAULT_FORMAT
        else:
            jsonformat = PatroniLogger.DEFAULT_FORMAT
            rename_fields = {}
            _LOGGER.warning('Expected log format to be a string or a list, but got "%s"', _type(logformat))

        try:
            try:
                from pythonjsonlogger import json as jsonlogger  # pyright: ignore
            except ImportError:  # pragma: no cover
                from pythonjsonlogger import jsonlogger
                if hasattr(jsonlogger, 'RESERVED_ATTRS') \
                        and 'taskName' not in jsonlogger.RESERVED_ATTRS:  # pyright: ignore [reportPrivateImportUsage]
                    # compatibility with python 3.12, that added a new attribute to LogRecord
                    jsonlogger.RESERVED_ATTRS += ('taskName',)  # pyright: ignore

            return jsonlogger.JsonFormatter(  # pyright: ignore [reportPrivateImportUsage]
                jsonformat,
                dateformat,
                rename_fields=rename_fields,
                static_fields=static_fields
            )
        except ImportError as e:
            _LOGGER.error('Failed to import "python-json-logger" library: %r. Falling back to the plain logger', e)
        except Exception as e:
            _LOGGER.error('Failed to initialize JsonFormatter: %r. Falling back to the plain logger', e)

        return self._get_plain_formatter(jsonformat, dateformat)

    def _get_formatter(self, config: Dict[str, Any]) -> logging.Formatter:
        """Returns a logging formatter based on the type of logger in the given configuration.

        :param config: ``log`` section from Patroni configuration.

        :returns: A :class:`logging.Formatter` object that can be used to format log records.
        """
        logtype = config.get('type', PatroniLogger.DEFAULT_TYPE)
        logformat: type_logformat = config.get('format', PatroniLogger.DEFAULT_FORMAT)
        dateformat = config.get('dateformat') or None  # Convert empty string to `None`
        static_fields = config.get('static_fields', {})

        if dateformat is not None and not isinstance(dateformat, str):
            _LOGGER.warning('Expected log dateformat to be a string, but got "%s"', _type(dateformat))
            dateformat = None

        if logtype == 'json':
            formatter = self._get_json_formatter(logformat, dateformat, static_fields)
        else:
            formatter = self._get_plain_formatter(logformat, dateformat)

        return formatter

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

            handler = self.log_handler

            if 'dir' in config:
                mode = parse_int(config.get('mode'))
                if not isinstance(handler, PatroniFileHandler):
                    handler = PatroniFileHandler(os.path.join(config['dir'], __name__), mode)
                handler.set_log_file_mode(mode)
                max_file_size = int(config.get('file_size', 25000000))
                handler.maxBytes = max_file_size  # pyright: ignore [reportAttributeAccessIssue]
                handler.backupCount = int(config.get('file_num', 4))
            # we can't use `if not isinstance(handler, logging.StreamHandler)` below,
            # because RotatingFileHandler and PatroniFileHandler are children of StreamHandler!!!
            elif handler is None or isinstance(handler, PatroniFileHandler):
                handler = logging.StreamHandler()

            is_new_handler = handler != self.log_handler

            if (self._is_config_changed(config) or is_new_handler) and handler:
                formatter = self._get_formatter(config)
                handler.setFormatter(formatter)

            if is_new_handler:
                with self.log_handler_lock:
                    if self.log_handler:
                        self._old_handlers.append(self.log_handler)
                    self.log_handler = handler

            self._config = config.copy()
            self.update_loggers(config.get('loggers') or {})

    def _close_old_handlers(self) -> None:
        """Close old log handlers.

        .. note::
            It is used to remove different handlers that were configured previous to a reload in the configuration,
            e.g. if we are switching from :class:`PatroniFileHandler` to
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
        prev_hb_msg = ''

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
                        if self._is_heartbeat_msg(record):
                            config = self._config or {}
                            deduplicate_heartbeat_logs = config.get('deduplicate_heartbeat_logs', False)
                            if record.msg == prev_hb_msg and deduplicate_heartbeat_logs:
                                record = None
                            else:
                                prev_hb_msg = record.msg
                        else:
                            self.log_handler.handle(prev_record)
                            prev_hb_msg = None
                        prev_record = None

            if record:
                self.log_handler.handle(record)

            self._queue_handler.queue.task_done()

    @staticmethod
    def _is_heartbeat_msg(record: logging.LogRecord) -> bool:
        """Checks if the given record contains a heartbeat message.

        :param record: the record to check.

        :returns: ``True`` if the record contains a heartbeat message, ``False`` otherwise.
        """
        return record.msg.startswith('no action. ') or record.msg.startswith('PAUSE: no action')

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
