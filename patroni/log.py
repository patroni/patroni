import logging
import os
import sys

from copy import deepcopy
from logging.handlers import RotatingFileHandler
from patroni.utils import deep_compare
from six.moves.queue import Queue, Full
from threading import Lock, Thread

_LOGGER = logging.getLogger(__name__)


class QueueHandler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)
        self.queue = Queue()
        self._records_lost = 0

    def _put_record(self, record):
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        self.queue.put_nowait(record)

    def _try_to_report_lost_records(self):
        if self._records_lost:
            try:
                record = _LOGGER.makeRecord(_LOGGER.name, logging.WARNING, __file__, 0,
                                            'QueueHandler has lost %s log records',
                                            (self._records_lost,), None, 'emit')
                self._put_record(record)
                self._records_lost = 0
            except Exception:
                pass

    def emit(self, record):
        try:
            self._put_record(record)
            self._try_to_report_lost_records()
        except Exception:
            self._records_lost += 1

    @property
    def records_lost(self):
        return self._records_lost


class PatroniLogger(Thread):

    DEFAULT_LEVEL = 'INFO'
    DEFAULT_FORMAT = '%(asctime)s %(levelname)s: %(message)s'

    NORMAL_LOG_QUEUE_SIZE = 2  # When everything goes normal Patroni writes only 2 messages per HA loop
    DEFAULT_MAX_QUEUE_SIZE = 1000
    LOGGING_BROKEN_EXIT_CODE = 5

    def __init__(self):
        super(PatroniLogger, self).__init__()
        self._queue_handler = QueueHandler()
        self._root_logger = logging.getLogger()
        self._root_logger.addHandler(self._queue_handler)
        self._config = None
        self._log_handler = None
        self._old_handlers = []
        self._log_handler_lock = Lock()
        self.reload_config({'level': 'DEBUG'})
        self.start()

    def update_loggers(self):
        loggers = deepcopy(self._config.get('loggers') or {})
        for name, logger in self._root_logger.manager.loggerDict.items():
            if not isinstance(logger, logging.PlaceHolder):
                level = loggers.pop(name, logging.NOTSET)
                logger.setLevel(level)

        for name, level in loggers.items():
            logger = self._root_logger.manager.getLogger(name)
            logger.setLevel(level)

    def reload_config(self, config):
        if self._config is None or not deep_compare(self._config, config):
            with self._queue_handler.queue.mutex:
                self._queue_handler.queue.maxsize = config.get('max_queue_size', self.DEFAULT_MAX_QUEUE_SIZE)

            self._root_logger.setLevel(config.get('level', PatroniLogger.DEFAULT_LEVEL))

            new_handler = None
            if 'dir' in config:
                if not isinstance(self._log_handler, RotatingFileHandler):
                    new_handler = RotatingFileHandler(os.path.join(config['dir'], __name__))
                handler = new_handler or self._log_handler
                handler.maxBytes = int(config.get('file_size', 25000000))
                handler.backupCount = int(config.get('file_num', 4))
            else:
                if self._log_handler is None or isinstance(self._log_handler, RotatingFileHandler):
                    new_handler = logging.StreamHandler()
                handler = new_handler or self._log_handler

            oldlogformat = (self._config or {}).get('format', PatroniLogger.DEFAULT_FORMAT)
            logformat = config.get('format', PatroniLogger.DEFAULT_FORMAT)

            olddateformat = (self._config or {}).get('dateformat') or None
            dateformat = config.get('dateformat') or None  # Convert empty string to `None`

            if oldlogformat != logformat or olddateformat != dateformat or new_handler:
                handler.setFormatter(logging.Formatter(logformat, dateformat))

            if new_handler:
                with self._log_handler_lock:
                    if self._log_handler:
                        self._old_handlers.append(self._log_handler)
                    self._log_handler = new_handler

            self._config = config.copy()
            self.update_loggers()

    def _close_old_handlers(self):
        while True:
            with self._log_handler_lock:
                if not self._old_handlers:
                    break
                handler = self._old_handlers.pop()
            try:
                handler.close()
            except Exception:
                _LOGGER.exception('Failed to close the old log handler %s', handler)

    def run(self):
        while True:
            self._close_old_handlers()

            record = self._queue_handler.queue.get(True)
            if record is None:
                break

            self._log_handler.handle(record)
            self._queue_handler.queue.task_done()

    def shutdown(self):
        try:
            self._queue_handler.queue.put_nowait(None)
        except Full:  # Queue is full.
            # It seems that logging is not working, exiting with non-standard exit-code is the best we can do.
            sys.exit(self.LOGGING_BROKEN_EXIT_CODE)
        self.join()
        logging.shutdown()

    @property
    def queue_size(self):
        return self._queue_handler.queue.qsize()

    @property
    def records_lost(self):
        return self._queue_handler.records_lost
