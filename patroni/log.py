import logging
import os

from copy import deepcopy
from logging.handlers import RotatingFileHandler
from patroni.utils import deep_compare
from six.moves.queue import Queue
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

    def __init__(self):
        super(PatroniLogger, self).__init__()
        self.daemon = True
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
            self._queue_handler.queue.maxsize = config.get('max_queue_size', 1000)
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
            handler.close()

    def run(self):
        while True:
            self._close_old_handlers()

            record = self._queue_handler.queue.get(True)
            if record is None:
                break

            self._log_handler.handle(record)
            self._queue_handler.queue.task_done()

    def shutdown(self):
        self._queue_handler.queue.put_nowait(None)
        self.join()

    @property
    def queue_size(self):
        return self._queue_handler.queue.qsize()

    @property
    def records_lost(self):
        return self._queue_handler.records_lost
