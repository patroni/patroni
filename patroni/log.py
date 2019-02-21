import logging
import os

from copy import deepcopy
from logging.handlers import RotatingFileHandler
from patroni.utils import deep_compare


class PatroniLogger(object):

    DEFAULT_LEVEL = 'INFO'
    DEFAULT_FORMAT = '%(asctime)s %(levelname)s: %(message)s'

    def __init__(self):
        self.root_logger = logging.getLogger()
        self.config = None
        self.handler = None
        self.reload_config({'level': 'DEBUG'})

    def update_loggers(self):
        loggers = deepcopy(self.config.get('loggers') or {})
        for name, logger in self.root_logger.manager.loggerDict.items():
            if not isinstance(logger, logging.PlaceHolder):
                level = loggers.pop(name, logging.NOTSET)
                logger.setLevel(level)

        for name, level in loggers.items():
            logger = self.root_logger.manager.getLogger(name)
            logger.setLevel(level)

    def reload_config(self, config):
        if self.config is None or not deep_compare(self.config, config):
            self.root_logger.setLevel(config.get('level', PatroniLogger.DEFAULT_LEVEL))

            add_handler = None
            if 'dir' in config:
                if not isinstance(self.handler, RotatingFileHandler):
                    add_handler = RotatingFileHandler(os.path.join(config['dir'], __name__))
                handler = add_handler or self.handler
                handler.maxBytes = int(config.get('file_size', 25000000))
                handler.backupCount = int(config.get('file_num', 4))
            else:
                if self.handler is None or isinstance(self.handler, RotatingFileHandler):
                    add_handler = logging.StreamHandler()
                handler = add_handler or self.handler

            oldlogformat = (self.config or {}).get('format', PatroniLogger.DEFAULT_FORMAT)
            logformat = config.get('format', PatroniLogger.DEFAULT_FORMAT)

            olddateformat = (self.config or {}).get('dateformat') or None
            dateformat = config.get('dateformat') or None  # Convert empty string to `None`

            if oldlogformat != logformat or olddateformat != dateformat or add_handler:
                handler.setFormatter(logging.Formatter(logformat, dateformat))

            if add_handler:
                self.root_logger.addHandler(add_handler)

                if self.handler is not None:
                    self.root_logger.removeHandler(self.handler)
                    self.handler.close()

                self.handler = add_handler

            self.config = config.copy()
            self.update_loggers()
