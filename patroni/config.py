import json
import logging
import os
import tempfile
import yaml

from copy import deepcopy
from patroni.dcs import ClusterConfig
from patroni.postgresql import Postgresql
from patroni.utils import deep_compare

logger = logging.getLogger(__name__)


class Config(object):
    """
    This class is responsible for:

      1) Building and giving access to `effective_configuration` from:
         * `Config.__DEFAULT_CONFIG` -- some sane default values
         * `dynamic_configuration` -- configuration stored in DCS
         * `local_configuration` -- configuration from `config.yml` or environment

      2) Saving and loading `dynamic_configuration` into 'patroni.dynamic.json' file
         located in local_configuration['postgresql']['data_dir'] directory.
         This is necessary to be able to restore `dynamic_configuration`
         if DCS was accidentally wiped

      3) Loading of configuration file in the old format and converting it into new format

      4) Mimicking some of the `dict` interfaces to make it possible
         to work with it as with the old `config` object.
    """

    __CACHE_FILENAME = 'patroni.dynamic.json'
    __DEFAULT_CONFIG = {
        'ttl': 30, 'loop_wait': 10, 'retry_timeout': 10,
        'maximum_lag_on_failover': 1048576,
        'postgresql': {
            'use_slots': True,
            'parameters': {p: v[0] for p, v in Postgresql.CMDLINE_OPTIONS.items()}
        }
    }

    def __init__(self, config_file=None, config_env=None):
        self._config_file = None if config_env else config_file
        self._modify_index = -1
        self._dynamic_configuration = {}
        self._local_configuration = yaml.safe_load(config_env) if config_env else self._load_config_file()
        self.__effective_configuration = self._build_effective_configuration(self._dynamic_configuration,
                                                                             self._local_configuration)
        self._data_dir = self.__effective_configuration['postgresql']['data_dir']
        self._cache_file = os.path.join(self._data_dir, self.__CACHE_FILENAME)
        self._load_cache()
        self._cache_needs_saving = False

    @property
    def config_file(self):
        return self._config_file

    @property
    def dynamic_configuration(self):
        return deepcopy(self._dynamic_configuration)

    def _load_config_file(self):
        with open(self._config_file) as f:
            return yaml.safe_load(f)

    def _load_cache(self):
        if os.path.isfile(self._cache_file):
            try:
                with open(self._cache_file) as f:
                    self.set_dynamic_configuration(json.load(f))
            except Exception:
                logger.exception('Exception when loading file: %s', self._cache_file)

    def save_cache(self):
        if self._cache_needs_saving:
            tmpfile = fd = None
            try:
                (fd, tmpfile) = tempfile.mkstemp(prefix=self.__CACHE_FILENAME, dir=self._data_dir)
                with os.fdopen(fd, 'w') as f:
                    fd = None
                    json.dump(self.dynamic_configuration, f)
                tmpfile = os.rename(tmpfile, self._cache_file)
                self._cache_needs_saving = False
            except Exception:
                logger.exception('Exception when saving file: %s', self._cache_file)
                if fd:
                    try:
                        os.close(fd)
                    except Exception:
                        logger.error('Can not close temporary file %s', tmpfile)
                if tmpfile and os.path.exists(tmpfile):
                    try:
                        os.remove(tmpfile)
                    except Exception:
                        logger.error('Can not remove temporary file %s', tmpfile)

    # configuration could be either ClusterConfig or dict
    def set_dynamic_configuration(self, configuration):
        if isinstance(configuration, ClusterConfig):
            if self._modify_index == configuration.modify_index:
                return False  # If the index didn't changed there is nothing to do
            self._modify_index = configuration.modify_index
            configuration = configuration.data

        if not deep_compare(self._dynamic_configuration, configuration):
            try:
                self.__effective_configuration = self._build_effective_configuration(configuration,
                                                                                     self._local_configuration)
                self._dynamic_configuration = configuration
                self._cache_needs_saving = True
                return True
            except Exception:
                logger.exception('Exception when setting dynamic_configuration')

    def reload_local_configuration(self, dry_run=False):
        if self.config_file:
            try:
                configuration = self._load_config_file()
                if not deep_compare(self._local_configuration, configuration):
                    new_configuration = self._build_effective_configuration(self._dynamic_configuration, configuration)
                    if dry_run:
                        return not deep_compare(new_configuration, self.__effective_configuration)
                    self._local_configuration = configuration
                    self.__effective_configuration = new_configuration
                    return True
            except Exception:
                logger.exception('Exception when reloading local configuration from %s', self.config_file)
                if dry_run:
                    raise

    @staticmethod
    def _process_postgresql_parameters(parameters, is_local=False):
        ret = {}
        for name, value in (parameters or {}).items():
            if name not in Postgresql.CMDLINE_OPTIONS or not is_local and Postgresql.CMDLINE_OPTIONS[name][1](value):
                ret[name] = value
        return ret

    def _safe_copy_dynamic_configuration(self, dynamic_configuration):
        config = deepcopy(self.__DEFAULT_CONFIG)

        for name, value in dynamic_configuration.items():
            if name == 'postgresql':
                for name, value in (value or {}).items():
                    if name == 'parameters':
                        config['postgresql'][name].update(self._process_postgresql_parameters(value))
                    elif name not in ('connect_address', 'listen', 'data_dir', 'pgpass', 'authentication'):
                        config['postgresql'][name] = deepcopy(value)
            elif name in config:
                config[name] = int(value)
        return config

    def _build_effective_configuration(self, dynamic_configuration, local_configuration):
        config = self._safe_copy_dynamic_configuration(dynamic_configuration)
        for name, value in local_configuration.items():
            if name == 'postgresql':
                for name, value in (value or {}).items():
                    if name == 'parameters':
                        config['postgresql'][name].update(self._process_postgresql_parameters(value, True))
                    elif name != 'use_slots':  # replication slots must be enabled/disabled globally
                        config['postgresql'][name] = deepcopy(value)
            elif name not in config:
                config[name] = deepcopy(value) if value else {}

        # special treatment for old config

        # 'exhibitor' inside 'zookeeper':
        if 'zookeeper' in config and 'exhibitor' in config['zookeeper']:
            config['exhibitor'] = config['zookeeper'].pop('exhibitor')
            config.pop('zookeeper')

        pg_config = config['postgresql']
        # no 'authentication' in 'postgresql', but 'replication' and 'superuser'
        if 'authentication' not in pg_config:
            pg_config['use_pg_rewind'] = 'pg_rewind' in pg_config
            pg_config['authentication'] = {u: pg_config[u] for u in ('replication', 'superuser') if u in pg_config}
        # no 'superuser' in 'postgresql'.'authentication'
        if 'superuser' not in pg_config['authentication'] and 'pg_rewind' in pg_config:
            pg_config['authentication']['superuser'] = pg_config['pg_rewind']

        # no 'name' in config
        if 'name' not in config and 'name' in pg_config:
            config['name'] = pg_config['name']

        pg_config.update({p: config[p] for p in ('name', 'scope', 'retry_timeout',
                          'maximum_lag_on_failover') if p in config})

        return config

    def get(self, key, default=None):
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key):
        return key in self.__effective_configuration

    def __getitem__(self, key):
        return self.__effective_configuration[key]
