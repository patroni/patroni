import json
import logging
import os
import sys
import tempfile
import yaml

from collections import defaultdict
from copy import deepcopy
from patroni.dcs import ClusterConfig
from patroni.postgresql import Postgresql
from patroni.utils import deep_compare, parse_int, patch_config

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

    PATRONI_ENV_PREFIX = 'PATRONI_'
    PATRONI_CONFIG_VARIABLE = PATRONI_ENV_PREFIX + 'CONFIGURATION'

    __CACHE_FILENAME = 'patroni.dynamic.json'
    __DEFAULT_CONFIG = {
        'ttl': 30, 'loop_wait': 10, 'retry_timeout': 10,
        'maximum_lag_on_failover': 1048576,
        'master_start_timeout': 300,
        'synchronous_mode': False,
        'synchronous_mode_strict': False,
        'postgresql': {
            'bin_dir': '',
            'use_slots': True,
            'parameters': {p: v[0] for p, v in Postgresql.CMDLINE_OPTIONS.items()}
        },
        'watchdog': {
            'mode': 'automatic',
        }
    }

    def __init__(self):
        self._modify_index = -1
        self._dynamic_configuration = {}

        self.__environment_configuration = self._build_environment_configuration()

        # Patroni reads the configuration from the command-line argument if it exists, otherwise from the environment
        self._config_file = len(sys.argv) >= 2 and os.path.isfile(sys.argv[1]) and sys.argv[1]
        if self._config_file:
            self._local_configuration = self._load_config_file()
        else:
            config_env = os.environ.pop(self.PATRONI_CONFIG_VARIABLE, None)
            self._local_configuration = config_env and yaml.safe_load(config_env) or self.__environment_configuration
            if not self._local_configuration:
                print('Usage: {0} config.yml'.format(sys.argv[0]))
                print('\tPatroni may also read the configuration from the {0} environment variable'.
                      format(self.PATRONI_CONFIG_VARIABLE))
                sys.exit(1)

        self.__effective_configuration = self._build_effective_configuration({}, self._local_configuration)
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
        """Loads config.yaml from filesystem and applies some values which were set via ENV"""
        with open(self._config_file) as f:
            config = yaml.safe_load(f)
            patch_config(config, self.__environment_configuration)
            return config

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
            elif name in config:  # only variables present in __DEFAULT_CONFIG allowed to be overriden from DCS
                if name in ('synchronous_mode', 'synchronous_mode_strict'):
                    config[name] = value
                else:
                    config[name] = int(value)
        return config

    @staticmethod
    def _build_environment_configuration():
        ret = defaultdict(dict)

        def _popenv(name):
            return os.environ.pop(Config.PATRONI_ENV_PREFIX + name.upper(), None)

        for param in ('name', 'namespace', 'scope'):
            value = _popenv(param)
            if value:
                ret[param] = value

        def _set_section_values(section, params):
            for param in params:
                value = _popenv(section + '_' + param)
                if value:
                    ret[section][param] = value

        _set_section_values('restapi', ['listen', 'connect_address', 'certfile', 'keyfile'])
        _set_section_values('postgresql', ['listen', 'connect_address', 'data_dir', 'pgpass', 'bin_dir'])

        def _get_auth(name):
            ret = {}
            for param in ('username', 'password'):
                value = _popenv(name + '_' + param)
                if value:
                    ret[param] = value
            return ret

        restapi_auth = _get_auth('restapi')
        if restapi_auth:
            ret['restapi']['authentication'] = restapi_auth

        authentication = {}
        for user_type in ('replication', 'superuser'):
            entry = _get_auth(user_type)
            if entry:
                authentication[user_type] = entry

        if authentication:
            ret['postgresql']['authentication'] = authentication

        users = {}

        def _parse_list(value):
            if not (value.strip().startswith('-') or '[' in value):
                value = '[{0}]'.format(value)
            try:
                return yaml.safe_load(value)
            except Exception:
                logger.exception('Exception when parsing list %s', value)
                return None

        for param in list(os.environ.keys()):
            if param.startswith(Config.PATRONI_ENV_PREFIX):
                name, suffix = (param[8:].split('_', 1) + [''])[:2]
                if name and suffix:
                    # PATRONI_(ETCD|CONSUL|ZOOKEEPER|EXHIBITOR|...)_(HOSTS?|PORT|..)
                    if suffix in ('HOST', 'HOSTS', 'PORT', 'SRV', 'URL', 'PROXY', 'CACERT', 'CERT',
                                  'KEY', 'VERIFY', 'TOKEN', 'CHECKS', 'DC', 'NAMESPACE', 'CONTEXT',
                                  'USE_ENDPOINTS', 'SCOPE_LABEL', 'ROLE_LABEL', 'POD_IP', 'PORTS', 'LABELS'):
                        value = os.environ.pop(param)
                        if suffix == 'PORT':
                            value = value and parse_int(value)
                        elif suffix in ('HOSTS', 'PORTS', 'CHECKS'):
                            value = value and _parse_list(value)
                        elif suffix == 'LABELS':
                            if not value.strip().startswith('{'):
                                value = '{{{0}}}'.format(value)
                            try:
                                value = yaml.safe_load(value)
                            except Exception:
                                logger.exception('Exception when parsing dict %s', value)
                                value = None
                        if value:
                            ret[name.lower()][suffix.lower()] = value
                    # PATRONI_<username>_PASSWORD=<password>, PATRONI_<username>_OPTIONS=<option1,option2,...>
                    # CREATE USER "<username>" WITH <OPTIONS> PASSWORD '<password>'
                    elif suffix == 'PASSWORD':
                        password = os.environ.pop(param)
                        if password:
                            users[name] = {'password': password}
                            options = os.environ.pop(param[:-9] + '_OPTIONS', None)
                            options = options and _parse_list(options)
                            if options:
                                users[name]['options'] = options
        if users:
            ret['bootstrap']['users'] = users

        return ret

    def _build_effective_configuration(self, dynamic_configuration, local_configuration):
        config = self._safe_copy_dynamic_configuration(dynamic_configuration)
        for name, value in local_configuration.items():
            if name == 'postgresql':
                for name, value in (value or {}).items():
                    if name == 'parameters':
                        config['postgresql'][name].update(self._process_postgresql_parameters(value, True))
                    elif name != 'use_slots':  # replication slots must be enabled/disabled globally
                        config['postgresql'][name] = deepcopy(value)
            elif name not in config or name in ['watchdog']:
                config[name] = deepcopy(value) if value else {}

        # restapi server expects to get restapi.auth = 'username:password'
        if 'authentication' in config['restapi']:
            config['restapi']['auth'] = '{username}:{password}'.format(**config['restapi']['authentication'])

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
                          'synchronous_mode', 'maximum_lag_on_failover') if p in config})

        return config

    def get(self, key, default=None):
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key):
        return key in self.__effective_configuration

    def __getitem__(self, key):
        return self.__effective_configuration[key]

    def copy(self):
        return deepcopy(self.__effective_configuration)
