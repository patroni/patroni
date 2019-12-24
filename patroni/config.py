import json
import logging
import os
import shutil
import tempfile
import yaml

from collections import defaultdict
from copy import deepcopy
from patroni import PATRONI_ENV_PREFIX
from patroni.exceptions import ConfigParseError
from patroni.dcs import ClusterConfig
from patroni.postgresql.config import CaseInsensitiveDict, ConfigHandler
from patroni.utils import deep_compare, parse_bool, parse_int, patch_config

logger = logging.getLogger(__name__)

_AUTH_ALLOWED_PARAMETERS = (
    'username',
    'password',
    'sslmode',
    'sslcert',
    'sslkey',
    'sslrootcert',
    'sslcrl'
)


def default_validator(conf):
    if not conf:
        return "Config is empty."


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

    PATRONI_CONFIG_VARIABLE = PATRONI_ENV_PREFIX + 'CONFIGURATION'

    __CACHE_FILENAME = 'patroni.dynamic.json'
    __DEFAULT_CONFIG = {
        'ttl': 30, 'loop_wait': 10, 'retry_timeout': 10,
        'maximum_lag_on_failover': 1048576,
        'check_timeline': False,
        'master_start_timeout': 300,
        'synchronous_mode': False,
        'synchronous_mode_strict': False,
        'standby_cluster': {
            'create_replica_methods': '',
            'host': '',
            'port': '',
            'primary_slot_name': '',
            'restore_command': '',
            'archive_cleanup_command': '',
            'recovery_min_apply_delay': ''
        },
        'postgresql': {
            'bin_dir': '',
            'use_slots': True,
            'parameters': CaseInsensitiveDict({p: v[0] for p, v in ConfigHandler.CMDLINE_OPTIONS.items()})
        },
        'watchdog': {
            'mode': 'automatic',
        }
    }

    def __init__(self, configfile, validator=default_validator):
        self._modify_index = -1
        self._dynamic_configuration = {}

        self.__environment_configuration = self._build_environment_configuration()

        # Patroni reads the configuration from the command-line argument if it exists, otherwise from the environment
        self._config_file = configfile and os.path.isfile(configfile) and configfile
        if self._config_file:
            self._local_configuration = self._load_config_file()
        else:
            config_env = os.environ.pop(self.PATRONI_CONFIG_VARIABLE, None)
            self._local_configuration = config_env and yaml.safe_load(config_env) or self.__environment_configuration
        if validator:
            error = validator(self._local_configuration)
            if error:
                raise ConfigParseError(error)

        self.__effective_configuration = self._build_effective_configuration({}, self._local_configuration)
        self._data_dir = self.__effective_configuration.get('postgresql', {}).get('data_dir', "")
        self._cache_file = os.path.join(self._data_dir, self.__CACHE_FILENAME)
        self._load_cache()
        self._cache_needs_saving = False

    @property
    def config_file(self):
        return self._config_file

    @property
    def dynamic_configuration(self):
        return deepcopy(self._dynamic_configuration)

    def check_mode(self, mode):
        return bool(parse_bool(self._dynamic_configuration.get(mode)))

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
                tmpfile = shutil.move(tmpfile, self._cache_file)
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

    def reload_local_configuration(self):
        if self.config_file:
            try:
                configuration = self._load_config_file()
                if not deep_compare(self._local_configuration, configuration):
                    new_configuration = self._build_effective_configuration(self._dynamic_configuration, configuration)
                    self._local_configuration = configuration
                    self.__effective_configuration = new_configuration
                    return True
                else:
                    logger.info('No local configuration items changed.')
            except Exception:
                logger.exception('Exception when reloading local configuration from %s', self.config_file)

    @staticmethod
    def _process_postgresql_parameters(parameters, is_local=False):
        return {name: value for name, value in (parameters or {}).items()
                if name not in ConfigHandler.CMDLINE_OPTIONS or
                not is_local and ConfigHandler.CMDLINE_OPTIONS[name][1](value)}

    def _safe_copy_dynamic_configuration(self, dynamic_configuration):
        config = deepcopy(self.__DEFAULT_CONFIG)

        for name, value in dynamic_configuration.items():
            if name == 'postgresql':
                for name, value in (value or {}).items():
                    if name == 'parameters':
                        config['postgresql'][name].update(self._process_postgresql_parameters(value))
                    elif name not in ('connect_address', 'listen', 'data_dir', 'pgpass', 'authentication'):
                        config['postgresql'][name] = deepcopy(value)
            elif name == 'standby_cluster':
                for name, value in (value or {}).items():
                    if name in self.__DEFAULT_CONFIG['standby_cluster']:
                        config['standby_cluster'][name] = deepcopy(value)
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
            return os.environ.pop(PATRONI_ENV_PREFIX + name.upper(), None)

        for param in ('name', 'namespace', 'scope'):
            value = _popenv(param)
            if value:
                ret[param] = value

        def _fix_log_env(name, oldname):
            value = _popenv(oldname)
            name = PATRONI_ENV_PREFIX + 'LOG_' + name.upper()
            if value and name not in os.environ:
                os.environ[name] = value

        for name, oldname in (('level', 'loglevel'), ('format', 'logformat'), ('dateformat', 'log_datefmt')):
            _fix_log_env(name, oldname)

        def _set_section_values(section, params):
            for param in params:
                value = _popenv(section + '_' + param)
                if value:
                    ret[section][param] = value

        _set_section_values('restapi', ['listen', 'connect_address', 'certfile', 'keyfile', 'cafile', 'verify_client'])
        _set_section_values('ctl', ['insecure', 'cacert', 'certfile', 'keyfile'])
        _set_section_values('postgresql', ['listen', 'connect_address', 'config_dir', 'data_dir', 'pgpass', 'bin_dir'])
        _set_section_values('log', ['level', 'traceback_level', 'format', 'dateformat', 'max_queue_size',
                                    'dir', 'file_size', 'file_num', 'loggers'])

        def _parse_dict(value):
            if not value.strip().startswith('{'):
                value = '{{{0}}}'.format(value)
            try:
                return yaml.safe_load(value)
            except Exception:
                logger.exception('Exception when parsing dict %s', value)
                return None

        value = ret.get('log', {}).pop('loggers', None)
        if value:
            value = _parse_dict(value)
            if value:
                ret['log']['loggers'] = value

        def _get_auth(name, params=None):
            ret = {}
            for param in params or _AUTH_ALLOWED_PARAMETERS[:2]:
                value = _popenv(name + '_' + param)
                if value:
                    ret[param] = value
            return ret

        restapi_auth = _get_auth('restapi')
        if restapi_auth:
            ret['restapi']['authentication'] = restapi_auth

        authentication = {}
        for user_type in ('replication', 'superuser', 'rewind'):
            entry = _get_auth(user_type, _AUTH_ALLOWED_PARAMETERS)
            if entry:
                authentication[user_type] = entry

        if authentication:
            ret['postgresql']['authentication'] = authentication

        def _parse_list(value):
            if not (value.strip().startswith('-') or '[' in value):
                value = '[{0}]'.format(value)
            try:
                return yaml.safe_load(value)
            except Exception:
                logger.exception('Exception when parsing list %s', value)
                return None

        for param in list(os.environ.keys()):
            if param.startswith(PATRONI_ENV_PREFIX):
                # PATRONI_(ETCD|CONSUL|ZOOKEEPER|EXHIBITOR|...)_(HOSTS?|PORT|..)
                name, suffix = (param[8:].split('_', 1) + [''])[:2]
                if suffix in ('HOST', 'HOSTS', 'PORT', 'USE_PROXIES', 'PROTOCOL', 'SRV', 'URL', 'PROXY',
                              'CACERT', 'CERT', 'KEY', 'VERIFY', 'TOKEN', 'CHECKS', 'DC', 'CONSISTENCY',
                              'REGISTER_SERVICE', 'SERVICE_CHECK_INTERVAL', 'NAMESPACE', 'CONTEXT',
                              'USE_ENDPOINTS', 'SCOPE_LABEL', 'ROLE_LABEL', 'POD_IP', 'PORTS', 'LABELS') and name:
                    value = os.environ.pop(param)
                    if suffix == 'PORT':
                        value = value and parse_int(value)
                    elif suffix in ('HOSTS', 'PORTS', 'CHECKS'):
                        value = value and _parse_list(value)
                    elif suffix == 'LABELS':
                        value = _parse_dict(value)
                    elif suffix in ('USE_PROXIES', 'REGISTER_SERVICE'):
                        value = parse_bool(value)
                    if value:
                        ret[name.lower()][suffix.lower()] = value
        if 'etcd' in ret:
            ret['etcd'].update(_get_auth('etcd'))

        users = {}
        for param in list(os.environ.keys()):
            if param.startswith(PATRONI_ENV_PREFIX):
                name, suffix = (param[8:].rsplit('_', 1) + [''])[:2]
                # PATRONI_<username>_PASSWORD=<password>, PATRONI_<username>_OPTIONS=<option1,option2,...>
                # CREATE USER "<username>" WITH <OPTIONS> PASSWORD '<password>'
                if name and suffix == 'PASSWORD':
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
        if 'restapi' in config and 'authentication' in config['restapi']:
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

        # handle setting additional connection parameters that may be available
        # in the configuration file, such as SSL connection parameters
        for name, value in pg_config['authentication'].items():
            pg_config['authentication'][name] = {n: v for n, v in value.items() if n in _AUTH_ALLOWED_PARAMETERS}

        # no 'name' in config
        if 'name' not in config and 'name' in pg_config:
            config['name'] = pg_config['name']

        updated_fields = (
            'name',
            'scope',
            'retry_timeout',
            'synchronous_mode',
            'synchronous_mode_strict',
        )

        pg_config.update({p: config[p] for p in updated_fields if p in config})

        return config

    def get(self, key, default=None):
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key):
        return key in self.__effective_configuration

    def __getitem__(self, key):
        return self.__effective_configuration[key]

    def copy(self):
        return deepcopy(self.__effective_configuration)
