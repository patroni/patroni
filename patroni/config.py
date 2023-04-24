import json
import logging
import os
import shutil
import tempfile
import yaml

from collections import defaultdict
from copy import deepcopy
from typing import Any, Callable, Collection, Dict, List, Optional, Union

from . import PATRONI_ENV_PREFIX
from .collections import CaseInsensitiveDict
from .dcs import ClusterConfig, Cluster
from .exceptions import ConfigParseError
from .postgresql.config import ConfigHandler
from .utils import deep_compare, parse_bool, parse_int, patch_config

logger = logging.getLogger(__name__)

_AUTH_ALLOWED_PARAMETERS = (
    'username',
    'password',
    'sslmode',
    'sslcert',
    'sslkey',
    'sslpassword',
    'sslrootcert',
    'sslcrl',
    'sslcrldir',
    'gssencmode',
    'channel_binding'
)


def default_validator(conf: Dict[str, Any]) -> List[str]:
    if not conf:
        raise ConfigParseError("Config is empty.")
    return []


class GlobalConfig(object):

    """A class that wrapps global configuration and provides convinient methods to access/check values.

    It is instantiated by calling :func:`Config.global_config` method which picks either a
    configuration from provided :class:`Cluster` object (the most up-to-date) or from the
    local cache if :class::`ClusterConfig` is not initialized or doesn't have a valid config.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize :class:`GlobalConfig` object.

        :param config: current configuration either from
                       :class:`ClusterConfig` or from :class:`Config.dynamic_configuration`
        """
        self.__config = config

    def get(self, name: str) -> Any:
        """Gets global configuration value by name.

        :param name: parameter name
        :returns: configuration value or `None` if it is missing
        """
        return self.__config.get(name)

    def check_mode(self, mode: str) -> bool:
        """Checks whether the certain parameter is enabled.

        :param mode: parameter name could be: synchronous_mode, failsafe_mode, pause, check_timeline, and so on
        :returns: `True` if *mode* is enabled in the global configuration.
        """
        return bool(parse_bool(self.__config.get(mode)))

    @property
    def is_paused(self) -> bool:
        """:returns: `True` if cluster is in maintenance mode."""
        return self.check_mode('pause')

    @property
    def is_synchronous_mode(self) -> bool:
        """:returns: `True` if synchronous replication is requested."""
        return self.check_mode('synchronous_mode')

    @property
    def is_synchronous_mode_strict(self) -> bool:
        """:returns: `True` if at least one synchronous node is required."""
        return self.check_mode('synchronous_mode_strict')

    def get_standby_cluster_config(self) -> Union[Dict[str, Any], Any]:
        """:returns: "standby_cluster" configuration."""
        return deepcopy(self.get('standby_cluster'))

    @property
    def is_standby_cluster(self) -> bool:
        """:returns: `True` if global configuration has a valid "standby_cluster" section."""
        config = self.get_standby_cluster_config()
        return isinstance(config, dict) and\
            bool(config.get('host') or config.get('port') or config.get('restore_command'))

    def get_int(self, name: str, default: int = 0) -> int:
        """Gets current value from the global configuration and trying to return it as int.

        :param name: name of the parameter
        :param default: default value if *name* is not in the configuration or invalid
        :returns: currently configured value from the global configuration or *default* if it is not set or invalid.
        """
        ret = parse_int(self.get(name))
        return default if ret is None else ret

    @property
    def min_synchronous_nodes(self) -> int:
        """:returns: the minimal number of synchronous nodes based on whether strict mode is requested or not."""
        return 1 if self.is_synchronous_mode_strict else 0

    @property
    def synchronous_node_count(self) -> int:
        """:returns: currently configured value from the global configuration or 1 if it is not set or invalid."""
        return max(self.get_int('synchronous_node_count', 1), self.min_synchronous_nodes)

    @property
    def maximum_lag_on_failover(self) -> int:
        """:returns: currently configured value from the global configuration or 1048576 if it is not set or invalid."""
        return self.get_int('maximum_lag_on_failover', 1048576)

    @property
    def maximum_lag_on_syncnode(self) -> int:
        """:returns: currently configured value from the global configuration or -1 if it is not set or invalid."""
        return self.get_int('maximum_lag_on_syncnode', -1)

    @property
    def primary_start_timeout(self) -> int:
        """:returns: currently configured value from the global configuration or 300 if it is not set or invalid."""
        default = 300
        return self.get_int('primary_start_timeout', default)\
            if 'primary_start_timeout' in self.__config else self.get_int('master_start_timeout', default)

    @property
    def primary_stop_timeout(self) -> int:
        """:returns: currently configured value from the global configuration or 300 if it is not set or invalid."""
        default = 0
        return self.get_int('primary_stop_timeout', default)\
            if 'primary_stop_timeout' in self.__config else self.get_int('master_stop_timeout', default)


def get_global_config(cluster: Union[Cluster, None], default: Optional[Dict[str, Any]] = None) -> GlobalConfig:
    """Instantiates :class:`GlobalConfig` based on the input.

    :param cluster: the currently known cluster state from DCS
    :param default: default configuration, which will be used if there is no valid *cluster.config*
    :returns: :class:`GlobalConfig` object
    """
    # Try to protect from the case when DCS was wiped out
    if cluster and cluster.config and cluster.config.modify_index:
        config = cluster.config.data
    else:
        config = default or {}
    return GlobalConfig(deepcopy(config))


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
    __DEFAULT_CONFIG: Dict[str, Any] = {
        'ttl': 30, 'loop_wait': 10, 'retry_timeout': 10,
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
            'parameters': CaseInsensitiveDict({p: v[0] for p, v in ConfigHandler.CMDLINE_OPTIONS.items()
                                               if p not in ('wal_keep_segments', 'wal_keep_size')})
        },
        'watchdog': {
            'mode': 'automatic',
        }
    }

    def __init__(self, configfile: str,
                 validator: Optional[Callable[[Dict[str, Any]], List[str]]] = default_validator) -> None:
        self._modify_index = -1
        self._dynamic_configuration = {}

        self.__environment_configuration = self._build_environment_configuration()

        # Patroni reads the configuration from the command-line argument if it exists, otherwise from the environment
        self._config_file = configfile if configfile and os.path.exists(configfile) else None
        if self._config_file:
            self._local_configuration = self._load_config_file()
        else:
            config_env = os.environ.pop(self.PATRONI_CONFIG_VARIABLE, None)
            self._local_configuration = config_env and yaml.safe_load(config_env) or self.__environment_configuration

        if validator:
            errors = validator(self._local_configuration)
            if errors:
                raise ConfigParseError("\n".join(errors))

        self.__effective_configuration = self._build_effective_configuration({}, self._local_configuration)
        self._data_dir = self.__effective_configuration.get('postgresql', {}).get('data_dir', "")
        self._cache_file = os.path.join(self._data_dir, self.__CACHE_FILENAME)
        self._load_cache()
        self._cache_needs_saving = False

    @property
    def config_file(self) -> Union[str, None]:
        return self._config_file

    @property
    def dynamic_configuration(self) -> Dict[str, Any]:
        return deepcopy(self._dynamic_configuration)

    def _load_config_path(self, path: str) -> Dict[str, Any]:
        """
        If path is a file, loads the yml file pointed to by path.
        If path is a directory, loads all yml files in that directory in alphabetical order
        """
        if os.path.isfile(path):
            files = [path]
        elif os.path.isdir(path):
            files = [os.path.join(path, f) for f in sorted(os.listdir(path))
                     if (f.endswith('.yml') or f.endswith('.yaml')) and os.path.isfile(os.path.join(path, f))]
        else:
            logger.error('config path %s is neither directory nor file', path)
            raise ConfigParseError('invalid config path')

        overall_config: Dict[str, Any] = {}
        for fname in files:
            with open(fname) as f:
                config = yaml.safe_load(f)
                patch_config(overall_config, config)
        return overall_config

    def _load_config_file(self) -> Dict[str, Any]:
        """Loads config.yaml from filesystem and applies some values which were set via ENV"""
        assert self._config_file is not None
        config = self._load_config_path(self._config_file)
        patch_config(config, self.__environment_configuration)
        return config

    def _load_cache(self) -> None:
        if os.path.isfile(self._cache_file):
            try:
                with open(self._cache_file) as f:
                    self.set_dynamic_configuration(json.load(f))
            except Exception:
                logger.exception('Exception when loading file: %s', self._cache_file)

    def save_cache(self) -> None:
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
    def set_dynamic_configuration(self, configuration: Union[ClusterConfig, Dict[str, Any]]) -> bool:
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
        return False

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
    def _process_postgresql_parameters(parameters: Dict[str, Any], is_local: bool = False) -> Dict[str, Any]:
        return {name: value for name, value in (parameters or {}).items()
                if name not in ConfigHandler.CMDLINE_OPTIONS
                or not is_local and ConfigHandler.CMDLINE_OPTIONS[name][1](value)}

    def _safe_copy_dynamic_configuration(self, dynamic_configuration: Dict[str, Any]) -> Dict[str, Any]:
        config = deepcopy(self.__DEFAULT_CONFIG)

        for name, value in dynamic_configuration.items():
            if name == 'postgresql':
                for name, value in (value or {}).items():
                    if name == 'parameters':
                        config['postgresql'][name].update(self._process_postgresql_parameters(value))
                    elif name not in ('connect_address', 'proxy_address', 'listen',
                                      'config_dir', 'data_dir', 'pgpass', 'authentication'):
                        config['postgresql'][name] = deepcopy(value)
            elif name == 'standby_cluster':
                for name, value in (value or {}).items():
                    if name in self.__DEFAULT_CONFIG['standby_cluster']:
                        config['standby_cluster'][name] = deepcopy(value)
            elif name in config:  # only variables present in __DEFAULT_CONFIG allowed to be overridden from DCS
                config[name] = int(value)
        return config

    @staticmethod
    def _build_environment_configuration() -> Dict[str, Any]:
        ret: Dict[str, Any] = defaultdict(dict)

        def _popenv(name: str) -> Union[str, None]:
            return os.environ.pop(PATRONI_ENV_PREFIX + name.upper(), None)

        for param in ('name', 'namespace', 'scope'):
            value = _popenv(param)
            if value:
                ret[param] = value

        def _fix_log_env(name: str, oldname: str) -> None:
            value = _popenv(oldname)
            name = PATRONI_ENV_PREFIX + 'LOG_' + name.upper()
            if value and name not in os.environ:
                os.environ[name] = value

        for name, oldname in (('level', 'loglevel'), ('format', 'logformat'), ('dateformat', 'log_datefmt')):
            _fix_log_env(name, oldname)

        def _set_section_values(section: str, params: List[str]) -> None:
            for param in params:
                value = _popenv(section + '_' + param)
                if value:
                    ret[section][param] = value

        _set_section_values('restapi', ['listen', 'connect_address', 'certfile', 'keyfile', 'keyfile_password',
                                        'cafile', 'ciphers', 'verify_client', 'http_extra_headers',
                                        'https_extra_headers', 'allowlist', 'allowlist_include_members',
                                        'request_queue_size'])
        _set_section_values('ctl', ['insecure', 'cacert', 'certfile', 'keyfile', 'keyfile_password'])
        _set_section_values('postgresql', ['listen', 'connect_address', 'proxy_address',
                                           'config_dir', 'data_dir', 'pgpass', 'bin_dir'])
        _set_section_values('log', ['level', 'traceback_level', 'format', 'dateformat', 'max_queue_size',
                                    'dir', 'file_size', 'file_num', 'loggers'])
        _set_section_values('raft', ['data_dir', 'self_addr', 'partner_addrs', 'password', 'bind_addr'])

        for first, second in (('restapi', 'allowlist_include_members'), ('ctl', 'insecure')):
            value = ret.get(first, {}).pop(second, None)
            if value:
                value = parse_bool(value)
                if value is not None:
                    ret[first][second] = value

        for first, params in (('restapi', ('request_queue_size',)),
                              ('log', ('max_queue_size', 'file_size', 'file_num'))):
            for second in params:
                value = ret.get(first, {}).pop(second, None)
                if value:
                    value = parse_int(value)
                    if value is not None:
                        ret[first][second] = value

        def _parse_list(value: str) -> Union[List[str], None]:
            if not (value.strip().startswith('-') or '[' in value):
                value = '[{0}]'.format(value)
            try:
                return yaml.safe_load(value)
            except Exception:
                logger.exception('Exception when parsing list %s', value)
                return None

        for first, second in (('raft', 'partner_addrs'), ('restapi', 'allowlist')):
            value = ret.get(first, {}).pop(second, None)
            if value:
                value = _parse_list(value)
                if value:
                    ret[first][second] = value

        def _parse_dict(value: str) -> Union[Dict[str, Any], None]:
            if not value.strip().startswith('{'):
                value = '{{{0}}}'.format(value)
            try:
                return yaml.safe_load(value)
            except Exception:
                logger.exception('Exception when parsing dict %s', value)
                return None

        for first, params in (('restapi', ('http_extra_headers', 'https_extra_headers')), ('log', ('loggers',))):
            for second in params:
                value = ret.get(first, {}).pop(second, None)
                if value:
                    value = _parse_dict(value)
                    if value:
                        ret[first][second] = value

        def _get_auth(name: str, params: Optional[Collection[str]] = None) -> Dict[str, str]:
            ret: Dict[str, str] = {}
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

        for param in list(os.environ.keys()):
            if param.startswith(PATRONI_ENV_PREFIX):
                # PATRONI_(ETCD|CONSUL|ZOOKEEPER|EXHIBITOR|...)_(HOSTS?|PORT|..)
                name, suffix = (param[8:].split('_', 1) + [''])[:2]
                if suffix in ('HOST', 'HOSTS', 'PORT', 'USE_PROXIES', 'PROTOCOL', 'SRV', 'SRV_SUFFIX', 'URL', 'PROXY',
                              'CACERT', 'CERT', 'KEY', 'VERIFY', 'TOKEN', 'CHECKS', 'DC', 'CONSISTENCY',
                              'REGISTER_SERVICE', 'SERVICE_CHECK_INTERVAL', 'SERVICE_CHECK_TLS_SERVER_NAME',
                              'SERVICE_TAGS', 'NAMESPACE', 'CONTEXT', 'USE_ENDPOINTS', 'SCOPE_LABEL', 'ROLE_LABEL',
                              'POD_IP', 'PORTS', 'LABELS', 'BYPASS_API_SERVICE', 'RETRIABLE_HTTP_CODES', 'KEY_PASSWORD',
                              'USE_SSL', 'SET_ACLS', 'GROUP', 'DATABASE') and name:
                    value = os.environ.pop(param)
                    if name == 'CITUS':
                        if suffix == 'GROUP':
                            value = parse_int(value)
                        elif suffix != 'DATABASE':
                            continue
                    elif suffix == 'PORT':
                        value = value and parse_int(value)
                    elif suffix in ('HOSTS', 'PORTS', 'CHECKS', 'SERVICE_TAGS', 'RETRIABLE_HTTP_CODES'):
                        value = value and _parse_list(value)
                    elif suffix in ('LABELS', 'SET_ACLS'):
                        value = _parse_dict(value)
                    elif suffix in ('USE_PROXIES', 'REGISTER_SERVICE', 'USE_ENDPOINTS', 'BYPASS_API_SERVICE', 'VERIFY'):
                        value = parse_bool(value)
                    if value is not None:
                        ret[name.lower()][suffix.lower()] = value
        for dcs in ('etcd', 'etcd3'):
            if dcs in ret:
                ret[dcs].update(_get_auth(dcs))

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

    def _build_effective_configuration(self, dynamic_configuration: Dict[str, Any],
                                       local_configuration: Dict[str, Union[Dict[str, Any], Any]]) -> Dict[str, Any]:
        config = self._safe_copy_dynamic_configuration(dynamic_configuration)
        for name, value in local_configuration.items():
            if name == 'citus':  # remove invalid citus configuration
                if isinstance(value, dict) and isinstance(value.get('group'), int)\
                        and isinstance(value.get('database'), str):
                    config[name] = value
            elif name == 'postgresql':
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

        # when bootstrapping the new Citus cluster (coordinator/worker) enable sync replication in global configuration
        if 'citus' in config:
            bootstrap = config.setdefault('bootstrap', {})
            dcs = bootstrap.setdefault('dcs', {})
            dcs.setdefault('synchronous_mode', True)

        updated_fields = (
            'name',
            'scope',
            'retry_timeout',
            'citus'
        )

        pg_config.update({p: config[p] for p in updated_fields if p in config})

        return config

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self.__effective_configuration

    def __getitem__(self, key: str) -> Any:
        return self.__effective_configuration[key]

    def copy(self) -> Dict[str, Any]:
        return deepcopy(self.__effective_configuration)

    def get_global_config(self, cluster: Union[Cluster, None]) -> GlobalConfig:
        """Instantiate :class:`GlobalConfig` based on input.

        Use the configuration from provided *cluster* (the most up-to-date) or from the
        local cache if *cluster.config* is not initialized or doesn't have a valid config.
        :param cluster: the currently known cluster state from DCS
        :returns: :class:`GlobalConfig` object
        """
        return get_global_config(cluster, self._dynamic_configuration)
