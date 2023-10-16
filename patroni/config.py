"""Facilities related to Patroni configuration."""
import json
import logging
import os
import shutil
import tempfile
import yaml

from collections import defaultdict
from copy import deepcopy
from typing import Any, Callable, Collection, Dict, List, Optional, Union, TYPE_CHECKING

from . import PATRONI_ENV_PREFIX
from .collections import CaseInsensitiveDict
from .dcs import ClusterConfig, Cluster
from .exceptions import ConfigParseError
from .file_perm import pg_perm
from .postgresql.config import ConfigHandler
from .validator import IntValidator
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
    """Ensure *conf* is not empty.

    Designed to be used as default validator for :class:`Config` objects, if no specific validator is provided.

    :param conf: configuration to be validated.

    :returns: an empty list -- :class:`Config` expects the validator to return a list of 0 or more issues found while
        validating the configuration.

    :raises:
        :class:`ConfigParseError`: if *conf* is empty.
    """
    if not conf:
        raise ConfigParseError("Config is empty.")
    return []


class GlobalConfig(object):
    """A class that wraps global configuration and provides convenient methods to access/check values.

    It is instantiated either by calling :func:`get_global_config` or :meth:`Config.get_global_config`, which picks
    either a configuration from provided :class:`Cluster` object (the most up-to-date) or from the
    local cache if :class:`ClusterConfig` is not initialized or doesn't have a valid config.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize :class:`GlobalConfig` object with given *config*.

        :param config: current configuration either from
                       :class:`ClusterConfig` or from :func:`Config.dynamic_configuration`.
        """
        self.__config = config

    def get(self, name: str) -> Any:
        """Gets global configuration value by *name*.

        :param name: parameter name.

        :returns: configuration value or ``None`` if it is missing.
        """
        return self.__config.get(name)

    def check_mode(self, mode: str) -> bool:
        """Checks whether the certain parameter is enabled.

        :param mode: parameter name, e.g. ``synchronous_mode``, ``failsafe_mode``, ``pause``, ``check_timeline``, and
            so on.

        :returns: ``True`` if parameter *mode* is enabled in the global configuration.
        """
        return bool(parse_bool(self.__config.get(mode)))

    @property
    def is_paused(self) -> bool:
        """``True`` if cluster is in maintenance mode."""
        return self.check_mode('pause')

    @property
    def is_synchronous_mode(self) -> bool:
        """``True`` if synchronous replication is requested and it is not a standby cluster config."""
        return self.check_mode('synchronous_mode') and not self.is_standby_cluster

    @property
    def is_synchronous_mode_strict(self) -> bool:
        """``True`` if at least one synchronous node is required."""
        return self.check_mode('synchronous_mode_strict')

    def get_standby_cluster_config(self) -> Union[Dict[str, Any], Any]:
        """Get ``standby_cluster`` configuration.

        :returns: a copy of ``standby_cluster`` configuration.
        """
        return deepcopy(self.get('standby_cluster'))

    @property
    def is_standby_cluster(self) -> bool:
        """``True`` if global configuration has a valid ``standby_cluster`` section."""
        config = self.get_standby_cluster_config()
        return isinstance(config, dict) and\
            bool(config.get('host') or config.get('port') or config.get('restore_command'))

    def get_int(self, name: str, default: int = 0) -> int:
        """Gets current value of *name* from the global configuration and try to return it as :class:`int`.

        :param name: name of the parameter.
        :param default: default value if *name* is not in the configuration or invalid.

        :returns: currently configured value of *name* from the global configuration or *default* if it is not set or
            invalid.
        """
        ret = parse_int(self.get(name))
        return default if ret is None else ret

    @property
    def min_synchronous_nodes(self) -> int:
        """The minimal number of synchronous nodes based on whether ``synchronous_mode_strict`` is enabled or not."""
        return 1 if self.is_synchronous_mode_strict else 0

    @property
    def synchronous_node_count(self) -> int:
        """Currently configured value of ``synchronous_node_count`` from the global configuration.

        Assume ``1`` if it is not set or invalid.
        """
        return max(self.get_int('synchronous_node_count', 1), self.min_synchronous_nodes)

    @property
    def maximum_lag_on_failover(self) -> int:
        """Currently configured value of ``maximum_lag_on_failover`` from the global configuration.

        Assume ``1048576`` if it is not set or invalid.
        """
        return self.get_int('maximum_lag_on_failover', 1048576)

    @property
    def maximum_lag_on_syncnode(self) -> int:
        """Currently configured value of ``maximum_lag_on_syncnode`` from the global configuration.

        Assume ``-1`` if it is not set or invalid.
        """
        return self.get_int('maximum_lag_on_syncnode', -1)

    @property
    def primary_start_timeout(self) -> int:
        """Currently configured value of ``primary_start_timeout`` from the global configuration.

        Assume ``300`` if it is not set or invalid.

        .. note::
            ``master_start_timeout`` is still supported to keep backward compatibility.
        """
        default = 300
        return self.get_int('primary_start_timeout', default)\
            if 'primary_start_timeout' in self.__config else self.get_int('master_start_timeout', default)

    @property
    def primary_stop_timeout(self) -> int:
        """Currently configured value of ``primary_stop_timeout`` from the global configuration.

        Assume ``0`` if it is not set or invalid.

        .. note::
            ``master_stop_timeout`` is still supported to keep backward compatibility.
        """
        default = 0
        return self.get_int('primary_stop_timeout', default)\
            if 'primary_stop_timeout' in self.__config else self.get_int('master_stop_timeout', default)


def get_global_config(cluster: Optional[Cluster], default: Optional[Dict[str, Any]] = None) -> GlobalConfig:
    """Instantiates :class:`GlobalConfig` based on the input.

    :param cluster: the currently known cluster state from DCS.
    :param default: default configuration, which will be used if there is no valid *cluster.config*.

    :returns: :class:`GlobalConfig` object.
    """
    # Try to protect from the case when DCS was wiped out
    if cluster and cluster.config and cluster.config.modify_version:
        config = cluster.config.data
    else:
        config = default or {}
    return GlobalConfig(deepcopy(config))


class Config(object):
    """Handle Patroni configuration.

    This class is responsible for:

      1) Building and giving access to ``effective_configuration`` from:

         * ``Config.__DEFAULT_CONFIG`` -- some sane default values;
         * ``dynamic_configuration`` -- configuration stored in DCS;
         * ``local_configuration`` -- configuration from `config.yml` or environment.

      2) Saving and loading ``dynamic_configuration`` into 'patroni.dynamic.json' file
         located in local_configuration['postgresql']['data_dir'] directory.
         This is necessary to be able to restore ``dynamic_configuration``
         if DCS was accidentally wiped.

      3) Loading of configuration file in the old format and converting it into new format.

      4) Mimicking some ``dict`` interfaces to make it possible
         to work with it as with the old ``config`` object.

    :cvar PATRONI_CONFIG_VARIABLE: name of the environment variable that can be used to load Patroni configuration from.
    :cvar __CACHE_FILENAME: name of the file used to cache dynamic configuration under Postgres data directory.
    :cvar __DEFAULT_CONFIG: default configuration values for some Patroni settings.
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
            'use_slots': True,
            'parameters': CaseInsensitiveDict({p: v[0] for p, v in ConfigHandler.CMDLINE_OPTIONS.items()
                                               if v[0] is not None and p not in ('wal_keep_segments', 'wal_keep_size')})
        }
    }

    def __init__(self, configfile: str,
                 validator: Optional[Callable[[Dict[str, Any]], List[str]]] = default_validator) -> None:
        """Create a new instance of :class:`Config` and validate the loaded configuration using *validator*.

        .. note::
            Patroni will read configuration from these locations in this order:

              * file or directory path passed as command-line argument (*configfile*), if it exists and the file or
                files found in the directory can be parsed (see :meth:`~Config._load_config_path`), otherwise
              * YAML file passed via the environment variable (see :attr:`PATRONI_CONFIG_VARIABLE`), if the referenced
                file exists and can be parsed, otherwise
              * from configuration values defined as environment variables, see
                :meth:`~Config._build_environment_configuration`.

        :param configfile: path to Patroni configuration file.
        :param validator: function used to validate Patroni configuration. It should receive a dictionary which
            represents Patroni configuration, and return a list of zero or more error messages based on validation.

        :raises:
            :class:`ConfigParseError`: if any issue is reported by *validator*.
        """
        self._modify_version = -1
        self._dynamic_configuration = {}

        self.__environment_configuration = self._build_environment_configuration()

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
        if validator:  # patronictl uses validator=None and we don't want to load anything from local cache in this case
            self._load_cache()
        self._cache_needs_saving = False

    @property
    def config_file(self) -> Optional[str]:
        """Path to Patroni configuration file, if any, else ``None``."""
        return self._config_file

    @property
    def dynamic_configuration(self) -> Dict[str, Any]:
        """Deep copy of cached Patroni dynamic configuration."""
        return deepcopy(self._dynamic_configuration)

    @property
    def local_configuration(self) -> Dict[str, Any]:
        """Deep copy of cached Patroni local configuration.

        :returns: copy of :attr:`~Config._local_configuration`
        """
        return deepcopy(dict(self._local_configuration))

    @classmethod
    def get_default_config(cls) -> Dict[str, Any]:
        """Deep copy default configuration.

        :returns: copy of :attr:`~Config.__DEFAULT_CONFIG`
        """
        return deepcopy(cls.__DEFAULT_CONFIG)

    def _load_config_path(self, path: str) -> Dict[str, Any]:
        """Load Patroni configuration file(s) from *path*.

        If *path* is a file, load the yml file pointed to by *path*.
        If *path* is a directory, load all yml files in that directory in alphabetical order.

        :param path: path to either an YAML configuration file, or to a folder containing YAML configuration files.

        :returns: configuration after reading the configuration file(s) from *path*.

        :raises:
            :class:`ConfigParseError`: if *path* is invalid.
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
        """Load configuration file(s) from filesystem and apply values which were set via environment variables.

        :returns: final configuration after merging configuration file(s) and environment variables.
        """
        if TYPE_CHECKING:  # pragma: no cover
            assert self.config_file is not None
        config = self._load_config_path(self.config_file)
        patch_config(config, self.__environment_configuration)
        return config

    def _load_cache(self) -> None:
        """Load dynamic configuration from ``patroni.dynamic.json``."""
        if os.path.isfile(self._cache_file):
            try:
                with open(self._cache_file) as f:
                    self.set_dynamic_configuration(json.load(f))
            except Exception:
                logger.exception('Exception when loading file: %s', self._cache_file)

    def save_cache(self) -> None:
        """Save dynamic configuration to ``patroni.dynamic.json`` under Postgres data directory.

        .. note::
            ``patroni.dynamic.jsonXXXXXX`` is created as a temporary file and than renamed to ``patroni.dynamic.json``,
            where ``XXXXXX`` is a random suffix.
        """
        if self._cache_needs_saving:
            tmpfile = fd = None
            try:
                pg_perm.set_permissions_from_data_directory(self._data_dir)
                (fd, tmpfile) = tempfile.mkstemp(prefix=self.__CACHE_FILENAME, dir=self._data_dir)
                with os.fdopen(fd, 'w') as f:
                    fd = None
                    json.dump(self.dynamic_configuration, f)
                tmpfile = shutil.move(tmpfile, self._cache_file)
                os.chmod(self._cache_file, pg_perm.file_create_mode)
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

    def __get_and_maybe_adjust_int_value(self, config: Dict[str, Any], param: str, min_value: int) -> int:
        """Get, validate and maybe adjust a *param* integer value from the *config* :class:`dict`.

        .. note:
            If the value is smaller than provided *min_value* we update the *config*.

            This method may raise an exception if value isn't :class:`int` or cannot be casted to :class:`int`.

        :param config: :class:`dict` object with new global configuration.
        :param param: name of the configuration parameter we want to read/validate/adjust.
        :param min_value: the minimum possible value that a given *param* could have.

        :returns: an integer value which corresponds to a provided *param*.
        """
        value = int(config.get(param, self.__DEFAULT_CONFIG[param]))
        if value < min_value:
            logger.warning("%s=%d can't be smaller than %d, adjusting...", param, value, min_value)
            value = config[param] = min_value
        return value

    def _validate_and_adjust_timeouts(self, config: Dict[str, Any]) -> None:
        """Validate and adjust ``loop_wait``, ``retry_timeout``, and ``ttl`` values if necessary.

        Minimum values:

            * ``loop_wait``: 1 second;
            * ``retry_timeout``: 3 seconds.
            * ``ttl``: 20 seconds;

        Maximum values:
        In case if values don't fulfill the following rule, ``retry_timeout`` and ``loop_wait``
        are reduced so that the rule is fulfilled:

            .. code-block:: python

                loop_wait + 2 * retry_timeout <= ttl

        .. note:
            We prefer to reduce ``loop_wait`` and will reduce ``retry_timeout`` only if ``loop_wait``
            is already set to a minimal possible value.

        :param config: :class:`dict` object with new global configuration.
        """

        min_loop_wait = 1
        loop_wait = self. __get_and_maybe_adjust_int_value(config, 'loop_wait', min_loop_wait)
        retry_timeout = self. __get_and_maybe_adjust_int_value(config, 'retry_timeout', 3)
        ttl = self. __get_and_maybe_adjust_int_value(config, 'ttl', 20)

        if min_loop_wait + 2 * retry_timeout > ttl:
            config['loop_wait'] = min_loop_wait
            config['retry_timeout'] = (ttl - min_loop_wait) // 2
            logger.warning('Violated the rule "loop_wait + 2*retry_timeout <= ttl", where ttl=%d. '
                           'Adjusting loop_wait from %d to %d and retry_timeout from %d to %d',
                           ttl, loop_wait, min_loop_wait, retry_timeout, config['retry_timeout'])
        elif loop_wait + 2 * retry_timeout > ttl:
            config['loop_wait'] = ttl - 2 * retry_timeout
            logger.warning('Violated the rule "loop_wait + 2*retry_timeout <= ttl", where ttl=%d and retry_timeout=%d.'
                           ' Adjusting loop_wait from %d to %d', ttl, retry_timeout, loop_wait, config['loop_wait'])

    # configuration could be either ClusterConfig or dict
    def set_dynamic_configuration(self, configuration: Union[ClusterConfig, Dict[str, Any]]) -> bool:
        """Set dynamic configuration values with given *configuration*.

        :param configuration: new dynamic configuration values. Supports :class:`dict` for backward compatibility.

        :returns: ``True`` if changes have been detected between current dynamic configuration and the new dynamic
            *configuration*, ``False`` otherwise.
        """
        if isinstance(configuration, ClusterConfig):
            if self._modify_version == configuration.modify_version:
                return False  # If the version didn't change there is nothing to do
            self._modify_version = configuration.modify_version
            configuration = configuration.data

        if not deep_compare(self._dynamic_configuration, configuration):
            try:
                self._validate_and_adjust_timeouts(configuration)
                self.__effective_configuration = self._build_effective_configuration(configuration,
                                                                                     self._local_configuration)
                self._dynamic_configuration = configuration
                self._cache_needs_saving = True
                return True
            except Exception:
                logger.exception('Exception when setting dynamic_configuration')
        return False

    def reload_local_configuration(self) -> Optional[bool]:
        """Reload configuration values from the configuration file(s).

        .. note::
            Designed to be used when user applies changes to configuration file(s), so Patroni can use the new values
            with a reload instead of a restart.

        :returns: ``True`` if changes have been detected between current local configuration
        """
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
        """Process Postgres *parameters*.

        .. note::
            If *is_local* configuration discard any setting from *parameters* that is listed under
            :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` as those are supposed to be set only
            through dynamic configuration.

            When setting parameters from :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` through
            dynamic configuration their value will be validated as per the validator defined in that very same
            attribute entry. If the given value cannot be validated, a warning will be logged and the default value of
            the GUC will be used instead.

            Some parameters from :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` cannot be set even if
            not *is_local* configuration:

                * ``listen_addresses``: inferred from ``postgresql.listen`` local configuration or from
                    ``PATRONI_POSTGRESQL_LISTEN`` environment variable;
                * ``port``: inferred from ``postgresql.listen`` local configuration or from
                    ``PATRONI_POSTGRESQL_LISTEN`` environment variable;
                * ``cluster_name``: set through ``scope`` local configuration or through ``PATRONI_SCOPE`` environment
                    variable;
                * ``hot_standby``: always enabled;
                * ``wal_log_hints``: always enabled.

        :param parameters: Postgres parameters to be processed. Should be the parsed YAML value of
            ``postgresql.parameters`` configuration, either from local or from dynamic configuration.

        :param is_local: should be ``True`` if *parameters* refers to local configuration, or ``False`` if *parameters*
            refers to dynamic configuration.

        :returns: new value for ``postgresql.parameters`` after processing and validating *parameters*.
        """
        pg_params: Dict[str, Any] = {}

        for name, value in (parameters or {}).items():
            if name not in ConfigHandler.CMDLINE_OPTIONS:
                pg_params[name] = value
            elif not is_local:
                validator = ConfigHandler.CMDLINE_OPTIONS[name][1]
                if validator(value):
                    int_val = parse_int(value) if isinstance(validator, IntValidator) else None
                    pg_params[name] = int_val if isinstance(int_val, int) else value
                else:
                    logger.warning("postgresql parameter %s=%s failed validation, defaulting to %s",
                                   name, value, ConfigHandler.CMDLINE_OPTIONS[name][0])

        return pg_params

    def _safe_copy_dynamic_configuration(self, dynamic_configuration: Dict[str, Any]) -> Dict[str, Any]:
        """Create a copy of *dynamic_configuration*.

        Merge *dynamic_configuration* with :attr:`__DEFAULT_CONFIG` (*dynamic_configuration* takes precedence), and
        process ``postgresql.parameters`` from *dynamic_configuration* through :func:`_process_postgresql_parameters`,
        if present.

        .. note::
            The following settings are not allowed in ``postgresql`` section as they are intended to be local
            configuration, and are removed if present:

                * ``connect_address``;
                * ``proxy_address``;
                * ``listen``;
                * ``config_dir``;
                * ``data_dir``;
                * ``pgpass``;
                * ``authentication``;

            Besides that any setting present in *dynamic_configuration* but absent from :attr:`__DEFAULT_CONFIG` is
            discarded.

        :param dynamic_configuration: Patroni dynamic configuration.

        :returns: copy of *dynamic_configuration*, merged with default dynamic configuration and with some sanity checks
            performed over it.
        """
        config = self.get_default_config()

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
        """Get local configuration settings that were specified through environment variables.

        :returns: dictionary containing the found environment variables and their values, respecting the expected
            structure of Patroni configuration.
        """
        ret: Dict[str, Any] = defaultdict(dict)

        def _popenv(name: str) -> Optional[str]:
            """Get value of environment variable *name*.

            .. note::
                *name* is prefixed with :data:`~patroni.PATRONI_ENV_PREFIX` when searching in the environment.

                Also, the corresponding environment variable is removed from the environment upon reading its value.

            :param name: name of the environment variable.

            :returns: value of *name*, if present in the environment, otherwise ``None``.
            """
            return os.environ.pop(PATRONI_ENV_PREFIX + name.upper(), None)

        for param in ('name', 'namespace', 'scope'):
            value = _popenv(param)
            if value:
                ret[param] = value

        def _fix_log_env(name: str, oldname: str) -> None:
            """Normalize a log related environment variable.

            .. note::
                Patroni used to support different names for log related environment variables in the past. As the
                environment variables were renamed, this function takes care of mapping and normalizing the environment.

                *name* is prefixed with :data:`~patroni.PATRONI_ENV_PREFIX` and ``LOG`` when searching in the
                environment.

                *oldname* is prefixed with :data:`~patroni.PATRONI_ENV_PREFIX` when searching in the environment.

                If both *name* and *oldname* are set in the environment, *name* takes precedence.

            :param name: new name of a log related environment variable.
            :param oldname: original name of a log related environment variable.
            :type oldname: str
            """
            value = _popenv(oldname)
            name = PATRONI_ENV_PREFIX + 'LOG_' + name.upper()
            if value and name not in os.environ:
                os.environ[name] = value

        for name, oldname in (('level', 'loglevel'), ('format', 'logformat'), ('dateformat', 'log_datefmt')):
            _fix_log_env(name, oldname)

        def _set_section_values(section: str, params: List[str]) -> None:
            """Get value of *params* environment variables that are related with *section*.

            .. note::
                The values are retrieved from the environment and updated directly into the returning dictionary of
                :func:`_build_environment_configuration`.

            :param section: configuration section the *params* belong to.
            :param params: name of the Patroni settings.
            """
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

        for binary in ('pg_ctl', 'initdb', 'pg_controldata', 'pg_basebackup', 'postgres', 'pg_isready', 'pg_rewind'):
            value = _popenv('POSTGRESQL_BIN_' + binary)
            if value:
                ret['postgresql'].setdefault('bin_name', {})[binary] = value

        # parse all values retrieved from the environment as Python objects, according to the expected type
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

        def _parse_list(value: str) -> Optional[List[str]]:
            """Parse an YAML list *value* as a :class:`list`.

            :param value: YAML list as a string.

            :returns: *value* as :class:`list`.
            """
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

        def _parse_dict(value: str) -> Optional[Dict[str, Any]]:
            """Parse an YAML dictionary *value* as a :class:`dict`.

            :param value: YAML dictionary as a string.

            :returns: *value* as :class:`dict`.
            """
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

        def _get_auth(name: str, params: Collection[str] = _AUTH_ALLOWED_PARAMETERS[:2]) -> Dict[str, str]:
            """Get authorization related environment variables *params* from section *name*.

            :param name: name of a configuration section that may contain authorization *params*.
            :param params: the authorization settings that may be set under section *name*.

            :returns: dictionary containing environment values for authorization *params* of section *name*.
            """
            ret: Dict[str, str] = {}
            for param in params:
                value = _popenv(name + '_' + param)
                if value:
                    ret[param] = value
            return ret

        for section in ('ctl', 'restapi'):
            auth = _get_auth(section)
            if auth:
                ret[section]['authentication'] = auth

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
                name, suffix = (param[len(PATRONI_ENV_PREFIX):].split('_', 1) + [''])[:2]
                if suffix in ('HOST', 'HOSTS', 'PORT', 'USE_PROXIES', 'PROTOCOL', 'SRV', 'SRV_SUFFIX', 'URL', 'PROXY',
                              'CACERT', 'CERT', 'KEY', 'VERIFY', 'TOKEN', 'CHECKS', 'DC', 'CONSISTENCY',
                              'REGISTER_SERVICE', 'SERVICE_CHECK_INTERVAL', 'SERVICE_CHECK_TLS_SERVER_NAME',
                              'SERVICE_TAGS', 'NAMESPACE', 'CONTEXT', 'USE_ENDPOINTS', 'SCOPE_LABEL', 'ROLE_LABEL',
                              'POD_IP', 'PORTS', 'LABELS', 'BYPASS_API_SERVICE', 'RETRIABLE_HTTP_CODES', 'KEY_PASSWORD',
                              'USE_SSL', 'SET_ACLS', 'GROUP', 'DATABASE', 'LEADER_LABEL_VALUE', 'FOLLOWER_LABEL_VALUE',
                              'STANDBY_LEADER_LABEL_VALUE', 'TMP_ROLE_LABEL') and name:
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
                name, suffix = (param[len(PATRONI_ENV_PREFIX):].rsplit('_', 1) + [''])[:2]
                # PATRONI_<username>_PASSWORD=<password>, PATRONI_<username>_OPTIONS=<option1,option2,...>
                # CREATE USER "<username>" WITH <OPTIONS> PASSWORD '<password>'
                if name and suffix == 'PASSWORD':
                    password = os.environ.pop(param)
                    if password:
                        users[name] = {'password': password}
                        options = os.environ.pop(param[:-9] + '_OPTIONS', None)  # replace "_PASSWORD" with "_OPTIONS"
                        options = options and _parse_list(options)
                        if options:
                            users[name]['options'] = options
        if users:
            ret['bootstrap']['users'] = users

        return ret

    def _build_effective_configuration(self, dynamic_configuration: Dict[str, Any],
                                       local_configuration: Dict[str, Union[Dict[str, Any], Any]]) -> Dict[str, Any]:
        """Build effective configuration by merging *dynamic_configuration* and *local_configuration*.

        .. note::
            *local_configuration* takes precedence over *dynamic_configuration* if a setting is defined in both.

        :param dynamic_configuration: Patroni dynamic configuration.
        :param local_configuration: Patroni local configuration.

        :returns: _description_
        """
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

        # restapi server expects to get restapi.auth = 'username:password' and similarly for `ctl`
        for section in ('ctl', 'restapi'):
            if section in config and 'authentication' in config[section]:
                config[section]['auth'] = '{username}:{password}'.format(**config[section]['authentication'])

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
        """Get effective value of ``key`` setting from Patroni configuration root.

        Designed to work the same way as :func:`dict.get`.

        :param key: name of the setting.
        :param default: default value if *key* is not present in the effective configuration.

        :returns: value of *key*, if present in the effective configuration, otherwise *default*.
        """
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key: str) -> bool:
        """Check if setting *key* is present in the effective configuration.

        Designed to work the same way as :func:`dict.__contains__`.

        :param key: name of the setting to be checked.

        :returns: ``True`` if setting *key* exists in effective configuration, else ``False``.
        """
        return key in self.__effective_configuration

    def __getitem__(self, key: str) -> Any:
        """Get value of setting *key* from effective configuration.

        Designed to work the same way as :func:`dict.__getitem__`.

        :param key: name of the setting.

        :returns: value of setting *key*.

        :raises:
            :class:`KeyError`: if *key* is not present in effective configuration.
        """
        return self.__effective_configuration[key]

    def copy(self) -> Dict[str, Any]:
        """Get a deep copy of effective Patroni configuration.

        :returns: a deep copy of the Patroni configuration.
        """
        return deepcopy(self.__effective_configuration)

    def get_global_config(self, cluster: Optional[Cluster]) -> GlobalConfig:
        """Instantiate :class:`GlobalConfig` based on input.

        Use the configuration from provided *cluster* (the most up-to-date) or from the
        local cache if *cluster.config* is not initialized or doesn't have a valid config.

        :param cluster: the currently known cluster state from DCS.

        :returns: :class:`GlobalConfig` object.
        """
        return get_global_config(cluster, self._dynamic_configuration)
