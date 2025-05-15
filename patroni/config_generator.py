"""patroni ``--generate-config`` machinery."""
import abc
import logging
import os
import socket
import sys

from contextlib import contextmanager
from getpass import getpass, getuser
from typing import Any, Dict, Iterator, List, Optional, TextIO, Tuple, TYPE_CHECKING, Union

import psutil
import yaml

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor

from . import psycopg
from .collections import EMPTY_DICT
from .config import Config
from .exceptions import PatroniException
from .log import PatroniLogger
from .postgresql.config import ConfigHandler, parse_dsn
from .postgresql.misc import postgres_major_version_to_int
from .utils import get_major_version, parse_bool, patch_config, read_stripped

# Mapping between the libpq connection parameters and the environment variables.
# This dict should be kept in sync with `patroni.utils._AUTH_ALLOWED_PARAMETERS`
# (we use "username" in the Patroni config for some reason, other parameter names are the same).
_AUTH_ALLOWED_PARAMETERS_MAPPING = {
    'user': 'PGUSER',
    'password': 'PGPASSWORD',
    'sslmode': 'PGSSLMODE',
    'sslcert': 'PGSSLCERT',
    'sslkey': 'PGSSLKEY',
    'sslpassword': '',
    'sslrootcert': 'PGSSLROOTCERT',
    'sslcrl': 'PGSSLCRL',
    'sslcrldir': 'PGSSLCRLDIR',
    'gssencmode': 'PGGSSENCMODE',
    'channel_binding': 'PGCHANNELBINDING',
    'sslnegotiation': 'PGSSLNEGOTIATION'
}
NO_VALUE_MSG = '#FIXME'


def get_address() -> Tuple[str, str]:
    """Try to get hostname and the ip address for it returned by :func:`~socket.gethostname`.

    .. note::
        Can also return local ip.

    :returns: tuple consisting of the hostname returned by :func:`~socket.gethostname`
        and the first element in the sorted list of the addresses returned by :func:`~socket.getaddrinfo`.
        Sorting guarantees it will prefer IPv4.
        If an exception occurred, hostname and ip values are equal to :data:`~patroni.config_generator.NO_VALUE_MSG`.
    """
    hostname = None
    try:
        hostname = socket.gethostname()
        # Filter out unexpected results when python is compiled with --disable-ipv6 and running on IPv6 system.
        addrs = [(a[0], a[4][0]) for a in socket.getaddrinfo(hostname, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0)
                 if isinstance(a[4][0], str)]
        return hostname, sorted(addrs, key=lambda x: x[0])[0][1]
    except Exception as err:
        logging.warning('Failed to obtain address: %r', err)
        return NO_VALUE_MSG, NO_VALUE_MSG


class AbstractConfigGenerator(abc.ABC):
    """Object representing the generated Patroni config.

    :ivar output_file: full path to the output file to be used.
    :ivar pg_major: integer representation of the major PostgreSQL version.
    :ivar config: dictionary used for the generated configuration storage.
    """

    def __init__(self, output_file: Optional[str]) -> None:
        """Set up the output file (if passed), helper vars and the minimal config structure.

        :param output_file: full path to the output file to be used.
        """
        self.output_file = output_file
        self.pg_major = 0
        self.config = self.get_template_config()

        self.generate()

    @classmethod
    def get_template_config(cls) -> Dict[str, Any]:
        """Generate a template config for further extension (e.g. in the inherited classes).

        :returns: dictionary with the values gathered from Patroni env, hopefully defined hostname and ip address
                  (otherwise set to :data:`~patroni.config_generator.NO_VALUE_MSG`), and some sane defaults.
        """
        _HOSTNAME, _IP = get_address()

        template_config: Dict[str, Any] = {
            'scope': NO_VALUE_MSG,
            'name': _HOSTNAME,
            'restapi': {
                'connect_address': _IP + ':8008',
                'listen': _IP + ':8008'
            },
            'log': {
                'type': PatroniLogger.DEFAULT_TYPE,
                'level': PatroniLogger.DEFAULT_LEVEL,
                'traceback_level': PatroniLogger.DEFAULT_TRACEBACK_LEVEL,
                'format': PatroniLogger.DEFAULT_FORMAT,
                'max_queue_size': PatroniLogger.DEFAULT_MAX_QUEUE_SIZE
            },
            'postgresql': {
                'data_dir': NO_VALUE_MSG,
                'connect_address': _IP + ':5432',
                'listen': _IP + ':5432',
                'bin_dir': '',
                'authentication': {
                    'superuser': {
                        'username': 'postgres',
                        'password': NO_VALUE_MSG
                    },
                    'replication': {
                        'username': 'replicator',
                        'password': NO_VALUE_MSG
                    }
                }
            },
            'tags': {
                'failover_priority': 1,
                'sync_priority': 1,
                'noloadbalance': False,
                'clonefrom': True,
                'nosync': False,
                'nostream': False,
            }
        }

        dynamic_config = Config.get_default_config()
        # to properly dump CaseInsensitiveDict as YAML later
        dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
        config = Config('', None).local_configuration  # Get values from env
        config.setdefault('bootstrap', {})['dcs'] = dynamic_config
        config.setdefault('postgresql', {})
        del config['bootstrap']['dcs']['standby_cluster']

        patch_config(template_config, config)
        return template_config

    @abc.abstractmethod
    def generate(self) -> None:
        """Generate config and store in :attr:`~AbstractConfigGenerator.config`."""

    @staticmethod
    def _format_block(block: Any, line_prefix: str = '') -> str:
        """Format a single YAML block.

        .. note::
            Optionally the formatted block could be indented with the *line_prefix*

        :param block: the object that should be formatted to YAML.
        :param line_prefix: is used for indentation.

        :returns: a formatted and indented *block*.
        """
        return line_prefix + yaml.safe_dump(block, default_flow_style=False, line_break='\n',
                                            allow_unicode=True, indent=2).strip().replace('\n', '\n' + line_prefix)

    def _format_config_section(self, section_name: str) -> Iterator[str]:
        """Format and yield as single section of the current :attr:`~AbstractConfigGenerator.config`.

        .. note::
            If the section is a :class:`dict` object we put an empty line before it.

        :param section_name: a section name in the :attr:`~AbstractConfigGenerator.config`.

        :yields: a formatted section in case if it exists in the :attr:`~AbstractConfigGenerator.config`.
        """
        if section_name in self.config:
            if isinstance(self.config[section_name], dict):
                yield ''
            yield self._format_block({section_name: self.config[section_name]})

    def _format_config(self) -> Iterator[str]:
        """Format current :attr:`~AbstractConfigGenerator.config` and enrich it with some comments.

        :yields: formatted lines or blocks that represent a text output of the YAML document.
        """
        for name in ('scope', 'namespace', 'name', 'log', 'restapi', 'ctl', 'citus',
                     'consul', 'etcd', 'etcd3', 'exhibitor', 'kubernetes', 'raft', 'zookeeper'):
            yield from self._format_config_section(name)

        if 'bootstrap' in self.config:
            yield '\n# The bootstrap configuration. Works only when the cluster is not yet initialized.'
            yield '# If the cluster is already initialized, all changes in the `bootstrap` section are ignored!'
            yield 'bootstrap:'
            if 'dcs' in self.config['bootstrap']:
                yield '  # This section will be written into <dcs>:/<namespace>/<scope>/config after initializing'
                yield '  # new cluster and all other cluster members will use it as a `global configuration`.'
                yield '  # WARNING! If you want to change any of the parameters that were set up'
                yield '  # via `bootstrap.dcs` section, please use `patronictl edit-config`!'
                yield '  dcs:'
                for name in ('loop_wait', 'retry_timeout', 'ttl'):
                    if name in self.config['bootstrap']['dcs']:
                        yield self._format_block({name: self.config['bootstrap']['dcs'].pop(name)}, '    ')

                for name, value in self.config['bootstrap']['dcs'].items():
                    yield self._format_block({name: value}, '    ')

        for name in ('postgresql', 'watchdog', 'tags'):
            yield from self._format_config_section(name)

    def _write_config_to_fd(self, fd: TextIO) -> None:
        """Format and write current :attr:`~AbstractConfigGenerator.config` to provided file descriptor.

        :param fd: where to write the config file. Could be ``sys.stdout`` or the real file.
        """
        fd.write('\n'.join(self._format_config()))

    def write_config(self) -> None:
        """Write current :attr:`~AbstractConfigGenerator.config` to the output file if provided, to stdout otherwise."""
        if self.output_file:
            dir_path = os.path.dirname(self.output_file)
            if dir_path and not os.path.isdir(dir_path):
                os.makedirs(dir_path)
            with open(self.output_file, 'w', encoding='UTF-8') as output_file:
                self._write_config_to_fd(output_file)
        else:
            self._write_config_to_fd(sys.stdout)


class SampleConfigGenerator(AbstractConfigGenerator):
    """Object representing the generated sample Patroni config.

    Sane defaults are used based on the gathered PG version.
    """

    @property
    def get_auth_method(self) -> str:
        """Return the preferred authentication method for a specific PG version if provided or the default ``md5``.

        :returns: :class:`str` value for the preferred authentication method.
        """
        return 'scram-sha-256' if self.pg_major and self.pg_major >= 100000 else 'md5'

    def _get_int_major_version(self) -> int:
        """Get major PostgreSQL version from the binary as an integer.

        :returns: an integer PostgreSQL major version representation gathered from the PostgreSQL binary.
                  See :func:`~patroni.postgresql.misc.postgres_major_version_to_int` and
                  :func:`~patroni.utils.get_major_version`.
        """
        postgres_bin = ((self.config.get('postgresql')
                         or EMPTY_DICT).get('bin_name') or EMPTY_DICT).get('postgres', 'postgres')
        return postgres_major_version_to_int(get_major_version(self.config['postgresql'].get('bin_dir'), postgres_bin))

    def generate(self) -> None:
        """Generate sample config using some sane defaults and update :attr:`~AbstractConfigGenerator.config`."""
        self.pg_major = self._get_int_major_version()

        self.config['postgresql']['parameters'] = {'password_encryption': self.get_auth_method}
        username = self.config["postgresql"]["authentication"]["replication"]["username"]
        self.config['postgresql']['pg_hba'] = [
            f'host all all all {self.get_auth_method}',
            f'host replication {username} all {self.get_auth_method}'
        ]

        # add version-specific configuration
        wal_keep_param = 'wal_keep_segments' if self.pg_major < 130000 else 'wal_keep_size'
        self.config['bootstrap']['dcs']['postgresql']['parameters'][wal_keep_param] = \
            ConfigHandler.CMDLINE_OPTIONS[wal_keep_param][0]

        wal_level = 'hot_standby' if self.pg_major < 90600 else 'replica'
        self.config['bootstrap']['dcs']['postgresql']['parameters']['wal_level'] = wal_level

        self.config['bootstrap']['dcs']['postgresql']['use_pg_rewind'] = \
            parse_bool(self.config['bootstrap']['dcs']['postgresql']['parameters']['wal_log_hints']) is True
        if self.pg_major >= 110000:
            self.config['postgresql']['authentication'].setdefault(
                'rewind', {'username': 'rewind_user'}).setdefault('password', NO_VALUE_MSG)


class RunningClusterConfigGenerator(AbstractConfigGenerator):
    """Object representing the Patroni config generated using information gathered from the running instance.

    :ivar dsn: DSN string for the local instance to get GUC values from (if provided).
    :ivar parsed_dsn: DSN string parsed into a dictionary (see :func:`~patroni.postgresql.config.parse_dsn`).
    """

    def __init__(self, output_file: Optional[str] = None, dsn: Optional[str] = None) -> None:
        """Additionally store the passed dsn (if any) in both original and parsed version and run config generation.

        :param output_file: full path to the output file to be used.
        :param dsn: DSN string for the local instance to get GUC values from.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if DSN parsing failed.
        """
        self.dsn = dsn
        self.parsed_dsn = {}

        super().__init__(output_file)

    @property
    def _get_hba_conn_types(self) -> Tuple[str, ...]:
        """Return the connection types allowed.

        If :attr:`~RunningClusterConfigGenerator.pg_major` is defined, adds additional parameters
        for PostgreSQL version >=16.

        :returns: tuple of the connection methods allowed.
        """
        allowed_types = ('local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', 'hostnogssenc')
        if self.pg_major and self.pg_major >= 160000:
            allowed_types += ('include', 'include_if_exists', 'include_dir')
        return allowed_types

    @property
    def _required_pg_params(self) -> List[str]:
        """PG configuration parameters that have to be always present in the generated config.

        :returns: list of the parameter names.
        """
        return ['hba_file', 'ident_file', 'config_file', 'data_directory'] + \
            list(ConfigHandler.CMDLINE_OPTIONS.keys())

    def _get_bin_dir_from_running_instance(self) -> str:
        """Define the directory postgres binaries reside using postmaster's pid executable.

        :returns: path to the PostgreSQL binaries directory.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if:

                * pid could not be obtained from the ``postmaster.pid`` file; or
                * :exc:`OSError` occurred during ``postmaster.pid`` file handling; or
                * the obtained postmaster pid doesn't exist.
        """
        postmaster_pid = None
        data_dir = self.config['postgresql']['data_dir']
        try:
            with open(f"{data_dir}/postmaster.pid", 'r') as pid_file:
                postmaster_pid = pid_file.readline()
                if not postmaster_pid:
                    raise PatroniException('Failed to obtain postmaster pid from postmaster.pid file')
                postmaster_pid = int(postmaster_pid.strip())
        except OSError as err:
            raise PatroniException(f'Error while reading postmaster.pid file: {err}')
        try:
            return os.path.dirname(psutil.Process(postmaster_pid).exe())
        except psutil.NoSuchProcess:
            raise PatroniException("Obtained postmaster pid doesn't exist.")

    @contextmanager
    def _get_connection_cursor(self) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Get cursor for the PG connection established based on the stored information.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if :exc:`psycopg.Error` occurred.
        """
        try:
            conn = psycopg.connect(dsn=self.dsn,
                                   password=self.config['postgresql']['authentication']['superuser']['password'])
            with conn.cursor() as cur:
                yield cur
            conn.close()
        except psycopg.Error as e:
            raise PatroniException(f'Failed to establish PostgreSQL connection: {e}')

    def _set_pg_params(self, cur: Union['cursor', 'Cursor[Any]']) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the actual PG GUCs values.

        THe following GUC values are set:

            * Non-internal having configuration file, postmaster command line or environment variable
              as a source.

            * List of the always required parameters (see :meth:`~RunningClusterConfigGenerator._required_pg_params`).

        :param cur: connection cursor to use.
        """
        cur.execute("SELECT name, pg_catalog.current_setting(name) FROM pg_catalog.pg_settings "
                    "WHERE context <> 'internal' "
                    "AND source IN ('configuration file', 'command line', 'environment variable') "
                    "AND category <> 'Write-Ahead Log / Recovery Target' "
                    "AND setting <> '(disabled)' "
                    "OR name = ANY(%s)", (self._required_pg_params,))

        helper_dict = dict.fromkeys(['port', 'listen_addresses'])
        self.config['postgresql'].setdefault('parameters', {})
        for param, value in cur.fetchall():
            if param == 'data_directory':
                self.config['postgresql']['data_dir'] = value
            elif param == 'cluster_name' and value:
                self.config['scope'] = value
            elif param in ('archive_command', 'restore_command',
                           'archive_cleanup_command', 'recovery_end_command',
                           'ssl_passphrase_command', 'hba_file',
                           'ident_file', 'config_file'):
                # write commands to the local config due to security implications
                # write hba/ident/config_file to local config to ensure they are not removed later
                self.config['postgresql']['parameters'][param] = value
            elif param in helper_dict:
                helper_dict[param] = value
            else:
                self.config['bootstrap']['dcs']['postgresql']['parameters'][param] = value

        connect_ip = self.config['postgresql']['connect_address'].rsplit(':')[0]
        connect_port = self.parsed_dsn.get('port', os.getenv('PGPORT', helper_dict['port']))
        self.config['postgresql']['connect_address'] = f'{connect_ip}:{connect_port}'
        self.config['postgresql']['listen'] = f'{helper_dict["listen_addresses"]}:{helper_dict["port"]}'

    def _set_su_params(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the superuser auth information.

        Information set is based on the options used for connection.
        """
        su_params: Dict[str, str] = {}
        for conn_param, env_var in _AUTH_ALLOWED_PARAMETERS_MAPPING.items():
            val = self.parsed_dsn.get(conn_param, os.getenv(env_var))
            if val:
                su_params[conn_param] = val
        patroni_env_su_username = ((self.config.get('authentication')
                                    or EMPTY_DICT).get('superuser') or EMPTY_DICT).get('username')
        patroni_env_su_pwd = ((self.config.get('authentication')
                               or EMPTY_DICT).get('superuser') or EMPTY_DICT).get('password')
        # because we use "username" in the config for some reason
        su_params['username'] = su_params.pop('user', patroni_env_su_username) or getuser()
        su_params['password'] = su_params.get('password', patroni_env_su_pwd) or \
            getpass('Please enter the user password:')
        self.config['postgresql']['authentication'] = {
            'superuser': su_params,
            'replication': {'username': NO_VALUE_MSG, 'password': NO_VALUE_MSG}
        }

    def _set_conf_files(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with ``pg_hba.conf`` and ``pg_ident.conf`` content.

        .. note::
            This function only defines ``postgresql.pg_hba`` and ``postgresql.pg_ident`` when
            ``hba_file`` and ``ident_file`` are set to the defaults. It may happen these files
            are located outside of ``PGDATA`` and Patroni doesn't have write permissions for them.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if :exc:`OSError` occurred during the conf files handling.
        """
        default_hba_path = os.path.join(self.config['postgresql']['data_dir'], 'pg_hba.conf')
        if self.config['postgresql']['parameters']['hba_file'] == default_hba_path:
            try:
                self.config['postgresql']['pg_hba'] = list(
                    filter(lambda i: i and i.split()[0] in self._get_hba_conn_types, read_stripped(default_hba_path)))
            except OSError as err:
                raise PatroniException(f'Failed to read pg_hba.conf: {err}')

        default_ident_path = os.path.join(self.config['postgresql']['data_dir'], 'pg_ident.conf')
        if self.config['postgresql']['parameters']['ident_file'] == default_ident_path:
            try:
                self.config['postgresql']['pg_ident'] = [i for i in read_stripped(default_ident_path)
                                                         if i and not i.startswith('#')]
            except OSError as err:
                raise PatroniException(f'Failed to read pg_ident.conf: {err}')
            if not self.config['postgresql']['pg_ident']:
                del self.config['postgresql']['pg_ident']

    def _enrich_config_from_running_instance(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the values gathered from the running instance.

        Retrieve the following information from the running PostgreSQL instance:

        * superuser auth parameters (see :meth:`~RunningClusterConfigGenerator._set_su_params`);
        * some GUC values (see :meth:`~RunningClusterConfigGenerator._set_pg_params`);
        * ``postgresql.connect_address``, ``postgresql.listen``;
        * ``postgresql.pg_hba`` and ``postgresql.pg_ident`` (see :meth:`~RunningClusterConfigGenerator._set_conf_files`)

        And redefine ``scope`` with the ``cluster_name`` GUC value if set.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if the provided user doesn't have superuser privileges.
        """
        self._set_su_params()

        with self._get_connection_cursor() as cur:
            self.pg_major = getattr(cur.connection, 'server_version', 0)

            if not parse_bool(getattr(cur.connection, 'get_parameter_status')('is_superuser')):
                raise PatroniException('The provided user does not have superuser privilege')

            self._set_pg_params(cur)

        self._set_conf_files()

    def generate(self) -> None:
        """Generate config using the info gathered from the specified running PG instance.

        Result is written to :attr:`~RunningClusterConfigGenerator.config`.
        """
        if self.dsn:
            self.parsed_dsn = parse_dsn(self.dsn) or {}
            if not self.parsed_dsn:
                raise PatroniException('Failed to parse DSN string')

        self._enrich_config_from_running_instance()
        self.config['postgresql']['bin_dir'] = self._get_bin_dir_from_running_instance()


def generate_config(output_file: str, sample: bool, dsn: Optional[str]) -> None:
    """Generate Patroni configuration file.

    :param output_file: Full path to the configuration file to be used. If not provided, result is sent to ``stdout``.
    :param sample: Optional flag. If set, no source instance will be used - generate config with some sane defaults.
    :param dsn: Optional DSN string for the local instance to get GUC values from.
    """
    try:
        if sample:
            config_generator = SampleConfigGenerator(output_file)
        else:
            config_generator = RunningClusterConfigGenerator(output_file, dsn)

        config_generator.write_config()
    except PatroniException as e:
        sys.exit(str(e))
    except Exception as e:
        sys.exit(f'Unexpected exception: {e}')
