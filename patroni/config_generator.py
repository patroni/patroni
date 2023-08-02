"""patroni --generate-config machinery."""
import os
import psutil
import socket
import sys
import yaml

from getpass import getuser, getpass
from typing import Any, Dict, Optional, Tuple

from . import psycopg
from .config import Config
from .exceptions import PatroniException
from .postgresql.config import parse_dsn
from .postgresql.config import ConfigHandler
from .postgresql.misc import postgres_major_version_to_int
from .utils import get_major_version, patch_config, read_stripped


"""Mapping between the libpq connection parameters and the environment variables.
This dict should be kept in sync with `patroni.utils._AUTH_ALLOWED_PARAMETERS`
(we use "username" in the Patroni config for some reason, other parameter names are the same).
"""
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
    'channel_binding': 'PGCHANNELBINDING'
}
_NO_VALUE_MSG = '#FIXME'


def get_address() -> Tuple[str, str]:
    """Try to get hostname and the ip address for it returned by :func:`~socket.gethostname`.

    .. note::
        Can also return local ip

    .. note::
        :exc:`OSError` raised leads to execution termination.

    :returns: tuple consisting of the hostname returned by `~socket.gethostname`
        and the first element in the sorted list of the addresses returned by :func:`~socket.getaddrinfo`.
        Sorting guarantees it will prefer IPv4.
    """
    hostname = None
    try:
        hostname = socket.gethostname()
        return hostname, sorted(socket.getaddrinfo(hostname, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0),
                                key=lambda x: x[0])[0][4][0]
    except OSError as e:
        sys.exit(f'Failed to define ip address: {e}')


def get_int_major_version(config: Optional[Dict[str, Any]] = None) -> int:
    """Get major version of PostgreSQL from the binary as an integer.

    :param config: optional dictionary representing Patroni config. If contains bin_dir and/or the custom
        ``bin_name`` for postgres, the values are used for the version retrieval.

    :returns: an integer PostgreSQL major version representation gathered from the PostgreSQL binary.
        See :func:`~patroni.postgresql.misc.postgres_major_version_to_int` and
        :func:`~patroni.utils.get_major_version`.
    """
    config = config or {}
    postgres_bin = ((config.get('postgresql') or {}).get('bin_name') or {}).get('postgres', 'postgres')
    return postgres_major_version_to_int(get_major_version(config['postgresql'].get('bin_dir') or None, postgres_bin))


def get_hba_conn_types(pg_major: int) -> Tuple[str, ...]:
    """Return the connection types allowed in the specific PostgreSQL version.

    :param pg_major: integer representation of the PostgreSQL major version (see  :func:`get_int_major_version`)

    :returns: tuple of the connetcion methods allowed
    """
    allowed_types = ('local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', 'hostnogssenc')
    if pg_major >= 160000:
        allowed_types += ('include', 'include_if_exists', 'include_dir')
    return allowed_types


def get_auth_method(pg_major: Optional[int] = None) -> str:
    """Return the preferred authentication method for a specific PostgreSQL version if provided or the default 'md5'.

    :param pg_major: integer representation of the PostgreSQL major version (see  :func:`get_int_major_version`)

    :returns: :class:`str` value for the preferred authentication method
    """
    return 'scram-sha-256' if pg_major and pg_major >= 100000 else 'md5'


def get_bin_dir_from_running_instance(data_dir: str) -> str:
    """Define the directory postgres binaries reside using postmaster's pid executable.

    .. note::
        :exc:`OSError` or :exc:`psutil.NoSuchProcess` raised leads to execution termination.

    :param data_dir: the PostgreSQL data directory to search for postmaster.pid file in.

    :returns: path to the PostgreSQL binaries directory
    """
    postmaster_pid = None
    try:
        with open(f"{data_dir}/postmaster.pid", 'r') as pid_file:
            postmaster_pid = pid_file.readline()
            if not postmaster_pid:
                sys.exit('Failed to obtain postmaster pid from postmaster.pid file')
            postmaster_pid = int(postmaster_pid.strip())
    except OSError as e:
        sys.exit(f'Error while reading postmaster.pid file: {e}')
    try:
        return os.path.dirname(psutil.Process(postmaster_pid).exe())
    except psutil.NoSuchProcess:
        sys.exit("Obtained postmaster pid doesn't exist.")


def enrich_config_from_running_instance(config: Dict[str, Any], dsn: Optional[str] = None) -> None:
    """Extend the passed *config* dictionary with the values gathered from a running instance.

    Retrieve the following information from the running PostgreSQL instance:

    * non-internal GUC values having configuration file, postmaster command line or environment variable as a source
    * ``postgresql.connect_address``, postgresql.listen``
    * ``postgresql.pg_hba`` and ``postgresql.pg_ident`` if the ``hba_file`` or ``ident_file`` is set to the default
        value
    * superuser auth parameters (from the options used for connection)

    And redefine scope with the ``cluster_name`` GUC value if set.

    .. note::
        The following situations lead to the execution termination:
            * error raised during

                * DSN string parsing
                * PG connection establishing
                * working with pg_hba.conf/pg_ident.conf
                * PG binary dir extraction
                * ip address definition

            * the user provided lacking superuser privilege

    :param config: configuration parameters dict to be enriched
    :param dsn: optional DSN string for the source running instance
    """
    su_params: Dict[str, str] = {}
    parsed_dsn = {}

    if dsn:
        parsed_dsn = parse_dsn(dsn)
        if not parsed_dsn:
            sys.exit('Failed to parse DSN string')

    # gather auth parameters for the superuser config
    for conn_param, env_var in _AUTH_ALLOWED_PARAMETERS_MAPPING.items():
        val = parsed_dsn.get(conn_param, os.getenv(env_var))
        if val:
            su_params[conn_param] = val
    patroni_env_su_username = ((config.get('authentication') or {}).get('superuser') or {}).get('username')
    patroni_env_su_pwd = ((config.get('authentication') or {}).get('superuser') or {}).get('password')
    # because we use "username" in the config for some reason
    su_params['username'] = su_params.pop('user', patroni_env_su_username) or getuser()
    su_params['password'] = su_params.get('password', patroni_env_su_pwd) or getpass('Please enter the user password:')

    try:
        conn = psycopg.connect(dsn=dsn, password=su_params['password'])
    except psycopg.Error as e:
        sys.exit(f'Failed to establish PostgreSQL connection: {e}')

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s AND rolsuper", (su_params['username'],))
        if cur.rowcount < 1:
            sys.exit('The provided user does not have superuser privilege')

        required_params = ['hba_file', 'ident_file', 'config_file',
                           'data_directory'] + list(ConfigHandler.CMDLINE_OPTIONS.keys())
        cur.execute("SELECT name, current_setting(name) FROM pg_settings "
                    "WHERE context <> 'internal' "
                    "AND source IN ('configuration file', 'command line', 'environment variable') "
                    "AND category <> 'Write-Ahead Log / Recovery Target' "
                    "AND setting <> '(disabled)' "
                    "OR name = ANY(%s)", (required_params,))

        helper_dict = dict.fromkeys(['port', 'listen_addresses'])
        # adjust values
        config['postgresql'].setdefault('parameters', {})
        for p, v in cur.fetchall():
            if p == 'data_directory':
                config['postgresql']['data_dir'] = v
            elif p == 'cluster_name' and v:
                config['scope'] = v
            elif p in ('archive_command', 'restore_command', 'archive_cleanup_command',
                       'recovery_end_command', 'ssl_passphrase_command',
                       'hba_file', 'ident_file', 'config_file'):
                # write commands to the local config due to security implications
                # write hba/ident/config_file to local config to ensure they are not removed later
                config['postgresql']['parameters'][p] = v
            elif p in helper_dict:
                helper_dict[p] = v
            else:
                config['bootstrap']['dcs']['postgresql']['parameters'][p] = v

    conn.close()

    connect_port = parsed_dsn.get('port', os.getenv('PGPORT', helper_dict['port']))
    config['postgresql']['connect_address'] = f'{get_address()[1]}:{connect_port}'
    config['postgresql']['listen'] = f'{helper_dict["listen_addresses"]}:{helper_dict["port"]}'

    # it makes sense to define postgresql.pg_hba/pg_ident only if hba_file/ident_file are set to defaults
    default_hba_path = os.path.join(config['postgresql']['data_dir'], 'pg_hba.conf')
    if config['postgresql']['parameters']['hba_file'] == default_hba_path:
        try:
            config['postgresql']['pg_hba'] = list(filter(lambda i: i and i.split()[0]
                                                         in get_hba_conn_types(get_int_major_version(config)),
                                                         read_stripped(default_hba_path)))
        except OSError as e:
            sys.exit(f'Failed to read pg_hba.conf: {e}')

    default_ident_path = os.path.join(config['postgresql']['data_dir'], 'pg_ident.conf')
    if config['postgresql']['parameters']['ident_file'] == default_ident_path:
        try:
            config['postgresql']['pg_ident'] = [i for i in read_stripped(default_ident_path)
                                                if i and not i.startswith('#')]
        except OSError as e:
            sys.exit(f'Failed to read pg_ident.conf: {e}')
        if not config['postgresql']['pg_ident']:
            del config['postgresql']['pg_ident']

    config['postgresql']['authentication'] = {
        'superuser': su_params,
        'replication': {'username': _NO_VALUE_MSG, 'password': _NO_VALUE_MSG}
    }


def generate_config(file: str, sample: bool, dsn: Optional[str]) -> None:
    """Generate Patroni configuration file.

    Gather all the available non-internal GUC values having configuration file, postmaster command line or environment
    variable as a source and store them in the appropriate part of Patroni configuration (``postgresql.parameters`` or
    ``bootsrtap.dcs.postgresql.parameters``). Either the provided DSN (takes precedence) or PG ENV vars will be used
    for the connection. If password is not provided, it should be entered via prompt.

    The created configuration contains:
    * ``scope``: cluster_name GUC value or PATRONI_SCOPE ENV variable value if available
    * ``name``: PATRONI_NAME ENV variable value if set, otherewise hostname
    * ``bootsrtap.dcs``: section with all the parameters (incl. the majority of PG GUCs) set to their default values
      defined by Patroni and adjusted by the source instances's configuration values.
    * ``postgresql.parameters``: the source instance's archive_command, restore_command, archive_cleanup_command,
      recovery_end_command, ssl_passphrase_command, hba_file, ident_file, config_file GUC values
    * ``postgresql.bin_dir``: path to Postgres binaries gathered from the running instance or, if not available,
      the value of PATRONI_POSTGRESQL_BIN_DIR ENV variable. Otherwise, an empty string.
    * ``postgresql.datadir``: the value gathered from the corresponding PG GUC
    * ``postgresql.listen``: source instance's listen_addresses and port GUC values
    * ``postgresql.connect_address``: if possible, generated from the connection params
    * ``postgresql.authentication``:

        * superuser and replication users defined (if possible, usernames are set from the respective Patroni ENV vars,
          otherwise the default 'postgres' and 'replicator' values are used).
          If not a sample config, either DSN or PG ENV vars are used to define superuser authentication parameters.
        * rewind user is defined for a sample config if PG version can be defined and PG version is 11+
          (if possible, username is set from the respective Patroni ENV var)

    * ``bootsrtap.dcs.postgresql.use_pg_rewind``
    * ``postgresql.pg_hba`` defaults or the lines gathered from the source instance's hba_file
    * ``postgresql.pg_ident`` the lines gathered from the source instance's ident_file

    :param file: Full path to the configuration file to be used. If not provided, result is sent to stdout.
    :param sample: Optional flag. If set, no source instance will be used - generate config with some sane defaults.
    :param dsn: Optional DSN string for the local instance to get GUC values from.
    """
    pg_version = None
    hotname, local_ip = get_address()

    config = Config('', None).local_configuration  # Get values from env
    dynamic_config = Config.get_default_config()
    dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
    config.setdefault('bootstrap', {})['dcs'] = dynamic_config
    config.setdefault('postgresql', {})

    template_config: Dict[str, Any] = {
        'scope': _NO_VALUE_MSG,
        'name': hotname,
        'postgresql': {
            'data_dir': _NO_VALUE_MSG,
            'connect_address': _NO_VALUE_MSG + ':5432',
            'listen': _NO_VALUE_MSG + ':5432',
            'bin_dir': '',
            'authentication': {
                'superuser': {
                    'username': 'postgres',
                    'password': _NO_VALUE_MSG
                },
                'replication': {
                    'username': 'replicator',
                    'password': _NO_VALUE_MSG
                }
            }
        },
        'restapi': {
            'connect_address': local_ip + ':8008',
            'listen': local_ip + ':8008'
        }
    }

    if not sample:
        enrich_config_from_running_instance(config, dsn)
        config['postgresql']['bin_dir'] = get_bin_dir_from_running_instance(config['postgresql']['data_dir'])

    try:
        pg_version = get_int_major_version(config)
    except PatroniException as e:
        sys.exit(str(e))

    patch_config(template_config, config)
    config = template_config

    # generate sample config
    if sample:
        auth_method = get_auth_method(pg_version)
        config['postgresql']['parameters'] = {'password_encryption': auth_method}
        config['postgresql']['pg_hba'] = [
            f'host all all all {auth_method}',
            f'host replication {config["postgresql"]["authentication"]["replication"]["username"]} all {auth_method}'
        ]

        # add version-specific configuration
        wal_keep_param = 'wal_keep_segments' if pg_version < 130000 else 'wal_keep_size'
        config['bootstrap']['dcs']['postgresql']['parameters'][wal_keep_param] =\
            ConfigHandler.CMDLINE_OPTIONS[wal_keep_param][0]

        config['bootstrap']['dcs']['postgresql']['use_pg_rewind'] = True
        if pg_version >= 110000:
            config['postgresql']['authentication'].setdefault(
                'rewind', {'username': 'rewind_user'}).setdefault('password', _NO_VALUE_MSG)

    # redundant values from the default config
    del config['bootstrap']['dcs']['standby_cluster']

    if file:
        dir_path = os.path.dirname(file)
        if dir_path and not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        with open(file, 'w', encoding='UTF-8') as output_file:
            yaml.safe_dump(config, output_file, default_flow_style=False, allow_unicode=True)
    else:
        yaml.safe_dump(config, sys.stdout, default_flow_style=False, allow_unicode=True)
