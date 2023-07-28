"""patroni --generate-config machinery."""
import os
import socket
import sys
import yaml

from typing import Any, Dict, Optional

from .config import Config
from .exceptions import PatroniException
from .postgresql.config import ConfigHandler
from .postgresql.misc import postgres_major_version_to_int
from .utils import get_major_version, patch_config


def get_ip() -> str:
    """
    Try to get an ip address of the hostname returned by :func:`gethostname`.

    .. note::
            Can also return local ip

    :returns: the first element in the sorted list of the addresses returned by :func:`getaddrinfo`.
              Sorting guarantees it will prefer ipv4.
    """
    hostname = None
    try:
        hostname = socket.gethostname()
        return sorted(socket.getaddrinfo(hostname, 0, socket.AF_UNSPEC, socket.SOCK_STREAM, 0),
                      key=lambda x: x[0])[0][4][0]
    except OSError as e:
        sys.exit(f'Failed to define ip address: {e}')
    except IndexError:
        sys.exit(f'Failed to define ip address. No address returned by getaddrinfo for {hostname}')


def get_int_major_version(config: Optional[Dict[str, Any]] = None) -> int:
    """Get major version of PostgreSQL from the binary as an integer.

    :param config: optional dictionary representing Patroni config. If contains bin_dir and/or the custom
    bin_name for postgres, the values are used for the version retrieval.
    """
    config = config or {}
    postgres_bin = ((config.get('postgresql') or {}).get('bin_name') or {}).get('postgres', 'postgres')
    return postgres_major_version_to_int(get_major_version(config['postgresql'].get('bin_dir') or None, postgres_bin))


def get_bin_dir_from_running_instance(data_dir: str) -> str:
    """Define the directory postgres binaries reside using postmaster's pid executable."""
    postmaster_pid = None
    try:
        with open(f"{data_dir}/postmaster.pid", 'r') as f:
            postmaster_pid = f.readline()
            if not postmaster_pid:
                sys.exit('Failed to obtain postmaster pid from postmaster.pid file')
            postmaster_pid = int(postmaster_pid.strip())
    except OSError as e:
        sys.exit(f'Error while reading postmaster.pid file: {e}')
    import psutil
    try:
        return os.path.dirname(psutil.Process(postmaster_pid).exe())
    except psutil.NoSuchProcess:
        sys.exit('Obtained postmaster pid doesn\'t exist')


def enrich_config_from_running_instance(config: Dict[str, Any], no_value_msg: str, dsn: Optional[str] = None) -> None:
    """Extend the passed ``config`` dictionary with the values gathered from a running instance.

    Get
    - non-internal GUC values having configuration file, postmaster command line or environment variable as a source
    - postgresql.connect_address, postgresql.listen,
    - postgresql.pg_hba and postgresql.pg_ident if hba_file/ident_file is set to the default value
    - superuser auth parameters (from the options used for connection)
    And redefine scope with the clister_name GUC value if set

    :param config: configuration parameters dict to be enriched
    :param no_value_msg: str value to be used when a parameter value is not available
    :param dsn: optional DSN string for the source running instance
    """
    from getpass import getuser, getpass
    from patroni.postgresql.config import parse_dsn
    from patroni.config import AUTH_ALLOWED_PARAMETERS_MAPPING

    su_params: Dict[str, str] = {}
    parsed_dsn = {}

    if dsn:
        parsed_dsn = parse_dsn(dsn)
        if not parsed_dsn:
            sys.exit('Failed to parse DSN string')

    # gather auth parameters for the superuser config
    for conn_param, env_var in AUTH_ALLOWED_PARAMETERS_MAPPING.items():
        val = parsed_dsn.get(conn_param, os.getenv(env_var))
        if val:
            su_params[conn_param] = val
    patroni_env_su_username = ((config.get('authentication') or {}).get('superuser') or {}).get('username')
    patroni_env_su_pwd = ((config.get('authentication') or {}).get('superuser') or {}).get('password')
    # because we use "username" in the config for some reason
    su_params['username'] = su_params.pop('user', patroni_env_su_username) or getuser()
    su_params['password'] = su_params.get('password', patroni_env_su_pwd) or getpass('Please enter the user password:')

    from . import psycopg
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
                config['postgresql'].setdefault('parameters', {})
                config['postgresql']['parameters'][p] = v
            elif p in helper_dict:
                helper_dict[p] = v
            else:
                config['bootstrap']['dcs']['postgresql']['parameters'][p] = v

    conn.close()

    connect_port = parsed_dsn.get('port', os.getenv('PGPORT', helper_dict['port']))
    config['postgresql']['connect_address'] = f'{get_ip()}:{connect_port}'
    config['postgresql']['listen'] = f'{helper_dict["listen_addresses"]}:{helper_dict["port"]}'

    # it makes sense to define postgresql.pg_hba/pg_ident only if hba_file/ident_file are set to defaults
    default_hba_path = os.path.join(config['postgresql']['data_dir'], 'pg_hba.conf')
    if config['postgresql']['parameters']['hba_file'] == default_hba_path:
        try:
            allowed_records = ['local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', 'hostnogssenc']
            if get_int_major_version(config) >= 16:
                allowed_records += ['include', 'include_if_exists', 'include_dir']
            with open(default_hba_path, 'r') as f:
                config['postgresql']['pg_hba'] = list(filter(lambda i: i and i.split()[0] in allowed_records,
                                                             (li.strip() for li in f.readlines())))
        except OSError as e:
            sys.exit(f'Failed to read pg_hba.conf: {e}')

    default_ident_path = os.path.join(config['postgresql']['data_dir'], 'pg_ident.conf')
    if config['postgresql']['parameters']['ident_file'] == default_ident_path:
        try:
            with open(default_ident_path, 'r') as f:
                config['postgresql']['pg_ident'] = [i.strip() for i in f.readlines()
                                                    if i.strip() and not i.startswith('#')]
        except OSError as e:
            sys.exit(f'Failed to read pg_ident.conf: {e}')
        if not config['postgresql']['pg_ident']:
            del config['postgresql']['pg_ident']

    config['postgresql']['authentication'] = {
        'superuser': su_params,
        'replication': {'username': no_value_msg, 'password': no_value_msg}
    }


def generate_config(file: str, sample: bool, dsn: Optional[str]) -> None:
    """Generate Patroni configuration file.

    Gather all the available non-internal GUC values having configuration file, postmaster command line or environment
    variable as a source and store them in the appropriate part of Patroni configuration (``postgresql.parameters`` or
    ``bootsrtap.dcs.postgresql.parameters``). Either the provided DSN (takes precedence) or PG ENV vars will be used
    for the connection. If password is not provided, it should be entered via prompt.

    The created configuration contains:
    - ``scope``: cluster_name GUC value or PATRONI_SCOPE ENV variable value if available
    - ``name``: PATRONI_NAME ENV variable value if set, otherewise hostname
    - ``bootsrtap.dcs``: section with all the parameters (incl. the majority of PG GUCs) set to their default values
      defined by Patroni and adjusted by the source instances's configuration values.
    - ``postgresql.parameters``: the source instance's archive_command, restore_command, archive_cleanup_command,
      recovery_end_command, ssl_passphrase_command, hba_file, ident_file, config_file GUC values
    - ``postgresql.bin_dir``: path to Postgres binaries gathered from the running instance or, if not available,
      the value of PATRONI_POSTGRESQL_BIN_DIR ENV variable. Otherwise, an empty string.
    - ``postgresql.datadir``: the value gathered from the corresponding PG GUC
    - ``postgresql.listen``: source instance's listen_addresses and port GUC values
    - ``postgresql.connect_address``: if possible, generated from the connection params
    - ``postgresql.authentication``:
        - superuser and replication users defined (if possible, usernames are set from the respective Patroni ENV vars,
          otherwise the default 'postgres' and 'replicator' values are used).
          If not a sample config, either DSN or PG ENV vars are used to define superuser authentication parameters.
        - rewind user is defined for a sample config if PG version can be defined and PG version is 11+
          (if possible, username is set from the respective Patroni ENV var)
    - ``bootsrtap.dcs.postgresql.use_pg_rewind``
    - ``postgresql.pg_hba`` defaults or the lines gathered from the source instance's hba_file
    - ``postgresql.pg_ident`` the lines gathered from the source instance's ident_file

    :param file: Full path to the configuration file to be used. If not provided, result is sent to stdout.
    :param sample: Optional flag. If set, no source instance will be used - generate config with some sane defaults.
    :param dsn: Optional DSN string for the local instance to get GUC values from.
    """
    no_value_msg = '#FIXME'
    pg_version = None
    local_ip = get_ip()

    config = Config('', None).local_configuration  # Get values from env
    dynamic_config = Config.get_default_config()
    dynamic_config['postgresql']['parameters'] = dict(dynamic_config['postgresql']['parameters'])
    config.setdefault('bootstrap', {})['dcs'] = dynamic_config
    config.setdefault('postgresql', {})

    template_config: Dict[str, Any] = {
        'scope': no_value_msg,
        'name': socket.gethostname(),
        'postgresql': {
            'data_dir': no_value_msg,
            'connect_address': no_value_msg + ':5432',
            'listen': no_value_msg + ':5432',
            'bin_dir': '',
            'authentication': {
                'superuser': {
                    'username': 'postgres',
                    'password': no_value_msg
                },
                'replication': {
                    'username': 'replicator',
                    'password': no_value_msg
                }
            }
        },
        'restapi': {
            'connect_address': local_ip + ':8008',
            'listen': local_ip + ':8008'
        }
    }

    if not sample:
        enrich_config_from_running_instance(config, no_value_msg, dsn)
        config['postgresql']['bin_dir'] = get_bin_dir_from_running_instance(config['postgresql']['data_dir'])

    try:
        pg_version = get_int_major_version(config)
    except PatroniException as e:
        sys.exit(str(e))

    patch_config(template_config, config)
    config = template_config

    # generate sample config
    if sample:
        auth_method = 'scram-sha-256' if pg_version and pg_version >= 100000 else 'md5'
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
                'rewind', {'username': 'rewind_user'}).setdefault(
                    'password', no_value_msg)

    # redundant values from the default config
    del config['bootstrap']['dcs']['standby_cluster']

    if file:
        dir_path = os.path.dirname(file)
        if dir_path and not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        with open(file, 'w') as fd:
            yaml.safe_dump(config, fd, default_flow_style=False)
    else:
        yaml.safe_dump(config, sys.stdout, default_flow_style=False)
