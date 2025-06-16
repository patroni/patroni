import logging
import os
import re
import shutil
import socket
import stat
import time

from contextlib import contextmanager
from types import TracebackType
from typing import Any, Callable, Collection, Dict, Iterator, List, Optional, Tuple, Type, TYPE_CHECKING, Union
from urllib.parse import parse_qsl, unquote, urlparse

from .. import global_config
from ..collections import CaseInsensitiveDict, CaseInsensitiveSet, EMPTY_DICT
from ..dcs import Leader, Member, RemoteMember, slot_name_from_member_name
from ..exceptions import PatroniFatalException, PostgresConnectionException
from ..file_perm import pg_perm
from ..psycopg import parse_conninfo
from ..utils import compare_values, get_postgres_version, is_subpath, \
    maybe_convert_from_base_unit, parse_bool, parse_int, split_host_port, uri, validate_directory
from ..validator import EnumValidator, IntValidator
from .misc import get_major_from_minor_version, postgres_version_to_int, PostgresqlRole, PostgresqlState
from .validator import recovery_parameters, transform_postgresql_parameter_value, transform_recovery_parameter_value

if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql

logger = logging.getLogger(__name__)

PARAMETER_RE = re.compile(r'([a-z_]+)\s*=\s*')


def _conninfo_uri_parse(dsn: str) -> Dict[str, str]:
    """
    >>> r = _conninfo_uri_parse('postgresql://u%2Fse:pass@:%2f123/db%2Fsdf?application_name=mya%2Fpp&ssl=true')
    >>> r == {'application_name': 'mya/pp', 'dbname': 'db/sdf', 'sslmode': 'require',\
              'password': 'pass', 'port': '/123', 'user': 'u/se'}
    True
    >>> r = _conninfo_uri_parse('postgresql://u%2Fse:pass@[::1]/db%2Fsdf?application_name=mya%2Fpp&ssl=true')
    >>> r == {'application_name': 'mya/pp', 'dbname': 'db/sdf', 'host': '::1', 'sslmode': 'require',\
              'password': 'pass', 'user': 'u/se'}
    True
    """
    ret: Dict[str, str] = {}
    r = urlparse(dsn)
    if r.username:
        ret['user'] = r.username
    if r.password:
        ret['password'] = r.password
    if r.path[1:]:
        ret['dbname'] = r.path[1:]
    hosts: List[str] = []
    ports: List[str] = []
    for netloc in r.netloc.split('@')[-1].split(','):
        host = None
        if '[' in netloc and ']' in netloc:
            tmp = netloc.split(']') + ['']
            host = tmp[0][1:]
            netloc = ':'.join(tmp[:2])
        tmp = netloc.rsplit(':', 1)
        if host is None:
            host = tmp[0]
        hosts.append(host)
        ports.append(tmp[1] if len(tmp) == 2 else '')
    if any(map(len, hosts)):
        ret['host'] = ','.join(hosts)
    if any(map(len, ports)):
        ret['port'] = ','.join(ports)
    ret = {name: unquote(value) for name, value in ret.items()}
    ret.update({name: value for name, value in parse_qsl(r.query)})
    if ret.get('ssl') == 'true':
        del ret['ssl']
        ret['sslmode'] = 'require'
    return ret


def read_param_value(value: str) -> Union[Tuple[None, None], Tuple[str, int]]:
    length = len(value)
    ret = ''
    is_quoted = value[0] == "'"
    i = int(is_quoted)
    while i < length:
        if is_quoted:
            if value[i] == "'":
                return ret, i + 1
        elif value[i].isspace():
            break
        if value[i] == '\\':
            i += 1
            if i >= length:
                break
        ret += value[i]
        i += 1
    return (None, None) if is_quoted else (ret, i)


def _conninfo_dsn_parse(dsn: str) -> Optional[Dict[str, str]]:
    """
    >>> r = _conninfo_dsn_parse(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'requiressl': '1'}
    True
    >>> _conninfo_dsn_parse('requiressl = 0\\\\') == {'requiressl': '0'}
    True
    >>> _conninfo_dsn_parse("host=a foo = '") is None
    True
    >>> _conninfo_dsn_parse("host=a foo = ") is None
    True
    >>> _conninfo_dsn_parse("1") is None
    True
    """
    ret: Dict[str, str] = {}
    length = len(dsn)
    i = 0
    while i < length:
        if dsn[i].isspace():
            i += 1
            continue

        param_match = PARAMETER_RE.match(dsn[i:])
        if not param_match:
            return

        param = param_match.group(1)
        i += param_match.end()

        if i >= length:
            return

        value, end = read_param_value(dsn[i:])
        if value is None or end is None:
            return
        i += end
        ret[param] = value
    return ret


def _conninfo_parse(value: str) -> Optional[Dict[str, str]]:
    """
    Very simple equivalent of `psycopg2.extensions.parse_dsn` introduced in 2.7.0.
    Exists just for compatibility with 2.5.4+.

    >>> r = _conninfo_parse('postgresql://foo/postgres')
    >>> r == {'dbname': 'postgres', 'host': 'foo'}
    True
    >>> r = _conninfo_parse(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'sslmode': 'require'}
    True
    >>> _conninfo_parse('requiressl = 0\\\\') == {'sslmode': 'prefer'}
    True
    """

    if value.startswith('postgres://') or value.startswith('postgresql://'):
        ret = _conninfo_uri_parse(value)
    else:
        ret = _conninfo_dsn_parse(value)

    if ret and 'sslmode' not in ret:  # allow sslmode to take precedence over requiressl
        requiressl = ret.pop('requiressl', None)
        if requiressl == '1':
            ret['sslmode'] = 'require'
        elif requiressl is not None:
            ret['sslmode'] = 'prefer'
    return ret


def parse_dsn(value: str) -> Optional[Dict[str, str]]:
    """
    Compatibility layer on top of function from psycopg2/psycopg3, which parses connection strings.
    In this function sets the `sslmode`, 'gssencmode', and `channel_binding` to `prefer`
    and `sslnegotiation` to `postgres` if they are not present in the connection string.
    This is necessary to simplify comparison of the old and the new values.

    >>> r = parse_dsn('postgresql://foo/postgres')
    >>> r == {'dbname': 'postgres', 'host': 'foo', 'sslmode': 'prefer', 'gssencmode': 'prefer',\
              'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> r = parse_dsn(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'sslmode': 'require',\
              'gssencmode': 'prefer', 'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> parse_dsn('requiressl = 0\\\\') == {'sslmode': 'prefer', 'gssencmode': 'prefer',\
                                            'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> parse_dsn('foo=bar') == {'foo': 'bar', 'sslmode': 'prefer', 'gssencmode': 'prefer',\
                                 'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    """
    ret = parse_conninfo(value, _conninfo_parse)

    if ret:
        ret.setdefault('sslmode', 'prefer')
        ret.setdefault('gssencmode', 'prefer')
        ret.setdefault('channel_binding', 'prefer')
        ret.setdefault('sslnegotiation', 'postgres')
    return ret


def strip_comment(value: str) -> str:
    i = value.find('#')
    if i > -1:
        value = value[:i].strip()
    return value


def read_recovery_param_value(value: str) -> Optional[str]:
    """
    >>> read_recovery_param_value('') is None
    True
    >>> read_recovery_param_value("'") is None
    True
    >>> read_recovery_param_value("''a") is None
    True
    >>> read_recovery_param_value('a b') is None
    True
    >>> read_recovery_param_value("'''") is None
    True
    >>> read_recovery_param_value("'\\\\") is None
    True
    >>> read_recovery_param_value("'a' s#") is None
    True
    >>> read_recovery_param_value("'\\\\'''' #a")
    "''"
    >>> read_recovery_param_value('asd')
    'asd'
    """
    value = value.strip()
    length = len(value)
    if length == 0:
        return None
    elif value[0] == "'":
        if length == 1:
            return None
        ret = ''
        i = 1
        while i < length:
            if value[i] == '\\':
                i += 1
                if i >= length:
                    return None
            elif value[i] == "'":
                i += 1
                if i >= length:
                    break
                if value[i] in ('#', ' '):
                    if strip_comment(value[i:]):
                        return None
                    break
                if value[i] != "'":
                    return None
            ret += value[i]
            i += 1
        else:
            return None
        return ret
    else:
        value = strip_comment(value)
        if not value or ' ' in value or '\\' in value:
            return None
    return value


def mtime(filename: str) -> Optional[float]:
    try:
        return os.stat(filename).st_mtime
    except OSError:
        return None


class ConfigWriter(object):

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._fd = None

    def __enter__(self) -> 'ConfigWriter':
        self._fd = open(self._filename, 'w')
        self.writeline('# Do not edit this file manually!\n# It will be overwritten by Patroni!')
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        if self._fd:
            self._fd.close()

    def writeline(self, line: str) -> None:
        if self._fd:
            self._fd.write(line)
            self._fd.write('\n')

    def writelines(self, lines: List[Optional[str]]) -> None:
        for line in lines:
            if isinstance(line, str):
                self.writeline(line)

    @staticmethod
    def escape(value: Any) -> str:  # Escape (by doubling) any single quotes or backslashes in given string
        return re.sub(r'([\'\\])', r'\1\1', str(value))

    def write_param(self, param: str, value: Any) -> None:
        self.writeline("{0} = '{1}'".format(param, self.escape(value)))


def _false_validator(value: Any) -> bool:
    return False


def _bool_validator(value: Any) -> bool:
    return parse_bool(value) is not None


def _bool_is_true_validator(value: Any) -> bool:
    return parse_bool(value) is True


def get_param_diff(old_value: Any, new_value: Any,
                   vartype: Optional[str] = None, unit: Optional[str] = None) -> Dict[str, str]:
    """Get a dictionary representing a single PG parameter's value diff.

    :param old_value: current :class:`str` parameter value.
    :param new_value: :class:`str` value of the parameter after a restart.
    :param vartype: the target type to parse old/new_value. See ``vartype`` argument of
        :func:`~patroni.utils.maybe_convert_from_base_unit`.
    :param unit: unit of *old/new_value*. See ``base_unit`` argument of
        :func:`~patroni.utils.maybe_convert_from_base_unit`.

    :returns: a :class:`dict` object that contains two keys: ``old_value`` and ``new_value``
        with their values casted to :class:`str` and converted from base units (if possible).
    """
    str_value: Callable[[Any], str] = lambda x: '' if x is None else str(x)
    return {
        'old_value': (maybe_convert_from_base_unit(str_value(old_value), vartype, unit)
                      if vartype else str_value(old_value)),
        'new_value': (maybe_convert_from_base_unit(str_value(new_value), vartype, unit)
                      if vartype else str_value(new_value))
    }


class ConfigHandler(object):

    # List of parameters which must be always passed to postmaster as command line options
    # to make it not possible to change them with 'ALTER SYSTEM'.
    # Some of these parameters have sane default value assigned and Patroni doesn't allow
    # to decrease this value. E.g. 'wal_level' can't be lower then 'hot_standby' and so on.
    # These parameters could be changed only globally, i.e. via DCS.
    # P.S. 'listen_addresses' and 'port' are added here just for convenience, to mark them
    # as a parameters which should always be passed through command line.
    #
    # Format:
    #  key - parameter name
    #  value - tuple(default_value, check_function, min_version)
    #    default_value -- some sane default value
    #    check_function -- if the new value is not correct must return `!False`
    #    min_version -- major version of PostgreSQL when parameter was introduced
    CMDLINE_OPTIONS = CaseInsensitiveDict({
        'listen_addresses': (None, _false_validator, 90100),
        'port': (None, _false_validator, 90100),
        'cluster_name': (None, _false_validator, 90500),
        'wal_level': ('hot_standby', EnumValidator(('hot_standby', 'replica', 'logical')), 90100),
        'hot_standby': ('on', _bool_is_true_validator, 90100),
        'max_connections': (100, IntValidator(min=25), 90100),
        'max_wal_senders': (10, IntValidator(min=3), 90100),
        'wal_keep_segments': (8, IntValidator(min=1), 90100),
        'wal_keep_size': ('128MB', IntValidator(min=16, base_unit='MB'), 130000),
        'max_prepared_transactions': (0, IntValidator(min=0), 90100),
        'max_locks_per_transaction': (64, IntValidator(min=32), 90100),
        'track_commit_timestamp': ('off', _bool_validator, 90500),
        'max_replication_slots': (10, IntValidator(min=4), 90400),
        'max_worker_processes': (8, IntValidator(min=2), 90400),
        'wal_log_hints': ('on', _bool_validator, 90400)
    })

    _RECOVERY_PARAMETERS = CaseInsensitiveSet(recovery_parameters.keys())

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Any]) -> None:
        self._postgresql = postgresql
        self._config_dir = os.path.abspath(config.get('config_dir', '') or postgresql.data_dir)
        config_base_name = config.get('config_base_name', 'postgresql')
        self._postgresql_conf = os.path.join(self._config_dir, config_base_name + '.conf')
        self._postgresql_conf_mtime = None
        self._postgresql_base_conf_name = config_base_name + '.base.conf'
        self._postgresql_base_conf = os.path.join(self._config_dir, self._postgresql_base_conf_name)
        self._pg_hba_conf = os.path.join(self._config_dir, 'pg_hba.conf')
        self._pg_ident_conf = os.path.join(self._config_dir, 'pg_ident.conf')
        self._recovery_conf = os.path.join(postgresql.data_dir, 'recovery.conf')
        self._recovery_conf_mtime = None
        self._recovery_signal = os.path.join(postgresql.data_dir, 'recovery.signal')
        self._standby_signal = os.path.join(postgresql.data_dir, 'standby.signal')
        self._auto_conf = os.path.join(postgresql.data_dir, 'postgresql.auto.conf')
        self._auto_conf_mtime = None
        self._pgpass = os.path.abspath(config.get('pgpass') or os.path.join(os.path.expanduser('~'), 'pgpass'))
        if os.path.exists(self._pgpass) and not os.path.isfile(self._pgpass):
            raise PatroniFatalException("'{0}' exists and it's not a file, check your `postgresql.pgpass` configuration"
                                        .format(self._pgpass))
        self._passfile = None
        self._passfile_mtime = None
        self._postmaster_ctime = None
        self._current_recovery_params: Optional[CaseInsensitiveDict] = None
        self._config = {}
        self._recovery_params = CaseInsensitiveDict()
        self._server_parameters: CaseInsensitiveDict = CaseInsensitiveDict()
        self.reload_config(config)

    def load_current_server_parameters(self) -> None:
        """Read GUC's values from ``pg_settings`` when Patroni is joining the the postgres that is already running."""
        exclude = [name.lower() for name, value in self.CMDLINE_OPTIONS.items() if value[1] == _false_validator]
        keep_values = {k: self._server_parameters[k] for k in exclude}
        server_parameters = CaseInsensitiveDict({r[0]: r[1] for r in self._postgresql.query(
            "SELECT name, pg_catalog.current_setting(name) FROM pg_catalog.pg_settings"
            " WHERE (source IN ('command line', 'environment variable') OR sourcefile = %s"
            " OR pg_catalog.lower(name) = ANY(%s)) AND pg_catalog.lower(name) != ALL(%s)",
            self._postgresql_conf, [n.lower() for n in self.CMDLINE_OPTIONS.keys()], exclude)})
        recovery_params = CaseInsensitiveDict({k: server_parameters.pop(k) for k in self._RECOVERY_PARAMETERS
                                               if k in server_parameters})
        # We also want to load current settings of recovery parameters, including primary_conninfo
        # and primary_slot_name, otherwise patronictl restart will update postgresql.conf
        # and remove them, what in the worst case will cause another restart.
        # We are doing it only for PostgreSQL v12 onwards, because older version still have recovery.conf
        if not self._postgresql.is_primary() and self._postgresql.major_version >= 120000:
            # primary_conninfo is expected to be a dict, therefore we need to parse it
            recovery_params['primary_conninfo'] = parse_dsn(recovery_params.pop('primary_conninfo', '')) or {}
            self._recovery_params = recovery_params

        self._server_parameters = CaseInsensitiveDict({**server_parameters, **keep_values})

    def setup_server_parameters(self) -> None:
        self._server_parameters = self.get_server_parameters(self._config)
        self._adjust_recovery_parameters()

    def try_to_create_dir(self, d: str, msg: str) -> None:
        d = os.path.join(self._postgresql.data_dir, d)
        if (not is_subpath(self._postgresql.data_dir, d) or not self._postgresql.data_directory_empty()):
            validate_directory(d, msg)

    def check_directories(self) -> None:
        if "unix_socket_directories" in self._server_parameters:
            for d in self._server_parameters["unix_socket_directories"].split(","):
                self.try_to_create_dir(d.strip(), "'{}' is defined in unix_socket_directories, {}")
        if "stats_temp_directory" in self._server_parameters:
            self.try_to_create_dir(self._server_parameters["stats_temp_directory"],
                                   "'{}' is defined in stats_temp_directory, {}")
        if not self._krbsrvname:
            self.try_to_create_dir(os.path.dirname(self._pgpass),
                                   "'{}' is defined in `postgresql.pgpass`, {}")

    @property
    def config_dir(self) -> str:
        return self._config_dir

    @property
    def pg_version(self) -> int:
        """Current full postgres version if instance is running, major version otherwise.

        We can only use ``postgres --version`` output if major version there equals to the one
        in data directory. If it is not the case, we should use major version from the ``PG_VERSION``
        file.
        """
        if self._postgresql.state == PostgresqlState.RUNNING:
            try:
                return self._postgresql.server_version
            except AttributeError:
                pass
        bin_minor = postgres_version_to_int(get_postgres_version(bin_name=self._postgresql.pgcommand('postgres')))
        bin_major = get_major_from_minor_version(bin_minor)
        datadir_major = self._postgresql.major_version
        return datadir_major if bin_major != datadir_major else bin_minor

    @property
    def _configuration_to_save(self) -> List[str]:
        configuration = [os.path.basename(self._postgresql_conf)]
        if 'custom_conf' not in self._config:
            configuration.append(os.path.basename(self._postgresql_base_conf_name))
        if not self.hba_file:
            configuration.append('pg_hba.conf')
        if not self.ident_file:
            configuration.append('pg_ident.conf')
        return configuration

    def set_file_permissions(self, filename: str) -> None:
        """Set permissions of file *filename* according to the expected permissions.

        .. note::
            Use original umask if the file is not under PGDATA, use PGDATA
            permissions otherwise.

        :param filename: path to a file which permissions might need to be adjusted.
        """
        if is_subpath(self._postgresql.data_dir, filename):
            pg_perm.set_permissions_from_data_directory(self._postgresql.data_dir)
            os.chmod(filename, pg_perm.file_create_mode)
        else:
            os.chmod(filename, 0o666 & ~pg_perm.orig_umask)

    @contextmanager
    def config_writer(self, filename: str) -> Iterator[ConfigWriter]:
        """Create :class:`ConfigWriter` object and set permissions on a *filename*.

        :param filename: path to a config file.

        :yields: :class:`ConfigWriter` object.
        """
        with ConfigWriter(filename) as writer:
            yield writer
        self.set_file_permissions(filename)

    def save_configuration_files(self, check_custom_bootstrap: bool = False) -> bool:
        """
            copy postgresql.conf to postgresql.conf.backup to be able to retrieve configuration files
            - originally stored as symlinks, those are normally skipped by pg_basebackup
            - in case of WAL-E basebackup (see http://comments.gmane.org/gmane.comp.db.postgresql.wal-e/239)
        """
        if not (check_custom_bootstrap and self._postgresql.bootstrap.running_custom_bootstrap):
            try:
                for f in self._configuration_to_save:
                    config_file = os.path.join(self._config_dir, f)
                    backup_file = os.path.join(self._postgresql.data_dir, f + '.backup')
                    if os.path.isfile(config_file):
                        shutil.copy(config_file, backup_file)
                        self.set_file_permissions(backup_file)
            except IOError:
                logger.exception('unable to create backup copies of configuration files')
        return True

    def restore_configuration_files(self) -> None:
        """ restore a previously saved postgresql.conf """
        try:
            for f in self._configuration_to_save:
                config_file = os.path.join(self._config_dir, f)
                backup_file = os.path.join(self._postgresql.data_dir, f + '.backup')
                if not os.path.isfile(config_file):
                    if os.path.isfile(backup_file):
                        shutil.copy(backup_file, config_file)
                        self.set_file_permissions(config_file)
                    # Previously we didn't backup pg_ident.conf, if file is missing just create empty
                    elif f == 'pg_ident.conf':
                        open(config_file, 'w').close()
                        self.set_file_permissions(config_file)
        except IOError:
            logger.exception('unable to restore configuration files from backup')

    def write_postgresql_conf(self, configuration: Optional[CaseInsensitiveDict] = None) -> None:
        # rename the original configuration if it is necessary
        if 'custom_conf' not in self._config and not os.path.exists(self._postgresql_base_conf):
            os.rename(self._postgresql_conf, self._postgresql_base_conf)

        configuration = configuration or self._server_parameters.copy()
        # Due to the permanent logical replication slots configured we have to enable hot_standby_feedback
        if self._postgresql.enforce_hot_standby_feedback:
            configuration['hot_standby_feedback'] = 'on'

        with self.config_writer(self._postgresql_conf) as f:
            include = self._config.get('custom_conf') or self._postgresql_base_conf_name
            f.writeline("include '{0}'\n".format(ConfigWriter.escape(include)))
            version = self.pg_version
            for name, value in sorted((configuration).items()):
                value = transform_postgresql_parameter_value(version, name, value, self._postgresql.available_gucs)
                if value is not None and\
                        (name != 'hba_file' or not self._postgresql.bootstrap.running_custom_bootstrap):
                    f.write_param(name, value)
            # when we are doing custom bootstrap we assume that we don't know superuser password
            # and in order to be able to change it, we are opening trust access from a certain address
            # therefore we need to make sure that hba_file is not overridden
            # after changing superuser password we will "revert" all these "changes"
            if self._postgresql.bootstrap.running_custom_bootstrap or 'hba_file' not in self._server_parameters:
                f.write_param('hba_file', self._pg_hba_conf)
            if 'ident_file' not in self._server_parameters:
                f.write_param('ident_file', self._pg_ident_conf)

            if self._postgresql.major_version >= 120000:
                if self._recovery_params:
                    f.writeline('\n# recovery.conf')
                    self._write_recovery_params(f, self._recovery_params)

                if not self._postgresql.bootstrap.keep_existing_recovery_conf:
                    self._sanitize_auto_conf()

    def append_pg_hba(self, config: List[str]) -> bool:
        if not self.hba_file and not self._config.get('pg_hba'):
            with open(self._pg_hba_conf, 'a') as f:
                f.write('\n{}\n'.format('\n'.join(config)))
            self.set_file_permissions(self._pg_hba_conf)
        return True

    def replace_pg_hba(self) -> Optional[bool]:
        """
        Replace pg_hba.conf content in the PGDATA if hba_file is not defined in the
        `postgresql.parameters` and pg_hba is defined in `postgresql` configuration section.

        :returns: True if pg_hba.conf was rewritten.
        """

        # when we are doing custom bootstrap we assume that we don't know superuser password
        # and in order to be able to change it, we are opening trust access from a certain address
        if self._postgresql.bootstrap.running_custom_bootstrap:
            addresses = {} if os.name == 'nt' else {'': 'local'}  # windows doesn't yet support unix-domain sockets
            if 'host' in self.local_replication_address and not self.local_replication_address['host'].startswith('/'):
                addresses.update({sa[0] + '/32': 'host' for _, _, _, _, sa in socket.getaddrinfo(
                                  self.local_replication_address['host'], self.local_replication_address['port'],
                                  0, socket.SOCK_STREAM, socket.IPPROTO_TCP) if isinstance(sa[0], str)})
                # Filter out unexpected results when python is compiled with --disable-ipv6 and running on IPv6 system.

            with self.config_writer(self._pg_hba_conf) as f:
                for address, t in addresses.items():
                    f.writeline((
                        '{0}\treplication\t{1}\t{3}\ttrust\n'
                        '{0}\tall\t{2}\t{3}\ttrust'
                    ).format(t, self.replication['username'], self._superuser.get('username') or 'all', address))
        elif not self.hba_file and self._config.get('pg_hba'):
            with self.config_writer(self._pg_hba_conf) as f:
                f.writelines(self._config['pg_hba'])
            return True

    def replace_pg_ident(self) -> Optional[bool]:
        """
        Replace pg_ident.conf content in the PGDATA if ident_file is not defined in the
        `postgresql.parameters` and pg_ident is defined in the `postgresql` section.

        :returns: True if pg_ident.conf was rewritten.
        """

        if not self.ident_file and self._config.get('pg_ident'):
            with self.config_writer(self._pg_ident_conf) as f:
                f.writelines(self._config['pg_ident'])
            return True

    def primary_conninfo_params(self, member: Union[Leader, Member, None]) -> Optional[Dict[str, Any]]:
        if not member or not member.conn_url or member.name == self._postgresql.name:
            return None
        ret = member.conn_kwargs(self.replication)
        ret['application_name'] = self._postgresql.name
        ret.setdefault('sslmode', 'prefer')
        if self._postgresql.major_version >= 120000:
            ret.setdefault('gssencmode', 'prefer')
        if self._postgresql.major_version >= 130000:
            ret.setdefault('channel_binding', 'prefer')
        if self._postgresql.major_version >= 170000:
            ret.setdefault('sslnegotiation', 'postgres')
        if self._krbsrvname:
            ret['krbsrvname'] = self._krbsrvname
        if not ret.get('dbname'):
            ret['dbname'] = self._postgresql.database
        return ret

    def format_dsn(self, params: Dict[str, Any]) -> str:
        """Format connection string from connection parameters.

        .. note::
            only parameters from the below list are considered and values are escaped.

        :param params: :class:`dict` object with connection parameters.

        :returns: a connection string in a format "key1=value2 key2=value2"
        """
        # A list of keywords that can be found in a conninfo string. Follows what is acceptable by libpq
        keywords = ('dbname', 'user', 'passfile' if params.get('passfile') else 'password', 'host', 'port',
                    'sslmode', 'sslcompression', 'sslcert', 'sslkey', 'sslpassword', 'sslrootcert', 'sslcrl',
                    'sslcrldir', 'application_name', 'krbsrvname', 'gssencmode', 'channel_binding',
                    'target_session_attrs', 'sslnegotiation')

        def escape(value: Any) -> str:
            return re.sub(r'([\'\\ ])', r'\\\1', str(value))

        key_ver = {'target_session_attrs': 100000, 'gssencmode': 120000, 'channel_binding': 130000,
                   'sslpassword': 130000, 'sslcrldir': 140000, 'sslnegotiation': 170000}
        return ' '.join('{0}={1}'.format(kw, escape(params[kw])) for kw in keywords
                        if params.get(kw) is not None and self._postgresql.major_version >= key_ver.get(kw, 0))

    def _write_recovery_params(self, fd: ConfigWriter, recovery_params: CaseInsensitiveDict) -> None:
        if self._postgresql.major_version >= 90500:
            pause_at_recovery_target = parse_bool(recovery_params.pop('pause_at_recovery_target', None))
            if pause_at_recovery_target is not None:
                recovery_params.setdefault('recovery_target_action', 'pause' if pause_at_recovery_target else 'promote')
        else:
            if str(recovery_params.pop('recovery_target_action', None)).lower() == 'promote':
                recovery_params.setdefault('pause_at_recovery_target', 'false')
        for name, value in sorted(recovery_params.items()):
            if name == 'primary_conninfo':
                if self._postgresql.major_version >= 100000 and 'PGPASSFILE' in self.write_pgpass(value):
                    value['passfile'] = self._passfile = self._pgpass
                    self._passfile_mtime = mtime(self._pgpass)
                value = self.format_dsn(value)
            else:
                value = transform_recovery_parameter_value(self._postgresql.major_version, name, value,
                                                           self._postgresql.available_gucs)
                if value is None:
                    continue
            fd.write_param(name, value)

    def build_recovery_params(self, member: Union[Leader, Member, None]) -> CaseInsensitiveDict:
        default: Dict[str, Any] = {}
        recovery_params = CaseInsensitiveDict({p: v for p, v in (self.get('recovery_conf') or default).items()
                                               if not p.lower().startswith('recovery_target')
                                               and p.lower() not in ('primary_conninfo', 'primary_slot_name')})
        recovery_params.update({'standby_mode': 'on', 'recovery_target_timeline': 'latest'})
        if self._postgresql.major_version >= 120000:
            # on pg12 we want to protect from following params being set in one of included files
            # not doing so might result in a standby being paused, promoted or shutted down.
            recovery_params.update({'recovery_target': '', 'recovery_target_name': '', 'recovery_target_time': '',
                                    'recovery_target_xid': '', 'recovery_target_lsn': ''})

        is_remote_member = isinstance(member, RemoteMember)
        primary_conninfo = self.primary_conninfo_params(member)
        if primary_conninfo:
            use_slots = global_config.use_slots and self._postgresql.major_version >= 90400
            if use_slots and not (is_remote_member and member.no_replication_slot):
                primary_slot_name = member.primary_slot_name if is_remote_member else self._postgresql.name
                recovery_params['primary_slot_name'] = slot_name_from_member_name(primary_slot_name)
                # We are a standby leader and are using a replication slot. Make sure we connect to
                # the leader of the main cluster (in case more than one host is specified in the
                # connstr) by adding 'target_session_attrs=read-write' to primary_conninfo.
                if is_remote_member and ',' in primary_conninfo['host'] and self._postgresql.major_version >= 100000:
                    primary_conninfo['target_session_attrs'] = 'read-write'
            recovery_params['primary_conninfo'] = primary_conninfo

        # standby_cluster config might have different parameters, we want to override them
        standby_cluster_params = ['restore_command', 'archive_cleanup_command']\
            + (['recovery_min_apply_delay'] if is_remote_member else [])
        recovery_params.update({p: member.data.get(p) for p in standby_cluster_params if member and member.data.get(p)})
        return recovery_params

    def recovery_conf_exists(self) -> bool:
        if self._postgresql.major_version >= 120000:
            return os.path.exists(self._standby_signal) or os.path.exists(self._recovery_signal)
        return os.path.exists(self._recovery_conf)

    @property
    def triggerfile_good_name(self) -> str:
        return 'trigger_file' if self._postgresql.major_version < 120000 else 'promote_trigger_file'

    @property
    def _triggerfile_wrong_name(self) -> str:
        return 'trigger_file' if self._postgresql.major_version >= 120000 else 'promote_trigger_file'

    @property
    def _recovery_parameters_to_compare(self) -> CaseInsensitiveSet:
        skip_params = CaseInsensitiveSet({'pause_at_recovery_target', 'recovery_target_inclusive',
                                          'recovery_target_action', 'standby_mode', self._triggerfile_wrong_name})
        return CaseInsensitiveSet(self._RECOVERY_PARAMETERS - skip_params)

    def _read_recovery_params(self) -> Tuple[Optional[CaseInsensitiveDict], bool]:
        """Read current recovery parameters values.

        .. note::
            We query Postgres only if we detected that Postgresql was restarted
            or when at least one of the following files was updated:

                * ``postgresql.conf``;
                * ``postgresql.auto.conf``;
                * ``passfile`` that is used in the ``primary_conninfo``.

        :returns: a tuple with two elements:

            * :class:`CaseInsensitiveDict` object with current values of recovery parameters,
              or ``None`` if no configuration files were updated;

            * ``True`` if new values of recovery parameters were queried, ``False`` otherwise.
        """
        if self._postgresql.is_starting():
            return None, False

        pg_conf_mtime = mtime(self._postgresql_conf)
        auto_conf_mtime = mtime(self._auto_conf)
        passfile_mtime = mtime(self._passfile) if self._passfile else False
        postmaster_ctime = self._postgresql.is_running()
        if postmaster_ctime:
            postmaster_ctime = postmaster_ctime.create_time()

        if self._postgresql_conf_mtime == pg_conf_mtime and self._auto_conf_mtime == auto_conf_mtime \
                and self._passfile_mtime == passfile_mtime and self._postmaster_ctime == postmaster_ctime:
            return None, False

        try:
            values = self._get_pg_settings(self._recovery_parameters_to_compare).values()
            values = CaseInsensitiveDict({p[0]: [p[1], p[4] == 'postmaster', p[5]] for p in values})
            self._postgresql_conf_mtime = pg_conf_mtime
            self._auto_conf_mtime = auto_conf_mtime
            self._postmaster_ctime = postmaster_ctime
        except Exception as exc:
            if all((isinstance(exc, PostgresConnectionException),
                    self._postgresql_conf_mtime == pg_conf_mtime,
                    self._auto_conf_mtime == auto_conf_mtime,
                    self._passfile_mtime == passfile_mtime,
                    self._postmaster_ctime != postmaster_ctime)):
                # We detected that the connection to postgres fails, but the process creation time of the postmaster
                # doesn't match the old value. It is an indicator that Postgres crashed and either doing crash
                # recovery or down. In this case we return values like nothing changed in the config.
                return None, False
            values = None
        return values, True

    def _read_recovery_params_pre_v12(self) -> Tuple[Optional[CaseInsensitiveDict], bool]:
        recovery_conf_mtime = mtime(self._recovery_conf)
        passfile_mtime = mtime(self._passfile) if self._passfile else False
        if recovery_conf_mtime == self._recovery_conf_mtime and passfile_mtime == self._passfile_mtime:
            return None, False

        values = CaseInsensitiveDict()
        with open(self._recovery_conf, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                value = None
                match = PARAMETER_RE.match(line)
                if match:
                    value = read_recovery_param_value(line[match.end():])
                if match is None or value is None:
                    return None, True
                values[match.group(1)] = [value, True]
            self._recovery_conf_mtime = recovery_conf_mtime
        values.setdefault('recovery_min_apply_delay', ['0', True])
        values['recovery_min_apply_delay'][0] = parse_int(values['recovery_min_apply_delay'][0], 'ms')
        values.update({param: ['', True] for param in self._recovery_parameters_to_compare if param not in values})
        return values, True

    def _check_passfile(self, passfile: str, wanted_primary_conninfo: Dict[str, Any]) -> bool:
        # If there is a passfile in the primary_conninfo try to figure out that
        # the passfile contains the line(s) allowing connection to the given node.
        # We assume that the passfile was created by Patroni and therefore doing
        # the full match and not covering cases when host, port or user are set to '*'
        passfile_mtime = mtime(passfile)
        if passfile_mtime:
            try:
                with open(passfile) as f:
                    wanted_lines = (self._pgpass_content(wanted_primary_conninfo) or '').splitlines()
                    file_lines = f.read().splitlines()
                    if set(wanted_lines) == set(file_lines):
                        self._passfile = passfile
                        self._passfile_mtime = passfile_mtime
                        return True
            except Exception:
                logger.info('Failed to read %s', passfile)
        return False

    def _check_primary_conninfo(self, primary_conninfo: Dict[str, Any],
                                wanted_primary_conninfo: Dict[str, Any]) -> bool:
        # first we will cover corner cases, when we are replicating from somewhere while shouldn't
        # or there is no primary_conninfo but we should replicate from some specific node.
        if not wanted_primary_conninfo:
            return not primary_conninfo
        elif not primary_conninfo:
            return False

        if self._postgresql.major_version < 170000:
            # we want to compare dbname in primary_conninfo only for v17 onwards
            wanted_primary_conninfo.pop('dbname', None)

        if not self._postgresql.is_starting():
            wal_receiver_primary_conninfo = self._postgresql.primary_conninfo()
            if wal_receiver_primary_conninfo:
                wal_receiver_primary_conninfo = parse_dsn(wal_receiver_primary_conninfo)
                # when wal receiver is alive use primary_conninfo from pg_stat_wal_receiver for comparison
                if wal_receiver_primary_conninfo:
                    # dbname in pg_stat_wal_receiver is always `replication`, we need to use a "real" one
                    wal_receiver_primary_conninfo.pop('dbname', None)
                    dbname = primary_conninfo.get('dbname')
                    if dbname:
                        wal_receiver_primary_conninfo['dbname'] = dbname
                    primary_conninfo = wal_receiver_primary_conninfo
                    # There could be no password in the primary_conninfo or it is masked.
                    # Just copy the "desired" value in order to make comparison succeed.
                    if 'password' in wanted_primary_conninfo:
                        primary_conninfo['password'] = wanted_primary_conninfo['password']

        if 'passfile' in primary_conninfo and 'password' not in primary_conninfo \
                and 'password' in wanted_primary_conninfo:
            if self._check_passfile(primary_conninfo['passfile'], wanted_primary_conninfo):
                primary_conninfo['password'] = wanted_primary_conninfo['password']
            else:
                return False

        return all(str(primary_conninfo.get(p)) == str(v) for p, v in wanted_primary_conninfo.items() if v is not None)

    def check_recovery_conf(self, member: Union[Leader, Member, None]) -> Tuple[bool, bool]:
        """Returns a tuple. The first boolean element indicates that recovery params don't match
           and the second is set to `True` if the restart is required in order to apply new values"""

        # TODO: recovery.conf could be stale, would be nice to detect that.
        if self._postgresql.major_version >= 120000:
            if not os.path.exists(self._standby_signal):
                return True, True

            _read_recovery_params = self._read_recovery_params
        else:
            if not self.recovery_conf_exists():
                return True, True

            _read_recovery_params = self._read_recovery_params_pre_v12

        params, updated = _read_recovery_params()
        # updated indicates that mtime of postgresql.conf, postgresql.auto.conf, or recovery.conf
        # was changed and params were read either from the config or from the database connection.
        if updated:
            if params is None:  # exception or unparsable config
                return True, True

            # We will cache parsed value until the next config change.
            self._current_recovery_params = params
            primary_conninfo = params['primary_conninfo']
            if primary_conninfo[0]:
                primary_conninfo[0] = parse_dsn(params['primary_conninfo'][0])
                # If we failed to parse non-empty connection string this indicates that config if broken.
                if not primary_conninfo[0]:
                    return True, True
            else:  # empty string, primary_conninfo is not in the config
                primary_conninfo[0] = {}

        if not self._postgresql.is_starting() and self._current_recovery_params:
            # when wal receiver is alive take primary_slot_name from pg_stat_wal_receiver
            wal_receiver_primary_slot_name = self._postgresql.primary_slot_name()
            if not wal_receiver_primary_slot_name and self._postgresql.primary_conninfo():
                wal_receiver_primary_slot_name = ''
            if wal_receiver_primary_slot_name is not None:
                self._current_recovery_params['primary_slot_name'][0] = wal_receiver_primary_slot_name

        # Increment the 'reload' to enforce write of postgresql.conf when joining the running postgres
        required = {'restart': 0,
                    'reload': int(self._postgresql.major_version >= 120000
                                  and not self._postgresql.cb_called
                                  and not self._postgresql.is_starting())}

        def record_mismatch(mtype: bool) -> None:
            required['restart' if mtype else 'reload'] += 1

        wanted_recovery_params = self.build_recovery_params(member)
        for param, value in (self._current_recovery_params or EMPTY_DICT).items():
            # Skip certain parameters defined in the included postgres config files
            # if we know that they are not specified in the patroni configuration.
            if len(value) > 2 and value[2] not in (self._postgresql_conf, self._auto_conf) and \
                    param in ('archive_cleanup_command', 'promote_trigger_file', 'recovery_end_command',
                              'recovery_min_apply_delay', 'restore_command') and param not in wanted_recovery_params:
                continue
            if param == 'recovery_min_apply_delay':
                if not compare_values('integer', 'ms', value[0], wanted_recovery_params.get(param, 0)):
                    record_mismatch(value[1])
            elif param == 'standby_mode':
                if not compare_values('bool', None, value[0], wanted_recovery_params.get(param, 'on')):
                    record_mismatch(value[1])
            elif param == 'primary_conninfo':
                if not self._check_primary_conninfo(value[0], wanted_recovery_params.get('primary_conninfo', {})):
                    record_mismatch(value[1])
            elif (param != 'primary_slot_name' or wanted_recovery_params.get('primary_conninfo')) \
                    and str(value[0]) != str(wanted_recovery_params.get(param, '')):
                record_mismatch(value[1])
        return required['restart'] + required['reload'] > 0, required['restart'] > 0

    @staticmethod
    def _remove_file_if_exists(name: str) -> None:
        if os.path.isfile(name) or os.path.islink(name):
            os.unlink(name)

    @staticmethod
    def _pgpass_content(record: Dict[str, Any]) -> Optional[str]:
        """Generate content of `pgpassfile` based on connection parameters.

        .. note::
            In case if ``host`` is a comma separated string we generate one line per host.

        :param record: :class:`dict` object with connection parameters.
        :returns: a string with generated content of pgpassfile or ``None`` if there is no ``password``.
        """
        if 'password' in record:
            def escape(value: Any) -> str:
                return re.sub(r'([:\\])', r'\\\1', str(value))

            # 'host' could be several comma-separated hostnames, in this case we need to write on pgpass line per host
            hosts = [escape(host) for host in filter(None, map(str.strip,
                     (record.get('host', '') or '*').split(',')))]  # pyright: ignore [reportUnknownArgumentType]
            if any(host.startswith('/') for host in hosts) and 'localhost' not in hosts:
                hosts.append('localhost')
            record = {n: escape(record.get(n) or '*') for n in ('port', 'user', 'password')}
            return ''.join('{host}:{port}:*:{user}:{password}\n'.format(**record, host=host) for host in hosts)

    def write_pgpass(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Maybe creates :attr:`_passfile` based on connection parameters.

        :param record: :class:`dict` object with connection parameters.

        :returns: a copy of environment variables, that will include ``PGPASSFILE`` in case if the file was written.
        """
        content = self._pgpass_content(record)
        if not content:
            return os.environ.copy()

        with open(self._pgpass, 'w') as f:
            os.chmod(self._pgpass, stat.S_IWRITE | stat.S_IREAD)
            f.write(content)

        return {**os.environ, 'PGPASSFILE': self._pgpass}

    def write_recovery_conf(self, recovery_params: CaseInsensitiveDict) -> None:
        self._recovery_params = recovery_params
        if self._postgresql.major_version >= 120000:
            if parse_bool(recovery_params.pop('standby_mode', None)):
                open(self._standby_signal, 'w').close()
                self.set_file_permissions(self._standby_signal)
            else:
                self._remove_file_if_exists(self._standby_signal)
                open(self._recovery_signal, 'w').close()
                self.set_file_permissions(self._recovery_signal)

            def restart_required(name: str) -> bool:
                if self._postgresql.major_version >= 140000:
                    return False
                return name == 'restore_command' or (self._postgresql.major_version < 130000
                                                     and name in ('primary_conninfo', 'primary_slot_name'))

            self._current_recovery_params = CaseInsensitiveDict({n: [v, restart_required(n), self._postgresql_conf]
                                                                 for n, v in recovery_params.items()})
        else:
            with self.config_writer(self._recovery_conf) as f:
                self._write_recovery_params(f, recovery_params)

    def remove_recovery_conf(self) -> None:
        for name in (self._recovery_conf, self._standby_signal, self._recovery_signal):
            self._remove_file_if_exists(name)
        self._recovery_params = CaseInsensitiveDict()
        self._current_recovery_params = None

    def _sanitize_auto_conf(self) -> None:
        overwrite = False
        lines: List[str] = []

        if os.path.exists(self._auto_conf):
            try:
                with open(self._auto_conf) as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        match = PARAMETER_RE.match(line)
                        if match and match.group(1).lower() in self._RECOVERY_PARAMETERS:
                            overwrite = True
                        else:
                            lines.append(raw_line)
            except Exception:
                logger.info('Failed to read %s', self._auto_conf)

        if overwrite:
            try:
                with open(self._auto_conf, 'w') as f:
                    self.set_file_permissions(self._auto_conf)
                    for raw_line in lines:
                        f.write(raw_line)
            except Exception:
                logger.exception('Failed to remove some unwanted parameters from %s', self._auto_conf)

    def _adjust_recovery_parameters(self) -> None:
        # It is not strictly necessary, but we can make patroni configs crossi-compatible with all postgres versions.
        recovery_conf = {n: v for n, v in self._server_parameters.items() if n.lower() in self._RECOVERY_PARAMETERS}
        if recovery_conf:
            self._config['recovery_conf'] = recovery_conf

        if self.get('recovery_conf'):
            value = self._config['recovery_conf'].pop(self._triggerfile_wrong_name, None)
            if self.triggerfile_good_name not in self._config['recovery_conf'] and value:
                self._config['recovery_conf'][self.triggerfile_good_name] = value

    def get_server_parameters(self, config: Dict[str, Any]) -> CaseInsensitiveDict:
        parameters = config['parameters'].copy()
        listen_addresses, port = split_host_port(config['listen'], 5432)
        parameters.update(cluster_name=self._postgresql.scope, listen_addresses=listen_addresses, port=str(port))
        if global_config.is_synchronous_mode:
            synchronous_standby_names = self._server_parameters.get('synchronous_standby_names')
            if synchronous_standby_names is None:
                if global_config.is_synchronous_mode_strict\
                        and self._postgresql.role in (PostgresqlRole.PRIMARY, PostgresqlRole.PROMOTED):
                    parameters['synchronous_standby_names'] = '*'
                else:
                    parameters.pop('synchronous_standby_names', None)
            else:
                parameters['synchronous_standby_names'] = synchronous_standby_names

        # Handle hot_standby <-> replica rename
        if parameters.get('wal_level') == ('hot_standby' if self._postgresql.major_version >= 90600 else 'replica'):
            parameters['wal_level'] = 'replica' if self._postgresql.major_version >= 90600 else 'hot_standby'

        # Try to recalculate wal_keep_segments <-> wal_keep_size assuming that typical wal_segment_size is 16MB.
        # The real segment size could be estimated from pg_control, but we don't really care, because the only goal of
        # this exercise is improving cross version compatibility and user must set the correct parameter in the config.
        if self._postgresql.major_version >= 130000:
            wal_keep_segments = parameters.pop('wal_keep_segments', self.CMDLINE_OPTIONS['wal_keep_segments'][0])
            parameters.setdefault('wal_keep_size', str(int(wal_keep_segments) * 16) + 'MB')
        elif self._postgresql.major_version:
            wal_keep_size = parse_int(parameters.pop('wal_keep_size', self.CMDLINE_OPTIONS['wal_keep_size'][0]), 'MB')
            parameters.setdefault('wal_keep_segments', int(((wal_keep_size or 0) + 8) / 16))

        self._postgresql.mpp_handler.adjust_postgres_gucs(parameters)

        ret = CaseInsensitiveDict({k: v for k, v in parameters.items() if not self._postgresql.major_version
                                   or self._postgresql.major_version >= self.CMDLINE_OPTIONS.get(k, (0, 1, 90100))[2]})
        ret.update({k: os.path.join(self._config_dir, ret[k]) for k in ('hba_file', 'ident_file') if k in ret})
        return ret

    @staticmethod
    def _get_unix_local_address(unix_socket_directories: str) -> str:
        for d in unix_socket_directories.split(','):
            d = d.strip()
            if d.startswith('/'):  # Only absolute path can be used to connect via unix-socket
                return d
        return ''

    def _get_tcp_local_address(self) -> str:
        listen_addresses = self._server_parameters['listen_addresses'].split(',')

        for la in listen_addresses:
            if la.strip().lower() in ('*', 'localhost'):  # we are listening on '*' or localhost
                return 'localhost'  # connection via localhost is preferred
            if la.strip() in ('0.0.0.0', '127.0.0.1'):  # Postgres listens only on IPv4
                # localhost, but don't allow Windows to resolve to IPv6
                return '127.0.0.1' if os.name == 'nt' else 'localhost'
        return listen_addresses[0].strip()  # can't use localhost, take first address from listen_addresses

    def resolve_connection_addresses(self) -> None:
        """Calculates and sets local and remote connection urls and options.

        This method sets:
            * :attr:`Postgresql.connection_string <patroni.postgresql.Postgresql.connection_string>` attribute, which
              is later written to the member key in DCS as ``conn_url``.
            * :attr:`ConfigHandler.local_replication_address` attribute, which is used for replication connections to
              local postgres.
            * :attr:`ConnectionPool.conn_kwargs <patroni.postgresql.connection.ConnectionPool.conn_kwargs>` attribute,
              which is used for superuser connections to local postgres.

        .. note::
            If there is a valid directory in ``postgresql.parameters.unix_socket_directories`` in the Patroni
            configuration and ``postgresql.use_unix_socket`` and/or ``postgresql.use_unix_socket_repl``
            are set to ``True``, we respectively use unix sockets for superuser and replication connections
            to local postgres.

            If there is a requirement to use unix sockets, but nothing is set in the
            ``postgresql.parameters.unix_socket_directories``, we omit a ``host`` in connection parameters relying
            on the ability of ``libpq`` to connect via some default unix socket directory.

            If unix sockets are not requested we "switch" to TCP, preferring to use ``localhost`` if it is possible
            to deduce that Postgres is listening on a local interface address.

            Otherwise we just used the first address specified in the ``listen_addresses`` GUC.
        """
        port = self._server_parameters['port']
        tcp_local_address = self._get_tcp_local_address()
        netloc = self._config.get('connect_address') or tcp_local_address + ':' + port

        unix_local_address = {'port': port}
        unix_socket_directories = self._server_parameters.get('unix_socket_directories')
        if unix_socket_directories is not None:
            # fallback to tcp if unix_socket_directories is set, but there are no suitable values
            unix_local_address['host'] = self._get_unix_local_address(unix_socket_directories) or tcp_local_address

        tcp_local_address = {'host': tcp_local_address, 'port': port}

        self.local_replication_address = unix_local_address\
            if self._config.get('use_unix_socket_repl') else tcp_local_address

        self._postgresql.connection_string = uri('postgres', netloc, self._postgresql.database)

        local_address = unix_local_address if self._config.get('use_unix_socket') else tcp_local_address
        local_conn_kwargs = {
            **local_address,
            **self._superuser,
            'dbname': self._postgresql.database,
            'fallback_application_name': 'Patroni',
            'connect_timeout': 3,
            'options': '-c statement_timeout=2000'
        }
        # if the "username" parameter is present, it actually needs to be "user" for connecting to PostgreSQL
        if 'username' in local_conn_kwargs:
            local_conn_kwargs['user'] = local_conn_kwargs.pop('username')
        # "notify" connection_pool about the "new" local connection address
        self._postgresql.connection_pool.conn_kwargs = local_conn_kwargs

    def _get_pg_settings(self, names: Collection[str]) -> Dict[Any, Tuple[Any, ...]]:
        return {r[0]: r for r in self._postgresql.query(('SELECT name, setting, unit, vartype, context, sourcefile'
                                                         + ' FROM pg_catalog.pg_settings '
                                                         + ' WHERE pg_catalog.lower(name) = ANY(%s)'),
                                                        [n.lower() for n in names])}

    @staticmethod
    def _handle_wal_buffers(old_values: Dict[Any, Tuple[Any, ...]], changes: CaseInsensitiveDict) -> None:
        wal_block_size = parse_int(old_values['wal_block_size'][1]) or 8192
        wal_segment_size = old_values['wal_segment_size']
        wal_segment_unit = parse_int(wal_segment_size[2], 'B') or 8192 \
            if wal_segment_size[2] is not None and wal_segment_size[2][0].isdigit() else 1
        wal_segment_size = parse_int(wal_segment_size[1]) or (16777216 if wal_segment_size[2] is None else 2048)
        wal_segment_size *= wal_segment_unit / wal_block_size
        default_wal_buffers = min(max((parse_int(old_values['shared_buffers'][1]) or 16384) / 32, 8), wal_segment_size)

        wal_buffers = old_values['wal_buffers']
        new_value = str(changes['wal_buffers'] or -1)

        new_value = default_wal_buffers if new_value == '-1' else parse_int(new_value, wal_buffers[2])
        old_value = default_wal_buffers if wal_buffers[1] == '-1' else parse_int(*wal_buffers[1:3])

        if new_value == old_value:
            del changes['wal_buffers']

    def reload_config(self, config: Dict[str, Any], sighup: bool = False) -> None:
        self._superuser = config['authentication'].get('superuser', {})
        server_parameters = self.get_server_parameters(config)
        params_skip_changes = CaseInsensitiveSet((*self._RECOVERY_PARAMETERS, 'hot_standby'))

        conf_changed = hba_changed = ident_changed = local_connection_address_changed = False
        param_diff = CaseInsensitiveDict()
        if self._postgresql.state == PostgresqlState.RUNNING:
            changes = CaseInsensitiveDict({p: v for p, v in server_parameters.items()
                                           if p not in params_skip_changes})
            changes.update({p: None for p in self._server_parameters.keys()
                            if not (p in changes or p in params_skip_changes)})
            if changes:
                undef = []
                if 'wal_buffers' in changes:  # we need to calculate the default value of wal_buffers
                    undef = [p for p in ('shared_buffers', 'wal_segment_size', 'wal_block_size') if p not in changes]
                    changes.update({p: None for p in undef})
                # XXX: query can raise an exception
                old_values = self._get_pg_settings(changes.keys())
                if 'wal_buffers' in changes:
                    self._handle_wal_buffers(old_values, changes)
                    for p in undef:
                        del changes[p]

                for r in old_values.values():
                    if r[4] != 'internal' and r[0] in changes:
                        new_value = changes.pop(r[0])
                        if new_value is None or not compare_values(r[3], r[2], r[1], new_value):
                            conf_changed = True
                            if r[4] == 'postmaster':
                                param_diff[r[0]] = get_param_diff(r[1], new_value, r[3], r[2])
                                logger.info("Changed %s from '%s' to '%s' (restart might be required)",
                                            r[0], param_diff[r[0]]['old_value'], new_value)
                                if config.get('use_unix_socket') and r[0] == 'unix_socket_directories'\
                                        or r[0] in ('listen_addresses', 'port'):
                                    local_connection_address_changed = True
                            else:
                                logger.info("Changed %s from '%s' to '%s'",
                                            r[0], maybe_convert_from_base_unit(r[1], r[3], r[2]), new_value)
                        elif r[0] in self._server_parameters \
                                and not compare_values(r[3], r[2], r[1], self._server_parameters[r[0]]):
                            # Check if any parameter was set back to the current pg_settings value
                            # We can use pg_settings value here, as it is proved to be equal to new_value
                            logger.info("Changed %s from '%s' to '%s'", r[0], self._server_parameters[r[0]], new_value)
                            conf_changed = True
                for param, value in changes.items():
                    if '.' in param:
                        # Check that user-defined-parameters have changed (parameters with period in name)
                        if value is None or param not in self._server_parameters \
                                or str(value) != str(self._server_parameters[param]):
                            logger.info("Changed %s from '%s' to '%s'",
                                        param, self._server_parameters.get(param), value)
                            conf_changed = True
                    elif param in server_parameters:
                        logger.warning('Removing invalid parameter `%s` from postgresql.parameters', param)
                        server_parameters.pop(param)

            if (not server_parameters.get('hba_file') or server_parameters['hba_file'] == self._pg_hba_conf) \
                    and config.get('pg_hba'):
                hba_changed = self._config.get('pg_hba', []) != config['pg_hba']

            if (not server_parameters.get('ident_file') or server_parameters['ident_file'] == self._pg_ident_conf) \
                    and config.get('pg_ident'):
                ident_changed = self._config.get('pg_ident', []) != config['pg_ident']

        self._config = config
        self._server_parameters = server_parameters
        self._adjust_recovery_parameters()
        self._krbsrvname = config.get('krbsrvname')

        # for not so obvious connection attempts that may happen outside of pyscopg2
        if self._krbsrvname:
            os.environ['PGKRBSRVNAME'] = self._krbsrvname

        if not local_connection_address_changed:
            self.resolve_connection_addresses()

        proxy_addr = config.get('proxy_address')
        self._postgresql.proxy_url = uri('postgres', proxy_addr, self._postgresql.database) if proxy_addr else None

        if conf_changed or sighup:
            self.write_postgresql_conf()

        if hba_changed or sighup:
            self.replace_pg_hba()

        if ident_changed or sighup:
            self.replace_pg_ident()

        if sighup or conf_changed or hba_changed or ident_changed:
            logger.info('Reloading PostgreSQL configuration.')
            self._postgresql.reload()
            if self._postgresql.major_version >= 90500:
                time.sleep(1)
                try:
                    settings_diff: CaseInsensitiveDict = CaseInsensitiveDict()
                    for param, value, unit, vartype in self._postgresql.query(
                            'SELECT name, pg_catalog.current_setting(name), unit, vartype FROM pg_catalog.pg_settings'
                            ' WHERE pg_catalog.lower(name) != ALL(%s) AND pending_restart',
                            [n.lower() for n in params_skip_changes]):
                        new_value = self._postgresql.get_guc_value(param)
                        new_value = '?' if new_value is None else new_value
                        settings_diff[param] = get_param_diff(value, new_value, vartype, unit)
                    external_change = {param: value for param, value in settings_diff.items()
                                       if param not in param_diff or value != param_diff[param]}
                    if external_change:
                        logger.info("PostgreSQL configuration parameters requiring restart"
                                    " (%s) seem to be changed bypassing Patroni config."
                                    " Setting 'Pending restart' flag", ', '.join(external_change))
                    param_diff = settings_diff
                except Exception as e:
                    logger.warning('Exception %r when running query', e)
        else:
            logger.info('No PostgreSQL configuration items changed, nothing to reload.')

        self._postgresql.set_pending_restart_reason(param_diff)

    def set_synchronous_standby_names(self, value: Optional[str]) -> Optional[bool]:
        """Updates synchronous_standby_names and reloads if necessary.
        :returns: True if value was updated."""
        if value != self._server_parameters.get('synchronous_standby_names'):
            if value is None:
                self._server_parameters.pop('synchronous_standby_names', None)
            else:
                self._server_parameters['synchronous_standby_names'] = value
            if self._postgresql.state == PostgresqlState.RUNNING:
                self.write_postgresql_conf()
                self._postgresql.reload()
            return True

    @property
    def effective_configuration(self) -> CaseInsensitiveDict:
        """It might happen that the current value of one (or more) below parameters stored in
        the controldata is higher than the value stored in the global cluster configuration.

        Example: max_connections in global configuration is 100, but in controldata
        `Current max_connections setting: 200`. If we try to start postgres with
        max_connections=100, it will immediately exit.
        As a workaround we will start it with the values from controldata and set `pending_restart`
        to true as an indicator that current values of parameters are not matching expectations."""

        if self._postgresql.role == PostgresqlRole.PRIMARY:
            return self._server_parameters

        options_mapping = {
            'max_connections': 'max_connections setting',
            'max_prepared_transactions': 'max_prepared_xacts setting',
            'max_locks_per_transaction': 'max_locks_per_xact setting'
        }

        if self._postgresql.major_version >= 90400:
            options_mapping['max_worker_processes'] = 'max_worker_processes setting'

        if self._postgresql.major_version >= 120000:
            options_mapping['max_wal_senders'] = 'max_wal_senders setting'

        data = self._postgresql.controldata()
        effective_configuration = self._server_parameters.copy()

        param_diff = CaseInsensitiveDict()
        for name, cname in options_mapping.items():
            value = parse_int(effective_configuration[name])
            if cname not in data:
                logger.warning('%s is missing from pg_controldata output', cname)
                continue

            cvalue = parse_int(data[cname])
            if cvalue is not None and value is not None and cvalue > value:
                effective_configuration[name] = cvalue
                logger.info("%s value in pg_controldata: %d, in the global configuration: %d."
                            " pg_controldata value will be used. Setting 'Pending restart' flag", name, cvalue, value)
                param_diff[name] = get_param_diff(cvalue, value)
        self._postgresql.set_pending_restart_reason(param_diff)

        # If we are using custom bootstrap with PITR it could fail when values like max_connections
        # are increased, therefore we disable hot_standby if recovery_target_action == 'promote'.
        if self._postgresql.bootstrap.running_custom_bootstrap:
            disable_hot_standby = False
            if self._postgresql.bootstrap.keep_existing_recovery_conf:
                disable_hot_standby = True  # trust that pgBackRest does the right thing
            # `pause_at_recovery_target` has no effect if hot_standby is not enabled, therefore we consider only 9.5+
            elif self._postgresql.major_version >= 90500 and self._recovery_params:
                pause_at_recovery_target = parse_bool(self._recovery_params.get('pause_at_recovery_target'))
                recovery_target_action = self._recovery_params.get(
                    'recovery_target_action', 'promote' if pause_at_recovery_target is False else 'pause')
                disable_hot_standby = recovery_target_action == 'promote'

            if disable_hot_standby:
                effective_configuration['hot_standby'] = 'off'

        return effective_configuration

    @property
    def replication(self) -> Dict[str, Any]:
        return self._config['authentication']['replication']

    @property
    def superuser(self) -> Dict[str, Any]:
        return self._superuser

    @property
    def rewind_credentials(self) -> Dict[str, Any]:
        return self._config['authentication'].get('rewind', self._superuser) \
            if self._postgresql.major_version >= 110000 else self._superuser

    @property
    def ident_file(self) -> Optional[str]:
        ident_file = self._server_parameters.get('ident_file')
        return None if ident_file == self._pg_ident_conf else ident_file

    @property
    def hba_file(self) -> Optional[str]:
        hba_file = self._server_parameters.get('hba_file')
        return None if hba_file == self._pg_hba_conf else hba_file

    @property
    def pg_hba_conf(self) -> str:
        return self._pg_hba_conf

    @property
    def postgresql_conf(self) -> str:
        return self._postgresql_conf

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._config.get(key, default)

    def restore_command(self) -> Optional[str]:
        return (self.get('recovery_conf') or EMPTY_DICT).get('restore_command')

    @property
    def synchronous_standby_names(self) -> Optional[str]:
        """Get ``synchronous_standby_names`` value configured by the user.

        :returns: value of ``synchronous_standby_names`` in the Patroni configuration,
            if any, otherwise ``None``.
        """
        return (self.get('parameters') or EMPTY_DICT).get('synchronous_standby_names')
