import logging
import os
import re
import shutil
import socket
import stat

from requests.structures import CaseInsensitiveDict
from six.moves.urllib_parse import urlparse, parse_qsl, unquote

from ..utils import compare_values, parse_bool, parse_int, split_host_port, uri

logger = logging.getLogger(__name__)

SYNC_STANDBY_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z_0-9\$]*$')
PARAMETER_RE = re.compile(r'([a-z_]+)\s*=\s*')


def quote_ident(value):
    """Very simplified version of quote_ident"""
    return value if SYNC_STANDBY_NAME_RE.match(value) else '"' + value + '"'


def conninfo_uri_parse(dsn):
    ret = {}
    r = urlparse(dsn)
    if r.username:
        ret['user'] = r.username
    if r.password:
        ret['password'] = r.password
    if r.path[1:]:
        ret['dbname'] = r.path[1:]
    hosts = []
    ports = []
    for netloc in r.netloc.split('@')[-1].split(','):
        host = port = None
        if '[' in netloc and ']' in netloc:
            host = netloc.split(']')[0][1:]
        tmp = netloc.split(':', 1)
        if host is None:
            host = tmp[0]
        if len(tmp) == 2:
            host, port = tmp
        if host is not None:
            hosts.append(host)
        if port is not None:
            ports.append(port)
    if hosts:
        ret['host'] = ','.join(hosts)
    if ports:
        ret['port'] = ','.join(ports)
    ret = {name: unquote(value) for name, value in ret.items()}
    ret.update({name: value for name, value in parse_qsl(r.query)})
    if ret.get('ssl') == 'true':
        del ret['ssl']
        ret['sslmode'] = 'require'
    return ret


def read_param_value(value, is_quoted=False):
    length = len(value)
    ret = ''
    i = 0
    while i < length:
        if is_quoted:
            if value[i] == "'":
                return ret, i
        elif value[i].isspace():
            break
        if value[i] == '\\':
            i += 1
            if i >= length:
                break
        ret += value[i]
        i += 1
    return (None, None) if is_quoted else (ret, i)


def conninfo_parse(dsn):
    ret = {}
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

        is_quoted = dsn[i] == "'"
        i += int(is_quoted)
        value, end = read_param_value(dsn[i:], is_quoted)
        if value is None:
            return
        i += end + int(is_quoted)
        ret[param] = value
    return ret


def parse_dsn(value):
    """
    Very simple equivalent of `psycopg2.extensions.parse_dsn` introduced in 2.7.0.
    We are not using psycopg2 function in order to remain compatible with 2.5.4+.
    There is one minor difference though, this function removes `dbname` from the result
    and sets the sslmode` to `prefer` if it is not present in the connection string.
    This is necessary to simplify comparison of the old and the new values.

    >>> r = parse_dsn('postgresql://u%2Fse:pass@:%2f123,[%2Fhost2]/db%2Fsdf?application_name=mya%2Fpp&ssl=true')
    >>> r == {'application_name': 'mya/pp', 'host': ',/host2', 'sslmode': 'require',\
              'password': 'pass', 'port': '/123', 'user': 'u/se'}
    True
    >>> r = parse_dsn(" host = 'host' dbname = db\\ name requiressl=1 ")
    >>> r == {'host': 'host', 'sslmode': 'require'}
    True
    >>> parse_dsn('requiressl = 0\\\\') == {'sslmode': 'prefer'}
    True
    >>> parse_dsn("host=a foo = '") is None
    True
    >>> parse_dsn("host=a foo = ") is None
    True
    >>> parse_dsn("1") is None
    True
    """
    if value.startswith('postgres://') or value.startswith('postgresql://'):
        ret = conninfo_uri_parse(value)
    else:
        ret = conninfo_parse(value)

    if ret:
        if 'sslmode' not in ret:  # allow sslmode to take precedence over requiressl
            requiressl = ret.pop('requiressl', None)
            if requiressl == '1':
                ret['sslmode'] = 'require'
            elif requiressl is not None:
                ret['sslmode'] = 'prefer'
            ret.setdefault('sslmode', 'prefer')
        if 'dbname' in ret:
            del ret['dbname']
    return ret


def mtime(filename):
    try:
        return os.stat(filename).st_mtime
    except OSError:
        return None


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
        'listen_addresses': (None, lambda _: False, 90100),
        'port': (None, lambda _: False, 90100),
        'cluster_name': (None, lambda _: False, 90500),
        'wal_level': ('hot_standby', lambda v: v.lower() in ('hot_standby', 'replica', 'logical'), 90100),
        'hot_standby': ('on', lambda _: False, 90100),
        'max_connections': (100, lambda v: int(v) >= 100, 90100),
        'max_wal_senders': (10, lambda v: int(v) >= 10, 90100),
        'wal_keep_segments': (8, lambda v: int(v) >= 8, 90100),
        'max_prepared_transactions': (0, lambda v: int(v) >= 0, 90100),
        'max_locks_per_transaction': (64, lambda v: int(v) >= 64, 90100),
        'track_commit_timestamp': ('off', lambda v: parse_bool(v) is not None, 90500),
        'max_replication_slots': (10, lambda v: int(v) >= 10, 90400),
        'max_worker_processes': (8, lambda v: int(v) >= 8, 90400),
        'wal_log_hints': ('on', lambda _: False, 90400)
    })

    _RECOVERY_PARAMETERS = {
        'archive_cleanup_command',
        'restore_command',
        'recovery_end_command',
        'recovery_target',
        'recovery_target_name',
        'recovery_target_time',
        'recovery_target_xid',
        'recovery_target_lsn',
        'recovery_target_inclusive',
        'recovery_target_timeline',
        'recovery_target_action',
        'recovery_min_apply_delay',
        'primary_conninfo',
        'primary_slot_name',
        'promote_trigger_file',
        'trigger_file'
    }

    _CONFIG_WARNING_HEADER = '# Do not edit this file manually!\n# It will be overwritten by Patroni!\n'

    def __init__(self, postgresql, config):
        self._postgresql = postgresql
        self._config_dir = os.path.abspath(config.get('config_dir') or postgresql.data_dir)
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
        self._synchronous_standby_names = None
        self._postmaster_ctime = None
        self._primary_conninfo = None
        self._config = {}
        self._recovery_params = {}
        self.reload_config(config)

    def setup_server_parameters(self):
        self._server_parameters = self.get_server_parameters(self._config)
        self._adjust_recovery_parameters()

    @property
    def _configuration_to_save(self):
        configuration = [os.path.basename(self._postgresql_conf)]
        if 'custom_conf' not in self._config:
            configuration.append(os.path.basename(self._postgresql_base_conf_name))
        if not self.hba_file:
            configuration.append('pg_hba.conf')
        if not self._server_parameters.get('ident_file'):
            configuration.append('pg_ident.conf')
        return configuration

    def save_configuration_files(self, check_custom_bootstrap=False):
        """
            copy postgresql.conf to postgresql.conf.backup to be able to retrive configuration files
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
            except IOError:
                logger.exception('unable to create backup copies of configuration files')
        return True

    def restore_configuration_files(self):
        """ restore a previously saved postgresql.conf """
        try:
            for f in self._configuration_to_save:
                config_file = os.path.join(self._config_dir, f)
                backup_file = os.path.join(self._postgresql.data_dir, f + '.backup')
                if not os.path.isfile(config_file):
                    if os.path.isfile(backup_file):
                        shutil.copy(backup_file, config_file)
                    # Previously we didn't backup pg_ident.conf, if file is missing just create empty
                    elif f == 'pg_ident.conf':
                        open(config_file, 'w').close()
        except IOError:
            logger.exception('unable to restore configuration files from backup')

    def write_postgresql_conf(self, configuration=None):
        # rename the original configuration if it is necessary
        if 'custom_conf' not in self._config and not os.path.exists(self._postgresql_base_conf):
            os.rename(self._postgresql_conf, self._postgresql_base_conf)

        with open(self._postgresql_conf, 'w') as f:
            os.chmod(self._postgresql_conf, stat.S_IWRITE | stat.S_IREAD)
            f.write(self._CONFIG_WARNING_HEADER)
            f.write("include '{0}'\n\n".format(self._config.get('custom_conf') or self._postgresql_base_conf_name))
            for name, value in sorted((configuration or self._server_parameters).items()):
                if (not self._postgresql.bootstrap.running_custom_bootstrap or name != 'hba_file') \
                        and name not in self._RECOVERY_PARAMETERS:
                    f.write("{0} = '{1}'\n".format(name, value))
            # when we are doing custom bootstrap we assume that we don't know superuser password
            # and in order to be able to change it, we are opening trust access from a certain address
            # therefore we need to make sure that hba_file is not overriden
            # after changing superuser password we will "revert" all these "changes"
            if self._postgresql.bootstrap.running_custom_bootstrap or 'hba_file' not in self._server_parameters:
                f.write("hba_file = '{0}'\n".format(self._pg_hba_conf.replace('\\', '\\\\')))
            if 'ident_file' not in self._server_parameters:
                f.write("ident_file = '{0}'\n".format(self._pg_ident_conf.replace('\\', '\\\\')))

            if self._postgresql.major_version >= 120000:
                if self._recovery_params:
                    f.write('\n# recovery.conf\n')
                    for name, value in sorted(self._recovery_params.items()):
                        f.write("{0} = '{1}'\n".format(name, value))

                if not self._postgresql.bootstrap.keep_existing_recovery_conf:
                    self._sanitize_auto_conf()

    def append_pg_hba(self, config):
        if not self.hba_file and not self._config.get('pg_hba'):
            with open(self._pg_hba_conf, 'a') as f:
                f.write('\n{}\n'.format('\n'.join(config)))
        return True

    def replace_pg_hba(self):
        """
        Replace pg_hba.conf content in the PGDATA if hba_file is not defined in the
        `postgresql.parameters` and pg_hba is defined in `postgresql` configuration section.

        :returns: True if pg_hba.conf was rewritten.
        """

        # when we are doing custom bootstrap we assume that we don't know superuser password
        # and in order to be able to change it, we are opening trust access from a certain address
        if self._postgresql.bootstrap.running_custom_bootstrap:
            addresses = {'': 'local'}
            if 'host' in self.local_replication_address and not self.local_replication_address['host'].startswith('/'):
                addresses.update({sa[0] + '/32': 'host' for _, _, _, _, sa in socket.getaddrinfo(
                                  self.local_replication_address['host'], self.local_replication_address['port'],
                                  0, socket.SOCK_STREAM, socket.IPPROTO_TCP)})

            with open(self._pg_hba_conf, 'w') as f:
                f.write(self._CONFIG_WARNING_HEADER)
                for address, t in addresses.items():
                    f.write((
                        '{0}\treplication\t{1}\t{3}\ttrust\n'
                        '{0}\tall\t{2}\t{3}\ttrust\n'
                    ).format(t, self.replication['username'], self._superuser.get('username') or 'all', address))
        elif not self.hba_file and self._config.get('pg_hba'):
            with open(self._pg_hba_conf, 'w') as f:
                f.write(self._CONFIG_WARNING_HEADER)
                for line in self._config['pg_hba']:
                    f.write('{0}\n'.format(line))
            return True

    def replace_pg_ident(self):
        """
        Replace pg_ident.conf content in the PGDATA if ident_file is not defined in the
        `postgresql.parameters` and pg_ident is defined in the `postgresql` section.

        :returns: True if pg_ident.conf was rewritten.
        """

        if not self._server_parameters.get('ident_file') and self._config.get('pg_ident'):
            with open(self._pg_ident_conf, 'w') as f:
                f.write(self._CONFIG_WARNING_HEADER)
                for line in self._config['pg_ident']:
                    f.write('{0}\n'.format(line))
            return True

    def primary_conninfo_params(self, member):
        name = self._postgresql.name
        if not (member and member.conn_url) or member.name == name:
            return None
        ret = member.conn_kwargs(self.replication)
        ret.update(application_name=name, sslmode='prefer')
        if self._krbsrvname:
            ret['krbsrvname'] = self._krbsrvname
        if 'database' in ret:
            del ret['database']
        return ret

    def primary_conninfo(self, member):
        r = self.primary_conninfo_params(member)
        if not r:
            return None
        keywords = 'user password host port sslmode application_name krbsrvname'.split()
        return ' '.join('{0}={{{0}}}'.format(kw) for kw in keywords if r.get(kw)).format(**r)

    def recovery_conf_exists(self):
        if self._postgresql.major_version >= 120000:
            return os.path.exists(self._standby_signal) or os.path.exists(self._recovery_signal)
        return os.path.exists(self._recovery_conf)

    def _read_primary_conninfo(self):
        pg_conf_mtime = mtime(self._postgresql_conf)
        auto_conf_mtime = mtime(self._auto_conf)
        postmaster_ctime = self._postgresql.is_running()
        if postmaster_ctime:
            postmaster_ctime = postmaster_ctime.create_time()

        if self._postgresql_conf_mtime == pg_conf_mtime and self._auto_conf_mtime == auto_conf_mtime \
                and self._postmaster_ctime == postmaster_ctime:
            return None, False

        try:
            primary_conninfo = self._postgresql.query('SHOW primary_conninfo').fetchone()[0]
            self._postgresql_conf_mtime = pg_conf_mtime
            self._auto_conf_mtime = auto_conf_mtime
            self._postmaster_ctime = postmaster_ctime
        except Exception:
            primary_conninfo = None
        return primary_conninfo, True

    def _read_primary_conninfo_pre_v12(self):
        recovery_conf_mtime = mtime(self._recovery_conf)
        if recovery_conf_mtime == self._recovery_conf_mtime:
            return None, False

        primary_conninfo = ''
        with open(self._recovery_conf, 'r') as f:
            for line in f:
                match = PARAMETER_RE.match(line)
                if match and match.group(1) == 'primary_conninfo':
                    i = match.end()
                    if i < len(line):
                        is_quoted = line[i] == "'"
                        i += int(is_quoted)
                        primary_conninfo, _ = read_param_value(line[i:], is_quoted)
            self._recovery_conf_mtime = recovery_conf_mtime
        return primary_conninfo, True

    def check_recovery_conf(self, member):  # Name is confusing. In fact it checks the value of primary_conninfo
        # TODO: recovery.conf could be stale, would be nice to detect that.
        if self._postgresql.major_version >= 120000:
            if not os.path.exists(self._standby_signal):
                return False

            _read_primary_conninfo = self._read_primary_conninfo
        else:
            if not self.recovery_conf_exists():
                return False

            _read_primary_conninfo = self._read_primary_conninfo_pre_v12

        primary_conninfo, updated = _read_primary_conninfo()
        # updated indicates that mtime of postgresql.conf, postgresql.auto.conf, or recovery.conf was changed
        # and the primary_conninfo value was read either from config or from the database connection.
        if updated:
            # primary_conninfo is one of:
            # - None (exception or unparsable config)
            # - '' (not in config)
            # - or the actual dsn value
            self._primary_conninfo = primary_conninfo
            if primary_conninfo:
                # We will cache parsed value until the next config change.
                self._primary_conninfo = parse_dsn(primary_conninfo)
                # If we failed to parse non-empty connection string this indicates that config if broken.
                if not self._primary_conninfo:
                    return False
            elif primary_conninfo is not None:
                self._primary_conninfo = {}
            else:  # primary_conninfo is None, config is probably broken
                return False

        wanted_primary_conninfo = self.primary_conninfo_params(member)
        # first we will cover corner cases, when we are replicating from somewhere while shouldn't
        # or there is no primary_conninfo but we should replicate from some specific node.
        if not wanted_primary_conninfo:
            return not self._primary_conninfo
        elif not self._primary_conninfo:
            return False

        return all(self._primary_conninfo.get(p) == str(v) for p, v in wanted_primary_conninfo.items())

    @staticmethod
    def _remove_file_if_exists(name):
        if os.path.isfile(name) or os.path.islink(name):
            os.unlink(name)

    def write_recovery_conf(self, recovery_params):
        if self._postgresql.major_version >= 120000:
            if parse_bool(recovery_params.pop('standby_mode', None)):
                open(self._standby_signal, 'w').close()
            else:
                self._remove_file_if_exists(self._standby_signal)
                open(self._recovery_signal, 'w').close()
            self._recovery_params = recovery_params
        else:
            with open(self._recovery_conf, 'w') as f:
                os.chmod(self._recovery_conf, stat.S_IWRITE | stat.S_IREAD)
                for name, value in recovery_params.items():
                    f.write("{0} = '{1}'\n".format(name, value))

    def remove_recovery_conf(self):
        for name in (self._recovery_conf, self._standby_signal, self._recovery_signal):
            self._remove_file_if_exists(name)
        self._recovery_params = {}

    def _sanitize_auto_conf(self):
        overwrite = False
        lines = []

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
                    for raw_line in lines:
                        f.write(raw_line)
            except Exception:
                logger.exception('Failed to remove some unwanted parameters from %s', self._auto_conf)

    def _adjust_recovery_parameters(self):
        # It is not strictly necessary, but we can make patroni configs crossi-compatible with all postgres versions.
        recovery_conf = {n: v for n, v in self._server_parameters.items() if n.lower() in self._RECOVERY_PARAMETERS}
        if recovery_conf:
            self._config['recovery_conf'] = recovery_conf

        if self.get('recovery_conf'):
            good_name, bad_name = 'trigger_file', 'promote_trigger_file'
            if self._postgresql.major_version >= 120000:
                good_name, bad_name = bad_name, good_name

            value = self._config['recovery_conf'].pop(bad_name, None)
            if good_name not in self._config['recovery_conf'] and value:
                self._config['recovery_conf'][good_name] = value

    def get_server_parameters(self, config):
        parameters = config['parameters'].copy()
        listen_addresses, port = split_host_port(config['listen'], 5432)
        parameters.update(cluster_name=self._postgresql.scope, listen_addresses=listen_addresses, port=str(port))
        if config.get('synchronous_mode', False):
            if self._synchronous_standby_names is None:
                if config.get('synchronous_mode_strict', False):
                    parameters['synchronous_standby_names'] = '*'
                else:
                    parameters.pop('synchronous_standby_names', None)
            else:
                parameters['synchronous_standby_names'] = self._synchronous_standby_names
        if self._postgresql.major_version >= 90600 and parameters['wal_level'] == 'hot_standby':
            parameters['wal_level'] = 'replica'
        ret = CaseInsensitiveDict({k: v for k, v in parameters.items() if not self._postgresql.major_version or
                                   self._postgresql.major_version >= self.CMDLINE_OPTIONS.get(k, (0, 1, 90100))[2]})
        ret.update({k: os.path.join(self._config_dir, ret[k]) for k in ('hba_file', 'ident_file') if k in ret})
        return ret

    @staticmethod
    def _get_unix_local_address(unix_socket_directories):
        for d in unix_socket_directories.split(','):
            d = d.strip()
            if d.startswith('/'):  # Only absolute path can be used to connect via unix-socket
                return d
        return ''

    def _get_tcp_local_address(self):
        listen_addresses = self._server_parameters['listen_addresses'].split(',')

        for la in listen_addresses:
            if la.strip().lower() in ('*', '0.0.0.0', '127.0.0.1', 'localhost'):  # we are listening on '*' or localhost
                return 'localhost'  # connection via localhost is preferred
        return listen_addresses[0].strip()  # can't use localhost, take first address from listen_addresses

    @property
    def local_connect_kwargs(self):
        ret = self._local_address.copy()
        ret.update({'database': self._postgresql.database,
                    'fallback_application_name': 'Patroni',
                    'connect_timeout': 3,
                    'options': '-c statement_timeout=2000'})
        if 'username' in self._superuser:
            ret['user'] = self._superuser['username']
        if 'password' in self._superuser:
            ret['password'] = self._superuser['password']
        return ret

    def resolve_connection_addresses(self):
        port = self._server_parameters['port']
        tcp_local_address = self._get_tcp_local_address()

        local_address = {'port': port}
        if self._config.get('use_unix_socket'):
            unix_socket_directories = self._server_parameters.get('unix_socket_directories')
            if unix_socket_directories is not None:
                # fallback to tcp if unix_socket_directories is set, but there are no sutable values
                local_address['host'] = self._get_unix_local_address(unix_socket_directories) or tcp_local_address

            # if unix_socket_directories is not specified, but use_unix_socket is set to true - do our best
            # to use default value, i.e. don't specify a host neither in connection url nor arguments
        else:
            local_address['host'] = tcp_local_address

        self._local_address = local_address
        self.local_replication_address = {'host': tcp_local_address, 'port': port}

        netloc = self._config.get('connect_address') or tcp_local_address + ':' + port
        self._postgresql.connection_string = uri('postgres', netloc, self._postgresql.database)

        self._postgresql.set_connection_kwargs(self.local_connect_kwargs)

    def reload_config(self, config):
        self._superuser = config['authentication'].get('superuser', {})
        server_parameters = self.get_server_parameters(config)

        conf_changed = hba_changed = ident_changed = local_connection_address_changed = pending_restart = False
        if self._postgresql.state == 'running':
            changes = CaseInsensitiveDict({p: v for p, v in server_parameters.items() if '.' not in p})
            changes.update({p: None for p in self._server_parameters.keys() if not ('.' in p or p in changes)})
            if changes:
                # XXX: query can raise an exception
                for r in self._postgresql.query(('SELECT name, setting, unit, vartype, context '
                                                 + 'FROM pg_catalog.pg_settings ' +
                                                 ' WHERE pg_catalog.lower(name) IN ('
                                                 + ', '.join(['%s'] * len(changes)) +
                                                 ')'), *(k.lower() for k in changes.keys())):
                    if r[4] != 'internal' and r[0] in changes:
                        new_value = changes.pop(r[0])
                        if new_value is None or not compare_values(r[3], r[2], r[1], new_value):
                            if r[4] == 'postmaster':
                                pending_restart = True
                                logger.info('Changed %s from %s to %s (restart required)', r[0], r[1], new_value)
                                if config.get('use_unix_socket') and r[0] == 'unix_socket_directories'\
                                        or r[0] in ('listen_addresses', 'port'):
                                    local_connection_address_changed = True
                            else:
                                logger.info('Changed %s from %s to %s', r[0], r[1], new_value)
                                conf_changed = True
                for param in changes:
                    if param in server_parameters:
                        logger.warning('Removing invalid parameter `%s` from postgresql.parameters', param)
                        server_parameters.pop(param)

            # Check that user-defined-paramters have changed (parameters with period in name)
            if not conf_changed:
                for p, v in server_parameters.items():
                    if '.' in p and (p not in self._server_parameters or str(v) != str(self._server_parameters[p])):
                        logger.info('Changed %s from %s to %s', p, self._server_parameters.get(p), v)
                        conf_changed = True
                        break
                if not conf_changed:
                    for p, v in self._server_parameters.items():
                        if '.' in p and (p not in server_parameters or str(v) != str(server_parameters[p])):
                            logger.info('Changed %s from %s to %s', p, v, server_parameters.get(p))
                            conf_changed = True
                            break

            if not server_parameters.get('hba_file') and config.get('pg_hba'):
                hba_changed = self._config.get('pg_hba', []) != config['pg_hba']

            if not server_parameters.get('ident_file') and config.get('pg_ident'):
                ident_changed = self._config.get('pg_ident', []) != config['pg_ident']

        self._config = config
        self._postgresql.set_pending_restart(pending_restart)
        self._server_parameters = server_parameters
        self._adjust_recovery_parameters()
        self._connect_address = config.get('connect_address')
        self._krbsrvname = config.get('krbsrvname')

        # for not so obvious connection attempts that may happen outside of pyscopg2
        if self._krbsrvname:
            os.environ['PGKRBSRVNAME'] = self._krbsrvname

        if not local_connection_address_changed:
            self.resolve_connection_addresses()

        if conf_changed:
            self.write_postgresql_conf()

        if hba_changed:
            self.replace_pg_hba()

        if ident_changed:
            self.replace_pg_ident()

        if conf_changed or hba_changed or ident_changed:
            logger.info('PostgreSQL configuration items changed, reloading configuration.')
            self._postgresql.reload()
        elif not pending_restart:
            logger.info('No PostgreSQL configuration items changed, nothing to reload.')

    def set_synchronous_standby(self, name):
        """Sets a node to be synchronous standby and if changed does a reload for PostgreSQL."""
        if name and name != '*':
            name = quote_ident(name)
        if name != self._synchronous_standby_names:
            if name is None:
                self._server_parameters.pop('synchronous_standby_names', None)
            else:
                self._server_parameters['synchronous_standby_names'] = name
            self._synchronous_standby_names = name
            if self._postgresql.state == 'running':
                self.write_postgresql_conf()
                self._postgresql.reload()

    @property
    def effective_configuration(self):
        """It might happen that the current value of one (or more) below parameters stored in
        the controldata is higher than the value stored in the global cluster configuration.

        Example: max_connections in global configuration is 100, but in controldata
        `Current max_connections setting: 200`. If we try to start postgres with
        max_connections=100, it will immediately exit.
        As a workaround we will start it with the values from controldata and set `pending_restart`
        to true as an indicator that current values of parameters are not matching expectations."""

        if self._postgresql.role == 'master':
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

        for name, cname in options_mapping.items():
            value = parse_int(effective_configuration[name])
            cvalue = parse_int(data[cname])
            if cvalue > value:
                effective_configuration[name] = cvalue
                self._postgresql.set_pending_restart(True)
        return effective_configuration

    @property
    def replication(self):
        return self._config['authentication']['replication']

    @property
    def superuser(self):
        return self._superuser

    @property
    def rewind_credentials(self):
        return self._config['authentication'].get('rewind', self._superuser) \
                if self._postgresql.major_version >= 110000 else self._superuser

    @property
    def hba_file(self):
        return self._server_parameters.get('hba_file')

    @property
    def pg_hba_conf(self):
        return self._pg_hba_conf

    @property
    def postgresql_conf(self):
        return self._postgresql_conf

    def get(self, key, default=None):
        return self._config.get(key, default)
