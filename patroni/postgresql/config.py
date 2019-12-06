import logging
import os
import re
import shutil
import socket
import stat
import time

from six.moves.urllib_parse import urlparse, parse_qsl, unquote
from urllib3.response import HTTPHeaderDict

from ..dcs import slot_name_from_member_name, RemoteMember
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


def read_param_value(value):
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

        value, end = read_param_value(dsn[i:])
        if value is None:
            return
        i += end
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
    >>> r = parse_dsn(" host = 'host' dbname = db\\\\ name requiressl=1 ")
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


def strip_comment(value):
    i = value.find('#')
    if i > -1:
        value = value[:i].strip()
    return value


def read_recovery_param_value(value):
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


def mtime(filename):
    try:
        return os.stat(filename).st_mtime
    except OSError:
        return None


class ConfigWriter(object):

    def __init__(self, filename):
        self._filename = filename
        self._fd = None

    def __enter__(self):
        self._fd = open(self._filename, 'w')
        self.writeline('# Do not edit this file manually!\n# It will be overwritten by Patroni!')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._fd:
            self._fd.close()

    def writeline(self, line):
        self._fd.write(line)
        self._fd.write('\n')

    def writelines(self, lines):
        for line in lines:
            self.writeline(line)

    @staticmethod
    def escape(value):  # Escape (by doubling) any single quotes or backslashes in given string
        return re.sub(r'([\'\\])', r'\1\1', str(value))

    def write_param(self, param, value):
        self.writeline("{0} = '{1}'".format(param, self.escape(value)))


class CaseInsensitiveDict(HTTPHeaderDict):

    def add(self, key, val):
        self[key] = val

    def __getitem__(self, key):
        return self._container[key.lower()][1]

    def __repr__(self):
        return str(dict(self.items()))

    def copy(self):
        return CaseInsensitiveDict(self._container.values())


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
        'max_connections': (100, lambda v: int(v) >= 25, 90100),
        'max_wal_senders': (10, lambda v: int(v) >= 3, 90100),
        'wal_keep_segments': (8, lambda v: int(v) >= 1, 90100),
        'max_prepared_transactions': (0, lambda v: int(v) >= 0, 90100),
        'max_locks_per_transaction': (64, lambda v: int(v) >= 32, 90100),
        'track_commit_timestamp': ('off', lambda v: parse_bool(v) is not None, 90500),
        'max_replication_slots': (10, lambda v: int(v) >= 4, 90400),
        'max_worker_processes': (8, lambda v: int(v) >= 2, 90400),
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
        self._pgpass = config.get('pgpass') or os.path.join(os.path.expanduser('~'), 'pgpass')
        self._passfile = None
        self._passfile_mtime = None
        self._synchronous_standby_names = None
        self._postmaster_ctime = None
        self._current_recovery_params = None
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

        with ConfigWriter(self._postgresql_conf) as f:
            include = self._config.get('custom_conf') or self._postgresql_base_conf_name
            f.writeline("include '{0}'\n".format(ConfigWriter.escape(include)))
            for name, value in sorted((configuration or self._server_parameters).items()):
                if (not self._postgresql.bootstrap.running_custom_bootstrap or name != 'hba_file') \
                        and name not in self._RECOVERY_PARAMETERS:
                    f.write_param(name, value)
            # when we are doing custom bootstrap we assume that we don't know superuser password
            # and in order to be able to change it, we are opening trust access from a certain address
            # therefore we need to make sure that hba_file is not overriden
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

            with ConfigWriter(self._pg_hba_conf) as f:
                for address, t in addresses.items():
                    f.writeline((
                        '{0}\treplication\t{1}\t{3}\ttrust\n'
                        '{0}\tall\t{2}\t{3}\ttrust'
                    ).format(t, self.replication['username'], self._superuser.get('username') or 'all', address))
        elif not self.hba_file and self._config.get('pg_hba'):
            with ConfigWriter(self._pg_hba_conf) as f:
                f.writelines(self._config['pg_hba'])
            return True

    def replace_pg_ident(self):
        """
        Replace pg_ident.conf content in the PGDATA if ident_file is not defined in the
        `postgresql.parameters` and pg_ident is defined in the `postgresql` section.

        :returns: True if pg_ident.conf was rewritten.
        """

        if not self._server_parameters.get('ident_file') and self._config.get('pg_ident'):
            with ConfigWriter(self._pg_ident_conf) as f:
                f.writelines(self._config['pg_ident'])
            return True

    def primary_conninfo_params(self, member):
        if not (member and member.conn_url) or member.name == self._postgresql.name:
            return None
        ret = member.conn_kwargs(self.replication)
        ret['application_name'] = self._postgresql.name
        ret.setdefault('sslmode', 'prefer')
        if self._krbsrvname:
            ret['krbsrvname'] = self._krbsrvname
        if 'database' in ret:
            del ret['database']
        return ret

    def format_dsn(self, params, include_dbname=False):
        # A list of keywords that can be found in a conninfo string. Follows what is acceptable by libpq
        keywords = ('dbname', 'user', 'passfile' if params.get('passfile') else 'password', 'host', 'port', 'sslmode',
                    'sslcompression', 'sslcert', 'sslkey', 'sslrootcert', 'sslcrl', 'application_name', 'krbsrvname')
        if include_dbname:
            params = params.copy()
            params['dbname'] = params.get('database') or self._postgresql.database
            # we are abusing information about the necessity of dbname
            # dsn should contain passfile or password only if there is no dbname in it (it is used in recovery.conf)
            skip = {'passfile', 'password'}
        else:
            skip = {'dbname'}

        def escape(value):
            return re.sub(r'([\'\\ ])', r'\\\1', str(value))

        return ' '.join('{0}={1}'.format(kw, escape(params[kw])) for kw in keywords
                        if kw not in skip and params.get(kw) is not None)

    def _write_recovery_params(self, fd, recovery_params):
        for name, value in sorted(recovery_params.items()):
            if name == 'primary_conninfo':
                if 'password' in value and self._postgresql.major_version >= 100000:
                    self.write_pgpass(value)
                    value['passfile'] = self._passfile = self._pgpass
                    self._passfile_mtime = mtime(self._pgpass)
                value = self.format_dsn(value)
            fd.write_param(name, value)

    def build_recovery_params(self, member):
        recovery_params = CaseInsensitiveDict({p: v for p, v in self.get('recovery_conf', {}).items()
                                               if not p.lower().startswith('recovery_target') and
                                               p.lower() not in ('primary_conninfo', 'primary_slot_name')})
        recovery_params.update({'standby_mode': 'on', 'recovery_target_timeline': 'latest'})
        if self._postgresql.major_version >= 120000:
            # on pg12 we want to protect from following params being set in one of included files
            # not doing so might result in a standby being paused, promoted or shutted down.
            recovery_params.update({'recovery_target': '', 'recovery_target_name': '', 'recovery_target_time': '',
                                    'recovery_target_xid': '', 'recovery_target_lsn': ''})

        is_remote_master = isinstance(member, RemoteMember)
        primary_conninfo = self.primary_conninfo_params(member)
        if primary_conninfo:
            recovery_params['primary_conninfo'] = primary_conninfo
            if self.get('use_slots', True) and self._postgresql.major_version >= 90400 \
                    and not (is_remote_master and member.no_replication_slot):
                recovery_params['primary_slot_name'] = member.primary_slot_name if is_remote_master \
                        else slot_name_from_member_name(self._postgresql.name)

        if is_remote_master:  # standby_cluster config might have different parameters, we want to override them
            recovery_params.update({p: member.data.get(p) for p in ('restore_command', 'recovery_min_apply_delay',
                                                                    'archive_cleanup_command') if member.data.get(p)})
        return recovery_params

    def recovery_conf_exists(self):
        if self._postgresql.major_version >= 120000:
            return os.path.exists(self._standby_signal) or os.path.exists(self._recovery_signal)
        return os.path.exists(self._recovery_conf)

    @property
    def _triggerfile_good_name(self):
        return 'trigger_file' if self._postgresql.major_version < 120000 else 'promote_trigger_file'

    @property
    def _triggerfile_wrong_name(self):
        return 'trigger_file' if self._postgresql.major_version >= 120000 else 'promote_trigger_file'

    @property
    def _recovery_parameters_to_compare(self):
        skip_params = {'recovery_target_inclusive', 'recovery_target_action', self._triggerfile_wrong_name}
        return self._RECOVERY_PARAMETERS - skip_params

    def _read_recovery_params(self):
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
            values = {p[0]: [p[1], p[4] == 'postmaster'] for p in values}
            self._postgresql_conf_mtime = pg_conf_mtime
            self._auto_conf_mtime = auto_conf_mtime
            self._postmaster_ctime = postmaster_ctime
        except Exception:
            values = None
        return values, True

    def _read_recovery_params_pre_v12(self):
        recovery_conf_mtime = mtime(self._recovery_conf)
        passfile_mtime = mtime(self._passfile) if self._passfile else False
        if recovery_conf_mtime == self._recovery_conf_mtime and passfile_mtime == self._passfile_mtime:
            return None, False

        values = {}
        with open(self._recovery_conf, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                value = None
                match = PARAMETER_RE.match(line)
                if match:
                    value = read_recovery_param_value(line[match.end():])
                if value is None:
                    return None, True
                values[match.group(1)] = [value, True]
            self._recovery_conf_mtime = recovery_conf_mtime
        values.setdefault('recovery_min_apply_delay', ['0', True])
        values.update({param: ['', True] for param in self._recovery_parameters_to_compare if param not in values})
        return values, True

    def _check_passfile(self, passfile, wanted_primary_conninfo):
        # If there is a passfile in the primary_conninfo try to figure out that
        # the passfile contains the line allowing connection to the given node.
        # We assume that the passfile was created by Patroni and therefore doing
        # the full match and not covering cases when host, port or user are set to '*'
        passfile_mtime = mtime(passfile)
        if passfile_mtime:
            try:
                with open(passfile) as f:
                    wanted_line = self._pgpass_line(wanted_primary_conninfo).strip()
                    for raw_line in f:
                        if raw_line.strip() == wanted_line:
                            self._passfile = passfile
                            self._passfile_mtime = passfile_mtime
                            return True
            except Exception:
                logger.info('Failed to read %s', passfile)
        return False

    def _check_primary_conninfo(self, primary_conninfo, wanted_primary_conninfo):
        # first we will cover corner cases, when we are replicating from somewhere while shouldn't
        # or there is no primary_conninfo but we should replicate from some specific node.
        if not wanted_primary_conninfo:
            return not primary_conninfo
        elif not primary_conninfo:
            return False

        if 'passfile' in primary_conninfo and 'password' not in primary_conninfo \
                and 'password' in wanted_primary_conninfo:
            if self._check_passfile(primary_conninfo['passfile'], wanted_primary_conninfo):
                primary_conninfo['password'] = wanted_primary_conninfo['password']
            else:
                return False

        return all(primary_conninfo.get(p) == str(v) for p, v in wanted_primary_conninfo.items())

    def check_recovery_conf(self, member):
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

        required = {'restart': 0, 'reload': 0}

        def record_missmatch(mtype):
            required['restart' if mtype else 'reload'] += 1

        wanted_recovery_params = self.build_recovery_params(member)
        for param, value in self._current_recovery_params.items():
            if param == 'recovery_min_apply_delay':
                if not compare_values('integer', 'ms', value[0], wanted_recovery_params.get(param, 0)):
                    record_missmatch(value[1])
            elif param == 'primary_conninfo':
                if not self._check_primary_conninfo(value[0], wanted_recovery_params.get('primary_conninfo', {})):
                    record_missmatch(value[1])
            elif (param != 'primary_slot_name' or wanted_recovery_params.get('primary_conninfo')) \
                    and str(value[0]) != str(wanted_recovery_params.get(param, '')):
                record_missmatch(value[1])
        return required['restart'] + required['reload'] > 0, required['restart'] > 0

    @staticmethod
    def _remove_file_if_exists(name):
        if os.path.isfile(name) or os.path.islink(name):
            os.unlink(name)

    @staticmethod
    def _pgpass_line(record):
        if 'password' in record:
            def escape(value):
                return re.sub(r'([:\\])', r'\\\1', str(value))

            record = {n: escape(record.get(n, '*')) for n in ('host', 'port', 'user', 'password')}
            return '{host}:{port}:*:{user}:{password}'.format(**record)

    def write_pgpass(self, record):
        line = self._pgpass_line(record)
        if not line:
            return os.environ.copy()

        with open(self._pgpass, 'w') as f:
            os.chmod(self._pgpass, stat.S_IWRITE | stat.S_IREAD)
            f.write(line)

        env = os.environ.copy()
        env['PGPASSFILE'] = self._pgpass
        return env

    def write_recovery_conf(self, recovery_params):
        if self._postgresql.major_version >= 120000:
            if parse_bool(recovery_params.pop('standby_mode', None)):
                open(self._standby_signal, 'w').close()
            else:
                self._remove_file_if_exists(self._standby_signal)
                open(self._recovery_signal, 'w').close()
            self._recovery_params = recovery_params
        else:
            with ConfigWriter(self._recovery_conf) as f:
                os.chmod(self._recovery_conf, stat.S_IWRITE | stat.S_IREAD)
                self._write_recovery_params(f, recovery_params)

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
            value = self._config['recovery_conf'].pop(self._triggerfile_wrong_name, None)
            if self._triggerfile_good_name not in self._config['recovery_conf'] and value:
                self._config['recovery_conf'][self._triggerfile_good_name] = value

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
        # add all of the other connection settings that are available
        ret.update(self._superuser)
        # if the "username" parameter is present, it actually needs to be "user"
        # for connecting to PostgreSQL
        if 'username' in self._superuser:
            ret['user'] = self._superuser['username']
            del ret['username']
        # ensure certain Patroni configurations are available
        ret.update({'database': self._postgresql.database,
                    'fallback_application_name': 'Patroni',
                    'connect_timeout': 3,
                    'options': '-c statement_timeout=2000'})
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

    def _get_pg_settings(self, names):
        return {r[0]: r for r in self._postgresql.query(('SELECT name, setting, unit, vartype, context '
                                                         + ' FROM pg_catalog.pg_settings ' +
                                                         ' WHERE pg_catalog.lower(name) = ANY(%s)'),
                                                        [n.lower() for n in names])}

    @staticmethod
    def _handle_wal_buffers(old_values, changes):
        wal_block_size = parse_int(old_values['wal_block_size'][1])
        wal_segment_size = old_values['wal_segment_size']
        wal_segment_unit = parse_int(wal_segment_size[2], 'B') if wal_segment_size[2][0].isdigit() else 1
        wal_segment_size = parse_int(wal_segment_size[1]) * wal_segment_unit / wal_block_size
        default_wal_buffers = min(max(parse_int(old_values['shared_buffers'][1]) / 32, 8), wal_segment_size)

        wal_buffers = old_values['wal_buffers']
        new_value = str(changes['wal_buffers'] or -1)

        new_value = default_wal_buffers if new_value == '-1' else parse_int(new_value, wal_buffers[2])
        old_value = default_wal_buffers if wal_buffers[1] == '-1' else parse_int(*wal_buffers[1:3])

        if new_value == old_value:
            del changes['wal_buffers']

    def reload_config(self, config, sighup=False):
        self._superuser = config['authentication'].get('superuser', {})
        server_parameters = self.get_server_parameters(config)

        conf_changed = hba_changed = ident_changed = local_connection_address_changed = pending_restart = False
        if self._postgresql.state == 'running':
            changes = CaseInsensitiveDict({p: v for p, v in server_parameters.items()
                                           if '.' not in p and p.lower() not in self._RECOVERY_PARAMETERS})
            changes.update({p: None for p in self._server_parameters.keys()
                            if not ('.' in p or p in changes or p.lower() in self._RECOVERY_PARAMETERS)})
            if changes:
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
                                pending_restart = True
                                logger.info('Changed %s from %s to %s (restart might be required)',
                                            r[0], r[1], new_value)
                                if config.get('use_unix_socket') and r[0] == 'unix_socket_directories'\
                                        or r[0] in ('listen_addresses', 'port'):
                                    local_connection_address_changed = True
                            else:
                                logger.info('Changed %s from %s to %s', r[0], r[1], new_value)
                for param in changes:
                    if param in server_parameters:
                        logger.warning('Removing invalid parameter `%s` from postgresql.parameters', param)
                        server_parameters.pop(param)

            # Check that user-defined-paramters have changed (parameters with period in name)
            for p, v in server_parameters.items():
                if '.' in p and (p not in self._server_parameters or str(v) != str(self._server_parameters[p])):
                    logger.info('Changed %s from %s to %s', p, self._server_parameters.get(p), v)
                    conf_changed = True
            for p, v in self._server_parameters.items():
                if '.' in p and (p not in server_parameters or str(v) != str(server_parameters[p])):
                    logger.info('Changed %s from %s to %s', p, v, server_parameters.get(p))
                    conf_changed = True

            if not server_parameters.get('hba_file') and config.get('pg_hba'):
                hba_changed = self._config.get('pg_hba', []) != config['pg_hba']

            if not server_parameters.get('ident_file') and config.get('pg_ident'):
                ident_changed = self._config.get('pg_ident', []) != config['pg_ident']

        self._config = config
        self._postgresql.set_pending_restart(pending_restart)
        self._server_parameters = server_parameters
        self._adjust_recovery_parameters()
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

        if sighup or conf_changed or hba_changed or ident_changed:
            logger.info('Reloading PostgreSQL configuration.')
            self._postgresql.reload()
            if self._postgresql.major_version >= 90500:
                time.sleep(1)
                try:
                    pending_restart = self._postgresql.query('SELECT COUNT(*) FROM pg_catalog.pg_settings'
                                                             ' WHERE pending_restart').fetchone()[0] > 0
                    self._postgresql.set_pending_restart(pending_restart)
                except Exception as e:
                    logger.warning('Exception %r when running query', e)
        else:
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
