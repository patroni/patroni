import logging
import os
import shlex
import tempfile
import time

from typing import Any, Callable, cast, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from ..async_executor import CriticalTask
from ..collections import EMPTY_DICT
from ..dcs import Leader, Member, RemoteMember
from ..psycopg import quote_ident, quote_literal
from ..utils import deep_compare, unquote
from .misc import PostgresqlState

if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql

logger = logging.getLogger(__name__)


class Bootstrap(object):

    def __init__(self, postgresql: 'Postgresql') -> None:
        self._postgresql = postgresql
        self._running_custom_bootstrap = False

    @property
    def running_custom_bootstrap(self) -> bool:
        return self._running_custom_bootstrap

    @property
    def keep_existing_recovery_conf(self) -> bool:
        return self._running_custom_bootstrap and self._keep_existing_recovery_conf

    @staticmethod
    def process_user_options(tool: str, options: Any,
                             not_allowed_options: Tuple[str, ...],
                             error_handler: Callable[[str], None]) -> List[str]:
        """Format *options* in a list or dictionary format into command line long form arguments.

        .. note::
            The format of the output of this method is to prepare arguments for use in the ``initdb``
            method of `self._postgres`.

        :Example:

            The *options* can be defined as a dictionary of key, values to be converted into arguments:
            >>> Bootstrap.process_user_options('foo', {'foo': 'bar'}, (), print)
            ['--foo=bar']

            Or as a list of single string arguments
            >>> Bootstrap.process_user_options('foo', ['yes'], (), print)
            ['--yes']

            Or as a list of key, value options
            >>> Bootstrap.process_user_options('foo', [{'foo': 'bar'}], (), print)
            ['--foo=bar']

            Or a combination of single and key, values
            >>> Bootstrap.process_user_options('foo', ['yes', {'foo': 'bar'}], (), print)
            ['--yes', '--foo=bar']

            Options that contain spaces are passed as is to ``subprocess.call``
            >>> Bootstrap.process_user_options('foo', [{'foo': 'bar baz'}], (), print)
            ['--foo=bar baz']

            Options that are quoted will be unquoted, so the quotes aren't interpreted
            literally by the postgres command
            >>> Bootstrap.process_user_options('foo', [{'foo': '"bar baz"'}], (), print)
            ['--foo=bar baz']

        .. note::
            The *error_handler* is called when any of these conditions are met:

            * Key, value dictionaries in the list form contains multiple keys.
            * If a key is listed in *not_allowed_options*.
            * If the options list is not in the required structure.

        :param tool: The name of the tool used in error reports to *error_handler*
        :param options: Options to parse as a list of key, values or single values, or a dictionary
        :param not_allowed_options: List of keys that cannot be used in the list of key, value formatted options
        :param error_handler: A function which will be called when an error condition is encountered
        :returns: List of long form arguments to pass to the named tool
        """
        user_options: List[str] = []

        def option_is_allowed(name: str) -> bool:
            ret = name not in not_allowed_options
            if not ret:
                error_handler('{0} option for {1} is not allowed'.format(name, tool))
            return ret

        if isinstance(options, dict):
            for key, val in cast(Dict[str, str], options).items():
                if key and val:
                    user_options.append('--{0}={1}'.format(key, unquote(val)))
        elif isinstance(options, list):
            for opt in cast(List[Any], options):
                if isinstance(opt, str) and option_is_allowed(opt):
                    user_options.append('--{0}'.format(opt))
                elif isinstance(opt, dict):
                    args = cast(Dict[str, Any], opt)
                    keys = list(args.keys())
                    if len(keys) == 1 and isinstance(args[keys[0]], str) and option_is_allowed(keys[0]):
                        user_options.append('--{0}={1}'.format(keys[0], unquote(args[keys[0]])))
                    else:
                        error_handler('Error when parsing {0} key-value option {1}: only one key-value is allowed'
                                      ' and value should be a string'.format(tool, args[keys[0]]))
                else:
                    error_handler('Error when parsing {0} option {1}: value should be string value'
                                  ' or a single key-value pair'.format(tool, opt))
        else:
            error_handler('{0} options must be list or dict'.format(tool))
        return user_options

    def _initdb(self, config: Any) -> bool:
        self._postgresql.set_state(PostgresqlState.INITDB)
        not_allowed_options = ('pgdata', 'nosync', 'pwfile', 'sync-only', 'version')

        def error_handler(e: str) -> None:
            raise Exception(e)

        options = self.process_user_options('initdb', config or [], not_allowed_options, error_handler)
        pwfile = None

        if self._postgresql.config.superuser:
            if 'username' in self._postgresql.config.superuser:
                options.append('--username={0}'.format(self._postgresql.config.superuser['username']))
            if 'password' in self._postgresql.config.superuser:
                (fd, pwfile) = tempfile.mkstemp()
                os.write(fd, self._postgresql.config.superuser['password'].encode('utf-8'))
                os.close(fd)
                options.append('--pwfile={0}'.format(pwfile))

        ret = self._postgresql.initdb(*options)
        if pwfile:
            os.remove(pwfile)
        if ret:
            self._postgresql.configure_server_parameters()
        else:
            self._postgresql.set_state(PostgresqlState.INITDB_FAILED)
        return ret

    def _post_restore(self) -> None:
        self._postgresql.config.restore_configuration_files()
        self._postgresql.configure_server_parameters()

        # make sure there is no trigger file or postgres will be automatically promoted
        trigger_file = self._postgresql.config.triggerfile_good_name
        trigger_file = (self._postgresql.config.get('recovery_conf') or EMPTY_DICT).get(trigger_file) or 'promote'
        trigger_file = os.path.abspath(os.path.join(self._postgresql.data_dir, trigger_file))
        if os.path.exists(trigger_file):
            os.unlink(trigger_file)

    def _custom_bootstrap(self, config: Any) -> bool:
        """Bootstrap a fresh Patroni cluster using a custom method provided by the user.

        :param config: configuration used for running a custom bootstrap method. It comes from the Patroni YAML file,
            so it is expected to be a :class:`dict`.

        .. note::
            *config* must contain a ``command`` key, which value is the command or script to perform the custom
            bootstrap procedure. The exit code of the ``command`` dictates if the bootstrap succeeded or failed.

            When calling ``command``, Patroni will pass the following arguments to the ``command`` call:

                * ``--scope``: contains the value of ``scope`` configuration;
                * ``--data_dir``: contains the value of the ``postgresql.data_dir`` configuration.

            You can avoid that behavior by filling the optional key ``no_params`` with the value ``False`` in the
            configuration file, which will instruct Patroni to not pass these parameters to the ``command`` call.

            Besides that, a couple more keys are supported in *config*, but optional:

                * ``keep_existing_recovery_conf``: if ``True``, instruct Patroni to not remove the existing
                  ``recovery.conf`` (PostgreSQL <= 11), to not discard recovery parameters from the configuration
                  (PostgreSQL >= 12), and to not remove the files ``recovery.signal`` or ``standby.signal``
                  (PostgreSQL >= 12). This is specially useful when you are restoring backups through tools like
                  pgBackRest and Barman, in which case they generated the appropriate recovery settings for you;
                * ``recovery_conf``: a section containing a map, where each key is the name of a recovery related
                  setting, and the value is the value of the corresponding setting.

            Any key/value other than the ones that were described above will be interpreted as additional arguments for
            the ``command`` call. They will all be added to the call in the format ``--key=value``.

        :returns: ``True`` if the bootstrap was successful, i.e. the execution of the custom ``command`` from *config*
            exited with code ``0``, ``False`` otherwise.
        """
        self._postgresql.set_state(PostgresqlState.CUSTOM_BOOTSTRAP)
        params = [] if config.get('no_params') else ['--scope=' + self._postgresql.scope,
                                                     '--datadir=' + self._postgresql.data_dir]
        # Add custom parameters specified by the user
        reserved_args = {'command', 'no_params', 'keep_existing_recovery_conf', 'recovery_conf', 'scope', 'datadir'}
        params += [f"--{arg}={val}" for arg, val in config.items() if arg not in reserved_args]

        try:
            logger.info('Running custom bootstrap script: %s', config['command'])
            if self._postgresql.cancellable.call(shlex.split(config['command']) + params) != 0:
                self._postgresql.set_state(PostgresqlState.CUSTOM_BOOTSTRAP_FAILED)
                return False
        except Exception:
            logger.exception('Exception during custom bootstrap')
            return False
        self._post_restore()

        if 'recovery_conf' in config:
            self._postgresql.config.write_recovery_conf(config['recovery_conf'])
        elif not self.keep_existing_recovery_conf:
            self._postgresql.config.remove_recovery_conf()
        return True

    def call_post_bootstrap(self, config: Dict[str, Any]) -> bool:
        """
        runs a script after initdb or custom bootstrap script is called and waits until completion.
        """
        cmd = config.get('post_bootstrap') or config.get('post_init')
        if cmd:
            r = self._postgresql.connection_pool.conn_kwargs

            # https://www.postgresql.org/docs/current/static/libpq-pgpass.html
            # A host name of localhost matches both TCP (host name localhost) and Unix domain socket
            # (pghost empty or the default socket directory) connections coming from the local machine.
            env = self._postgresql.config.write_pgpass({'host': 'localhost', **r})
            env['PGOPTIONS'] = '-c synchronous_commit=local -c statement_timeout=0'
            connstring = self._postgresql.config.format_dsn({**r, 'password': None})

            try:
                ret = self._postgresql.cancellable.call(shlex.split(cmd) + [connstring], env=env)
            except OSError:
                logger.error('post_init script %s failed', cmd)
                return False
            if ret != 0:
                logger.error('post_init script %s returned non-zero code %d', cmd, ret)
                return False
        return True

    def create_replica(self, clone_member: Union[Leader, Member, None],
                       clone_from_leader: bool = False) -> Optional[int]:
        """
            create the replica according to the replica_method
            defined by the user.  this is a list, so we need to
            loop through all methods the user supplies
        """

        self._postgresql.set_state(PostgresqlState.CREATING_REPLICA)
        self._postgresql.schedule_sanity_checks_after_pause()

        is_remote_member = isinstance(clone_member, RemoteMember)

        # get list of replica methods either from clone member or from
        # the config. If there is no configuration key, or no value is
        # specified, use basebackup
        replica_methods = (clone_member.create_replica_methods if is_remote_member
                           else self._postgresql.create_replica_methods) or ['basebackup']

        # If '--from-leader' parameter is set when reinit, always use basebackup
        if clone_from_leader:
            replica_methods = ['basebackup']

        if clone_member and clone_member.conn_url:
            r = clone_member.conn_kwargs(self._postgresql.config.replication)
            # add the credentials to connect to the replica origin to pgpass.
            env = self._postgresql.config.write_pgpass(r)
            connstring = self._postgresql.config.format_dsn({**r, 'password': None})
        else:
            connstring = ''
            env = os.environ.copy()
            # if we don't have any source, leave only replica methods that work without it
            replica_methods = [r for r in replica_methods
                               if self._postgresql.replica_method_can_work_without_replication_connection(r)]

        # go through them in priority order
        ret = 1
        for replica_method in replica_methods:
            if self._postgresql.cancellable.is_cancelled:
                break

            method_config = self._postgresql.replica_method_options(replica_method)

            # if the method is basebackup, then use the built-in
            if replica_method == "basebackup":
                ret = self.basebackup(connstring, env, method_config)
                if ret == 0:
                    logger.info("replica has been created using basebackup")
                    # if basebackup succeeds, exit with success
                    break
            else:
                if not self._postgresql.data_directory_empty():
                    if method_config.get('keep_data', False):
                        logger.info('Leaving data directory uncleaned')
                    else:
                        self._postgresql.remove_data_directory()

                cmd = replica_method
                # user-defined method; check for configuration
                # not required, actually
                if method_config:
                    # look to see if the user has supplied a full command path
                    # if not, use the method name as the command
                    cmd = method_config.pop('command', cmd)

                # add the default parameters
                if not method_config.get('no_params', False):
                    method_config.update({"scope": self._postgresql.scope,
                                          "role": "replica",
                                          "datadir": self._postgresql.data_dir,
                                          "connstring": connstring})
                else:
                    for param in ('no_params', 'no_leader', 'keep_data'):
                        method_config.pop(param, None)
                params = ["--{0}={1}".format(arg, val) for arg, val in method_config.items()]
                try:
                    # call script with the full set of parameters
                    ret = self._postgresql.cancellable.call(shlex.split(cmd) + params, env=env)
                    # if we succeeded, stop
                    if ret == 0:
                        logger.info('replica has been created using %s', replica_method)
                        break
                    else:
                        logger.error('Error creating replica using method %s: %s exited with code=%s',
                                     replica_method, cmd, ret)
                except Exception:
                    logger.exception('Error creating replica using method %s', replica_method)
                    ret = 1

                # replica creation method failed, clean up data directory if configuration allows
                if not method_config.get('keep_data', False) and not self._postgresql.data_directory_empty():
                    self._postgresql.remove_data_directory()

        self._postgresql.set_state(PostgresqlState.STOPPED)
        return ret

    def basebackup(self, conn_url: str, env: Dict[str, str], options: Dict[str, Any]) -> Optional[int]:
        # creates a replica data dir using pg_basebackup.
        # this is the default, built-in create_replica_methods
        # tries twice, then returns failure (as 1)
        # uses "stream" as the xlog-method to avoid sync issues
        # supports additional user-supplied options, those are not validated
        maxfailures = 2
        ret = 1
        not_allowed_options = ('pgdata', 'format', 'wal-method', 'xlog-method', 'gzip',
                               'version', 'compress', 'dbname', 'host', 'port', 'username', 'password')
        user_options = self.process_user_options('basebackup', options, not_allowed_options, logger.error)
        cmd = [
            self._postgresql.pgcommand("pg_basebackup"),
            "--pgdata=" + self._postgresql.data_dir,
            "-X",
            "stream",
            "--dbname=" + conn_url,
        ] + user_options

        for bbfailures in range(0, maxfailures):
            if self._postgresql.cancellable.is_cancelled:
                break
            if not self._postgresql.data_directory_empty():
                self._postgresql.remove_data_directory()
            try:
                logger.debug('calling: %r', cmd)
                ret = self._postgresql.cancellable.call(cmd, env=env)
                if ret == 0:
                    break
                else:
                    logger.error('Error when fetching backup: pg_basebackup exited with code=%s', ret)

            except Exception as e:
                logger.error('Error when fetching backup with pg_basebackup: %s', e)

            if bbfailures < maxfailures - 1:
                logger.warning('Trying again in 5 seconds')
                time.sleep(5)
            elif not self._postgresql.data_directory_empty():
                # pg_basebackup failed, clean up data directory
                self._postgresql.remove_data_directory()

        return ret

    def clone(self, clone_member: Union[Leader, Member, None], clone_from_leader: bool = False) -> bool:
        """
             - initialize the replica from an existing member (primary or replica)
             - initialize the replica using the replica creation method that
               works without the replication connection (i.e. restore from on-disk
               base backup)
        """

        ret = self.create_replica(clone_member, clone_from_leader) == 0
        if ret:
            self._post_restore()
        return ret

    def bootstrap(self, config: Dict[str, Any]) -> bool:
        """ Initialize a new node from scratch and start it. """
        pg_hba = config.get('pg_hba', [])
        method = config.get('method') or 'initdb'
        if method != 'initdb' and method in config and 'command' in config[method]:
            self._keep_existing_recovery_conf = config[method].get('keep_existing_recovery_conf')
            self._running_custom_bootstrap = True
            do_initialize = self._custom_bootstrap
        else:
            method = 'initdb'
            do_initialize = self._initdb
        return do_initialize(config.get(method)) and self._postgresql.config.append_pg_hba(pg_hba) \
            and self._postgresql.config.save_configuration_files() and bool(self._postgresql.start())

    def create_or_update_role(self, name: str, password: Optional[str], options: List[str]) -> None:
        options = list(map(str.upper, options))
        if 'NOLOGIN' not in options and 'LOGIN' not in options:
            options.append('LOGIN')

        if password:
            options.extend(['PASSWORD', quote_literal(password)])

        sql = """DO $$
BEGIN
    SET local synchronous_commit = 'local';
    PERFORM * FROM pg_catalog.pg_authid WHERE rolname = {0};
    IF FOUND THEN
        ALTER ROLE {1} WITH {2};
    ELSE
        CREATE ROLE {1} WITH {2};
    END IF;
END;$$""".format(quote_literal(name), quote_ident(name, self._postgresql.connection()), ' '.join(options))
        self._postgresql.query('SET log_statement TO none')
        self._postgresql.query('SET log_min_duration_statement TO -1')
        self._postgresql.query("SET log_min_error_statement TO 'log'")
        self._postgresql.query("SET pg_stat_statements.track_utility to 'off'")
        self._postgresql.query("SET pgaudit.log TO none")
        try:
            self._postgresql.query(sql)
        finally:
            self._postgresql.query('RESET log_min_error_statement')
            self._postgresql.query('RESET log_min_duration_statement')
            self._postgresql.query('RESET log_statement')
            self._postgresql.query('RESET pg_stat_statements.track_utility')
            self._postgresql.query('RESET pgaudit.log')

    def post_bootstrap(self, config: Dict[str, Any], task: CriticalTask) -> Optional[bool]:
        try:
            postgresql = self._postgresql
            superuser = postgresql.config.superuser
            if 'username' in superuser and 'password' in superuser:
                self.create_or_update_role(superuser['username'], superuser['password'], ['SUPERUSER'])

            task.complete(self.call_post_bootstrap(config))
            if task.result:
                replication = postgresql.config.replication
                self.create_or_update_role(replication['username'], replication.get('password'), ['REPLICATION'])

                rewind = postgresql.config.rewind_credentials
                if not deep_compare(rewind, superuser):
                    self.create_or_update_role(rewind['username'], rewind.get('password'), [])
                    for f in ('pg_ls_dir(text, boolean, boolean)', 'pg_stat_file(text, boolean)',
                              'pg_read_binary_file(text)', 'pg_read_binary_file(text, bigint, bigint, boolean)'):
                        sql = """DO $$
BEGIN
    SET local synchronous_commit = 'local';
    GRANT EXECUTE ON function pg_catalog.{0} TO {1};
END;$$""".format(f, quote_ident(rewind['username'], postgresql.connection()))
                        postgresql.query(sql)

                if config.get('users'):
                    logger.error('User creation is not be supported starting from v4.0.0. '
                                 'Please use "bootstrap.post_bootstrap" script to create users.')

                # We were doing a custom bootstrap instead of running initdb, therefore we opened trust
                # access from certain addresses to be able to reach cluster and change password
                if self._running_custom_bootstrap:
                    self._running_custom_bootstrap = False
                    # If we don't have custom configuration for pg_hba.conf we need to restore original file
                    if not postgresql.config.get('pg_hba'):
                        if os.path.exists(postgresql.config.pg_hba_conf):
                            os.unlink(postgresql.config.pg_hba_conf)
                        postgresql.config.restore_configuration_files()
                    postgresql.config.write_postgresql_conf()
                    postgresql.config.replace_pg_ident()

                    # at this point there should be no recovery.conf
                    postgresql.config.remove_recovery_conf()

                    if postgresql.config.hba_file:
                        postgresql.restart()
                    else:
                        postgresql.config.replace_pg_hba()
                        if postgresql.pending_restart_reason:
                            postgresql.restart()
                        else:
                            postgresql.reload()
                            time.sleep(1)  # give a time to postgres to "reload" configuration files
                            postgresql.connection().close()  # close connection to reconnect with a new password
                else:  # initdb
                    # We may want create database and extension for some MPP clusters
                    self._postgresql.mpp_handler.bootstrap()
        except Exception:
            logger.exception('post_bootstrap')
            task.complete(False)
        return task.result
