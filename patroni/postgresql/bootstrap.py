import logging
import os
import shlex
import tempfile
import time

from patroni.dcs import RemoteMember
from patroni.utils import deep_compare
from six import string_types

logger = logging.getLogger(__name__)


class Bootstrap(object):

    def __init__(self, postgresql):
        self._postgresql = postgresql
        self._running_custom_bootstrap = False

    @property
    def running_custom_bootstrap(self):
        return self._running_custom_bootstrap

    @property
    def keep_existing_recovery_conf(self):
        return self._running_custom_bootstrap and self._keep_existing_recovery_conf

    @staticmethod
    def process_user_options(tool, options, not_allowed_options, error_handler):
        user_options = []

        def option_is_allowed(name):
            ret = name not in not_allowed_options
            if not ret:
                error_handler('{0} option for {1} is not allowed'.format(name, tool))
            return ret

        if isinstance(options, dict):
            for k, v in options.items():
                if k and v:
                    user_options.append('--{0}={1}'.format(k, v))
        elif isinstance(options, list):
            for opt in options:
                if isinstance(opt, string_types) and option_is_allowed(opt):
                    user_options.append('--{0}'.format(opt))
                elif isinstance(opt, dict):
                    keys = list(opt.keys())
                    if len(keys) != 1 or not isinstance(opt[keys[0]], string_types) or not option_is_allowed(keys[0]):
                        error_handler('Error when parsing {0} key-value option {1}: only one key-value is allowed'
                                      ' and value should be a string'.format(tool, opt[keys[0]]))
                    user_options.append('--{0}={1}'.format(keys[0], opt[keys[0]]))
                else:
                    error_handler('Error when parsing {0} option {1}: value should be string value'
                                  ' or a single key-value pair'.format(tool, opt))
        else:
            error_handler('{0} options must be list ot dict'.format(tool))
        return user_options

    def _initdb(self, config):
        self._postgresql.set_state('initalizing new cluster')
        not_allowed_options = ('pgdata', 'nosync', 'pwfile', 'sync-only', 'version')

        def error_handler(e):
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
        options = ['-o', ' '.join(options)] if options else []

        ret = self._postgresql.pg_ctl('initdb', *options)
        if pwfile:
            os.remove(pwfile)
        if ret:
            self._postgresql.configure_server_parameters()
        else:
            self._postgresql.set_state('initdb failed')
        return ret

    def _post_restore(self):
        self._postgresql.config.restore_configuration_files()
        self._postgresql.configure_server_parameters()

        # make sure there is no trigger file or postgres will be automatically promoted
        trigger_file = 'promote_trigger_file' if self._postgresql.major_version >= 120000 else 'trigger_file'
        trigger_file = self._postgresql.config.get('recovery_conf', {}).get(trigger_file) or 'promote'
        trigger_file = os.path.abspath(os.path.join(self._postgresql.data_dir, trigger_file))
        if os.path.exists(trigger_file):
            os.unlink(trigger_file)

    def _custom_bootstrap(self, config):
        self._postgresql.set_state('running custom bootstrap script')
        params = ['--scope=' + self._postgresql.scope, '--datadir=' + self._postgresql.data_dir]
        try:
            logger.info('Running custom bootstrap script: %s', config['command'])
            if self._postgresql.cancellable.call(shlex.split(config['command']) + params) != 0:
                self._postgresql.set_state('custom bootstrap failed')
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

    def call_post_bootstrap(self, config):
        """
        runs a script after initdb or custom bootstrap script is called and waits until completion.
        """
        cmd = config.get('post_bootstrap') or config.get('post_init')
        if cmd:
            r = self._postgresql.config.local_connect_kwargs
            connstring = self._postgresql.config.format_dsn(r, True)
            if 'host' not in r:
                # https://www.postgresql.org/docs/current/static/libpq-pgpass.html
                # A host name of localhost matches both TCP (host name localhost) and Unix domain socket
                # (pghost empty or the default socket directory) connections coming from the local machine.
                r['host'] = 'localhost'  # set it to localhost to write into pgpass

            env = self._postgresql.config.write_pgpass(r) if 'password' in r else None

            try:
                ret = self._postgresql.cancellable.call(shlex.split(cmd) + [connstring], env=env)
            except OSError:
                logger.error('post_init script %s failed', cmd)
                return False
            if ret != 0:
                logger.error('post_init script %s returned non-zero code %d', cmd, ret)
                return False
        return True

    def create_replica(self, clone_member):
        """
            create the replica according to the replica_method
            defined by the user.  this is a list, so we need to
            loop through all methods the user supplies
        """

        self._postgresql.set_state('creating replica')
        self._postgresql.schedule_sanity_checks_after_pause()

        is_remote_master = isinstance(clone_member, RemoteMember)

        # get list of replica methods either from clone member or from
        # the config. If there is no configuration key, or no value is
        # specified, use basebackup
        replica_methods = (clone_member.create_replica_methods if is_remote_master
                           else self._postgresql.create_replica_methods) or ['basebackup']

        if clone_member and clone_member.conn_url:
            r = clone_member.conn_kwargs(self._postgresql.config.replication)
            # add the credentials to connect to the replica origin to pgpass.
            env = self._postgresql.config.write_pgpass(r)
            connstring = self._postgresql.config.format_dsn(r, True)
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
                    for param in ('no_params', 'no_master', 'keep_data'):
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

        self._postgresql.set_state('stopped')
        return ret

    def basebackup(self, conn_url, env, options):
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

        for bbfailures in range(0, maxfailures):
            if self._postgresql.cancellable.is_cancelled:
                break
            if not self._postgresql.data_directory_empty():
                self._postgresql.remove_data_directory()
            try:
                ret = self._postgresql.cancellable.call([self._postgresql.pgcommand('pg_basebackup'),
                                                         '--pgdata=' + self._postgresql.data_dir, '-X', 'stream',
                                                         '--dbname=' + conn_url] + user_options, env=env)
                if ret == 0:
                    break
                else:
                    logger.error('Error when fetching backup: pg_basebackup exited with code=%s', ret)

            except Exception as e:
                logger.error('Error when fetching backup with pg_basebackup: %s', e)

            if bbfailures < maxfailures - 1:
                logger.warning('Trying again in 5 seconds')
                time.sleep(5)

        return ret

    def clone(self, clone_member):
        """
             - initialize the replica from an existing member (master or replica)
             - initialize the replica using the replica creation method that
               works without the replication connection (i.e. restore from on-disk
               base backup)
        """

        ret = self.create_replica(clone_member) == 0
        if ret:
            self._post_restore()
        return ret

    def bootstrap(self, config):
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
            and self._postgresql.config.save_configuration_files() and self._postgresql.start()

    def create_or_update_role(self, name, password, options):
        options = list(map(str.upper, options))
        if 'NOLOGIN' not in options and 'LOGIN' not in options:
            options.append('LOGIN')

        params = [name]
        if password:
            options.extend(['PASSWORD', '%s'])
            params.extend([password, password])

        sql = """DO $$
BEGIN
    SET local synchronous_commit = 'local';
    PERFORM * FROM pg_authid WHERE rolname = %s;
    IF FOUND THEN
        ALTER ROLE "{0}" WITH {1};
    ELSE
        CREATE ROLE "{0}" WITH {1};
    END IF;
END;$$""".format(name, ' '.join(options))
        self._postgresql.query(sql, *params)

    def post_bootstrap(self, config, task):
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
    GRANT EXECUTE ON function pg_catalog.{0} TO "{1}";
END;$$""".format(f, rewind['username'])
                        postgresql.query(sql)

                for name, value in (config.get('users') or {}).items():
                    if all(name != a.get('username') for a in (superuser, replication, rewind)):
                        self.create_or_update_role(name, value.get('password'), value.get('options', []))

                # We were doing a custom bootstrap instead of running initdb, therefore we opened trust
                # access from certain addresses to be able to reach cluster and change password
                if self._running_custom_bootstrap:
                    self._running_custom_bootstrap = False
                    # If we don't have custom configuration for pg_hba.conf we need to restore original file
                    if not postgresql.config.get('pg_hba'):
                        os.unlink(postgresql.config.pg_hba_conf)
                        postgresql.config.restore_configuration_files()
                    postgresql.config.write_postgresql_conf()
                    postgresql.config.replace_pg_ident()

                    # at this point there should be no recovery.conf
                    postgresql.config.remove_recovery_conf()

                    if postgresql.config.hba_file and postgresql.config.hba_file != postgresql.config.pg_hba_conf:
                        postgresql.restart()
                    else:
                        postgresql.config.replace_pg_hba()
                        if postgresql.pending_restart:
                            postgresql.restart()
                        else:
                            postgresql.reload()
                            time.sleep(1)  # give a time to postgres to "reload" configuration files
                            postgresql.connection().close()  # close connection to reconnect with a new password
        except Exception:
            logger.exception('post_bootstrap')
            task.complete(False)
        return task.result
