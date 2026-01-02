import logging
import operator
import re
import shlex
import subprocess
from typing import Dict, Any, List, Optional

from . import UpgradePreparePlugin, UpgradePluginManager
from .. import Postgresql

logger = logging.getLogger(__name__)

@UpgradePluginManager.register
class DropExtensions(UpgradePreparePlugin):
    name = 'drop_extensions'
    per_db = True

    def __init__(self, config: Dict[str, Any]):
        extensions = config.get('extensions', [])
        if not isinstance(extensions, list) or any(not isinstance(e, str) for e in extensions):
            raise TypeError('extensions must be a list of strings')

        self.extensions = extensions
        super().__init__(config)

    def prepare_database(self, postgresql: Postgresql, cur, dbname: str):
        logger.info('Dropping extensions from database "%s" which could be incompatible', dbname)
        for ext in self.extensions:
            logger.info('Executing "DROP EXTENSION IF EXISTS %s" in the database="%s"', ext, dbname)
            cur.execute("DROP EXTENSION IF EXISTS {0}".format(ext))

@UpgradePluginManager.register
class TruncateUnlogged(UpgradePreparePlugin):
    name = 'truncate_unlogged'

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def prepare_database(self, postgresql: Postgresql, cur, dbname: str):
        cur.execute("SELECT oid::regclass FROM pg_catalog.pg_class"
                    " WHERE relpersistence = 'u' AND relkind = 'r'")
        for unlogged in cur.fetchall():
            logger.info('Truncating unlogged table %s', unlogged[0])
            try:
                cur.execute('TRUNCATE {0}'.format(unlogged[0]))
            except Exception as e:
                logger.error('Failed: %r', e)

@UpgradePluginManager.register
class ShellCommandPlugin(UpgradePreparePlugin):
    name = 'cmd'

    def __init__(self, config: Dict[str, Any]):
        self.shell: Optional[str] = config.get('shell', None)
        self.rollback: Optional[str]  = config.get('rollback', None)
        # TODO: trivial dummy implementation, needs error checking, background execution
        # maybe output handling etc.
        super().__init__(config)

    def prepare_database(self, postgresql: Postgresql, cur, dbname: str):
        #TODO: make it possible to pass a connstring to each database to a shell script
        pass

    def post_upgrade(self):
        if not self.shell:
            logger.warning('Shell command not specified')
            return

        return self._run_shell_command(self.shell)

    def rollback(self):
        if self.rollback:
            self._run_shell_command(self.rollback)

    def _run_shell_command(self, cmd: str):
        if not self.shell:
            logger.warning('Shell command not specified')
            return

        cmd = shlex.split(self.shell) + [] #TODO: what args would a shell command need?
        subprocess.call(cmd, shell=True)


VERSION_SPEC_RE = re.compile(
    r"""
    \s*
    (?P<name>[a-zA-Z_0-9-]+)
    (?:
        (?P<comparison> <|<=|>|>=|==|!= )
        (?P<version> \d+ (?: \. \d+ )* )
    )?
    \s*$
    """, re.VERBOSE)

OPS = {
    '<': operator.lt, '>': operator.gt, '==': operator.eq,
    '<=': operator.le, '>=': operator.ge, '!=': operator.ne,
}

def parse_version(v):
    return tuple(map(int, v.split('.')))


@UpgradePluginManager.register
class UpdateExtensions(UpgradePreparePlugin):
    name = 'update_extensions'
    per_db = True

    def __init__(self, config: Dict[str, Any]):

        self.skip = []

        skip_list = config.get('skip', [])
        if not isinstance(skip_list, list):
            raise TypeError('skip must be a list of strings')
        for spec in skip_list:
            matches = VERSION_SPEC_RE.match(spec)
            if not matches:
                logger.warning("Invalid skip spec: %s", spec)
                continue
            groups = matches.groupdict()
            try:
                version = parse_version(groups['version'])
            except ValueError:
                logger.warning("Invalid version %s in skip spec", groups['version'])
                continue
            self.skip.append((matches.group('name'), OPS.get(groups['comparison']), version))

        super().__init__(config)

    def matches_skip(self, extname, extversion):
        for name, op, version in self.skip:
            if name != extname:
                continue
            if op is not None:
                try:
                    if not op(parse_version(extversion), version):
                        continue
                except ValueError as e:
                    logger.warning("Can't compare invalid version %s, %s", extname, extversion)
                    continue
            return True
        return False

    def prepare_database(self, postgresql: Postgresql, cur, dbname: str):
        return self._update_extensions(postgresql, cur, dbname)

    def post_process_database(self, postgresql: Postgresql, cur, dbname: str):
        return self._update_extensions(postgresql, cur, dbname)

    def _update_extensions(self, postgresql: Postgresql, cur, dbname: str):
        cur.execute('SELECT quote_ident(extname), extversion FROM pg_catalog.pg_extension')
        for extname, version in cur.fetchall():
            if self.matches_skip(extname, version):
                logger.warning("Skipping update of '%s' in database=%s. "
                               "Extension version: %s. Consider manual update",
                               extname, dbname, version)
                continue
            query = 'ALTER EXTENSION {0} UPDATE'.format(extname)
            logger.info("Executing '%s' in the database=%s", query, dbname)
            try:
                cur.execute(query)
            except Exception as e:
                logger.error('Failed: %r', e)
