"""Implement ``patronictl``: a command-line application which utilises the REST API to perform cluster operations.

:var CONFIG_DIR_PATH: path to Patroni configuration directory as per :func:`click.get_app_dir` output.
:var CONFIG_FILE_PATH: default path to ``patronictl.yaml`` configuration file.
:var DCS_DEFAULTS: auxiliary dictionary to build the DCS section of the configuration file. Mainly used to help parsing
    ``--dcs-url`` command-line option of ``patronictl``.

.. note::
    Most of the ``patronictl`` commands (``restart``/``reinit``/``pause``/``resume``/``show-config``/``edit-config`` and
    similar) require the ``group`` argument and work only for that specific Citus ``group``.
    If not specified in the command line the ``group`` might be taken from the configuration file.
    If it is also missing in the configuration file we assume that this is just a normal Patroni cluster (not Citus).
"""

import codecs
import copy
import datetime
import difflib
import io
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

from collections import defaultdict
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import urlparse

import click
import dateutil.parser
import dateutil.tz
import urllib3
import yaml

from prettytable import PrettyTable

try:  # pragma: no cover
    from prettytable import HRuleStyle
    hrule_all = HRuleStyle.ALL
    hrule_frame = HRuleStyle.FRAME
except ImportError:  # pragma: no cover
    from prettytable import ALL as hrule_all, FRAME as hrule_frame

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor

try:  # pragma: no cover
    from ydiff import markup_to_pager  # pyright: ignore [reportMissingModuleSource]
    try:
        from ydiff import PatchStream  # pyright: ignore [reportMissingModuleSource]
    except ImportError:
        PatchStream = iter
except ImportError:  # pragma: no cover
    from cdiff import markup_to_pager, PatchStream  # pyright: ignore [reportMissingModuleSource]

from . import global_config
from .config import Config
from .dcs import AbstractDCS, Cluster, get_dcs as _get_dcs, Leader, Member
from .exceptions import PatroniException
from .postgresql.misc import postgres_version_to_int, PostgresqlRole, PostgresqlState
from .postgresql.mpp import get_mpp
from .request import PatroniRequest
from .utils import cluster_as_json, patch_config, polling_loop
from .version import __version__

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronictl.yaml')
DCS_DEFAULTS: Dict[str, Dict[str, Any]] = {
    'zookeeper': {'port': 2181, 'template': "zookeeper:\n hosts: ['{host}:{port}']"},
    'exhibitor': {'port': 8181, 'template': "exhibitor:\n hosts: [{host}]\n port: {port}"},
    'consul': {'port': 8500, 'template': "consul:\n host: '{host}:{port}'"},
    'etcd': {'port': 2379, 'template': "etcd:\n host: '{host}:{port}'"},
    'etcd3': {'port': 2379, 'template': "etcd3:\n host: '{host}:{port}'"}}


class CtlPostgresqlRole(str, Enum):

    LEADER = 'leader'
    PRIMARY = 'primary'
    STANDBY_LEADER = 'standby-leader'
    REPLICA = 'replica'
    STANDBY = 'standby'
    ANY = 'any'

    def __repr__(self) -> str:
        """Get a string representation of a :class:`CtlPostgresqlRole` member."""
        return self.value


class PatroniCtlException(click.ClickException):
    """Raised upon issues faced by ``patronictl`` utility."""

    pass


class PatronictlPrettyTable(PrettyTable):
    """Utilitary class to print pretty tables.

    Extend :class:`~prettytable.PrettyTable` to make it print custom information in the header line. The idea is to
    print a header line like this:

    ```
    + Cluster: batman --------+--------+---------+----+-----------+
    ```

    Instead of the default header line which would contain only dash and plus characters.
    """

    def __init__(self, header: str, *args: Any, **kwargs: Any) -> None:
        """Create a :class:`PatronictlPrettyTable` instance with the given *header*.

        :param header: custom string to be put in the first header line of the table.
        :param args: positional arguments to be passed to :class:`~prettytable.PrettyTable` constructor.
        :param kwargs: keyword arguments to be passed to :class:`~prettytable.PrettyTable` constructor.
        """
        super(PatronictlPrettyTable, self).__init__(*args, **kwargs)
        self.__table_header = header
        self.__hline_num = 0
        self.__hline: str

    def __build_header(self, line: str) -> str:
        """Build the custom header line for the table.

        .. note::
            Expected to be called only against the very first header line of the table.

        :param line: the original header line.

        :returns: the modified header line.
        """
        header = self.__table_header[:len(line) - 2]
        return "".join([line[0], header, line[1 + len(header):]])

    def _stringify_hrule(self, *args: Any, **kwargs: Any) -> str:
        """Get the string representation of a header line.

        Inject the custom header line, if processing the first header line.

        .. note::
            New implementation for injecting a custom header line, which is used from :mod:`prettytable` 2.2.0 onwards.

        :returns: string representation of a header line.
        """
        ret = super(PatronictlPrettyTable, self)._stringify_hrule(*args, **kwargs)
        where = args[1] if len(args) > 1 else kwargs.get('where')
        if where == 'top_' and self.__table_header:
            ret = self.__build_header(ret)
            self.__hline_num += 1
        return ret

    def _is_first_hline(self) -> bool:
        """Check if the current line being processed is the very first line of the header.

        :returns: ``True`` if processing the first header line, ``False`` otherwise.
        """
        return self.__hline_num == 0

    def _set_hline(self, value: str) -> None:
        """Set header line string representation.

        :param value: string representing a header line.
        """
        self.__hline = value

    def _get_hline(self) -> str:
        """Get string representation of a header line.

        Inject the custom header line, if processing the first header line.

        .. note::
            Original implementation for injecting a custom header line, and is used up to :mod:`prettytable` 2.2.0. From
            :mod:`prettytable` 2.2.0 onwards :func:`_stringify_hrule` is used instead.

        :returns: string representing a header line.
        """
        ret = self.__hline

        # Inject nice table header
        if self._is_first_hline() and self.__table_header:
            ret = self.__build_header(ret)

        self.__hline_num += 1
        return ret

    def _validate_field_names(self, *args: Any, **kwargs: Any) -> None:
        """Validate field names.

        Remove uniqueness constraint for field names from the original implementation.
        Required for having shorter ``Lag`` columns without specifying lag's type after ``LSN`` column already did so.
        """
        try:
            super(PatronictlPrettyTable, self)._validate_field_names(*args, **kwargs)
        except Exception as e:
            if 'Field names must be unique' not in str(e):
                raise e

    _hrule = property(_get_hline, _set_hline)


def parse_dcs(dcs: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse a DCS URL.

    :param dcs: the DCS URL in the format ``DCS://HOST:PORT/NAMESPACE``. ``DCS`` can be one among:

        * ``consul``
        * ``etcd``
        * ``etcd3``
        * ``exhibitor``
        * ``zookeeper``

        If ``DCS`` is not specified, assume ``etcd`` by default. If ``HOST`` is not specified, assume ``localhost`` by
        default. If ``PORT`` is not specified, assume the default port of the given ``DCS``. If ``NAMESPACE`` is not
        specified, use whatever is in config.

    :returns: ``None`` if *dcs* is ``None``, otherwise a dictionary. The dictionary represents *dcs* as if it were
        parsed from the Patroni configuration file. Additionally, if a namespace is specified in *dcs*, return a
        ``namespace`` key with the parsed value.

    :raises:
        :class:`PatroniCtlException`: if the DCS name in *dcs* is not valid.

    :Example:

        >>> parse_dcs('')
        {'etcd': {'host': 'localhost:2379'}}

        >>> parse_dcs('etcd://:2399')
        {'etcd': {'host': 'localhost:2399'}}

        >>> parse_dcs('etcd://test')
        {'etcd': {'host': 'test:2379'}}

        >>> parse_dcs('etcd3://random.com:2399')
        {'etcd3': {'host': 'random.com:2399'}}

        >>> parse_dcs('etcd3://random.com:2399/customnamespace')
        {'etcd3': {'host': 'random.com:2399'}, 'namespace': '/customnamespace'}
    """
    if dcs is None:
        return None
    elif '//' not in dcs:
        dcs = '//' + dcs

    parsed = urlparse(dcs)
    scheme = parsed.scheme
    port = int(parsed.port) if parsed.port else None

    if scheme == '':
        scheme = ([k for k, v in DCS_DEFAULTS.items() if v['port'] == port] or ['etcd'])[0]
    elif scheme not in DCS_DEFAULTS:
        raise PatroniCtlException('Unknown dcs scheme: {}'.format(scheme))

    default = DCS_DEFAULTS[scheme]
    ret = yaml.safe_load(default['template'].format(host=parsed.hostname or 'localhost', port=port or default['port']))

    if parsed.path and parsed.path.strip() != '/':
        ret['namespace'] = parsed.path.strip()

    return ret


def load_config(path: str, dcs_url: Optional[str]) -> Dict[str, Any]:
    """Load configuration file from *path* and optionally override its DCS configuration with *dcs_url*.

    :param path: path to the configuration file.
    :param dcs_url: the DCS URL in the format ``DCS://HOST:PORT/NAMESPACE``, e.g. ``etcd3://random.com:2399/service``.
        If given, override whatever DCS and ``namespace`` that are set in the configuration file. See :func:`parse_dcs`
        for more information.

    :returns: a dictionary representing the configuration.

    :raises:
        :class:`PatroniCtlException`: if *path* does not exist or is not readable.
    """
    if not (os.path.exists(path) and os.access(path, os.R_OK)):
        if path != CONFIG_FILE_PATH:    # bail if non-default config location specified but file not found / readable
            raise PatroniCtlException('Provided config file {0} not existing or no read rights.'
                                      ' Check the -c/--config-file parameter'.format(path))
        else:
            logging.debug('Ignoring configuration file "%s". It does not exists or is not readable.', path)
    else:
        logging.debug('Loading configuration from file %s', path)
    config = Config(path, validator=None).copy()

    dcs_kwargs = parse_dcs(dcs_url) or {}
    if dcs_kwargs:
        for d in DCS_DEFAULTS:
            config.pop(d, None)
        config.update(dcs_kwargs)
    return config


def _get_configuration() -> Dict[str, Any]:
    """Get configuration object.

    :returns: configuration object from the current context.
    """
    return click.get_current_context().obj['__config']


option_format = click.option('--format', '-f', 'fmt', help='Output format', default='pretty',
                             type=click.Choice(['pretty', 'tsv', 'json', 'yaml', 'yml']))
option_watchrefresh = click.option('-w', '--watch', type=float, help='Auto update the screen every X seconds')
option_watch = click.option('-W', is_flag=True, help='Auto update the screen every 2 seconds')
option_force = click.option('--force', is_flag=True, help='Do not ask for confirmation at any point')
arg_cluster_name = click.argument('cluster_name', required=False,
                                  default=lambda: _get_configuration().get('scope'))
option_default_citus_group = click.option('--group', required=False, type=int, help='Citus group',
                                          default=lambda: _get_configuration().get('citus', {}).get('group'))
option_citus_group = click.option('--group', required=False, type=int, help='Citus group')
role_choice = click.Choice([role.value for role in CtlPostgresqlRole])


@click.group(cls=click.Group)
@click.option('--config-file', '-c', help='Configuration file',
              envvar='PATRONICTL_CONFIG_FILE', default=CONFIG_FILE_PATH)
@click.option('--dcs-url', '--dcs', '-d', 'dcs_url', help='The DCS connect url', envvar='DCS_URL')
@click.option('-k', '--insecure', is_flag=True, help='Allow connections to SSL sites without certs')
@click.pass_context
def ctl(ctx: click.Context, config_file: str, dcs_url: Optional[str], insecure: bool) -> None:
    """Command-line interface for interacting with Patroni.
    \f
    Entry point of ``patronictl`` utility.

    Load the configuration file.

    .. note::
        Besides *dcs_url* and *insecure*, which are used to override DCS configuration section and ``ctl.insecure``
        setting, you can also override the value of ``log.level``, by default ``WARNING``, through either of these
        environment variables:
            * ``LOGLEVEL``
            * ``PATRONI_LOGLEVEL``
            * ``PATRONI_LOG_LEVEL``

    :param ctx: click context to be passed to sub-commands.
    :param config_file: path to the configuration file.
    :param dcs_url: the DCS URL in the format ``DCS://HOST:PORT``, e.g. ``etcd3://random.com:2399``. If given override
        whatever DCS is set in the configuration file.
    :param insecure: if ``True`` allow SSL connections without client certificates. Override what is configured through
        ``ctl.insecure` in the configuration file.
    """
    level = 'WARNING'
    for name in ('LOGLEVEL', 'PATRONI_LOGLEVEL', 'PATRONI_LOG_LEVEL'):
        level = os.environ.get(name, level)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    logging.captureWarnings(True)  # Capture eventual SSL warning
    config = load_config(config_file, dcs_url)
    # backward compatibility for configuration file where ctl section is not defined
    config.setdefault('ctl', {})['insecure'] = config.get('ctl', {}).get('insecure') or insecure
    ctx.obj = {'__config': config, '__mpp': get_mpp(config)}


def is_citus_cluster() -> bool:
    """Check if we are working with Citus cluster.

    :returns: ``True`` if configuration has ``citus`` section, otherwise ``False``.
    """
    return click.get_current_context().obj['__mpp'].is_enabled()


# Cache DCS instances for given scope and group
__dcs_cache: Dict[Tuple[str, Optional[int]], AbstractDCS] = {}


def get_dcs(scope: str, group: Optional[int]) -> AbstractDCS:
    """Get the DCS object.

    :param scope: cluster name.
    :param group: if *group* is defined, use it to select which alternative Citus group this DCS refers to. If *group*
        is ``None`` and a Citus configuration exists, assume this is the coordinator. Coordinator has the group ``0``.
        Refer to the module note for more details.

    :returns: a subclass of :class:`~patroni.dcs.AbstractDCS`, according to the DCS technology that is configured.

    :raises:
        :class:`PatroniCtlException`: if not suitable DCS configuration could be found.
    """
    if (scope, group) in __dcs_cache:
        return __dcs_cache[(scope, group)]
    config = _get_configuration()
    config.update({'scope': scope, 'patronictl': True})
    if group is not None:
        config['citus'] = {'group': group, 'database': 'postgres'}
    config.setdefault('name', scope)
    try:
        dcs = _get_dcs(config)
        if is_citus_cluster() and group is None:
            dcs.is_mpp_coordinator = lambda: True
        click.get_current_context().obj['__mpp'] = dcs.mpp
        __dcs_cache[(scope, group)] = dcs
        return dcs
    except PatroniException as e:
        raise PatroniCtlException(str(e))


def request_patroni(member: Member, method: str = 'GET',
                    endpoint: Optional[str] = None, data: Optional[Any] = None) -> urllib3.response.HTTPResponse:
    """Perform a request to Patroni REST API.

    :param member: DCS member, used to get the base URL of its REST API server.
    :param method: HTTP method to be used, e.g. ``GET``.
    :param endpoint: URL path of the request, e.g. ``patroni``.
    :param data: anything to be used as the request body.

    :returns: the response for the request.
    """
    ctx = click.get_current_context()  # the current click context
    request_executor = ctx.obj.get('__request_patroni')
    if not request_executor:
        request_executor = ctx.obj['__request_patroni'] = PatroniRequest(_get_configuration())
    return request_executor(member, method, endpoint, data)


def print_output(columns: Optional[List[str]], rows: List[List[Any]], alignment: Optional[Dict[str, str]] = None,
                 fmt: str = 'pretty', header: str = '', delimiter: str = '\t') -> None:
    """Print tabular information.

    :param columns: list of column names.
    :param rows: list of rows. Each item is a list of values for the columns.
    :param alignment: alignment to be applied to column values. Each key is the name of a column to be aligned, and the
        corresponding value can be one among:

        * ``l``: left-aligned
        * ``c``: center-aligned
        * ``r``: right-aligned

        A key in the dictionary is only required for a column that needs a specific alignment. Only apply when *fmt* is
        either ``pretty`` or ``topology``.
    :param fmt: the printing format. Can be one among:

        * ``json``: to print as a JSON string -- array of objects;
        * ``yaml`` or ``yml``: to print as a YAML string;
        * ``tsv``: to print a table of separated values, by default by tab;
        * ``pretty``: to print a pretty table;
        * ``topology``: similar to *pretty*, but with a topology view when printing cluster members.
    :param header: a string to be included in the first line of the table header, typically the cluster name. Only
        apply when *fmt* is either ``pretty`` or ``topology``.
    :param delimiter: the character to be used as delimiter when *fmt* is ``tsv``.
    """
    if fmt in {'json', 'yaml', 'yml'}:
        elements = [{k: v for k, v in zip(columns or [], r) if not header or str(v)} for r in rows]
        func = json.dumps if fmt == 'json' else format_config_for_editing
        click.echo(func(elements))
    elif fmt in {'pretty', 'tsv', 'topology'}:
        list_cluster = bool(header and columns and columns[0] == 'Cluster')
        if list_cluster and columns and 'Tags' in columns:  # we want to format member tags as YAML
            i = columns.index('Tags')
            for row in rows:
                if row[i]:
                    # Member tags are printed in YAML block format if *fmt* is ``pretty``. If *fmt* is either ``tsv``
                    # or ``topology``, then write in the YAML flow format, which is similar to JSON
                    row[i] = format_config_for_editing(row[i], fmt != 'pretty').strip()
        if list_cluster and header and fmt != 'tsv':  # skip cluster name and maybe Citus group if pretty-printing
            skip_cols = 2 if ' (group: ' in header else 1
            columns = columns[skip_cols:] if columns else []
            rows = [row[skip_cols:] for row in rows]

        # In ``tsv`` format print cluster name in every row as the first column
        if fmt == 'tsv':
            for r in ([columns] if columns else []) + rows:
                click.echo(delimiter.join(map(str, r)))
        # In ``pretty`` and ``topology`` formats print the cluster name only once, in the very first header line
        else:
            # If any value is multi-line, then add horizontal between all table rows while printing to get a clear
            # visual separation of rows.
            hrules = hrule_all if any(any(isinstance(c, str) and '\n' in c for c in r) for r in rows) else hrule_frame
            table = PatronictlPrettyTable(header, columns, hrules=hrules)
            table.align = 'l'
            for k, v in (alignment or {}).items():
                table.align[k] = v
            for r in rows:
                table.add_row(r)
            click.echo(table)


def watching(w: bool, watch: Optional[int], max_count: Optional[int] = None, clear: bool = True) -> Iterator[int]:
    """Yield a value every ``watch`` seconds.

    Used to run a command with a watch-based approach.

    :param w: if ``True`` and *watch* is ``None``, then *watch* assumes the value ``2``.
    :param watch: amount of seconds to wait before yielding another value.
    :param max_count: maximum number of yielded values. If ``None`` keep yielding values indefinitely.
    :param clear: if the screen should be cleared out at each iteration.

    :yields: ``0`` each time *watch* seconds have passed.

    :Example:

        >>> len(list(watching(True, 1, 0)))
        1
        >>> len(list(watching(True, 1, 1)))
        2
        >>> len(list(watching(True, None, 0)))
        1
    """
    if w and not watch:
        watch = 2
    if watch and clear:
        click.clear()
    yield 0

    if max_count is not None and max_count < 1:
        return

    counter = 1
    yield_time = time.time()
    while watch and counter <= (max_count or counter):
        elapsed = time.time() - yield_time
        time.sleep(max(0, watch - elapsed))
        counter += 1
        if clear:
            click.clear()
        yield_time = time.time()
        yield 0


def get_all_members(cluster: Cluster, group: Optional[int],
                    role: CtlPostgresqlRole = CtlPostgresqlRole.LEADER) -> Iterator[Member]:
    """Get all cluster members that have the given *role*.

    :param cluster: the Patroni cluster.
    :param group: filter which Citus group we should get members from. If ``None`` get from all groups.
    :param role: role to filter members. One of :class:`CtlPostgresqlRole` values.

    :yields: members that have the given *role*.
    """
    clusters = {0: cluster}
    if is_citus_cluster() and group is None:
        clusters.update(cluster.workers)
    if role in (CtlPostgresqlRole.LEADER, CtlPostgresqlRole.PRIMARY, CtlPostgresqlRole.STANDBY_LEADER):
        # In the DCS the members' role can be one among: ``primary``, ``master``, ``replica`` or ``standby_leader``.
        # ``primary`` and ``master`` are the same thing.
        for cluster in clusters.values():
            if cluster.leader is not None and cluster.leader.name and\
                (role == CtlPostgresqlRole.LEADER
                 or cluster.leader.data.get('role') not in (PostgresqlRole.PRIMARY, PostgresqlRole.MASTER)
                 and role == CtlPostgresqlRole.STANDBY_LEADER
                 or cluster.leader.data.get('role') != PostgresqlRole.STANDBY_LEADER
                 and role == CtlPostgresqlRole.PRIMARY):
                yield cluster.leader.member
        return

    for cluster in clusters.values():
        leader_name = (cluster.leader.member.name if cluster.leader else None)
        for m in cluster.members:
            if role == CtlPostgresqlRole.ANY or\
                    role in (CtlPostgresqlRole.REPLICA, CtlPostgresqlRole.STANDBY) and m.name != leader_name:
                yield m


def get_any_member(cluster: Cluster, group: Optional[int],
                   role: Optional[CtlPostgresqlRole] = None, member: Optional[str] = None) -> Optional[Member]:
    """Get the first found cluster member that has the given *role*.

    :param cluster: the Patroni cluster.
    :param group: filter which Citus group we should get members from. If ``None`` get from all groups.
    :param role: role to filter members. See :func:`get_all_members` for available options.
    :param member: if specified, then besides having the given *role*, the cluster member's name should be *member*.

    :returns: the first found cluster member that has the given *role*.

    :raises:
        :class:`PatroniCtlException`: if both *role* and *member* are provided.
    """
    if member is not None:
        if role is not None:
            raise PatroniCtlException('--role and --member are mutually exclusive options')
        role = CtlPostgresqlRole.ANY
    elif role is None:
        role = CtlPostgresqlRole.LEADER

    for m in get_all_members(cluster, group, role):
        if member is None or m.name == member:
            return m


def get_all_members_leader_first(cluster: Cluster) -> Iterator[Member]:
    """Get all cluster members, with the cluster leader being yielded first.

    .. note::
        Only yield members that have a ``restapi.connect_address`` configured.

    :yields: all cluster members, with the leader first.
    """
    leader_name = cluster.leader.member.name if cluster.leader and cluster.leader.member.api_url else None
    if leader_name and cluster.leader:
        yield cluster.leader.member
    for member in cluster.members:
        if member.api_url and member.name != leader_name:
            yield member


def get_cursor(cluster: Cluster, group: Optional[int], connect_parameters: Dict[str, Any],
               role: Optional[CtlPostgresqlRole] = None,
               member_name: Optional[str] = None) -> Union['cursor', 'Cursor[Any]', None]:
    """Get a cursor object to execute queries against a member that has the given *role* or *member_name*.

    .. note::
        Besides what is passed through *connect_parameters*, this function also sets the following parameters:
            * ``fallback_application_name``: as ``Patroni ctl``;
            * ``connect_timeout``: as ``5``.

    :param cluster: the Patroni cluster.
    :param group: filter which Citus group we should get members to create a cursor against. If ``None`` consider
        members from all groups.
    :param connect_parameters: database connection parameters.
    :param role: role to filter members. See :func:`get_all_members` for available options.
    :param member_name: if specified, then besides having the given *role*, the cluster member's name should be
        *member_name*.

    :returns: a cursor object to execute queries against the database. Can be either:

        * A :class:`psycopg.Cursor` if using :mod:`psycopg`; or
        * A :class:`psycopg2.extensions.cursor` if using :mod:`psycopg2`;
        * ``None`` if not able to get a cursor that attendees *role* and *member_name*.
    """
    member = get_any_member(cluster, group, role=role, member=member_name)
    if member is None:
        return None

    params = member.conn_kwargs(connect_parameters)
    params.update({'fallback_application_name': 'Patroni ctl', 'connect_timeout': '5'})
    if 'dbname' in connect_parameters:
        params['dbname'] = connect_parameters['dbname']
    else:
        params.pop('dbname')

    from . import psycopg
    conn = psycopg.connect(**params)
    cursor = conn.cursor()
    # If we want ``any`` node we are fine to return the cursor. ``None`` is similar to ``any`` at this point, as it's
    # been dealt with through :func:`get_any_member`.
    # If we want the Patroni leader node, :func:`get_any_member` already checks that for us
    if role in (None, CtlPostgresqlRole.ANY, CtlPostgresqlRole.LEADER):
        return cursor

    # If we want something other than ``any`` or ``leader``, then we do not rely only on the DCS information about
    # members, but rather double check the underlying Postgres status.
    cursor.execute('SELECT pg_catalog.pg_is_in_recovery()')
    row = cursor.fetchone()
    in_recovery = not row or row[0]

    if in_recovery and\
        role in (CtlPostgresqlRole.REPLICA, CtlPostgresqlRole.STANDBY, CtlPostgresqlRole.STANDBY_LEADER) or\
            not in_recovery and role == CtlPostgresqlRole.PRIMARY:
        return cursor

    conn.close()

    return None


def get_members(cluster: Cluster, cluster_name: str, member_names: List[str], role: CtlPostgresqlRole,
                force: bool, action: str, ask_confirmation: bool = True, group: Optional[int] = None) -> List[Member]:
    """Get the list of members based on the given filters.

    .. note::
        Contain some filtering and checks processing that are common to several actions that are exposed
        by `patronictl`, like:

            * Get members of *cluster* that respect the given *member_names*, *role*, and *group*;
            * Bypass confirmations;
            * Prompt user for information that has not been passed through the command-line options;
            * etc.

        Designed to handle both attended and unattended ``patronictl`` commands execution that need to retrieve and
        validate the members before doing anything.

        In the very end may call :func:`confirm_members_action` to ask if the user would like to proceed with *action*
        over the retrieved members. That won't actually perform the action, but it works as the "last confirmation"
        before the *action* is processed by the caller method.

        Additional checks can also be implemented in the caller method, in which case you might want to pass
        ``ask_confirmation=False``, and later call :func:`confirm_members_action` manually in the caller method. That
        way the workflow won't look broken to the user that is interacting with ``patronictl``.

    :param cluster: Patroni cluster.
    :param cluster_name: name of the Patroni cluster.
    :param member_names: used to filter which members should take the *action* based on their names. Each item is the
        name of a Patroni member, as per ``name`` configuration. If *member_names* is an empty :class:`tuple` no filters
        are applied based on names.
    :param role: used to filter which members should take the *action* based on their role. See :func:`get_all_members`
        for available options.
    :param force: if ``True``, then it won't ask for confirmations at any point nor prompt the user to select values
        for options that were not specified through the command-line.
    :param action: the action that is being processed, one among:

        * ``reload``: reload PostgreSQL configuration; or
        * ``restart``: restart PostgreSQL; or
        * ``reinitialize``: reinitialize PostgreSQL data directory; or
        * ``flush``: discard scheduled actions.
    :param ask_confirmation: if ``False``, then it won't ask for the final confirmation regarding the *action* before
        returning the list of members. Usually useful as ``False`` if you want to perform additional checks in
        the caller method besides the checks that are performed through this generic method.
    :param group: filter which Citus group we should get members from. If ``None`` consider members from all groups.

    :returns: a list of members that respect the given filters.

    :raises:
        :class:`PatroniCtlException`: if
            * Cluster does not have members that match the given *role*; or
            * Cluster does not have members that match the given *member_names*; or
            * No member with given *role* is found among the specified *member_names*.
    """
    members = list(get_all_members(cluster, group, role))

    candidates = {m.name for m in members}
    if not force or role:
        if not member_names and not candidates:
            raise PatroniCtlException('{0} cluster doesn\'t have any members'.format(cluster_name))
        output_members(cluster, cluster_name, group=group)

    if member_names:
        member_names = list(set(member_names) & candidates)
        if not member_names:
            raise PatroniCtlException('No {0} among provided members'.format(role))
    elif action != 'reinitialize':
        member_names = list(candidates)

    if not member_names and not force:
        member_names = [click.prompt('Which member do you want to {0} [{1}]?'.format(action,
                        ', '.join(candidates)), type=str, default='')]

    for member_name in member_names:
        if member_name not in candidates:
            raise PatroniCtlException('{0} is not a member of cluster'.format(member_name))

    members = [m for m in members if m.name in member_names]
    if ask_confirmation:
        confirm_members_action(members, force, action)
    return members


def confirm_members_action(members: List[Member], force: bool, action: str,
                           scheduled_at: Optional[datetime.datetime] = None, pending: Optional[bool] = None) -> None:
    """Ask for confirmation if *action* should be taken by *members*.

    :param members: list of member which will take the *action*.
    :param force: if ``True`` skip the confirmation prompt and allow the *action* to proceed.
    :param action: the action that is being processed, one among:

        * ``reload``: reload PostgreSQL configuration; or
        * ``restart``: restart PostgreSQL; or
        * ``reinitialize``: reinitialize PostgreSQL data directory; or
        * ``flush``: discard scheduled actions.
    :param scheduled_at: timestamp at which the *action* should be scheduled to. If ``None``  *action* is taken
        immediately.

    :raises:
        :class:`PatroniCtlException`: if the user aborted the *action*.
    """
    if scheduled_at:
        if not force:
            if pending:
                confirm = click.confirm('The nodes needing a restart will be identified at the scheduled time, and '
                                        'might be different from the ones showing pending restart right now. '
                                        'Is this fine?')
            else:
                confirm = click.confirm('Are you sure you want to schedule {0} of members {1} at {2}?'
                                        .format(action, ', '.join([m.name for m in members]), scheduled_at))
            if not confirm:
                raise PatroniCtlException('Aborted scheduled {0}'.format(action))
    else:
        if not force:
            confirm = click.confirm('Are you sure you want to {0} members {1}?'
                                    .format(action, ', '.join([m.name for m in members])))
            if not confirm:
                raise PatroniCtlException('Aborted {0}'.format(action))


@ctl.command('dsn', help='Generate a dsn for the provided member, defaults to a dsn of the leader')
@click.option('--role', '-r', help='Give a dsn of any member with this role', type=role_choice, default=None)
@click.option('--member', '-m', help='Generate a dsn for this member', type=str)
@arg_cluster_name
@option_citus_group
def dsn(cluster_name: str, group: Optional[int], role: Optional[CtlPostgresqlRole], member: Optional[str]) -> None:
    """Process ``dsn`` command of ``patronictl`` utility.

    Get DSN to connect to *member*.

    .. note::
        If no *role* nor *member* is given assume *role* as ``leader``.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should get members to get DSN from. Refer to the module note for more
        details.
    :param role: filter which members to get DSN from based on their role. See :func:`get_all_members` for available
        options.
    :param member: filter which member to get DSN from based on its name.

    :raises:
        :class:`PatroniCtlException`: if
            * both *role* and *member* are provided; or
            * No member matches requested *member* or *role*.
    """
    cluster = get_dcs(cluster_name, group).get_cluster()
    m = get_any_member(cluster, group, role=role, member=member)
    if m is None:
        raise PatroniCtlException('Can not find a suitable member')

    params = m.conn_kwargs()
    click.echo('host={host} port={port}'.format(**params))


@ctl.command('query', help='Query a Patroni PostgreSQL member')
@arg_cluster_name
@option_citus_group
@click.option('--format', 'fmt', help='Output format (pretty, tsv, json, yaml)', default='tsv')
@click.option('--file', '-f', 'p_file', help='Execute the SQL commands from this file', type=click.File('rb'))
@click.option('--password', help='force password prompt', is_flag=True)
@click.option('-U', '--username', help='database user name', type=str)
@option_watch
@option_watchrefresh
@click.option('--role', '-r', help='The role of the query', type=role_choice, default=None)
@click.option('--member', '-m', help='Query a specific member', type=str)
@click.option('--delimiter', help='The column delimiter', default='\t')
@click.option('--command', '-c', help='The SQL commands to execute')
@click.option('-d', '--dbname', help='database name to connect to', type=str)
def query(
    cluster_name: str,
    group: Optional[int],
    role: Optional[CtlPostgresqlRole],
    member: Optional[str],
    w: bool,
    watch: Optional[int],
    delimiter: str,
    command: Optional[str],
    p_file: Optional[io.BufferedReader],
    password: Optional[bool],
    username: Optional[str],
    dbname: Optional[str],
    fmt: str = 'tsv'
) -> None:
    """Process ``query`` command of ``patronictl`` utility.

    Perform a Postgres query in a Patroni node.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should get members from to perform the query. Refer to the module note for
        more details.
    :param role: filter which members to perform the query against based on their role. See :func:`get_all_members` for
        available options.
    :param member: filter which member to perform the query against based on its name.
    :param w: perform query with watch-based approach every 2 seconds.
    :param watch: perform query with watch-based approach every *watch* seconds.
    :param delimiter: column delimiter when *fmt* is ``tsv``.
    :param command: SQL query to execute.
    :param p_file: path to file containing SQL query to execute.
    :param password: if ``True`` then prompt for password.
    :param username: name of the database user.
    :param dbname: name of the database.
    :param fmt: the output table printing format. See :func:`print_output` for available options.

    :raises:
        :class:`PatroniCtlException`: if:
            * if * both *role* and *member* are provided; or
            * both *file* and *command* are provided; or
            * neither *file* nor *command* is provided.
    """
    if p_file is not None:
        if command is not None:
            raise PatroniCtlException('--file and --command are mutually exclusive options')
        sql = p_file.read().decode('utf-8')
    else:
        if command is None:
            raise PatroniCtlException('You need to specify either --command or --file')
        sql = command

    connect_parameters: Dict[str, str] = {}
    if username:
        connect_parameters['username'] = username
    if password:
        connect_parameters['password'] = click.prompt('Password', hide_input=True, type=str)
    if dbname:
        connect_parameters['dbname'] = dbname

    dcs = get_dcs(cluster_name, group)

    cluster = cursor = None
    for _ in watching(w, watch, clear=False):
        if cluster is None:
            cluster = dcs.get_cluster()

        output, header = query_member(cluster, group, cursor, member, role, sql, connect_parameters)
        print_output(header, output, fmt=fmt, delimiter=delimiter)


def query_member(cluster: Cluster, group: Optional[int], cursor: Union['cursor', 'Cursor[Any]', None],
                 member: Optional[str], role: Optional[CtlPostgresqlRole], command: str,
                 connect_parameters: Dict[str, Any]) -> Tuple[List[List[Any]], Optional[List[Any]]]:
    """Execute SQL *command* against a member.

    :param cluster: the Patroni cluster.
    :param group: filter which Citus group we should get members from to perform the query. Refer to the module note for
        more details.
    :param cursor: cursor through which *command* is executed. If ``None`` a new cursor is instantiated through
        :func:`get_cursor`.
    :param member: filter which member to create a cursor against based on its name, if *cursor* is ``None``.
    :param role: filter which member to create a cursor against based on their role, if *cursor* is ``None``. See
        :func:`get_all_members` for available options.
    :param command: SQL command to be executed.
    :param connect_parameters: connection parameters to be passed down to :func:`get_cursor`, if *cursor* is ``None``.

    :returns: a tuple composed of two items:

        * List of rows returned by the executed *command*;
        * List of columns related to the rows returned by the executed *command*.

        If an error occurs while executing *command*, then returns the following values in the tuple:

        * List with 2 items:

          * Current timestamp;
          * Error message.

        * ``None``.
    """
    from . import psycopg
    try:
        if cursor is None:
            cursor = get_cursor(cluster, group, connect_parameters, role=role, member_name=member)

        if cursor is None:
            if member is not None:
                message = f'No connection to member {member} is available'
            elif role is not None:
                message = f'No connection to role {role!r} is available'
            else:
                message = 'No connection is available'
            logging.debug(message)
            return [[timestamp(0), message]], None

        cursor.execute(command.encode('utf-8'))
        return [list(row) for row in cursor], cursor.description and [d.name for d in cursor.description]
    except psycopg.DatabaseError as de:
        logging.debug(de)
        if cursor is not None and not cursor.connection.closed:
            cursor.connection.close()
        message = de.diag.sqlstate or str(de)
        message = message.replace('\n', ' ')
        return [[timestamp(0), 'ERROR, SQLSTATE: {0}'.format(message)]], None


@ctl.command('remove', help='Remove cluster from DCS')
@click.argument('cluster_name')
@option_citus_group
@option_format
def remove(cluster_name: str, group: Optional[int], fmt: str) -> None:
    """Process ``remove`` command of ``patronictl`` utility.

    Remove cluster *cluster_name* from the DCS.

    :param cluster_name: name of the cluster which information will be wiped out of the DCS.
    :param group: which Citus group should have its information wiped out of the DCS. Refer to the module note for more
        details.
    :param fmt: the output table printing format. See :func:`print_output` for available options.

    :raises:
        :class:`PatroniCtlException`: if:
            * Patroni is running on a Citus cluster, but no *group* was specified; or
            * *cluster_name* does not exist; or
            * user did not type the expected confirmation message when prompted for confirmation; or
            * use did not type the correct leader name when requesting removal of a healthy cluster.

    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()

    if is_citus_cluster() and group is None:
        raise PatroniCtlException('For Citus clusters the --group must me specified')
    output_members(cluster, cluster_name, fmt=fmt)

    confirm = click.prompt('Please confirm the cluster name to remove', type=str)
    if confirm != cluster_name:
        raise PatroniCtlException('Cluster names specified do not match')

    message = 'Yes I am aware'
    confirm = \
        click.prompt('You are about to remove all information in DCS for {0}, please type: "{1}"'.format(cluster_name,
                     message), type=str)
    if message != confirm:
        raise PatroniCtlException('You did not exactly type "{0}"'.format(message))

    if cluster.leader and cluster.leader.name:
        confirm = click.prompt('This cluster currently is healthy. Please specify the leader name to continue')
        if confirm != cluster.leader.name:
            raise PatroniCtlException('You did not specify the current leader of the cluster')

    dcs.delete_cluster()


def check_response(response: urllib3.response.HTTPResponse, member_name: str,
                   action_name: str, silent_success: bool = False) -> bool:
    """Check an HTTP response and print a status message.

    :param response: the response to be checked.
    :param member_name: name of the member associated with the *response*.
    :param action_name: action associated with the *response*.
    :param silent_success: if a status message should be skipped upon a successful *response*.

    :returns: ``True`` if the response indicates a successful operation (HTTP status < ``400``), ``False`` otherwise.
    """
    if response.status >= 400:
        click.echo('Failed: {0} for member {1}, status code={2}, ({3})'.format(
            action_name, member_name, response.status, response.data.decode('utf-8')
        ))
        return False
    elif not silent_success:
        click.echo('Success: {0} for member {1}'.format(action_name, member_name))
    return True


def parse_scheduled(scheduled: Optional[str]) -> Optional[datetime.datetime]:
    """Parse a string *scheduled* timestamp as a :class:`~datetime.datetime` object.

    :param scheduled: string representation of the timestamp. May also be ``now``.

    :returns: the corresponding :class:`~datetime.datetime` object, if *scheduled* is not ``now``, otherwise ``None``.

    :raises:
        :class:`PatroniCtlException`: if unable to parse *scheduled* from :class:`str` to :class:`~datetime.datetime`.

    :Example:

        >>> parse_scheduled(None) is None
        True

        >>> parse_scheduled('now') is None
        True

        >>> parse_scheduled('2023-05-29T04:32:31')
        datetime.datetime(2023, 5, 29, 4, 32, 31, tzinfo=tzlocal())

        >>> parse_scheduled('2023-05-29T04:32:31-3')
        datetime.datetime(2023, 5, 29, 4, 32, 31, tzinfo=tzoffset(None, -10800))
    """
    if scheduled is not None and (scheduled or 'now') != 'now':
        try:
            scheduled_at = dateutil.parser.parse(scheduled)
            if scheduled_at.tzinfo is None:
                scheduled_at = scheduled_at.replace(tzinfo=dateutil.tz.tzlocal())
        except (ValueError, TypeError):
            message = 'Unable to parse scheduled timestamp ({0}). It should be in an unambiguous format (e.g. ISO 8601)'
            raise PatroniCtlException(message.format(scheduled))
        return scheduled_at

    return None


@ctl.command('reload', help='Reload cluster member configuration')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@option_citus_group
@click.option('--role', '-r', help='Reload only members with this role',
              type=role_choice, default=repr(CtlPostgresqlRole.ANY))
@option_force
def reload(cluster_name: str, member_names: List[str], group: Optional[int],
           force: bool, role: CtlPostgresqlRole) -> None:
    """Process ``reload`` command of ``patronictl`` utility.

    Reload configuration of cluster members based on given filters.

    :param cluster_name: name of the Patroni cluster.
    :param member_names: name of the members which configuration should be reloaded.
    :param group: filter which Citus group we should reload members. Refer to the module note for more details.
    :param force: perform the reload without asking for confirmations.
    :param role: role to filter members. See :func:`get_all_members` for available options.
    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()

    members = get_members(cluster, cluster_name, member_names, role, force, 'reload', group=group)

    for member in members:
        r = request_patroni(member, 'post', 'reload')
        if r.status == 200:
            click.echo('No changes to apply on member {0}'.format(member.name))
        elif r.status == 202:
            config = global_config.from_cluster(cluster)
            click.echo('Reload request received for member {0} and will be processed within {1} seconds'.format(
                member.name, config.get('loop_wait') or dcs.loop_wait)
            )
        else:
            click.echo('Failed: reload for member {0}, status code={1}, ({2})'.format(
                member.name, r.status, r.data.decode('utf-8'))
            )


@ctl.command('restart', help='Restart cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@option_citus_group
@click.option('--role', '-r', help='Restart only members with this role', type=role_choice,
              default=repr(CtlPostgresqlRole.ANY))
@click.option('--any', 'p_any', help='Restart a single member only', is_flag=True)
@click.option('--scheduled', help='Timestamp of a scheduled restart in unambiguous format (e.g. ISO 8601)',
              default=None)
@click.option('--pg-version', 'version', help='Restart if the PostgreSQL version is less than provided (e.g. 9.5.2)',
              default=None)
@click.option('--pending', help='Restart if pending', is_flag=True)
@click.option('--timeout', help='Return error and fail over if necessary when restarting takes longer than this.')
@option_force
def restart(cluster_name: str, group: Optional[int], member_names: List[str],
            force: bool, role: CtlPostgresqlRole, p_any: bool, scheduled: Optional[str], version: Optional[str],
            pending: bool, timeout: Optional[str]) -> None:
    """Process ``restart`` command of ``patronictl`` utility.

    Restart Postgres on cluster members based on given filters.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should restart members. Refer to the module note for more details.
    :param member_names: name of the members that should be restarted.
    :param force: perform the restart without asking for confirmations.
    :param role: role to filter members. See :func:`get_all_members` for available options.
    :param p_any: restart a single and random member among the ones that match the given filters.
    :param scheduled: timestamp when the restart should be scheduled to occur. If ``now`` restart immediately.
    :param version: restart only members which Postgres version is less than *version*.
    :param pending: restart only members that are flagged as ``pending restart``.
    :param timeout: timeout for the restart operation. If timeout is reached a failover may occur in the cluster.

    :raises:
        :class:`PatroniCtlException`: if:
            * *scheduled* could not be parsed; or
            * *version* could not be parsed; or
            * a restart is attempted against a cluster that is in maintenance mode.
    """
    cluster = get_dcs(cluster_name, group).get_cluster()

    members = get_members(cluster, cluster_name, member_names, role, force, 'restart', False, group=group)
    if scheduled is None and not force:
        next_hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
        scheduled = click.prompt('When should the restart take place (e.g. ' + next_hour + ') ',
                                 type=str, default='now')

    scheduled_at = parse_scheduled(scheduled)

    content: Dict[str, Any] = {}
    if pending:
        content['restart_pending'] = True
        # For scheduled restarts we don't filter the members now. If they really need a restart might change
        # until the scheduled time.
        if not scheduled_at:
            members = [m for m in members if m.data.get('pending_restart', False)]

    if p_any:
        random.shuffle(members)
        members = members[:1]

    confirm_members_action(members, force, 'restart', scheduled_at, pending)

    if version is None and not force:
        version = click.prompt('Restart if the PostgreSQL version is less than provided (e.g. 9.5.2) ',
                               type=str, default='')

    if version:
        try:
            postgres_version_to_int(version)
        except PatroniException as e:
            raise PatroniCtlException(e.value)

        content['postgres_version'] = version

    if scheduled_at:
        if global_config.from_cluster(cluster).is_paused:
            raise PatroniCtlException("Can't schedule restart in the paused state")
        content['schedule'] = scheduled_at.isoformat()

    if timeout is not None:
        content['timeout'] = timeout

    for member in members:
        if 'schedule' in content:
            if force and member.data.get('scheduled_restart'):
                r = request_patroni(member, 'delete', 'restart')
                check_response(r, member.name, 'flush scheduled restart', True)

        r = request_patroni(member, 'post', 'restart', content)
        if r.status == 200:
            click.echo('Success: restart on member {0}'.format(member.name))
        elif r.status == 202:
            click.echo('Success: restart scheduled on member {0}'.format(member.name))
        elif r.status == 409:
            click.echo('Failed: another restart is already scheduled on member {0}'.format(member.name))
        else:
            click.echo('Failed: restart for member {0}, status code={1}, ({2})'.format(
                member.name, r.status, r.data.decode('utf-8'))
            )


@ctl.command('reinit', help='Reinitialize cluster member')
@click.argument('cluster_name')
@option_citus_group
@click.argument('member_names', nargs=-1)
@option_force
@click.option('--from-leader', is_flag=True, help='Get basebackup from leader')
@click.option('--wait', help='Wait until reinitialization completes', is_flag=True)
def reinit(cluster_name: str, group: Optional[int], member_names: List[str], force: bool,
           from_leader: bool, wait: bool) -> None:
    """Process ``reinit`` command of ``patronictl`` utility.

    Reinitialize cluster members based on given filters.

    .. note::
        Only reinitialize replica members, not a leader.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should reinit members. Refer to the module note for more details.
    :param member_names: name of the members that should be reinitialized.
    :param force: perform the restart without asking for confirmations.
    :param from_leader: perform the reinit to get basebackup from the leader node.
    :param wait: wait for the operation to complete.
    """
    cluster = get_dcs(cluster_name, group).get_cluster()
    members = get_members(cluster, cluster_name, member_names, CtlPostgresqlRole.REPLICA,
                          force, 'reinitialize', group=group)

    wait_on_members: List[Member] = []
    for member in members:
        body: Dict[str, bool] = {'force': force, 'from_leader': from_leader}
        while True:
            r = request_patroni(member, 'post', 'reinitialize', body)
            started = check_response(r, member.name, 'reinitialize')
            if not started and r.data.endswith(b' already in progress') \
                    and not force and click.confirm('Do you want to cancel it and reinitialize anyway?'):
                body['force'] = True
                continue
            break
        if started and wait:
            wait_on_members.append(member)

    last_display = []
    while wait_on_members:
        if wait_on_members != last_display:
            click.echo('Waiting for reinitialize to complete on: {0}'.format(
                ", ".join(member.name for member in wait_on_members))
            )
            last_display[:] = wait_on_members
        time.sleep(2)
        for member in wait_on_members:
            data = json.loads(request_patroni(member, 'get', 'patroni').data.decode('utf-8'))
            if data.get('state') != PostgresqlState.CREATING_REPLICA:
                click.echo('Reinitialize is completed on: {0}'.format(member.name))
                wait_on_members.remove(member)


def _do_failover_or_switchover(action: str, cluster_name: str, group: Optional[int], candidate: Optional[str],
                               force: bool, switchover_leader: Optional[str] = None,
                               switchover_scheduled: Optional[str] = None) -> None:
    """Perform a failover or a switchover operation in the cluster.

    Informational messages are printed in the console during the operation, as well as the list of members before and
    after the operation, so the user can follow the operation status.

    .. note::
        If not able to perform the operation through the REST API, write directly to the DCS as a fall back.

    :param action: action to be taken -- ``failover`` or ``switchover``.
    :param cluster_name: name of the Patroni cluster.
    :param group: filter Citus group within we should perform a failover or switchover. If ``None``, user will be
        prompted for filling it -- unless *force* is ``True``, in which case an exception is raised.
    :param candidate: name of a standby member to be promoted. Nodes that are tagged with ``nofailover`` cannot be used.
    :param force: perform the failover or switchover without asking for confirmations.
    :param switchover_leader: name of the leader passed to the switchover command if any.
    :param switchover_scheduled: timestamp when the switchover should be scheduled to occur. If ``now``,
        perform immediately.

    :raises:
        :class:`PatroniCtlException`: if:
            * Patroni is running on a Citus cluster, but no *group* was specified; or
            * a switchover was requested by the cluster has no leader; or
            * *switchover_leader* does not match the current leader of the cluster; or
            * cluster has no candidates available for the operation; or
            * no *candidate* is given for a failover operation; or
            * current leader and *candidate* are the same; or
            * *candidate* is tagged as nofailover; or
            * *candidate* is not a member of the cluster; or
            * trying to schedule a switchover in a cluster that is in maintenance mode; or
            * user aborts the operation.
    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()
    click.echo('Current cluster topology')
    output_members(cluster, cluster_name, group=group)

    if is_citus_cluster() and group is None:
        if force:
            raise PatroniCtlException('For Citus clusters the --group must me specified')
        else:
            group = click.prompt('Citus group', type=int)
            dcs = get_dcs(cluster_name, group)
            cluster = dcs.get_cluster()

    config = global_config.from_cluster(cluster)

    cluster_leader = cluster.leader and cluster.leader.name
    # leader has to be be defined for switchover only
    if action == 'switchover':
        if not cluster_leader:
            raise PatroniCtlException('This cluster has no leader')

        if switchover_leader is None:
            if force:
                switchover_leader = cluster_leader
            else:
                prompt = 'Standby Leader' if config.is_standby_cluster else 'Primary'
                switchover_leader = click.prompt(prompt, type=str, default=cluster_leader)

        if cluster_leader != switchover_leader:
            raise PatroniCtlException(f'Member {switchover_leader} is not the leader of cluster {cluster_name}')

    # excluding members with nofailover tag
    candidate_names = [str(m.name) for m in cluster.members if m.name != cluster_leader and not m.nofailover]
    # We sort the names for consistent output to the client
    candidate_names.sort()

    if not candidate_names:
        raise PatroniCtlException('No candidates found to {0} to'.format(action))

    if candidate is None and not force:
        candidate = click.prompt('Candidate ' + str(candidate_names), type=str, default='')

    if action == 'failover' and not candidate:
        raise PatroniCtlException('Failover could be performed only to a specific candidate')

    if candidate and candidate not in candidate_names:
        if candidate == cluster_leader:
            raise PatroniCtlException(
                f'Member {candidate} is already the leader of cluster {cluster_name}')
        raise PatroniCtlException(
            f'Member {candidate} does not exist in cluster {cluster_name} or is tagged as nofailover')

    if all((not force,
            action == 'failover',
            config.is_synchronous_mode,
            not cluster.sync.is_empty,
            not cluster.sync.matches(candidate, True))):
        if not click.confirm(f'Are you sure you want to failover to the asynchronous node {candidate}?'):
            raise PatroniCtlException('Aborting ' + action)

    scheduled_at_str = None
    scheduled_at = None

    if action == 'switchover':
        if switchover_scheduled is None and not force:
            next_hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
            switchover_scheduled = click.prompt('When should the switchover take place (e.g. ' + next_hour + ' ) ',
                                                type=str, default='now')

        scheduled_at = parse_scheduled(switchover_scheduled)
        if scheduled_at:
            if config.is_paused:
                raise PatroniCtlException("Can't schedule switchover in the paused state")
            scheduled_at_str = scheduled_at.isoformat()

    failover_value = {'candidate': candidate}
    if action == 'switchover':
        failover_value['leader'] = switchover_leader
    if scheduled_at_str:
        failover_value['scheduled_at'] = scheduled_at_str

    logging.debug(failover_value)

    # By now we have established that the leader exists and the candidate exists
    if not force:
        demote_msg = f', demoting current leader {cluster_leader}' if cluster_leader else ''
        if scheduled_at_str:
            # only switchover can be scheduled
            if not click.confirm(f'Are you sure you want to schedule switchover of cluster '
                                 f'{cluster_name} at {scheduled_at_str}{demote_msg}?'):
                # action as a var to catch a regression in the tests
                raise PatroniCtlException('Aborting scheduled ' + action)
        else:
            if not click.confirm(f'Are you sure you want to {action} cluster {cluster_name}{demote_msg}?'):
                raise PatroniCtlException('Aborting ' + action)

    r = None
    try:
        member = cluster.leader.member if cluster.leader else candidate and cluster.get_member(candidate, False)
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(member, Member)
        r = request_patroni(member, 'post', action, failover_value)

        # probably old patroni, which doesn't support switchover yet
        if r.status == 501 and action == 'switchover' and b'Server does not support this operation' in r.data:
            r = request_patroni(member, 'post', 'failover', failover_value)

        if r.status in (200, 202):
            logging.debug(r)
            cluster = dcs.get_cluster()
            logging.debug(cluster)
            click.echo('{0} {1}'.format(timestamp(), r.data.decode('utf-8')))
        else:
            click.echo('{0} failed, details: {1}, {2}'.format(action.title(), r.status, r.data.decode('utf-8')))
            return
    except Exception:
        logging.exception(r)
        logging.warning('Failing over to DCS')
        click.echo('{0} Could not {1} using Patroni api, falling back to DCS'.format(timestamp(), action))
        dcs.manual_failover(switchover_leader, candidate, scheduled_at=scheduled_at)

    output_members(cluster, cluster_name, group=group)


@ctl.command('failover', help='Failover to a replica')
@arg_cluster_name
@option_citus_group
@click.option('--candidate', help='The name of the candidate', default=None)
@option_force
def failover(cluster_name: str, group: Optional[int], candidate: Optional[str], force: bool) -> None:
    """Process ``failover`` command of ``patronictl`` utility.

    Perform a failover operation immediately in the cluster.
    .. seealso::
        Refer to :func:`_do_failover_or_switchover` for details.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter Citus group within we should perform a failover or switchover. If ``None``, user will be
        prompted for filling it -- unless *force* is ``True``, in which case an exception is raised by
        :func:`_do_failover_or_switchover`.
    :param candidate: name of a standby member to be promoted. Nodes that are tagged with ``nofailover`` cannot be used.
    :param force: perform the failover or switchover without asking for confirmations.
    """
    _do_failover_or_switchover('failover', cluster_name, group, candidate, force)


@ctl.command('switchover', help='Switchover to a replica')
@arg_cluster_name
@option_citus_group
@click.option('--leader', '--primary', 'leader', help='The name of the current leader', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--scheduled', help='Timestamp of a scheduled switchover in unambiguous format (e.g. ISO 8601)',
              default=None)
@option_force
def switchover(cluster_name: str, group: Optional[int], leader: Optional[str],
               candidate: Optional[str], force: bool, scheduled: Optional[str]) -> None:
    """Process ``switchover`` command of ``patronictl`` utility.

    Perform a switchover operation in the cluster.

    .. seealso::
        Refer to :func:`_do_failover_or_switchover` for details.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter Citus group within we should perform a switchover. If ``None``, user will be prompted for
        filling it -- unless *force* is ``True``, in which case an exception is raised by
        :func:`_do_failover_or_switchover`.
    :param leader: name of the current leader member.
    :param candidate: name of a standby member to be promoted. Nodes that are tagged with ``nofailover`` cannot be used.
    :param force: perform the switchover without asking for confirmations.
    :param scheduled: timestamp when the switchover should be scheduled to occur. If ``now`` perform immediately.
    """
    _do_failover_or_switchover('switchover', cluster_name, group, candidate, force, leader, scheduled)


def generate_topology(level: int, member: Dict[str, Any],
                      topology: Dict[Optional[str], List[Dict[str, Any]]]) -> Iterator[Dict[str, Any]]:
    """Recursively yield members with their names adjusted according to their *level* in the cluster topology.

    .. note::
        The idea is to get a tree view of the members when printing their names. For example, suppose you have a
        cascading replication composed of 3 nodes, say ``postgresql0``, ``postgresql1``, and ``postgresql2``. This
        function would adjust their names to be like this:

        * ``'postgresql0'`` -> ``'postgresql0'``
        * ``'postgresql1'`` -> ``'+ postgresql1'``
        * ``'postgresql2'`` -> ``'  + postgresql2'``

        So, if you ever print their names line by line, you would see something like this:

        .. code-block::

            postgresql0
            + postgresql1
              + postgresql2

    :param level: the current level being inspected in the *topology*.
    :param member: information about the current member being inspected in *level* of *topology*. Should contain at
        least this key:
        * ``name``: name of the node, according to ``name`` configuration;

        But may contain others, which although ignored by this function, will be yielded as part of the resulting
        object. The value of key ``name`` is changed as explained in the note.

    :param topology: each key is the name of a node which has at least one replica attached to it. The corresponding
        value is a list of the attached replicas, each of them with the same structure described for *member*.

    :yields: the current member with its name changed. Besides that reyield values from recursive calls.
    """
    members = topology.get(member['name'], [])

    if level > 0:
        member['name'] = '{0}+ {1}'.format((' ' * (level - 1) * 2), member['name'])

    if member['name']:
        yield member

    for member in members:
        yield from generate_topology(level + 1, member, topology)


def topology_sort(members: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """Sort *members* according to their level in the replication topology tree.

    :param members: list of members in the cluster. Each item should contain at least these keys:

        * ``name``: name of the node, according to ``name`` configuration;
        * ``role``: ``leader``, ``standby_leader`` or ``replica``.

        Cascading replicas are identified through ``tags`` -> ``replicatefrom`` value -- if that is set, and they are
        in fact attached to another replica.

        Besides ``name``, ``role`` and ``tags`` keys, it may contain other keys, which although ignored by this
        function, will be yielded as part of the resulting object. The value of key ``name`` is changed through
        :func:`generate_topology`.

    :yields: *members* sorted by level in the topology, and with a new ``name`` value according to their level
        in the topology.
    """
    topology: Dict[Optional[str], List[Dict[str, Any]]] = defaultdict(list)
    leader = next((m for m in members if m['role'].endswith('leader')), {'name': None})
    replicas = set(member['name'] for member in members if not member['role'].endswith('leader'))
    for member in members:
        if not member['role'].endswith('leader'):
            parent = member.get('tags', {}).get('replicatefrom')
            parent = parent if parent and parent != member['name'] and parent in replicas else leader['name']
            topology[parent].append(member)
    for member in generate_topology(0, leader, topology):
        yield member


def get_cluster_service_info(cluster: Dict[str, Any]) -> List[str]:
    """Get complementary information about the cluster.

    :param cluster: a Patroni cluster represented as an object created through :func:`~patroni.utils.cluster_as_json`.

    :returns: a list of 0 or more informational messages. They can be about:

        * Cluster in maintenance mode;
        * Scheduled switchovers.
    """
    service_info: List[str] = []
    if cluster.get('pause'):
        service_info.append('Maintenance mode: on')

    if 'scheduled_switchover' in cluster:
        info = 'Switchover scheduled at: ' + cluster['scheduled_switchover']['at']
        for name in ('from', 'to'):
            if name in cluster['scheduled_switchover']:
                info += '\n{0:>24}: {1}'.format(name, cluster['scheduled_switchover'][name])
        service_info.append(info)
    return service_info


def output_members(cluster: Cluster, name: str, extended: bool = False,
                   fmt: str = 'pretty', group: Optional[int] = None) -> None:
    """Print information about the Patroni cluster and its members.

    Information is printed to console through :func:`print_output`, and contains:

        * ``Cluster``: name of the Patroni cluster, as per ``scope`` configuration;
        * ``Member``: name of the Patroni node, as per ``name`` configuration;
        * ``Host``: hostname (or IP) and port, as per ``postgresql.listen`` configuration;
        * ``Role``: ``Leader``, ``Standby Leader``, ``Sync Standby`` or ``Replica``;
        * ``State``: one of :class:`~patroni.postgresql.misc.PostgresqlState`;
        * ``TL``: current timeline in Postgres;
        * ``Receive LSN``: last received LSN (``pg_catalog.pg_last_(xlog|wal)_receive_(location|lsn)()``);
        * ``Receive Lag``: lag of the receive LSN in MB;
        * ``Replay LSN``: last replayed LSN (``pg_catalog.pg_last_(xlog|wal)_replay_(location|lsn)()``);
        * ``Replay Lag``: lag of the replay LSN in MB.

    Besides that it may also have:
        * ``Group``: Citus group ID -- showed only if Citus is enabled.
        * ``Pending restart``: if the node is pending a restart -- showed only if *extended*;
        * ``Scheduled restart``: timestamp for scheduled restart, if any -- showed only if *extended*;
        * ``Tags``: node tags, if any -- showed only if *extended*.

    The 3 extended columns are always included if *extended*, even if the member has no value for a given column.
    If not *extended*, these columns may still be shown if any of the members has any information for them.

    :param cluster: Patroni cluster.
    :param name: name of the Patroni cluster.
    :param extended: if extended information (pending restarts, scheduled restarts, node tags) should be printed, if
        available.
    :param fmt: the output table printing format. See :func:`print_output` for available options. If *fmt* is neither
        ``topology`` nor ``pretty``, then complementary information gathered through :func:`get_cluster_service_info` is
        not printed.
    :param group: filter which Citus group we should get members from. If ``None`` get from all groups.
    """
    rows: List[List[Any]] = []
    logging.debug(cluster)

    initialize = {None: 'uninitialized', '': 'initializing'}.get(cluster.initialize, cluster.initialize)
    columns = ['Cluster', 'Member', 'Host', 'Role', 'State', 'TL',
               'Receive LSN', 'Receive Lag', 'Replay LSN', 'Replay Lag']

    clusters = {group or 0: cluster_as_json(cluster)}

    if is_citus_cluster():
        columns.insert(1, 'Group')
        if group is None:
            clusters.update({g: cluster_as_json(c) for g, c in cluster.workers.items()})

    all_members = [m for c in clusters.values() for m in c['members'] if 'host' in m]

    for c in ('Pending restart', 'Pending restart reason', 'Scheduled restart', 'Tags'):
        if extended or any(m.get(c.lower().replace(' ', '_')) for m in all_members):
            columns.append(c)

    # Show Host as 'host:port' if somebody is running on non-standard port or two nodes are running on the same host
    append_port = any('port' in m and m['port'] != 5432 for m in all_members) or\
        len(set(m['host'] for m in all_members)) < len(all_members)

    sort = topology_sort if fmt == 'topology' else iter
    for g, c in sorted(clusters.items()):
        for member in sort(c['members']):
            logging.debug(member)

            def format_diff(param: str, values: Dict[str, str], hide_long: bool):
                full_diff = param + ': ' + values['old_value'] + '->' + values['new_value']
                return full_diff if not hide_long or len(full_diff) <= 50 else param + ': [hidden - too long]'
            restart_reason = '\n'.join([format_diff(k, v, fmt in ('pretty', 'topology'))
                                        for k, v in member.get('pending_restart_reason', {}).items()]) or ''

            receive_lag, replay_lag = member.get('receive_lag', ''), member.get('replay_lag', '')
            receive_lsn, replay_lsn = member.get('receive_lsn', ''), member.get('replay_lsn', '')
            lsn = member.get('lsn', '')
            # old lsn/lag implementation compatibility
            if 'unknown' == receive_lsn and replay_lsn == 'unknown' and lsn and lsn != 'unknown':
                lag = member.get('lag', '')
                if member['state'] == 'streaming':
                    receive_lag, receive_lsn = lag, lsn
                else:
                    replay_lag, replay_lsn = lag, lsn
            receive_lag = round(receive_lag / 1024 / 1024) if isinstance(receive_lag, int) \
                else '' if receive_lag == 'unknown' else receive_lag
            replay_lag = round(replay_lag / 1024 / 1024) if isinstance(replay_lag, int) \
                else '' if replay_lag == 'unknown' else replay_lag

            member.update(cluster=name, member=member['name'], group=g,
                          host=member.get('host', ''), tl=member.get('timeline', ''),
                          role=member['role'].replace('_', ' ').title(),
                          receive_lag=receive_lag, replay_lag=replay_lag,
                          receive_lsn=receive_lsn, replay_lsn=replay_lsn,
                          pending_restart='*' if member.get('pending_restart') else '',
                          pending_restart_reason=restart_reason)

            if append_port and member['host'] and member.get('port'):
                member['host'] = ':'.join([member['host'], str(member['port'])])

            if 'scheduled_restart' in member:
                value = member['scheduled_restart']['schedule']
                if 'postgres_version' in member['scheduled_restart']:
                    value += ' if version < {0}'.format(member['scheduled_restart']['postgres_version'])
                member['scheduled_restart'] = value

            rows.append([member.get(n.lower().replace(' ', '_'), '') for n in columns])

    if is_citus_cluster():
        title = 'Citus cluster'
        title_details = '' if group is None else f' (group: {group}, {initialize})'
    else:
        title = 'Cluster'
        title_details = f' ({initialize})'

    title = f' {title}: {name}{title_details} '
    if fmt in ('pretty', 'topology'):
        columns[columns.index('Replay Lag')] = columns[columns.index('Receive Lag')] = 'Lag'
    print_output(columns, rows,
                 {'Group': 'r', 'Receive LSN': 'r', 'Replay LSN': 'r', 'Lag': 'r', 'TL': 'r'}, fmt, title)

    if fmt not in ('pretty', 'topology'):  # Omit service info when using machine-readable formats
        return

    for g, c in sorted(clusters.items()):
        service_info = get_cluster_service_info(c)
        if service_info:
            if is_citus_cluster() and group is None:
                click.echo('Citus group: {0}'.format(g))
            click.echo(' ' + '\n '.join(service_info))


@ctl.command('list', help='List the Patroni members for a given Patroni')
@click.argument('cluster_names', nargs=-1)
@option_citus_group
@click.option('--extended', '-e', help='Show some extra information', is_flag=True)
@click.option('--timestamp', '-t', 'ts', help='Print timestamp', is_flag=True)
@option_format
@option_watch
@option_watchrefresh
def members(cluster_names: List[str], group: Optional[int], fmt: str,
            watch: Optional[int], w: bool, extended: bool, ts: bool) -> None:
    """Process ``list`` command of ``patronictl`` utility.

    Print information about the Patroni cluster through :func:`output_members`.

    :param cluster_names: name of clusters that should be printed. If ``None`` consider only the cluster present in
        ``scope`` key of the configuration.
    :param group: filter which Citus group we should get members from. Refer to the module note for more details.
    :param fmt: the output table printing format. See :func:`print_output` for available options.
    :param watch: if given print output every *watch* seconds.
    :param w: if ``True`` print output every 2 seconds.
    :param extended: if extended information should be printed. See ``extended`` argument of :func:`output_members` for
        more details.
    :param ts: if timestamp should be included in the output.
    """
    config = _get_configuration()
    if not cluster_names:
        if 'scope' in config:
            cluster_names = [config['scope']]
        if not cluster_names:
            return logging.warning('Listing members: No cluster names were provided')

    for _ in watching(w, watch):
        if ts:
            click.echo(timestamp(0))

        for cluster_name in cluster_names:
            dcs = get_dcs(cluster_name, group)

            cluster = dcs.get_cluster()
            output_members(cluster, cluster_name, extended, fmt, group)


@ctl.command('topology', help='Prints ASCII topology for given cluster')
@click.argument('cluster_names', nargs=-1)
@option_citus_group
@option_watch
@option_watchrefresh
@click.pass_context
def topology(ctx: click.Context, cluster_names: List[str], group: Optional[int], watch: Optional[int], w: bool) -> None:
    """Process ``topology`` command of ``patronictl`` utility.

    Print information about the cluster in ``topology`` format through :func:`members`.

    :param ctx: click context to be passed to :func:`members`.
    :param cluster_names: name of clusters that should be printed. See ``cluster_names`` argument of
        :func:`output_members` for more details.
    :param group: filter which Citus group we should get members from. See ``group`` argument of :func:`output_members`
        for more details.
    :param watch: if given print output every *watch* seconds.
    :param w: if ``True`` print output every 2 seconds.
    """
    ctx.forward(members, fmt='topology')


def timestamp(precision: int = 6) -> str:
    """Get current timestamp with given *precision* as a string.

    :param precision: Amount of digits to be present in the precision.

    :returns: the current timestamp with given *precision*.
    """
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:precision - 7]


@ctl.command('flush', help='Discard scheduled events')
@click.argument('cluster_name')
@option_citus_group
@click.argument('member_names', nargs=-1)
@click.argument('target', type=click.Choice(['restart', 'switchover']))
@click.option('--role', '-r', help='Flush only members with this role',
              type=role_choice, default=repr(CtlPostgresqlRole.ANY))
@option_force
def flush(cluster_name: str, group: Optional[int],
          member_names: List[str], force: bool, role: CtlPostgresqlRole, target: str) -> None:
    """Process ``flush`` command of ``patronictl`` utility.

    Discard scheduled restart or switchover events.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should flush an event. Refer to the module note for more details.
    :param member_names: name of the members which events should be flushed.
    :param force: perform the operation without asking for confirmations.
    :param role: role to filter members. See :func:`get_all_members` for available options.
    :param target: the event that should be flushed -- ``restart`` or ``switchover``.
    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()

    if target == 'restart':
        for member in get_members(cluster, cluster_name, member_names, role, force, 'flush', group=group):
            if member.data.get('scheduled_restart'):
                r = request_patroni(member, 'delete', 'restart')
                check_response(r, member.name, 'flush scheduled restart')
            else:
                click.echo('No scheduled restart for member {0}'.format(member.name))
    elif target == 'switchover':
        failover = cluster.failover
        if not failover or not failover.scheduled_at:
            return click.echo('No pending scheduled switchover')
        for member in get_all_members_leader_first(cluster):
            try:
                r = request_patroni(member, 'delete', 'switchover')
                if r.status in (200, 404):
                    prefix = 'Success' if r.status == 200 else 'Failed'
                    return click.echo('{0}: {1}'.format(prefix, r.data.decode('utf-8')))

                click.echo('Failed: member={0}, status_code={1}, ({2})'.format(
                    member.name, r.status, r.data.decode('utf-8')))
            except Exception as err:
                logging.warning(str(err))
                logging.warning('Member %s is not accessible', member.name)

        logging.warning('Failing over to DCS')
        click.echo('{0} Could not find any accessible member of cluster {1}'.format(timestamp(), cluster_name))
        dcs.manual_failover('', '', version=failover.version)


def wait_until_pause_is_applied(dcs: AbstractDCS, paused: bool, old_cluster: Cluster) -> None:
    """Wait for all members in the cluster to have ``pause`` state set to *paused*.

    :param dcs: DCS object from where to get fresh cluster information.
    :param paused: the desired state for ``pause`` in all nodes.
    :param old_cluster: original cluster information before pause or unpause has been requested. Used to report which
        nodes are still pending to have ``pause`` equal *paused* at a given point in time.
    """
    config = global_config.from_cluster(old_cluster)

    click.echo("'{0}' request sent, waiting until it is recognized by all nodes".format(paused and 'pause' or 'resume'))
    old = {m.name: m.version for m in old_cluster.members if m.api_url}
    loop_wait = config.get('loop_wait') or dcs.loop_wait

    cluster = None
    for _ in polling_loop(loop_wait + 1):
        cluster = dcs.get_cluster()
        if all(m.data.get('pause', False) == paused for m in cluster.members if m.name in old):
            break
    else:
        if TYPE_CHECKING:  # pragma: no cover
            assert cluster is not None
        remaining = [m.name for m in cluster.members if m.data.get('pause', False) != paused
                     and m.name in old and old[m.name] != m.version]
        if remaining:
            return click.echo("{0} members didn't recognized pause state after {1} seconds"
                              .format(', '.join(remaining), loop_wait))
    return click.echo('Success: cluster management is {0}'.format(paused and 'paused' or 'resumed'))


def toggle_pause(cluster_name: str, group: Optional[int], paused: bool, wait: bool) -> None:
    """Toggle the ``pause`` state in the cluster members.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should toggle the pause state of. Refer to the module note for more
        details.
    :param paused: the desired state for ``pause`` in all nodes.
    :param wait: ``True`` if it should block until the operation is finished or ``false`` for returning immediately.

    :raises:
        PatroniCtlException: if
            * ``pause`` state is already *paused*; or
            * cluster contains no accessible members.
    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()
    if global_config.from_cluster(cluster).is_paused == paused:
        raise PatroniCtlException('Cluster is {0} paused'.format(paused and 'already' or 'not'))

    for member in get_all_members_leader_first(cluster):
        try:
            r = request_patroni(member, 'patch', 'config', {'pause': paused or None})
        except Exception as err:
            logging.warning(str(err))
            logging.warning('Member %s is not accessible', member.name)
            continue

        if r.status == 200:
            if wait:
                wait_until_pause_is_applied(dcs, paused, cluster)
            else:
                click.echo('Success: cluster management is {0}'.format(paused and 'paused' or 'resumed'))
        else:
            click.echo('Failed: {0} cluster management status code={1}, ({2})'.format(
                       paused and 'pause' or 'resume', r.status, r.data.decode('utf-8')))
        break
    else:
        raise PatroniCtlException('Can not find accessible cluster member')


@ctl.command('pause', help='Disable auto failover')
@arg_cluster_name
@option_default_citus_group
@click.option('--wait', help='Wait until pause is applied on all nodes', is_flag=True)
def pause(cluster_name: str, group: Optional[int], wait: bool) -> None:
    """Process ``pause`` command of ``patronictl`` utility.

    Put the cluster in maintenance mode.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should pause. Refer to the module note for more details.
    :param wait: ``True`` if it should block until the operation is finished or ``false`` for returning immediately.
    """
    return toggle_pause(cluster_name, group, True, wait)


@ctl.command('resume', help='Resume auto failover')
@arg_cluster_name
@option_default_citus_group
@click.option('--wait', help='Wait until pause is cleared on all nodes', is_flag=True)
def resume(cluster_name: str, group: Optional[int], wait: bool) -> None:
    """Process ``unpause`` command of ``patronictl`` utility.

    Put the cluster out of maintenance mode.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should unpause. Refer to the module note for more details.
    :param wait: ``True`` if it should block until the operation is finished or ``false`` for returning immediately.
    """
    return toggle_pause(cluster_name, group, False, wait)


@contextmanager
def temporary_file(contents: bytes, suffix: str = '', prefix: str = 'tmp') -> Iterator[str]:
    """Create a temporary file with specified contents that persists for the context.

    :param contents: binary string that will be written to the file.
    :param prefix: will be prefixed to the filename.
    :param suffix: will be appended to the filename.

    :yields: path of the created file.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix=prefix, delete=False)
    with tmp:
        tmp.write(contents)

    try:
        yield tmp.name
    finally:
        os.unlink(tmp.name)


def show_diff(before_editing: str, after_editing: str) -> None:
    """Show a diff between two strings.

    Inputs are expected to be unicode strings.

    If the output is to a tty the diff will be colored.

    .. note::
        If tty it requires a pager program, and uses first found among:
            * Program given by ``PAGER`` environment variable; or
            * ``less``; or
            * ``more``.

    :param before_editing: string to be compared with *after_editing*.
    :param after_editing: string to be compared with *before_editing*.

    :raises:
        :class:`PatroniCtlException`: if no suitable pager can be found when printing diff output to a tty.
    """
    def listify(string: str) -> List[str]:
        return [line + '\n' for line in string.rstrip('\n').split('\n')]

    unified_diff = difflib.unified_diff(listify(before_editing), listify(after_editing))

    if sys.stdout.isatty():
        buf = io.BytesIO()
        for line in unified_diff:
            buf.write(line.encode('utf-8'))
        buf.seek(0)

        class opts:
            theme = 'default'
            side_by_side = False
            width = 80
            tab_width = 8
            wrap = True
            pager = next(
                (
                    os.path.basename(p)
                    for p in (os.environ.get('PAGER'), "less", "more")
                    if p is not None and bool(shutil.which(p))
                ),
                None,
            )
            pager_options = None

        if opts.pager is None:
            raise PatroniCtlException(
                'No pager could be found. Either set PAGER environment variable with '
                'your pager or install either "less" or "more" in the host.'
            )

        # if we end up selecting "less" as "pager" then we set "pager" attribute
        # to "None". "less" is the default pager for "ydiff" module, and that
        # module adds some command-line options to "less" when "pager" is "None"
        if opts.pager == 'less':
            opts.pager = None

        markup_to_pager(PatchStream(buf), opts)
    else:
        for line in unified_diff:
            click.echo(line.rstrip('\n'))


def format_config_for_editing(data: Any, default_flow_style: bool = False) -> str:
    """Format configuration as YAML for human consumption.

    :param data: configuration as nested dictionaries.
    :param default_flow_style: passed down as ``default_flow_style`` argument of :func:`yaml.safe_dump`.

    :returns: unicode YAML of the configuration.
    """
    return yaml.safe_dump(data, default_flow_style=default_flow_style, encoding=None, allow_unicode=True, width=200)


def apply_config_changes(before_editing: str, data: Dict[str, Any], kvpairs: List[str]) -> Tuple[str, Dict[str, Any]]:
    """Apply config changes specified as a list of key-value pairs.

    Keys are interpreted as dotted paths into the configuration data structure. Except for paths beginning with
    ``postgresql.parameters`` where rest of the path is used directly to allow for PostgreSQL GUCs containing dots.
    Values are interpreted as YAML values.

    :param before_editing: human representation before editing.
    :param data: configuration data structure.
    :param kvpairs: list of strings containing key value pairs separated by ``=``.

    :returns: tuple of human-readable, parsed data structure after changes.

    :raises:
        :class:`PatroniCtlException`: if any entry in *kvpairs* is ``None`` or not in the expected format.
    """
    changed_data = copy.deepcopy(data)

    def set_path_value(config: Dict[str, Any], path: List[str], value: Any, prefix: Tuple[str, ...] = ()) -> None:
        """Recursively walk through *config* and update setting specified by *path* with *value*.

        :param config: configuration data structure with all settings found under *prefix* path.
        :param path: dotted path split by dot as delimiter into a list. Used to control the recursive calls and identify
            when a leaf node is reached.
        :param value: value for configuration described by *path*. If ``None`` the configuration key is removed from
            *config*.
        :param prefix: previous parts of *path* that have already been opened by parent recursive calls. Used to know
            if we are changing a Postgres related setting or not. *prefix* plus *path* compose the original *path* given
            on the root call.
        """
        # Postgresql GUCs can't be nested, but can contain dots so we re-flatten the structure for this case
        if prefix == ('postgresql', 'parameters'):
            path = ['.'.join(path)]

        key = path[0]
        # When *path* contains a single item it means we reached a leaf node in the configuration, so we can remove or
        # update the configuration based on what has been requested by the user.
        if len(path) == 1:
            if value is None:
                config.pop(key, None)
            else:
                config[key] = value
        # Otherwise we need to keep navigating down in the configuration structure.
        else:
            if not isinstance(config.get(key), dict):
                config[key] = {}
            set_path_value(config[key], path[1:], value, prefix + (key,))
            if config[key] == {}:
                del config[key]

    for pair in kvpairs:
        if not pair or "=" not in pair:
            raise PatroniCtlException("Invalid parameter setting {0}".format(pair))
        key_path, value = pair.split("=", 1)
        set_path_value(changed_data, key_path.strip().split("."), yaml.safe_load(value))

    return format_config_for_editing(changed_data), changed_data


def apply_yaml_file(data: Dict[str, Any], filename: str) -> Tuple[str, Dict[str, Any]]:
    """Apply changes from a YAML file to configuration.

    :param data: configuration data structure.
    :param filename: name of the YAML file, ``-`` is taken to mean standard input.

    :returns: tuple of human-readable and parsed data structure after changes.
    """
    changed_data = copy.deepcopy(data)

    if filename == '-':
        new_options = yaml.safe_load(sys.stdin)
    else:
        with open(filename) as fd:
            new_options = yaml.safe_load(fd)

    patch_config(changed_data, new_options)

    return format_config_for_editing(changed_data), changed_data


def invoke_editor(before_editing: str, cluster_name: str) -> Tuple[str, Dict[str, Any]]:
    """Start editor command to edit configuration in human readable format.

    .. note::
        Requires an editor program, and uses first found among:
            * Program given by ``EDITOR`` environment variable; or
            * ``editor``; or
            * ``vi``.

    :param before_editing: human representation before editing.
    :param cluster_name: name of the Patroni cluster.

    :returns: tuple of human-readable, parsed data structure after changes.

    :raises:
        :class:`PatroniCtlException`: if
            * No suitable editor can be found; or
            * Editor call exits with unexpected return code.
    """
    editor_cmd = os.environ.get('EDITOR')
    if not editor_cmd:
        for editor in ('editor', 'vi'):
            editor_cmd = shutil.which(editor)
            if editor_cmd:
                logging.debug('Setting fallback editor_cmd=%s', editor)
                break
    if not editor_cmd:
        raise PatroniCtlException('EDITOR environment variable is not set. editor or vi are not available')

    safe_cluster_name = cluster_name.replace("/", "_")
    with temporary_file(contents=before_editing.encode('utf-8'),
                        suffix='.yaml',
                        prefix='{0}-config-'.format(safe_cluster_name)) as tmpfile:
        ret = subprocess.call([editor_cmd, tmpfile])
        if ret:
            raise PatroniCtlException("Editor exited with return code {0}".format(ret))

        with codecs.open(tmpfile, encoding='utf-8') as fd:
            after_editing = fd.read()

        return after_editing, yaml.safe_load(after_editing)


@ctl.command('edit-config', help="Edit cluster configuration")
@arg_cluster_name
@option_default_citus_group
@click.option('--quiet', '-q', is_flag=True, help='Do not show changes')
@click.option('--set', '-s', 'kvpairs', multiple=True,
              help='Set specific configuration value. Can be specified multiple times')
@click.option('--pg', '-p', 'pgkvpairs', multiple=True,
              help='Set specific PostgreSQL parameter value. Shorthand for -s postgresql.parameters. '
                   'Can be specified multiple times')
@click.option('--apply', 'apply_filename', help='Apply configuration from file. Use - for stdin.')
@click.option('--replace', 'replace_filename', help='Apply configuration from file, replacing existing configuration.'
              ' Use - for stdin.')
@option_force
def edit_config(cluster_name: str, group: Optional[int], force: bool, quiet: bool, kvpairs: List[str],
                pgkvpairs: List[str], apply_filename: Optional[str], replace_filename: Optional[str]) -> None:
    """Process ``edit-config`` command of ``patronictl`` utility.

    Update or replace Patroni configuration in the DCS.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group configuration we should edit. Refer to the module note for more details.
    :param force: if ``True`` apply config changes without asking for confirmations.
    :param quiet: if ``True`` skip showing config diff in the console.
    :param kvpairs: list of key value general parameters to be changed.
    :param pgkvpairs: list of key value Postgres parameters to be changed.
    :param apply_filename: name of the file which contains with new configuration parameters to be applied. Pass ``-``
        for using stdin instead.
    :param replace_filename: name of the file which contains the new configuration parameters to replace the existing
        configuration. Pass ``-`` for using stdin instead.

    :raises:
        :class:`PatroniCtlException`: if
            * Configuration is absent from DCS; or
            * Detected a concurrent modification of the configuration in the DCS.
    """
    dcs = get_dcs(cluster_name, group)
    cluster = dcs.get_cluster()

    if not cluster.config:
        raise PatroniCtlException('The config key does not exist in the cluster {0}'.format(cluster_name))

    before_editing = format_config_for_editing(cluster.config.data)

    after_editing = None  # Serves as a flag if any changes were requested
    changed_data = cluster.config.data

    if replace_filename:
        after_editing, changed_data = apply_yaml_file({}, replace_filename)

    if apply_filename:
        after_editing, changed_data = apply_yaml_file(changed_data, apply_filename)

    if kvpairs or pgkvpairs:
        all_pairs = list(kvpairs) + ['postgresql.parameters.' + v.lstrip() for v in pgkvpairs]
        after_editing, changed_data = apply_config_changes(before_editing, changed_data, all_pairs)

    # If no changes were specified on the command line invoke editor
    if after_editing is None:
        after_editing, changed_data = invoke_editor(before_editing, cluster_name)

    if cluster.config.data == changed_data:
        if not quiet:
            click.echo("Not changed")
        return

    if not quiet:
        show_diff(before_editing, after_editing)

    if (apply_filename == '-' or replace_filename == '-') and not force:
        click.echo("Use --force option to apply changes")
        return

    if force or click.confirm('Apply these changes?'):
        if not dcs.set_config_value(json.dumps(changed_data, separators=(',', ':')), cluster.config.version):
            raise PatroniCtlException("Config modification aborted due to concurrent changes")
        click.echo("Configuration changed")


@ctl.command('show-config', help="Show cluster configuration")
@arg_cluster_name
@option_default_citus_group
def show_config(cluster_name: str, group: Optional[int]) -> None:
    """Process ``show-config`` command of ``patronictl`` utility.

    Show Patroni configuration stored in the DCS.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group configuration we should show. Refer to the module note for more details.
    """
    cluster = get_dcs(cluster_name, group).get_cluster()
    if cluster.config:
        click.echo(format_config_for_editing(cluster.config.data))


@ctl.command('version', help='Output version of patronictl command or a running Patroni instance')
@click.argument('cluster_name', required=False)
@click.argument('member_names', nargs=-1)
@option_citus_group
def version(cluster_name: str, group: Optional[int], member_names: List[str]) -> None:
    """Process ``version`` command of ``patronictl`` utility.

    Show version of:
        * ``patronictl`` on invoker;
        * ``patroni`` on all members of the cluster;
        * ``PostgreSQL`` on all members of the cluster.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should get members from. Refer to the module note for more details.
    :param member_names: filter which members we should get version information from.
    """
    click.echo("patronictl version {0}".format(__version__))

    if not cluster_name:
        return

    click.echo("")
    cluster = get_dcs(cluster_name, group).get_cluster()
    for m in get_all_members(cluster, group, CtlPostgresqlRole.ANY):
        if m.api_url:
            if not member_names or m.name in member_names:
                try:
                    response = request_patroni(m)
                    data = json.loads(response.data.decode('utf-8'))
                    version = data.get('patroni', {}).get('version')
                    pg_version = data.get('server_version')
                    pg_version_str = " PostgreSQL {0}".format(format_pg_version(pg_version)) if pg_version else ""
                    click.echo("{0}: Patroni {1}{2}".format(m.name, version, pg_version_str))
                except Exception as e:
                    click.echo("{0}: failed to get version: {1}".format(m.name, e))


@ctl.command('history', help="Show the history of failovers/switchovers")
@arg_cluster_name
@option_default_citus_group
@option_format
def history(cluster_name: str, group: Optional[int], fmt: str) -> None:
    """Process ``history`` command of ``patronictl`` utility.

    Show the history of failover/switchover events in the cluster.

    Information is printed to console through :func:`print_output`, and contains:
        * ``TL``: Postgres timeline when the event occurred;
        * ``LSN``: Postgres LSN, in bytes, when the event occurred;
        * ``Reason``: the reason that motivated the event, if any;
        * ``Timestamp``: timestamp when the event occurred;
        * ``New Leader``: the Postgres node that was promoted during the event.

    :param cluster_name: name of the Patroni cluster.
    :param group: filter which Citus group we should get events from. Refer to the module note for more details.
    :param fmt: the output table printing format. See :func:`print_output` for available options.
    """
    cluster = get_dcs(cluster_name, group).get_cluster()
    cluster_history = cluster.history.lines if cluster.history else []
    history: List[List[Any]] = list(map(list, cluster_history))
    table_header_row = ['TL', 'LSN', 'Reason', 'Timestamp', 'New Leader']
    for line in history:
        if len(line) < len(table_header_row):
            add_column_num = len(table_header_row) - len(line)
            for _ in range(add_column_num):
                line.append('')
    print_output(table_header_row, history, {'TL': 'r', 'LSN': 'r'}, fmt)


def format_pg_version(version: int) -> str:
    """Format Postgres version for human consumption.

    :param version: Postgres version represented as an integer.

    :returns: Postgres version represented as a human-readable string.

    :Example:

        >>> format_pg_version(90624)
        '9.6.24'

        >>> format_pg_version(100000)
        '10.0'

        >>> format_pg_version(140008)
        '14.8'
    """
    if version < 100000:
        return "{0}.{1}.{2}".format(version // 10000, version // 100 % 100, version % 100)
    else:
        return "{0}.{1}".format(version // 10000, version % 100)


def change_cluster_role(cluster_name: str, force: bool, standby_config: Optional[Dict[str, Any]]) -> None:
    """Demote or promote cluster.

    :param cluster_name: name of the Patroni cluster.
    :param force: if ``True`` run cluster demotion without asking for confirmation.
    :param standby_config: standby cluster configuration to be applied if demotion is requested.
    """
    demote = bool(standby_config)
    action_name = 'demot' if demote else 'promot'
    target_role = PostgresqlRole.STANDBY_LEADER if demote else PostgresqlRole.PRIMARY

    dcs = get_dcs(cluster_name, None)
    cluster = dcs.get_cluster()
    leader_name = cluster.leader and cluster.leader.name
    if not leader_name:
        raise PatroniCtlException(f'Cluster has no leader, {action_name}ion is not possible')
    if cluster.leader and cluster.leader.data.get('role') == target_role:
        raise PatroniCtlException('Cluster is already in the required state')

    click.echo('Current cluster topology')
    output_members(cluster, cluster_name)
    if not force:
        confirm = click.confirm(f'Are you sure you want to {action_name}e {cluster_name} cluster?')
        if not confirm:
            raise PatroniCtlException(f'Aborted cluster {action_name}ion')

    try:
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(cluster.leader, Leader)
        r = request_patroni(cluster.leader.member, 'patch', 'config', {'standby_cluster': standby_config})

        if r.status != 200:
            raise PatroniCtlException(
                f'Failed to {action_name}e {cluster_name} cluster: '
                f'/config PATCH status code={r.status}, ({r.data.decode("utf-8")})')
    except Exception as err:
        raise PatroniCtlException(f'Failed to {action_name}e {cluster_name} cluster: {err}')

    for _ in watching(True, 1, clear=False):
        cluster = dcs.get_cluster()
        is_unlocked = cluster.is_unlocked()
        leader_role = cluster.leader and cluster.leader.data.get('role')
        leader_state = cluster.leader and cluster.leader.data.get('state')
        old_leader = cluster.get_member(leader_name, False)
        old_leader_state = old_leader and old_leader.data.get('state')

        if not is_unlocked and leader_role == target_role and leader_state == PostgresqlState.RUNNING:
            if not demote or old_leader_state == PostgresqlState.RUNNING:
                click.echo(
                    f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} cluster is successfully {action_name}ed')
                break

        state_prts = [f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} cluster is unlocked: {is_unlocked}',
                      f'leader role: {leader_role}',
                      f'leader state: {leader_state}']
        if demote and cluster.leader and leader_name != cluster.leader.name and old_leader_state:
            state_prts.append(f'previous leader state: {repr(old_leader_state)}')
        click.echo(", ".join(state_prts))
    output_members(cluster, cluster_name)


@ctl.command('demote-cluster', help="Demote cluster to a standby cluster")
@arg_cluster_name
@option_force
@click.option('--host', help='Address of the remote node', required=False)
@click.option('--port', help='Port of the remote node', type=int, required=False)
@click.option('--restore-command', help='Command to restore WAL records from the remote primary', required=False)
@click.option('--primary-slot-name', help='Name of the slot on the remote node to use for replication', required=False)
def demote_cluster(cluster_name: str, force: bool, host: Optional[str], port: Optional[int],
                   restore_command: Optional[str], primary_slot_name: Optional[str]) -> None:
    """Process ``demote-cluster`` command of ``patronictl`` utility.

    Demote cluster to a standby cluster.

    :param cluster_name: name of the Patroni cluster.
    :param force: if ``True`` run cluster demotion without asking for confirmation.
    :param host: address of the remote node.
    :param port: port of the remote node.
    :param restore_command: command to restore WAL records from the remote primary'.
    :param primary_slot_name: name of the slot on the remote node to use for replication.

    :raises:
        :class:`PatroniCtlException`: if:
            * neither ``host`` nor ``port`` nor ``restore_command`` is provided; or
            * cluster has no leader; or
            * cluster is already in the required state; or
            * operation is aborted.
    """
    if not any((host, port, restore_command)):
        raise PatroniCtlException('At least --host, --port or --restore-command should be specified')

    data = {k: v for k, v in {'host': host,
                              'port': port,
                              'primary_slot_name': primary_slot_name,
                              'restore_command': restore_command}.items() if v}
    change_cluster_role(cluster_name, force, data)


@ctl.command('promote-cluster', help="Promote cluster, make it run standalone")
@arg_cluster_name
@option_force
def promote_cluster(cluster_name: str, force: bool) -> None:
    """Process ``promote-cluster`` command of ``patronictl`` utility.

    Promote cluster, make it run standalone.

    :param cluster_name: name of the Patroni cluster.
    :param force: if ``True`` run cluster demotion without asking for confirmation.
    """
    change_cluster_role(cluster_name, force, None)
