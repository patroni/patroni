'''
Patroni Control
'''

import click
import codecs
import datetime
import dateutil.parser
import dateutil.tz
import cdiff
import copy
import difflib
import io
import json
import logging
import os
import random
import six
import subprocess
import sys
import tempfile
import time
import yaml

from click import ClickException
from collections import defaultdict
from contextlib import contextmanager
from patroni.dcs import get_dcs as _get_dcs
from patroni.exceptions import PatroniException
from patroni.postgresql import Postgresql
from patroni.postgresql.misc import postgres_version_to_int
from patroni.utils import cluster_as_json, patch_config, polling_loop
from patroni.request import PatroniRequest
from patroni.version import __version__
from prettytable import ALL, FRAME, PrettyTable
from six.moves.urllib_parse import urlparse

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronictl.yaml')
DCS_DEFAULTS = {'zookeeper': {'port': 2181, 'template': "zookeeper:\n hosts: ['{host}:{port}']"},
                'exhibitor': {'port': 8181, 'template': "exhibitor:\n hosts: [{host}]\n port: {port}"},
                'consul': {'port': 8500, 'template': "consul:\n host: '{host}:{port}'"},
                'etcd': {'port': 2379, 'template': "etcd:\n host: '{host}:{port}'"}}


class PatroniCtlException(ClickException):
    pass


class PatronictlPrettyTable(PrettyTable):

    def __init__(self, header, *args, **kwargs):
        PrettyTable.__init__(self, *args, **kwargs)
        self.__table_header = header
        self.__hline_num = 0
        self.__hline = None

    def _is_first_hline(self):
        return self.__hline_num == 0

    def _set_hline(self, value):
        self.__hline = value

    def _get_hline(self):
        ret = self.__hline

        # Inject nice table header
        if self._is_first_hline() and self.__table_header:
            header = self.__table_header[:len(ret) - 2]
            ret = "".join([ret[0], header, ret[1 + len(header):]])

        self.__hline_num += 1
        return ret

    _hrule = property(_get_hline, _set_hline)


def parse_dcs(dcs):
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
    return yaml.safe_load(default['template'].format(host=parsed.hostname or 'localhost', port=port or default['port']))


def load_config(path, dcs):
    from patroni.config import Config

    if not (os.path.exists(path) and os.access(path, os.R_OK)):
        if path != CONFIG_FILE_PATH:    # bail if non-default config location specified but file not found / readable
            raise PatroniCtlException('Provided config file {0} not existing or no read rights.'
                                      ' Check the -c/--config-file parameter'.format(path))
        else:
            logging.debug('Ignoring configuration file "%s". It does not exists or is not readable.', path)
    else:
        logging.debug('Loading configuration from file %s', path)
    config = Config(path, validator=None).copy()

    dcs = parse_dcs(dcs) or parse_dcs(config.get('dcs_api')) or {}
    if dcs:
        for d in DCS_DEFAULTS:
            config.pop(d, None)
        config.update(dcs)
    return config


def store_config(config, path):
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    with open(path, 'w') as fd:
        yaml.dump(config, fd)


option_format = click.option('--format', '-f', 'fmt', help='Output format (pretty, tsv, json, yaml)', default='pretty')
option_watchrefresh = click.option('-w', '--watch', type=float, help='Auto update the screen every X seconds')
option_watch = click.option('-W', is_flag=True, help='Auto update the screen every 2 seconds')
option_force = click.option('--force', is_flag=True, help='Do not ask for confirmation at any point')
arg_cluster_name = click.argument('cluster_name', required=False,
                                  default=lambda: click.get_current_context().obj.get('scope'))
option_insecure = click.option('-k', '--insecure', is_flag=True, help='Allow connections to SSL sites without certs')


@click.group()
@click.option('--config-file', '-c', help='Configuration file',
              envvar='PATRONICTL_CONFIG_FILE', default=CONFIG_FILE_PATH)
@click.option('--dcs', '-d', help='Use this DCS', envvar='DCS')
@option_insecure
@click.pass_context
def ctl(ctx, config_file, dcs, insecure):
    level = 'WARNING'
    for name in ('LOGLEVEL', 'PATRONI_LOGLEVEL', 'PATRONI_LOG_LEVEL'):
        level = os.environ.get(name, level)
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    logging.captureWarnings(True)  # Capture eventual SSL warning
    ctx.obj = load_config(config_file, dcs)
    # backward compatibility for configuration file where ctl section is not define
    ctx.obj.setdefault('ctl', {})['insecure'] = ctx.obj.get('ctl', {}).get('insecure') or insecure


def get_dcs(config, scope):
    config.update({'scope': scope, 'patronictl': True})
    config.setdefault('name', scope)
    try:
        return _get_dcs(config)
    except PatroniException as e:
        raise PatroniCtlException(str(e))


def request_patroni(member, method='GET', endpoint=None, data=None):
    ctx = click.get_current_context()  # the current click context
    request_executor = ctx.obj.get('__request_patroni')
    if not request_executor:
        request_executor = ctx.obj['__request_patroni'] = PatroniRequest(ctx.obj)
    return request_executor(member, method, endpoint, data)


def print_output(columns, rows, alignment=None, fmt='pretty', header=None, delimiter='\t'):
    if fmt in {'json', 'yaml', 'yml'}:
        elements = [{k: v for k, v in zip(columns, r) if not header or str(v)} for r in rows]
        func = json.dumps if fmt == 'json' else format_config_for_editing
        click.echo(func(elements))
    elif fmt in {'pretty', 'tsv', 'topology'}:
        list_cluster = bool(header and columns and columns[0] == 'Cluster')
        if list_cluster and 'Tags' in columns:  # we want to format member tags as YAML
            i = columns.index('Tags')
            for row in rows:
                if row[i]:
                    row[i] = format_config_for_editing(row[i], fmt != 'pretty').strip()
        if list_cluster and fmt != 'tsv':  # skip cluster name if pretty-printing
            columns = columns[1:] if columns else []
            rows = [row[1:] for row in rows]

        if fmt == 'tsv':
            for r in ([columns] if columns else []) + rows:
                click.echo(delimiter.join(map(str, r)))
        else:
            hrules = ALL if any(any(isinstance(c, six.string_types) and '\n' in c for c in r) for r in rows) else FRAME
            table = PatronictlPrettyTable(header, columns, hrules=hrules)
            table.align = 'l'
            for k, v in (alignment or {}).items():
                table.align[k] = v
            for r in rows:
                table.add_row(r)
            click.echo(table)


def watching(w, watch, max_count=None, clear=True):
    """
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
    while watch and counter <= (max_count or counter):
        time.sleep(watch)
        counter += 1
        if clear:
            click.clear()
        yield 0


def get_all_members(cluster, role='master'):
    if role == 'master':
        if cluster.leader is not None and cluster.leader.name:
            yield cluster.leader
        return

    leader_name = (cluster.leader.member.name if cluster.leader else None)
    for m in cluster.members:
        if role == 'any' or role == 'replica' and m.name != leader_name:
            yield m


def get_any_member(cluster, role='master', member=None):
    members = get_all_members(cluster, role)
    for m in members:
        if member is None or m.name == member:
            return m


def get_all_members_leader_first(cluster):
    leader_name = cluster.leader.member.name if cluster.leader and cluster.leader.member.api_url else None
    if leader_name:
        yield cluster.leader.member
    for member in cluster.members:
        if member.api_url and member.name != leader_name:
            yield member


def get_cursor(cluster, connect_parameters, role='master', member=None):
    member = get_any_member(cluster, role=role, member=member)
    if member is None:
        return None

    params = member.conn_kwargs(connect_parameters)
    params.update({'fallback_application_name': 'Patroni ctl', 'connect_timeout': '5'})
    if 'database' in connect_parameters:
        params['database'] = connect_parameters['database']
    else:
        params.pop('database')

    import psycopg2
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cursor = conn.cursor()
    if role == 'any':
        return cursor

    cursor.execute('SELECT pg_catalog.pg_is_in_recovery()')
    in_recovery = cursor.fetchone()[0]

    if in_recovery and role == 'replica' or not in_recovery and role == 'master':
        return cursor

    conn.close()

    return None


def get_members(cluster, cluster_name, member_names, role, force, action, ask_confirmation=True):
    candidates = {m.name: m for m in cluster.members}

    if not force or role:
        if not member_names and not candidates:
            raise PatroniCtlException('{0} cluster doesn\'t have any members'.format(cluster_name))
        output_members(cluster, cluster_name)

    if role:
        role_names = [m.name for m in get_all_members(cluster, role)]
        if member_names:
            member_names = list(set(member_names) & set(role_names))
            if not member_names:
                raise PatroniCtlException('No {0} among provided members'.format(role))
        else:
            member_names = role_names

    if not member_names and not force:
        member_names = [click.prompt('Which member do you want to {0} [{1}]?'.format(action,
                        ', '.join(candidates.keys())), type=str, default='')]

    for member_name in member_names:
        if member_name not in candidates:
            raise PatroniCtlException('{0} is not a member of cluster'.format(member_name))

    members = [candidates[n] for n in member_names]
    if ask_confirmation:
        confirm_members_action(members, force, action)
    return members


def confirm_members_action(members, force, action, scheduled_at=None):
    if scheduled_at:
        if not force:
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


@ctl.command('dsn', help='Generate a dsn for the provided member, defaults to a dsn of the master')
@click.option('--role', '-r', help='Give a dsn of any member with this role', type=click.Choice(['master', 'replica',
              'any']), default=None)
@click.option('--member', '-m', help='Generate a dsn for this member', type=str)
@arg_cluster_name
@click.pass_obj
def dsn(obj, cluster_name, role, member):
    if role is not None and member is not None:
        raise PatroniCtlException('--role and --member are mutually exclusive options')
    if member is None and role is None:
        role = 'master'

    cluster = get_dcs(obj, cluster_name).get_cluster()
    m = get_any_member(cluster, role=role, member=member)
    if m is None:
        raise PatroniCtlException('Can not find a suitable member')

    params = m.conn_kwargs()
    click.echo('host={host} port={port}'.format(**params))


@ctl.command('query', help='Query a Patroni PostgreSQL member')
@arg_cluster_name
@click.option('--format', 'fmt', help='Output format (pretty, tsv, json, yaml)', default='tsv')
@click.option('--file', '-f', 'p_file', help='Execute the SQL commands from this file', type=click.File('rb'))
@click.option('--password', help='force password prompt', is_flag=True)
@click.option('-U', '--username', help='database user name', type=str)
@option_watch
@option_watchrefresh
@click.option('--role', '-r', help='The role of the query', type=click.Choice(['master', 'replica', 'any']),
              default=None)
@click.option('--member', '-m', help='Query a specific member', type=str)
@click.option('--delimiter', help='The column delimiter', default='\t')
@click.option('--command', '-c', help='The SQL commands to execute')
@click.option('-d', '--dbname', help='database name to connect to', type=str)
@click.pass_obj
def query(
    obj,
    cluster_name,
    role,
    member,
    w,
    watch,
    delimiter,
    command,
    p_file,
    password,
    username,
    dbname,
    fmt='tsv',
):
    if role is not None and member is not None:
        raise PatroniCtlException('--role and --member are mutually exclusive options')
    if member is None and role is None:
        role = 'master'

    if p_file is not None and command is not None:
        raise PatroniCtlException('--file and --command are mutually exclusive options')

    if p_file is None and command is None:
        raise PatroniCtlException('You need to specify either --command or --file')

    connect_parameters = {}
    if username:
        connect_parameters['username'] = username
    if password:
        connect_parameters['password'] = click.prompt('Password', hide_input=True, type=str)
    if dbname:
        connect_parameters['database'] = dbname

    if p_file is not None:
        command = p_file.read()

    dcs = get_dcs(obj, cluster_name)

    cursor = None
    for _ in watching(w, watch, clear=False):
        if cursor is None:
            cluster = dcs.get_cluster()

        output, header = query_member(cluster, cursor, member, role, command, connect_parameters)
        print_output(header, output, fmt=fmt, delimiter=delimiter)


def query_member(cluster, cursor, member, role, command, connect_parameters):
    import psycopg2
    try:
        if cursor is None:
            cursor = get_cursor(cluster, connect_parameters, role=role, member=member)

        if cursor is None:
            if role is None:
                message = 'No connection to member {0} is available'.format(member)
            else:
                message = 'No connection to role={0} is available'.format(role)
            logging.debug(message)
            return [[timestamp(0), message]], None

        cursor.execute(command)
        return cursor.fetchall(), [d.name for d in cursor.description]
    except (psycopg2.OperationalError, psycopg2.DatabaseError) as oe:
        logging.debug(oe)
        if cursor is not None and not cursor.connection.closed:
            cursor.connection.close()
        message = oe.pgcode or oe.pgerror or str(oe)
        message = message.replace('\n', ' ')
        return [[timestamp(0), 'ERROR, SQLSTATE: {0}'.format(message)]], None


@ctl.command('remove', help='Remove cluster from DCS')
@click.argument('cluster_name')
@option_format
@click.pass_obj
def remove(obj, cluster_name, fmt):
    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()

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
        confirm = click.prompt('This cluster currently is healthy. Please specify the master name to continue')
        if confirm != cluster.leader.name:
            raise PatroniCtlException('You did not specify the current master of the cluster')

    dcs.delete_cluster()


def check_response(response, member_name, action_name, silent_success=False):
    if response.status >= 400:
        click.echo('Failed: {0} for member {1}, status code={2}, ({3})'.format(
            action_name, member_name, response.status, response.data.decode('utf-8')
        ))
        return False
    elif not silent_success:
        click.echo('Success: {0} for member {1}'.format(action_name, member_name))
    return True


def parse_scheduled(scheduled):
    if (scheduled or 'now') != 'now':
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
@click.option('--role', '-r', help='Reload only members with this role', default='any',
              type=click.Choice(['master', 'replica', 'any']))
@option_force
@click.pass_obj
def reload(obj, cluster_name, member_names, force, role):
    cluster = get_dcs(obj, cluster_name).get_cluster()

    members = get_members(cluster, cluster_name, member_names, role, force, 'reload')

    for member in members:
        r = request_patroni(member, 'post', 'reload')
        if r.status == 200:
            click.echo('No changes to apply on member {0}'.format(member.name))
        elif r.status == 202:
            click.echo('Reload request received for member {0} and will be processed within {1} seconds'.format(
                member.name, cluster.config.data.get('loop_wait'))
            )
        else:
            click.echo('Failed: reload for member {0}, status code={1}, ({2})'.format(
                member.name, r.status, r.data.decode('utf-8'))
            )


@ctl.command('restart', help='Restart cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@click.option('--role', '-r', help='Restart only members with this role', default='any',
              type=click.Choice(['master', 'replica', 'any']))
@click.option('--any', 'p_any', help='Restart a single member only', is_flag=True)
@click.option('--scheduled', help='Timestamp of a scheduled restart in unambiguous format (e.g. ISO 8601)',
              default=None)
@click.option('--pg-version', 'version', help='Restart if the PostgreSQL version is less than provided (e.g. 9.5.2)',
              default=None)
@click.option('--pending', help='Restart if pending', is_flag=True)
@click.option('--timeout',
              help='Return error and fail over if necessary when restarting takes longer than this.')
@option_force
@click.pass_obj
def restart(obj, cluster_name, member_names, force, role, p_any, scheduled, version, pending, timeout):
    cluster = get_dcs(obj, cluster_name).get_cluster()

    members = get_members(cluster, cluster_name, member_names, role, force, 'restart', False)
    if scheduled is None and not force:
        next_hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
        scheduled = click.prompt('When should the restart take place (e.g. ' + next_hour + ') ',
                                 type=str, default='now')

    scheduled_at = parse_scheduled(scheduled)
    confirm_members_action(members, force, 'restart', scheduled_at)

    if p_any:
        random.shuffle(members)
        members = members[:1]

    if version is None and not force:
        version = click.prompt('Restart if the PostgreSQL version is less than provided (e.g. 9.5.2) ',
                               type=str, default='')

    content = {}
    if pending:
        content['restart_pending'] = True

    if version:
        try:
            postgres_version_to_int(version)
        except PatroniException as e:
            raise PatroniCtlException(e.value)

        content['postgres_version'] = version

    if scheduled_at:
        if cluster.is_paused():
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
@click.argument('member_names', nargs=-1)
@option_force
@click.option('--wait', help='Wait until reinitialization completes', is_flag=True)
@click.pass_obj
def reinit(obj, cluster_name, member_names, force, wait):
    cluster = get_dcs(obj, cluster_name).get_cluster()
    members = get_members(cluster, cluster_name, member_names, None, force, 'reinitialize')

    wait_on_members = []
    for member in members:
        body = {'force': force}
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
            if data.get('state') != 'creating replica':
                click.echo('Reinitialize is completed on: {0}'.format(member.name))
                wait_on_members.remove(member)


def _do_failover_or_switchover(obj, action, cluster_name, master, candidate, force, scheduled=None):
    """
        We want to trigger a failover or switchover for the specified cluster name.

        We verify that the cluster name, master name and candidate name are correct.
        If so, we trigger an action and keep the client up to date.
    """

    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()

    if action == 'switchover' and (cluster.leader is None or not cluster.leader.name):
        raise PatroniCtlException('This cluster has no master')

    if master is None:
        if force or action == 'failover':
            master = cluster.leader and cluster.leader.name
        else:
            master = click.prompt('Master', type=str, default=cluster.leader.member.name)

    if master is not None and cluster.leader and cluster.leader.member.name != master:
        raise PatroniCtlException('Member {0} is not the leader of cluster {1}'.format(master, cluster_name))

    # excluding members with nofailover tag
    candidate_names = [str(m.name) for m in cluster.members if m.name != master and not m.nofailover]
    # We sort the names for consistent output to the client
    candidate_names.sort()

    if not candidate_names:
        raise PatroniCtlException('No candidates found to {0} to'.format(action))

    if candidate is None and not force:
        candidate = click.prompt('Candidate ' + str(candidate_names), type=str, default='')

    if action == 'failover' and not candidate:
        raise PatroniCtlException('Failover could be performed only to a specific candidate')

    if candidate == master:
        raise PatroniCtlException(action.title() + ' target and source are the same.')

    if candidate and candidate not in candidate_names:
        raise PatroniCtlException('Member {0} does not exist in cluster {1}'.format(candidate, cluster_name))

    scheduled_at_str = None
    scheduled_at = None

    if action == 'switchover':
        if scheduled is None and not force:
            next_hour = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
            scheduled = click.prompt('When should the switchover take place (e.g. ' + next_hour + ' ) ',
                                     type=str, default='now')

        scheduled_at = parse_scheduled(scheduled)
        if scheduled_at:
            if cluster.is_paused():
                raise PatroniCtlException("Can't schedule switchover in the paused state")
            scheduled_at_str = scheduled_at.isoformat()

    failover_value = {'leader': master, 'candidate': candidate, 'scheduled_at': scheduled_at_str}

    logging.debug(failover_value)

    # By now we have established that the leader exists and the candidate exists
    click.echo('Current cluster topology')
    output_members(dcs.get_cluster(), cluster_name)

    if not force:
        demote_msg = ', demoting current master ' + master if master else ''
        if scheduled_at_str:
            if not click.confirm('Are you sure you want to schedule {0} of cluster {1} at {2}{3}?'
                                 .format(action, cluster_name, scheduled_at_str, demote_msg)):
                raise PatroniCtlException('Aborting scheduled ' + action)
        else:
            if not click.confirm('Are you sure you want to {0} cluster {1}{2}?'
                                 .format(action, cluster_name, demote_msg)):
                raise PatroniCtlException('Aborting ' + action)

    r = None
    try:
        member = cluster.leader.member if cluster.leader else cluster.get_member(candidate, False)

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
        dcs.manual_failover(master, candidate, scheduled_at=scheduled_at)

    output_members(cluster, cluster_name)


@ctl.command('failover', help='Failover to a replica')
@arg_cluster_name
@click.option('--master', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@option_force
@click.pass_obj
def failover(obj, cluster_name, master, candidate, force):
    action = 'switchover' if master else 'failover'
    _do_failover_or_switchover(obj, action, cluster_name, master, candidate, force)


@ctl.command('switchover', help='Switchover to a replica')
@arg_cluster_name
@click.option('--master', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--scheduled', help='Timestamp of a scheduled switchover in unambiguous format (e.g. ISO 8601)',
              default=None)
@option_force
@click.pass_obj
def switchover(obj, cluster_name, master, candidate, force, scheduled):
    _do_failover_or_switchover(obj, 'switchover', cluster_name, master, candidate, force, scheduled)


def generate_topology(level, member, topology):
    members = topology.get(member['name'], [])

    if level > 0:
        member['name'] = '{0}+ {1}'.format((' ' * (level - 1) * 2), member['name'])

    if member['name']:
        yield member

    for member in members:
        for member in generate_topology(level + 1, member, topology):
            yield member


def topology_sort(members):
    topology = defaultdict(list)
    leader = next((m for m in members if m['role'].endswith('leader')), {'name': None})
    replicas = set(member['name'] for member in members if not member['role'].endswith('leader'))
    for member in members:
        if not member['role'].endswith('leader'):
            parent = member.get('tags', {}).get('replicatefrom')
            parent = parent if parent and parent != member['name'] and parent in replicas else leader['name']
            topology[parent].append(member)
    for member in generate_topology(0, leader, topology):
        yield member


def output_members(cluster, name, extended=False, fmt='pretty'):
    rows = []
    logging.debug(cluster)
    initialize = {None: 'uninitialized', '': 'initializing'}.get(cluster.initialize, cluster.initialize)
    cluster = cluster_as_json(cluster)

    columns = ['Cluster', 'Member', 'Host', 'Role', 'State', 'TL', 'Lag in MB']
    for c in ('Pending restart', 'Scheduled restart', 'Tags'):
        if extended or any(m.get(c.lower().replace(' ', '_')) for m in cluster['members']):
            columns.append(c)

    # Show Host as 'host:port' if somebody is running on non-standard port or two nodes are running on the same host
    members = [m for m in cluster['members'] if 'host' in m]
    append_port = any('port' in m and m['port'] != 5432 for m in members) or\
        len(set(m['host'] for m in members)) < len(members)

    sort = topology_sort if fmt == 'topology' else iter
    for m in sort(cluster['members']):
        logging.debug(m)

        lag = m.get('lag', '')
        m.update(cluster=name, member=m['name'], host=m.get('host', ''), tl=m.get('timeline', ''),
                 role=m['role'].replace('_', ' ').title(),
                 lag_in_mb=round(lag/1024/1024) if isinstance(lag, six.integer_types) else lag,
                 pending_restart='*' if m.get('pending_restart') else '')

        if append_port and m['host'] and m.get('port'):
            m['host'] = ':'.join([m['host'], str(m['port'])])

        if 'scheduled_restart' in m:
            value = m['scheduled_restart']['schedule']
            if 'postgres_version' in m['scheduled_restart']:
                value += ' if version < {0}'.format(m['scheduled_restart']['postgres_version'])
            m['scheduled_restart'] = value

        rows.append([m.get(n.lower().replace(' ', '_'), '') for n in columns])

    print_output(columns, rows, {'Lag in MB': 'r', 'TL': 'r'}, fmt, ' Cluster: {0} ({1}) '.format(name, initialize))

    if fmt not in ('pretty', 'topology'):  # Omit service info when using machine-readable formats
        return

    service_info = []
    if cluster.get('pause'):
        service_info.append('Maintenance mode: on')

    if 'scheduled_switchover' in cluster:
        info = 'Switchover scheduled at: ' + cluster['scheduled_switchover']['at']
        for name in ('from', 'to'):
            if name in cluster['scheduled_switchover']:
                info += '\n{0:>24}: {1}'.format(name, cluster['scheduled_switchover'][name])
        service_info.append(info)

    if service_info:
        click.echo(' ' + '\n '.join(service_info))


@ctl.command('list', help='List the Patroni members for a given Patroni')
@click.argument('cluster_names', nargs=-1)
@click.option('--extended', '-e', help='Show some extra information', is_flag=True)
@click.option('--timestamp', '-t', 'ts', help='Print timestamp', is_flag=True)
@option_format
@option_watch
@option_watchrefresh
@click.pass_obj
def members(obj, cluster_names, fmt, watch, w, extended, ts):
    if not cluster_names:
        if 'scope' in obj:
            cluster_names = [obj['scope']]
        if not cluster_names:
            return logging.warning('Listing members: No cluster names were provided')

    for cluster_name in cluster_names:
        dcs = get_dcs(obj, cluster_name)

        for _ in watching(w, watch):
            if ts:
                click.echo(timestamp(0))

            cluster = dcs.get_cluster()
            output_members(cluster, cluster_name, extended, fmt)


@ctl.command('topology', help='Prints ASCII topology for given cluster')
@click.argument('cluster_names', nargs=-1)
@option_watch
@option_watchrefresh
@click.pass_obj
@click.pass_context
def topology(ctx, obj, cluster_names, watch, w):
    ctx.forward(members, fmt='topology')


def timestamp(precision=6):
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:precision - 7]


@ctl.command('configure', help='Create configuration file')
@click.option('--config-file', '-c', help='Configuration file', prompt='Configuration file', default=CONFIG_FILE_PATH)
@click.option('--dcs', '-d', help='The DCS connect url', prompt='DCS connect url', default='etcd://localhost:2379')
@click.option('--namespace', '-n', help='The namespace', prompt='Namespace', default='/service/')
def configure(config_file, dcs, namespace):
    store_config({'dcs_api': str(dcs), 'namespace': str(namespace)}, config_file)


def touch_member(config, dcs):
    ''' Rip-off of the ha.touch_member without inter-class dependencies '''
    p = Postgresql(config['postgresql'])
    p.set_state('running')
    p.set_role('master')

    def restapi_connection_string(config):
        protocol = 'https' if config.get('certfile') else 'http'
        connect_address = config.get('connect_address')
        listen = config['listen']
        return '{0}://{1}/patroni'.format(protocol, connect_address or listen)

    data = {
        'conn_url': p.connection_string,
        'api_url': restapi_connection_string(config['restapi']),
        'state': p.state,
        'role': p.role
    }

    return dcs.touch_member(data, permanent=True)


def set_defaults(config, cluster_name):
    """fill-in some basic configuration parameters if config file is not set """
    config['postgresql'].setdefault('name', cluster_name)
    config['postgresql'].setdefault('scope', cluster_name)
    config['postgresql'].setdefault('listen', '127.0.0.1')
    config['postgresql']['authentication'] = {'replication': None}
    config['restapi']['listen'] = ':' in config['restapi']['listen'] and config['restapi']['listen'] or '127.0.0.1:8008'


@ctl.command('scaffold', help='Create a structure for the cluster in DCS')
@click.argument('cluster_name')
@click.option('--sysid', '-s', help='System ID of the cluster to put into the initialize key', default="")
@click.pass_obj
def scaffold(obj, cluster_name, sysid):
    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()
    if cluster and cluster.initialize is not None:
        raise PatroniCtlException("This cluster is already initialized")

    if not dcs.initialize(create_new=True, sysid=sysid):
        # initialize key already exists, don't touch this cluster
        raise PatroniCtlException("Initialize key for cluster {0} already exists".format(cluster_name))

    set_defaults(obj, cluster_name)

    # make sure the leader keys will never expire
    if not (touch_member(obj, dcs) and dcs.attempt_to_acquire_leader(permanent=True)):
        # we did initialize this cluster, but failed to write the leader or member keys, wipe it down completely.
        dcs.delete_cluster()
        raise PatroniCtlException("Unable to install permanent leader for cluster {0}".format(cluster_name))
    click.echo("Cluster {0} has been created successfully".format(cluster_name))


@ctl.command('flush', help='Discard scheduled events')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@click.argument('target', type=click.Choice(['restart', 'switchover']))
@click.option('--role', '-r', help='Flush only members with this role', default='any',
              type=click.Choice(['master', 'replica', 'any']))
@option_force
@click.pass_obj
def flush(obj, cluster_name, member_names, force, role, target):
    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()

    if target == 'restart':
        for member in get_members(cluster, cluster_name, member_names, role, force, 'flush'):
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
            except Exception as err:
                logging.warning(str(err))
                logging.warning('Member %s is not accessible', member.name)

            click.echo('Failed: member={0}, status_code={1}, ({2})'.format(
                member.name, r.status, r.data.decode('utf-8')))

        logging.warning('Failing over to DCS')
        click.echo('{0} Could not find any accessible member of cluster {1}'.format(timestamp(), cluster_name))
        dcs.manual_failover('', '', index=failover.index)


def wait_until_pause_is_applied(dcs, paused, old_cluster):
    click.echo("'{0}' request sent, waiting until it is recognized by all nodes".format(paused and 'pause' or 'resume'))
    old = {m.name: m.index for m in old_cluster.members if m.api_url}
    loop_wait = old_cluster.config.data.get('loop_wait', dcs.loop_wait)

    for _ in polling_loop(loop_wait + 1):
        cluster = dcs.get_cluster()
        if all(m.data.get('pause', False) == paused for m in cluster.members if m.name in old):
            break
    else:
        remaining = [m.name for m in cluster.members if m.data.get('pause', False) != paused
                     and m.name in old and old[m.name] != m.index]
        if remaining:
            return click.echo("{0} members didn't recognized pause state after {1} seconds"
                              .format(', '.join(remaining), loop_wait))
    return click.echo('Success: cluster management is {0}'.format(paused and 'paused' or 'resumed'))


def toggle_pause(config, cluster_name, paused, wait):
    dcs = get_dcs(config, cluster_name)
    cluster = dcs.get_cluster()
    if cluster.is_paused() == paused:
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
@click.pass_obj
@click.option('--wait', help='Wait until pause is applied on all nodes', is_flag=True)
def pause(obj, cluster_name, wait):
    return toggle_pause(obj, cluster_name, True, wait)


@ctl.command('resume', help='Resume auto failover')
@arg_cluster_name
@click.option('--wait', help='Wait until pause is cleared on all nodes', is_flag=True)
@click.pass_obj
def resume(obj, cluster_name, wait):
    return toggle_pause(obj, cluster_name, False, wait)


@contextmanager
def temporary_file(contents, suffix='', prefix='tmp'):
    """Creates a temporary file with specified contents that persists for the context.

    :param contents: binary string that will be written to the file.
    :param prefix: will be prefixed to the filename.
    :param suffix: will be appended to the filename.
    :returns path of the created file.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix=prefix, delete=False)
    with tmp:
        tmp.write(contents)

    try:
        yield tmp.name
    finally:
        os.unlink(tmp.name)


def show_diff(before_editing, after_editing):
    """Shows a diff between two strings.

    If the output is to a tty the diff will be colored. Inputs are expected to be unicode strings.
    """
    def listify(string):
        return [line + '\n' for line in string.rstrip('\n').split('\n')]

    unified_diff = difflib.unified_diff(listify(before_editing), listify(after_editing))

    if sys.stdout.isatty():
        buf = io.StringIO()
        for line in unified_diff:
            # Force cast to unicode as difflib on Python 2.7 returns a mix of unicode and str.
            buf.write(six.text_type(line))
        buf.seek(0)

        class opts:
            side_by_side = False
            width = 80
            tab_width = 8
        cdiff.markup_to_pager(cdiff.PatchStream(buf), opts)
    else:
        for line in unified_diff:
            click.echo(line.rstrip('\n'))


def format_config_for_editing(data, default_flow_style=False):
    """Formats configuration as YAML for human consumption.

    :param data: configuration as nested dictionaries
    :returns unicode YAML of the configuration"""
    return yaml.safe_dump(data, default_flow_style=default_flow_style, encoding=None, allow_unicode=True, width=200)


def apply_config_changes(before_editing, data, kvpairs):
    """Applies config changes specified as a list of key-value pairs.

    Keys are interpreted as dotted paths into the configuration data structure. Except for paths beginning with
    `postgresql.parameters` where rest of the path is used directly to allow for PostgreSQL GUCs containing dots.
    Values are interpreted as YAML values.

    :param before_editing: human representation before editing
    :param data: configuration datastructure
    :param kvpairs: list of strings containing key value pairs separated by =
    :returns tuple of human readable and parsed datastructure after changes
    """
    changed_data = copy.deepcopy(data)

    def set_path_value(config, path, value, prefix=()):
        # Postgresql GUCs can't be nested, but can contain dots so we re-flatten the structure for this case
        if prefix == ('postgresql', 'parameters'):
            path = ['.'.join(path)]

        key = path[0]
        if len(path) == 1:
            if value is None:
                config.pop(key, None)
            else:
                config[key] = value
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


def apply_yaml_file(data, filename):
    """Applies changes from a YAML file to configuration

    :param data: configuration datastructure
    :param filename: name of the YAML file, - is taken to mean standard input
    :returns tuple of human readable and parsed datastructure after changes
    """
    changed_data = copy.deepcopy(data)

    if filename == '-':
        new_options = yaml.safe_load(sys.stdin)
    else:
        with open(filename) as fd:
            new_options = yaml.safe_load(fd)

    patch_config(changed_data, new_options)

    return format_config_for_editing(changed_data), changed_data


def find_executable(executable, path=None):
    _, ext = os.path.splitext(executable)

    if (sys.platform == 'win32') and (ext != '.exe'):
        executable = executable + '.exe'

    if os.path.isfile(executable):
        return executable

    if path is None:
        path = os.environ.get('PATH', os.defpath)

    for p in path.split(os.pathsep):
        f = os.path.join(p, executable)
        if os.path.isfile(f):
            return f


def invoke_editor(before_editing, cluster_name):
    """Starts editor command to edit configuration in human readable format

    :param before_editing: human representation before editing
    :returns tuple of human readable and parsed datastructure after changes
    """

    editor_cmd = os.environ.get('EDITOR')
    if not editor_cmd:
        for editor in ('editor', 'vi'):
            editor_cmd = find_executable(editor)
            if editor_cmd:
                logging.debug('Setting fallback editor_cmd=%s', editor)
                break
    if not editor_cmd:
        raise PatroniCtlException('EDITOR environment variable is not set. editor or vi are not available')

    with temporary_file(contents=before_editing.encode('utf-8'),
                        suffix='.yaml',
                        prefix='{0}-config-'.format(cluster_name)) as tmpfile:
        ret = subprocess.call([editor_cmd, tmpfile])
        if ret:
            raise PatroniCtlException("Editor exited with return code {0}".format(ret))

        with codecs.open(tmpfile, encoding='utf-8') as fd:
            after_editing = fd.read()

        return after_editing, yaml.safe_load(after_editing)


@ctl.command('edit-config', help="Edit cluster configuration")
@arg_cluster_name
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
@click.pass_obj
def edit_config(obj, cluster_name, force, quiet, kvpairs, pgkvpairs, apply_filename, replace_filename):
    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()

    before_editing = format_config_for_editing(cluster.config.data)

    after_editing = None  # Serves as a flag if any changes were requested
    changed_data = cluster.config.data

    if replace_filename:
        after_editing, changed_data = apply_yaml_file({}, replace_filename)

    if apply_filename:
        after_editing, changed_data = apply_yaml_file(changed_data, apply_filename)

    if kvpairs or pgkvpairs:
        all_pairs = list(kvpairs) + ['postgresql.parameters.'+v.lstrip() for v in pgkvpairs]
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
        if not dcs.set_config_value(json.dumps(changed_data), cluster.config.index):
            raise PatroniCtlException("Config modification aborted due to concurrent changes")
        click.echo("Configuration changed")


@ctl.command('show-config', help="Show cluster configuration")
@arg_cluster_name
@click.pass_obj
def show_config(obj, cluster_name):
    cluster = get_dcs(obj, cluster_name).get_cluster()

    click.echo(format_config_for_editing(cluster.config.data))


@ctl.command('version', help='Output version of patronictl command or a running Patroni instance')
@click.argument('cluster_name', required=False)
@click.argument('member_names', nargs=-1)
@click.pass_obj
def version(obj, cluster_name, member_names):
    click.echo("patronictl version {0}".format(__version__))

    if not cluster_name:
        return

    click.echo("")
    cluster = get_dcs(obj, cluster_name).get_cluster()
    for m in cluster.members:
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
@option_format
@click.pass_obj
def history(obj, cluster_name, fmt):
    cluster = get_dcs(obj, cluster_name).get_cluster()
    history = cluster.history and cluster.history.lines or []
    for line in history:
        if len(line) < 4:
            line.append('')
    print_output(['TL', 'LSN', 'Reason', 'Timestamp'], history, {'TL': 'r', 'LSN': 'r'}, fmt)


def format_pg_version(version):
    if version < 100000:
        return "{0}.{1}.{2}".format(version // 10000, version // 100 % 100, version % 100)
    else:
        return "{0}.{1}".format(version // 10000, version % 100)
