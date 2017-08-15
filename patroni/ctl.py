'''
Patroni Control
'''

import base64
import click
import codecs
import datetime
import dateutil.parser
import cdiff
import copy
import difflib
import io
import json
import logging
import os
import psycopg2
import random
import requests
import subprocess
import sys
import tempfile
import time
import tzlocal
import yaml

from click import ClickException
from contextlib import contextmanager
from patroni.config import Config
from patroni.dcs import get_dcs as _get_dcs
from patroni.exceptions import PatroniException
from patroni.postgresql import Postgresql
from patroni.utils import is_valid_pg_version, patch_config
from prettytable import PrettyTable
from six.moves.urllib_parse import urlparse
from six import text_type

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronictl.yaml')
DCS_DEFAULTS = {'zookeeper': {'port': 2181, 'template': "zookeeper:\n hosts: ['{host}:{port}']"},
                'exhibitor': {'port': 8181, 'template': "exhibitor:\n hosts: [{host}]\n port: {port}"},
                'consul': {'port': 8500, 'template': "consul:\n host: '{host}:{port}'"},
                'etcd': {'port': 2379, 'template': "etcd:\n host: '{host}:{port}'"}}


class PatroniCtlException(ClickException):
    pass


def parse_dcs(dcs):
    if dcs is None:
        return None

    parsed = urlparse(dcs)
    scheme = parsed.scheme
    if scheme == '' and parsed.netloc == '':
        parsed = urlparse('//' + dcs)
    port = int(parsed.port) if parsed.port else None

    if scheme == '':
        scheme = ([k for k, v in DCS_DEFAULTS.items() if v['port'] == port] or ['etcd'])[0]
    elif scheme not in DCS_DEFAULTS:
        raise PatroniCtlException('Unknown dcs scheme: {}'.format(scheme))

    dcs_info = DCS_DEFAULTS[scheme]
    return yaml.load(dcs_info['template'].format(host=parsed.hostname or 'localhost', port=port or dcs_info['port']))


def load_config(path, dcs):
    logging.debug('Loading configuration from file %s', path)
    config = {}
    old_argv = list(sys.argv)
    try:
        sys.argv[1] = path
        if Config.PATRONI_CONFIG_VARIABLE not in os.environ:
            for p in ('PATRONI_RESTAPI_LISTEN', 'PATRONI_POSTGRESQL_DATA_DIR'):
                if p not in os.environ:
                    os.environ[p] = '.'
        config = Config().copy()
    finally:
        sys.argv = old_argv

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


option_format = click.option('--format', '-f', 'fmt', help='Output format (pretty, json)', default='pretty')
option_watchrefresh = click.option('-w', '--watch', type=float, help='Auto update the screen every X seconds')
option_watch = click.option('-W', is_flag=True, help='Auto update the screen every 2 seconds')
option_force = click.option('--force', is_flag=True, help='Do not ask for confirmation at any point')


@click.group()
@click.option('--config-file', '-c', help='Configuration file', default=CONFIG_FILE_PATH)
@click.option('--dcs', '-d', help='Use this DCS', envvar='DCS')
@click.pass_context
def ctl(ctx, config_file, dcs):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=os.environ.get('LOGLEVEL', 'WARNING'))
    ctx.obj = load_config(config_file, dcs)


def get_dcs(config, scope):
    config.update({'scope': scope, 'patronictl': True})
    config.setdefault('name', scope)
    try:
        return _get_dcs(config)
    except PatroniException as e:
        raise PatroniCtlException(str(e))


def auth_header(config):
    if config.get('restapi', {}).get('auth', ''):
        return {'Authorization': 'Basic ' + base64.b64encode(config['restapi']['auth'].encode('utf-8')).decode('utf-8')}


def request_patroni(member, request_type, endpoint, content=None, headers=None):
    headers = headers or {}
    url_parts = urlparse(member.api_url)
    logging.debug(url_parts)
    if 'Content-Type' not in headers:
        headers['Content-Type'] = 'application/json'

    url = '{0}://{1}/{2}'.format(url_parts.scheme, url_parts.netloc, endpoint)

    return getattr(requests, request_type)(url, headers=headers,
                                           data=json.dumps(content) if content else None, timeout=60)


def print_output(columns, rows=None, alignment=None, fmt='pretty', header=True, delimiter='\t'):
    rows = rows or []
    if fmt == 'pretty':
        t = PrettyTable(columns)
        for k, v in (alignment or {}).items():
            t.align[k] = v
        for r in rows:
            t.add_row(r)
        click.echo(t)
        return

    if fmt == 'json':
        elements = [dict(zip(columns, r)) for r in rows]
        click.echo(json.dumps(elements))

    if fmt == 'tsv':
        if columns is not None and header:
            click.echo(delimiter.join(columns) + '\n')

        for r in rows:
            c = [str(c) for c in r]
            click.echo(delimiter.join(c))


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
        if cluster.leader is not None:
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

    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cursor = conn.cursor()
    if role == 'any':
        return cursor

    cursor.execute('SELECT pg_is_in_recovery()')
    in_recovery = cursor.fetchone()[0]

    if in_recovery and role == 'replica' or not in_recovery and role == 'master':
        return cursor

    conn.close()

    return None


def get_members(cluster, cluster_name, member_names, role, force, action):
    candidates = {m.name: m for m in cluster.members}

    if not force or role:
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

    for mn in member_names:
        if mn not in candidates:
            raise PatroniCtlException('{0} is not a member of cluster'.format(mn))

    if not force:
        confirm = click.confirm('Are you sure you want to {0} members {1}?'.format(action, ', '.join(member_names)))
        if not confirm:
            raise PatroniCtlException('Aborted {0}'.format(action))

    return [candidates[n] for n in member_names]


@ctl.command('dsn', help='Generate a dsn for the provided member, defaults to a dsn of the master')
@click.option('--role', '-r', help='Give a dsn of any member with this role', type=click.Choice(['master', 'replica',
              'any']), default=None)
@click.option('--member', '-m', help='Generate a dsn for this member', type=str)
@click.argument('cluster_name')
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
@click.argument('cluster_name')
@option_format
@click.option('--format', 'fmt', help='Output format (pretty, json)', default='tsv')
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

        output, cursor = query_member(cluster, cursor, member, role, command, connect_parameters)
        print_output(None, output, fmt=fmt, delimiter=delimiter)


def query_member(cluster, cursor, member, role, command, connect_parameters):
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

        cursor.execute('SELECT pg_is_in_recovery()')
        in_recovery = cursor.fetchone()[0]

        if in_recovery and role == 'master' or not in_recovery and role == 'replica':
            cursor.connection.close()
            return None, None

        cursor.execute(command)
        return cursor.fetchall(), cursor
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

    if cluster.leader:
        confirm = click.prompt('This cluster currently is healthy. Please specify the master name to continue')
        if confirm != cluster.leader.name:
            raise PatroniCtlException('You did not specify the current master of the cluster')

    dcs.delete_cluster()


def check_response(response, member_name, action_name, silent_success=False):
    if response.status_code >= 400:
        click.echo('Failed: {0} for member {1}, status code={2}, ({3})'.format(
            action_name, member_name, response.status_code, response.text
        ))
    elif not silent_success:
        click.echo('Success: {0} for member {1}'.format(action_name, member_name))


def parse_scheduled(scheduled):
    if (scheduled or 'now') != 'now':
        try:
            scheduled_at = dateutil.parser.parse(scheduled)
            if scheduled_at.tzinfo is None:
                scheduled_at = tzlocal.get_localzone().localize(scheduled_at)
        except (ValueError, TypeError):
            message = 'Unable to parse scheduled timestamp ({0}). It should be in an unambiguous format (e.g. ISO 8601)'
            raise PatroniCtlException(message.format(scheduled))
        return scheduled_at

    return None


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

    members = get_members(cluster, cluster_name, member_names, role, force, 'restart')
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
        if not is_valid_pg_version(version):
            message = 'PostgreSQL version should be in the first.major.minor format'
            raise PatroniCtlException(message)
        else:
            content['postgres_version'] = version

    if scheduled is None and not force:
        scheduled = click.prompt('When should the restart take place (e.g. 2015-10-01T14:30) ', type=str, default='now')

    scheduled_at = parse_scheduled(scheduled)
    if scheduled_at:
        if cluster.is_paused():
            raise PatroniCtlException("Can't schedule restart in the paused state")
        content['schedule'] = scheduled_at.isoformat()

    if timeout is not None:
        content['timeout'] = timeout

    for member in members:
        if 'schedule' in content:
            if force and member.data.get('scheduled_restart'):
                r = request_patroni(member, 'delete', 'restart', headers=auth_header(obj))
                check_response(r, member.name, 'flush scheduled restart', True)

        r = request_patroni(member, 'post', 'restart', content, auth_header(obj))
        if r.status_code == 200:
            click.echo('Success: restart on member {0}'.format(member.name))
        elif r.status_code == 202:
            click.echo('Success: restart scheduled on member {0}'.format(member.name))
        elif r.status_code == 409:
            click.echo('Failed: another restart is already scheduled on member {0}'.format(member.name))
        else:
            click.echo('Failed: restart for member {0}, status code={1}, ({2})'.format(
                member.name, r.status_code, r.text)
            )


@ctl.command('reinit', help='Reinitialize cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@option_force
@click.pass_obj
def reinit(obj, cluster_name, member_names, force):
    cluster = get_dcs(obj, cluster_name).get_cluster()
    members = get_members(cluster, cluster_name, member_names, None, force, 'reinitialize')

    for member in members:
        r = request_patroni(member, 'post', 'reinitialize', headers=auth_header(obj))
        check_response(r, member.name, 'reinitialize')


@ctl.command('failover', help='Failover to a replica')
@click.argument('cluster_name')
@click.option('--master', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--scheduled', help='Timestamp of a scheduled failover in unambiguous format (e.g. ISO 8601)',
              default=None)
@option_force
@click.pass_obj
def failover(obj, cluster_name, master, candidate, force, scheduled):
    """
        We want to trigger a failover for the specified cluster name.

        We verify that the cluster name, master name and candidate name are correct.
        If so, we trigger a failover and keep the client up to date.
    """

    dcs = get_dcs(obj, cluster_name)
    cluster = dcs.get_cluster()

    if cluster.leader is None and not cluster.is_paused():
        raise PatroniCtlException('This cluster has no master')

    if master is None and (not cluster.is_paused() or cluster.leader):
        if force:
            master = cluster.leader.member.name
        else:
            master = click.prompt('Master', type=str, default=cluster.leader.member.name)

    if master is not None and cluster.leader and cluster.leader.member.name != master:
        raise PatroniCtlException('Member {0} is not the leader of cluster {1}'.format(master, cluster_name))

    candidate_names = [str(m.name) for m in cluster.members if m.name != master]
    # We sort the names for consistent output to the client
    candidate_names.sort()

    if not candidate_names:
        raise PatroniCtlException('No candidates found to failover to')

    if candidate is None and not force:
        candidate = click.prompt('Candidate ' + str(candidate_names), type=str, default='')

    if candidate == master:
        raise PatroniCtlException('Failover target and source are the same.')

    if candidate and candidate not in candidate_names:
        raise PatroniCtlException('Member {0} does not exist in cluster {1}'.format(candidate, cluster_name))

    if scheduled is None and not force:
        scheduled = click.prompt('When should the failover take place (e.g. 2015-10-01T14:30) ', type=str,
                                 default='now')

    scheduled_at = parse_scheduled(scheduled)

    scheduled_at_str = None
    if scheduled_at:
        if cluster.is_paused():
            raise PatroniCtlException("Can't schedule failover in the paused state")
        scheduled_at_str = scheduled_at.isoformat()

    failover_value = {'leader': master, 'candidate': candidate, 'scheduled_at': scheduled_at_str}

    logging.debug(failover_value)

    # By now we have established that the leader exists and the candidate exists
    click.echo('Current cluster topology')
    output_members(dcs.get_cluster(), cluster_name)

    if not force:
        a = \
            click.confirm('Are you sure you want to failover cluster {0}, demoting current master {1}?'.format(
                cluster_name, master))
        if not a:
            raise PatroniCtlException('Aborting failover')

    r = None
    try:
        member = cluster.leader.member if cluster.leader else [m for m in cluster.members if m.name == candidate][0]

        r = request_patroni(member, 'post', 'failover', failover_value, auth_header(obj))
        if r.status_code in (200, 202):
            logging.debug(r)
            cluster = dcs.get_cluster()
            logging.debug(cluster)
            click.echo('{0} {1}'.format(timestamp(), r.text))
        else:
            click.echo('Failover failed, details: {0}, {1}'.format(r.status_code, r.text))
            return
    except Exception:
        logging.exception(r)
        logging.warning('Failing over to DCS')
        click.echo(timestamp() + ' Could not failover using Patroni api, falling back to DCS')
        click.echo(timestamp() + ' Initializing failover from master {0}'.format(master))
        dcs.manual_failover(master, candidate, scheduled_at=scheduled_at)

    output_members(cluster, cluster_name)


def output_members(cluster, name, extended=False, fmt='pretty'):
    rows = []
    logging.debug(cluster)
    leader_name = None
    if cluster.leader:
        leader_name = cluster.leader.member.name

    xlog_location_cluster = cluster.last_leader_operation or 0

    # Mainly for consistent pretty printing and watching we sort the output
    cluster.members.sort(key=lambda x: x.name)
    for m in cluster.members:
        logging.debug(m)

        role = ''
        if m.name == leader_name:
            role = 'Leader'
        elif m.name == cluster.sync.sync_standby:
            role = 'Sync standby'

        host = m.conn_kwargs()['host']

        xlog_location = m.data.get('xlog_location') or 0
        lag = ''
        if xlog_location_cluster >= xlog_location:
            lag = round((xlog_location_cluster - xlog_location)/1024/1024)

        row = [
            name,
            m.name,
            host,
            role,
            m.data.get('state', ''),
            lag,
        ]
        if extended:
            value = ''
            scheduled_restart = m.data.get('scheduled_restart')
            if scheduled_restart:
                value = scheduled_restart['schedule']
                if 'postgres_version' in scheduled_restart:
                    value += ' if version < {0}'.format(scheduled_restart['postgres_version'])

            row.append(value)

        rows.append(row)

    columns = [
        'Cluster',
        'Member',
        'Host',
        'Role',
        'State',
        'Lag in MB',
    ]
    alignment = {'Cluster': 'l', 'Member': 'l', 'Host': 'l', 'Lag in MB': 'r'}

    if extended:
        columns.append('Scheduled restart')
        alignment['Scheduled restart'] = 'l'

    print_output(columns, rows, alignment, fmt)


@ctl.command('list', help='List the Patroni members for a given Patroni')
@click.argument('cluster_names', nargs=-1)
@click.option('--extended', '-e', help='Show some extra information', is_flag=True)
@option_format
@option_watch
@option_watchrefresh
@click.pass_obj
def members(obj, cluster_names, fmt, watch, w, extended):
    if not cluster_names:
        logging.warning('Listing members: No cluster names were provided')
        return

    for cluster_name in cluster_names:
        dcs = get_dcs(obj, cluster_name)

        for _ in watching(w, watch):
            cluster = dcs.get_cluster()
            output_members(cluster, cluster_name, extended, fmt)


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

    return dcs.touch_member(json.dumps(data, separators=(',', ':')), permanent=True)


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


@ctl.command('flush', help='Flush scheduled events')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@click.argument('target', type=click.Choice(['restart']))
@click.option('--role', '-r', help='Flush only members with this role', default='any',
              type=click.Choice(['master', 'replica', 'any']))
@option_force
@click.pass_obj
def flush(obj, cluster_name, member_names, force, role, target):
    cluster = get_dcs(obj, cluster_name).get_cluster()

    members = get_members(cluster, cluster_name, member_names, role, force, 'flush')
    for member in members:
        if target == 'restart':
            if member.data.get('scheduled_restart'):
                r = request_patroni(member, 'delete', 'restart', None, auth_header(obj))
                check_response(r, member.name, 'flush scheduled restart')
            else:
                click.echo('No scheduled restart for member {0}'.format(member.name))


def toggle_pause(config, cluster_name, paused):
    cluster = get_dcs(config, cluster_name).get_cluster()
    if cluster.is_paused() == paused:
        raise PatroniCtlException('Cluster is {0} paused'.format(paused and 'already' or 'not'))

    members = []
    if cluster.leader:
        members.append(cluster.leader.member)
    members.extend([m for m in cluster.members if m.api_url and (not members or members[0].name != m.name)])

    for member in members:
        try:
            r = request_patroni(member, 'patch', 'config', {'pause': paused or None}, auth_header(config))
        except Exception:
            logging.warning('Member %s is not accessible', member.name)
            continue

        if r.status_code == 200:
            click.echo('Success: cluster management is {0}'.format(paused and 'paused' or 'resumed'))
        else:
            click.echo('Failed: {0} cluster management status code={1}, ({2})'.format(
                       paused and 'pause' or 'resume', r.status_code, r.text))
        break
    else:
        raise PatroniCtlException('Can not find accessible cluster member')


@ctl.command('pause', help='Disable auto failover')
@click.argument('cluster_name')
@click.pass_obj
def pause(obj, cluster_name):
    return toggle_pause(obj, cluster_name, True)


@ctl.command('resume', help='Resume auto failover')
@click.argument('cluster_name')
@click.pass_obj
def resume(obj, cluster_name):
    return toggle_pause(obj, cluster_name, False)


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
        return [l+'\n' for l in string.rstrip('\n').split('\n')]

    unified_diff = difflib.unified_diff(listify(before_editing), listify(after_editing))

    if sys.stdout.isatty():
        buf = io.StringIO()
        for line in unified_diff:
            # Force cast to unicode as difflib on Python 2.7 returns a mix of unicode and str.
            buf.write(text_type(line))
        buf.seek(0)

        class opts:
            side_by_side = False
            width = 80
            tab_width = 8
        cdiff.markup_to_pager(cdiff.PatchStream(buf), opts)
    else:
        for line in unified_diff:
            click.echo(line.rstrip('\n'))


def format_config_for_editing(data):
    """Formats configuration as YAML for human consumption.

    :param data: configuration as nested dictionaries
    :returns unicode YAML of the configuration"""
    return yaml.safe_dump(data, default_flow_style=False, encoding=None, allow_unicode=True)


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


def invoke_editor(before_editing, cluster_name):
    """Starts editor command to edit configuration in human readable format

    :param before_editing: human representation before editing
    :returns tuple of human readable and parsed datastructure after changes
    """
    editor_cmd = os.environ.get('EDITOR')
    if not editor_cmd:
        raise PatroniCtlException('EDITOR environment variable is not set')

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
@click.argument('cluster_name')
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
@click.argument('cluster_name')
@click.pass_obj
def show_config(obj, cluster_name):
    cluster = get_dcs(obj, cluster_name).get_cluster()

    click.echo(format_config_for_editing(cluster.config.data))
