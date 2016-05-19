'''
Patroni Control
'''

import click
import datetime
import dateutil
import json
import logging
import os
import psycopg2
import random
import requests
import time
import tzlocal
import yaml

from click import ClickException
from patroni.dcs import get_dcs as _get_dcs
from patroni.exceptions import PatroniException
from patroni.postgresql import parseurl
from prettytable import PrettyTable
from six.moves.urllib_parse import urlparse

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronictl.yaml')
LOGLEVEL = 'WARNING'
DCS_DEFAULTS = {'zookeeper': {'port': 2181, 'template': "zookeeper:\n hosts: ['{host}:{port}']"},
                'exhibitor': {'port': 8181, 'template': "zookeeper:\n exhibitor:\n  hosts: [{host}]\n  port: {port}"},
                'consul': {'port': 8500, 'template': "consul:\n host: '{host}:{port}'"},
                'etcd': {'port': 4001, 'template': "etcd:\n host: '{host}:{port}'"}}


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
    config = dict()
    try:
        with open(path, 'rb') as fd:
            config = yaml.safe_load(fd)
    except (IOError, yaml.YAMLError):
        logging.exception('Could not load configuration file')

    config.update(parse_dcs(dcs) or parse_dcs(config.get('dcs_api')) or {})

    return config


def store_config(config, path):
    dir_path = os.path.dirname(path)
    if dir_path and not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    with open(path, 'w') as fd:
        yaml.dump(config, fd)


option_config_file = click.option('--config-file', '-c', help='Configuration file', default=CONFIG_FILE_PATH)
option_format = click.option('--format', '-f', 'fmt', help='Output format (pretty, json)', default='pretty')
option_dcs = click.option('--dcs', '-d', help='Use this DCS', envvar='DCS')
option_watchrefresh = click.option('-w', '--watch', type=float, help='Auto update the screen every X seconds')
option_watch = click.option('-W', is_flag=True, help='Auto update the screen every 2 seconds')
option_force = click.option('--force', is_flag=True, help='Do not ask for confirmation at any point')


@click.group()
@click.pass_context
def ctl(ctx):
    global LOGLEVEL
    LOGLEVEL = os.environ.get('LOGLEVEL', LOGLEVEL)

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=LOGLEVEL)


def get_dcs(config, scope):
    config.setdefault('scope', scope)
    try:
        return _get_dcs(scope, config)
    except PatroniException as e:
        raise PatroniCtlException(str(e))


def post_patroni(member, endpoint, content, headers=None):
    url = urlparse(member.api_url)
    logging.debug(url)
    return requests.post('{0}://{1}/{2}'.format(url.scheme, url.netloc, endpoint),
                         headers=headers or {'Content-Type': 'application/json'},
                         data=json.dumps(content), timeout=60)


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
        elements = list()
        for r in rows:
            elements.append(dict(zip(columns, r)))

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


def build_connect_parameters(conn_url, connect_parameters=None):
    params = (connect_parameters or {}).copy()
    parsed = parseurl(conn_url)
    params['host'] = parsed['host']
    params['port'] = parsed['port']
    params['fallback_application_name'] = 'Patroni ctl'
    params['connect_timeout'] = '5'

    return params


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


def get_cursor(cluster, role='master', member=None, connect_parameters=None):
    member = get_any_member(cluster, role=role, member=member)
    if member is None:
        return None

    params = build_connect_parameters(member.conn_url, connect_parameters)

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


@ctl.command('dsn', help='Generate a dsn for the provided member, defaults to a dsn of the master')
@click.option('--role', '-r', help='Give a dsn of any member with this role', type=click.Choice(['master', 'replica',
              'any']), default=None)
@click.option('--member', '-m', help='Generate a dsn for this member', type=str)
@option_dcs
@option_config_file
@click.argument('cluster_name')
def dsn(cluster_name, config_file, dcs, role, member):
    if role is not None and member is not None:
        raise PatroniCtlException('--role and --member are mutually exclusive options')
    if member is None and role is None:
        role = 'master'

    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)
    m = get_any_member(cluster, role=role, member=member)
    if m is None:
        raise PatroniCtlException('Can not find a suitable member')

    params = build_connect_parameters(m.conn_url)
    click.echo('host={host} port={port}'.format(**params))


@ctl.command('query', help='Query a Patroni PostgreSQL member')
@click.argument('cluster_name')
@option_config_file
@option_format
@click.option('--format', 'fmt', help='Output format (pretty, json)', default='tsv')
@click.option('--file', '-f', 'p_file', help='Execute the SQL commands from this file', type=click.File('rb'))
@click.option('--password', help='force password prompt', is_flag=True)
@click.option('-U', '--username', help='database user name', type=str)
@option_dcs
@option_watch
@option_watchrefresh
@click.option('--role', '-r', help='The role of the query', type=click.Choice(['master', 'replica', 'any']),
              default=None)
@click.option('--member', '-m', help='Query a specific member', type=str)
@click.option('--delimiter', help='The column delimiter', default='\t')
@click.option('--command', '-c', help='The SQL commands to execute')
@click.option('-d', '--dbname', help='database name to connect to', type=str)
def query(
    cluster_name,
    config_file,
    dcs,
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

    connect_parameters = dict()
    if username:
        connect_parameters['user'] = username
    if password:
        connect_parameters['password'] = click.prompt('Password', hide_input=True, type=str)
    if dbname:
        connect_parameters['database'] = dbname

    if p_file is not None:
        command = p_file.read()

    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    cursor = None
    for _ in watching(w, watch, clear=False):

        output, cursor = query_member(cluster, cursor, member, role, command, connect_parameters)
        print_output(None, output, fmt=fmt, delimiter=delimiter)

        if cursor is None:
            cluster = dcs.get_cluster()


def query_member(cluster, cursor, member, role, command, connect_parameters=None):
    try:
        if cursor is None:
            cursor = get_cursor(cluster, role=role, member=member, connect_parameters=connect_parameters)

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
@option_config_file
@option_format
@option_dcs
def remove(config_file, cluster_name, fmt, dcs):
    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    output_members(cluster, cluster_name, fmt)

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


def wait_for_leader(dcs, timeout=30):
    t_stop = time.time() + timeout
    timeout /= 2

    while time.time() < t_stop:
        dcs.watch(timeout)
        cluster = dcs.get_cluster()

        if cluster.leader:
            return cluster

    raise PatroniCtlException('Timeout occured')


def empty_post_to_members(cluster, member_names, force, endpoint):
    candidates = dict()
    for m in cluster.members:
        candidates[m.name] = m

    if not member_names:
        member_names = [click.prompt('Which member do you want to {0} [{1}]?'.format(endpoint,
                        ', '.join(candidates.keys())), type=str, default='')]

    for mn in member_names:
        if mn not in candidates.keys():
            raise PatroniCtlException('{0} is not a member of cluster'.format(mn))

    if not force:
        confirm = click.confirm('Are you sure you want to {0} members {1}?'.format(endpoint, ', '.join(member_names)))
        if not confirm:
            raise PatroniCtlException('Aborted {0}'.format(endpoint))

    for mn in member_names:
        r = post_patroni(candidates[mn], endpoint, '')
        if r.status_code != 200:
            click.echo('{0} failed for member {1}, status code={2}, ({3})'.format(endpoint, mn, r.status_code, r.text))
        else:
            click.echo('Succesful {0} on member {1}'.format(endpoint, mn))


def ctl_load_config(cluster_name, config_file, dcs):
    config = load_config(config_file, dcs)
    dcs = get_dcs(config, cluster_name)
    cluster = dcs.get_cluster()

    return config, dcs, cluster


@ctl.command('restart', help='Restart cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@click.option('--role', '-r', help='Restart only members with this role', default='any',
              type=click.Choice(['master', 'replica', 'any']))
@click.option('--any', 'p_any', help='Restart a single member only', is_flag=True)
@option_config_file
@option_force
@option_dcs
def restart(cluster_name, member_names, config_file, dcs, force, role, p_any):
    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    role_names = [m.name for m in get_all_members(cluster, role)]

    if member_names:
        member_names = list(set(member_names) & set(role_names))
    else:
        member_names = role_names

    if p_any:
        random.shuffle(member_names)
        member_names = member_names[:1]

    output_members(cluster, cluster_name)
    empty_post_to_members(cluster, member_names, force, 'restart')


@ctl.command('reinit', help='Reinitialize cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@option_config_file
@option_force
@option_dcs
def reinit(cluster_name, member_names, config_file, dcs, force):
    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)
    empty_post_to_members(cluster, member_names, force, 'reinitialize')


@ctl.command('failover', help='Failover to a replica')
@click.argument('cluster_name')
@click.option('--master', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--scheduled', help='Timestamp of a scheduled failover in unambiguous format (e.g. ISO 8601)',
              default=None)
@click.option('--force', is_flag=True)
@option_config_file
@option_dcs
def failover(config_file, cluster_name, master, candidate, force, dcs, scheduled):
    """
        We want to trigger a failover for the specified cluster name.

        We verify that the cluster name, master name and candidate name are correct.
        If so, we trigger a failover and keep the client up to date.
    """

    _, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    if cluster.leader is None:
        raise PatroniCtlException('This cluster has no master')

    if master is None:
        if force:
            master = cluster.leader.member.name
        else:
            master = click.prompt('Master', type=str, default=cluster.leader.member.name)

    if cluster.leader.member.name != master:
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

    if (scheduled or 'now') == 'now':
        scheduled_at = None
    else:
        try:
            scheduled_at = dateutil.parser.parse(scheduled)
            if scheduled_at.tzinfo is None:
                scheduled_at = tzlocal.get_localzone().localize(scheduled_at)
        except (ValueError, TypeError):
            message = 'Unable to parse scheduled timestamp ({0}). It should be in an unambiguous format (e.g. ISO 8601)'
            raise PatroniCtlException(message.format(scheduled))
        scheduled_at = scheduled_at.isoformat()

    failover_value = {'leader': master, 'candidate': candidate, 'scheduled_at': scheduled_at}
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
        r = post_patroni(cluster.leader.member, 'failover', failover_value)
        if r.status_code == 200:
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
        dcs.manual_failover(master, candidate, scheduled_at=failover_value)

    output_members(cluster, cluster_name)


def output_members(cluster, name, fmt='pretty'):
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

        leader = ''
        if m.name == leader_name:
            leader = '*'

        host = build_connect_parameters(m.conn_url)['host']

        xlog_location = m.data.get('xlog_location') or 0
        lag = ''
        if (xlog_location_cluster >= xlog_location):
            lag = round((xlog_location_cluster - xlog_location)/1024/1024)

        rows.append([
            name,
            m.name,
            host,
            leader,
            m.data.get('state', ''),
            lag
        ])

    columns = [
        'Cluster',
        'Member',
        'Host',
        'Leader',
        'State',
        'Lag in MB',
    ]
    alignment = {'Cluster': 'l', 'Member': 'l', 'Host': 'l', 'Lag in MB': 'r'}

    print_output(columns, rows, alignment, fmt)


@ctl.command('list', help='List the Patroni members for a given Patroni')
@click.argument('cluster_names', nargs=-1)
@option_config_file
@option_format
@option_watch
@option_watchrefresh
@option_dcs
def members(config_file, cluster_names, fmt, watch, w, dcs):
    if not cluster_names:
        logging.warning('Listing members: No cluster names were provided')
        return

    config = load_config(config_file, dcs)
    for cluster_name in cluster_names:
        dcs = get_dcs(config, cluster_name)

        for _ in watching(w, watch):
            output_members(dcs.get_cluster(), cluster_name, fmt)


def timestamp(precision=6):
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:precision - 7]


@ctl.command('configure', help='Create configuration file')
@click.option('--config-file', '-c', help='Configuration file', prompt='Configuration file', default=CONFIG_FILE_PATH)
@click.option('--dcs', '-d', help='The DCS connect url', prompt='DCS connect url', default='etcd://localhost:4001')
@click.option('--namespace', '-n', help='The namespace', prompt='Namespace', default='/service/')
def configure(config_file, dcs, namespace):
    config = dict()
    config['dcs_api'] = str(dcs)
    config['namespace'] = str(namespace)
    store_config(config, config_file)
