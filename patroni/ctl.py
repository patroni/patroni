'''
Patroni Control
'''

import click
import os
import yaml
import json
import time
import psycopg2
import random
import requests
import datetime
from prettytable import PrettyTable
from six.moves.urllib_parse import urlparse
import logging

from .etcd import Etcd
from .exceptions import PatroniCtlException
from .postgresql import parseurl

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronictl.yaml')
LOGLEVEL = 'WARNING'


def parse_dcs(dcs):
    """
    Break up the provided dcs string
    >>> parse_dcs('localhost') == {'scheme': 'etcd', 'hostname': 'localhost', 'port': 4001}
    True
    >>> parse_dcs('localhost:8500') == {'scheme': 'consul', 'hostname': 'localhost', 'port': 8500}
    True
    >>> parse_dcs('zookeeper://localhost') == {'scheme': 'zookeeper', 'hostname': 'localhost', 'port': 2181}
    True
    """

    if not dcs:
        return {}

    parsed = urlparse(dcs)
    scheme = parsed.scheme
    if scheme == '' and parsed.netloc == '':
        parsed = urlparse('//' + dcs)

    if scheme == '':
        default_schemes = {'2181': 'zookeeper', '8500': 'consul'}
        scheme = default_schemes.get(str(parsed.port), 'etcd')

    port = parsed.port
    if port is None:
        default_ports = {'consul': 8500, 'zookeeper': 2181}
        port = default_ports.get(str(scheme), 4001)

    return {'scheme': str(scheme), 'hostname': str(parsed.hostname), 'port': int(port)}


def load_config(path, dcs):
    logging.debug('Loading configuration from file {}'.format(path))
    config = dict()
    try:
        with open(path, 'rb') as fd:
            config = yaml.safe_load(fd)
    except:
        logging.exception('Could not load configuration file')

    if dcs:
        config['dcs'] = parse_dcs(dcs)
    else:
        config['dcs'] = parse_dcs(config.get('dcs_api'))

    return config


def store_config(config, path):
    dir_path = os.path.dirname(path)
    if dir_path:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
    with open(path, 'w') as fd:
        yaml.dump(config, fd)


option_config_file = click.option('--config-file', '-c', help='Configuration file', default=CONFIG_FILE_PATH)
option_format = click.option('--format', '-f', help='Output format (pretty, json)', default='pretty')
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
    scheme, hostname, port = map(config.get('dcs', {}).get, ('scheme', 'hostname', 'port'))

    if scheme == 'etcd':
        return Etcd(name=scope, config={'scope': scope, 'host': '{}:{}'.format(hostname, port)})

    raise PatroniCtlException('Can not find suitable configuration of distributed configuration store')


def post_patroni(member, endpoint, content, headers={'Content-Type': 'application/json'}):
    url = urlparse(member.api_url)
    logging.debug(url)
    return requests.post('{}://{}/{}'.format(url.scheme, url.netloc, endpoint), headers=headers,
                         data=json.dumps(content), timeout=60)


def print_output(columns, rows=[], alignment=None, format='pretty', header=True, delimiter='\t'):
    if format == 'pretty':
        t = PrettyTable(columns)
        for k, v in (alignment or {}).items():
            t.align[k] = v
        for r in rows:
            t.add_row(r)
        click.echo(t)
        return

    if format == 'json':
        elements = list()
        for r in rows:
            elements.append(dict(zip(columns, r)))

        click.echo(json.dumps(elements))

    if format == 'tsv':
        if columns is not None and header:
            click.echo(delimiter.join(columns) + '\n')

        for r in rows or []:
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


def build_connect_parameters(conn_url, connect_parameters={}):
    params = connect_parameters.copy()
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
    members = get_all_members(cluster=cluster, role=role)
    for m in members:
        if member is None or m.name == member:
            return m

    return None


def get_cursor(cluster, role='master', member=None, connect_parameters={}):
    member = get_any_member(cluster=cluster, role=role, member=member)
    if member is None:
        return None

    params = build_connect_parameters(member.conn_url, connect_parameters=connect_parameters)

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

    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)
    m = get_any_member(cluster=cluster, role=role, member=member)
    if m is None:
        raise PatroniCtlException('Can not find a suitable member')

    params = build_connect_parameters(m.conn_url)
    click.echo('host={} port={}'.format(params['host'], params['port']))


@ctl.command('query', help='Query a Patroni PostgreSQL member')
@click.argument('cluster_name')
@option_config_file
@option_format
@click.option('--format', help='Output format (pretty, json)', default='tsv')
@click.option('--file', '-f', help='Execute the SQL commands from this file', type=click.File('rb'))
@option_dcs
@option_watch
@option_watchrefresh
@click.option('--role', '-r', help='The role of the query', type=click.Choice(['master', 'replica', 'any']),
              default=None)
@click.option('--member', '-m', help='Query a specific member', type=str)
@click.option('--delimiter', help='The column delimiter', default='\t')
@click.option('--command', '-c', help='The SQL commands to execute')
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
    file,
    format='tsv',
):
    if role is not None and member is not None:
        raise PatroniCtlException('--role and --member are mutually exclusive options')
    if member is None and role is None:
        role = 'master'

    if file is not None and command is not None:
        raise PatroniCtlException('--file and --command are mutually exclusive options')

    if file is not None:
        command = file.read()

    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    cursor = None
    for _ in watching(w, watch, clear=False):

        output, cursor = query_member(cluster=cluster, cursor=cursor, member=member, role=role, command=command)
        print_output(None, output, format=format, delimiter=delimiter)

        if cursor is None:
            cluster = dcs.get_cluster()


def query_member(cluster, cursor, member, role, command):
    try:
        if cursor is None:
            cursor = get_cursor(cluster, role=role, member=member)

        if cursor is None:
            if role is None:
                message = 'No connection to member {} is available'.format(member)
            else:
                message = 'No connection to role={} is available'.format(role)
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
        return [[timestamp(0), 'ERROR, SQLSTATE: {}'.format(message)]], None


@ctl.command('remove', help='Remove cluster from DCS')
@click.argument('cluster_name')
@option_config_file
@option_format
@option_dcs
def remove(config_file, cluster_name, format, dcs):
    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    if not isinstance(dcs, Etcd):
        raise PatroniCtlException('We have not implemented this for DCS of type {}'.format(type(dcs)))

    output_members(cluster, format=format)

    confirm = click.prompt('Please confirm the cluster name to remove', type=str)
    if confirm != cluster_name:
        raise PatroniCtlException('Cluster names specified do not match')

    message = 'Yes I am aware'
    confirm = \
        click.prompt('You are about to remove all information in DCS for {}, please type: "{}"'.format(cluster_name,
                     message), type=str)
    if message != confirm:
        raise PatroniCtlException('You did not exactly type "{}"'.format(message))

    if cluster.leader:
        confirm = click.prompt('This cluster currently is healthy. Please specify the master name to continue')
        if confirm != cluster.leader.name:
            raise PatroniCtlException('You did not specify the current master of the cluster')

    dcs.client.delete(dcs._base_path, recursive=True)


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

    if len(member_names) == 0:
        member_names = [click.prompt('Which member do you want to {} [{}]?'.format(endpoint,
                        ', '.join(candidates.keys())), type=str, default='')]

    for mn in member_names:
        if mn not in candidates.keys():
            raise PatroniCtlException('{} is not a member of cluster'.format(mn))

    if not force:
        confirm = click.confirm('Are you sure you want to {} members {}?'.format(endpoint, ', '.join(member_names)))
        if not confirm:
            raise PatroniCtlException('Aborted {}'.format(endpoint))

    for mn in member_names:
        r = post_patroni(candidates[mn], endpoint, '')
        if r.status_code != 200:
            click.echo('{} failed for member {}, status code={}, ({})'.format(endpoint, mn, r.status_code, r.text))
        else:
            click.echo('Succesful {} on member {}'.format(endpoint, mn))


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
@click.option('--any', help='Restart a single member only', is_flag=True)
@option_config_file
@option_force
@option_dcs
def restart(cluster_name, member_names, config_file, dcs, force, role, any):
    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    role_names = [m.name for m in get_all_members(cluster=cluster, role=role)]

    if len(member_names) > 0:
        member_names = list(set(member_names) & set(role_names))
    else:
        member_names = role_names

    if any:
        random.shuffle(member_names)
        member_names = member_names[:1]

    output_members(cluster)
    empty_post_to_members(cluster, member_names, force, 'restart')


@ctl.command('reinit', help='Reinitialize cluster member')
@click.argument('cluster_name')
@click.argument('member_names', nargs=-1)
@option_config_file
@option_force
@option_dcs
def reinit(cluster_name, member_names, config_file, dcs, force):
    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)
    empty_post_to_members(cluster, member_names, force, 'reinitialize')


@ctl.command('failover', help='Failover to a replica')
@click.argument('cluster_name')
@click.option('--master', help='The name of the current master', default=None)
@click.option('--candidate', help='The name of the candidate', default=None)
@click.option('--force', is_flag=True)
@option_config_file
@option_dcs
def failover(config_file, cluster_name, master, candidate, force, dcs):
    """
        We want to trigger a failover for the specified cluster name.

        We verify that the cluster name, master name and candidate name are correct.
        If so, we trigger a failover and keep the client up to date.
    """

    config, dcs, cluster = ctl_load_config(cluster_name, config_file, dcs)

    if cluster.leader is None:
        raise PatroniCtlException('This cluster has no master')

    if master is None:
        if force:
            master = cluster.leader.member.name
        else:
            master = click.prompt('Master', type=str, default=cluster.leader.member.name)

    if cluster.leader.member.name != master:
        raise PatroniCtlException('Member {} is not the leader of cluster {}'.format(master, cluster_name))

    candidate_names = [str(m.name) for m in cluster.members if m.name != master]
    # We sort the names for consistent output to the client
    candidate_names.sort()

    if len(candidate_names) == 0:
        raise PatroniCtlException('No candidates found to failover to')

    if candidate is None and not force:
        candidate = click.prompt('Candidate ' + str(candidate_names), type=str, default='')

    if candidate == master:
        raise PatroniCtlException('Failover target and source are the same.')

    if candidate and candidate not in candidate_names:
        raise PatroniCtlException('Member {} does not exist in cluster {}'.format(candidate, cluster_name))

    # By now we have established that the leader exists and the candidate exists
    click.echo('Current cluster topology')
    output_members(dcs.get_cluster(), name=cluster_name)

    if not force:
        a = \
            click.confirm('Are you sure you want to failover cluster {}, demoting current master {}?'.format(
                cluster_name, master))
        if not a:
            raise PatroniCtlException('Aborting failover')

    failover_value = '{}:{}'.format(master, candidate or '')

    t_started = time.time()
    r = None
    try:
        r = post_patroni(cluster.leader.member, 'failover', {'leader': master, 'member': candidate or ''})
        if r.status_code == 200:
            logging.debug(r)
            logging.debug(r.text)
            cluster = dcs.get_cluster()
            click.echo(timestamp() + ' Failing over to new leader: {}'.format(cluster.leader.member.name))
        else:
            click.echo('Failover failed, details: {}, {}'.format(r.status_code, r.text))
            return
    except:
        logging.exception(r)
        logging.warning('Failing over to DCS')
        click.echo(timestamp() + ' Could not failover using Patroni api, falling back to DCS')
        dcs.set_failover_value(failover_value)
        click.echo(timestamp() + ' Initialized failover from master {}'.format(master))
        # The failover process should within a minute update the failover key, we will keep watching it until it changes
        # or we timeout
        cluster = wait_for_leader(dcs, timeout=60)
        if cluster.leader.member.name == master:
            click.echo('Failover failed, master did not change after {:0.1f} seconds'.format(time.time() - t_started))
            return

    click.echo(timestamp() + ' Failover completed in {:0.1f} seconds, new leader is {}'.format(time.time() - t_started,
               str(cluster.leader.member.name)))
    output_members(cluster, name=cluster_name)


def output_members(cluster, name=None, format='pretty'):
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

        xlog_location = m.data.get('xlog_location')
        if xlog_location is None or (xlog_location_cluster < xlog_location):
            lag = ''
        else:
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

    print_output(columns, rows, alignment, format)


@ctl.command('list', help='List the Patroni members for a given Patroni')
@click.argument('cluster_names', nargs=-1)
@option_config_file
@option_format
@option_watch
@option_watchrefresh
@option_dcs
def members(config_file, cluster_names, format, watch, w, dcs):
    if len(cluster_names) == 0:
        logging.warning('Listing members: No cluster names were provided')
        return

    config = load_config(config_file, dcs)
    for cn in cluster_names:
        dcs = get_dcs(config, cn)

        for _ in watching(w, watch):
            output_members(dcs.get_cluster(), name=cn, format=format)


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
