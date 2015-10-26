#!/usr/bin/env python3
'''
Patroni Command Line Client
'''

import click
import os
import yaml
import json
import time
import requests
import datetime
from prettytable import PrettyTable
from six.moves.urllib_parse import urlparse
import logging

from .etcd import Etcd

CONFIG_DIR_PATH = click.get_app_dir('patroni')
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR_PATH, 'patronicli.yaml')
LOGLEVEL = 'DEBUG'


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
        parsed = urlparse('//'+dcs)

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


@click.group()
@click.pass_context
def cli(ctx):
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=LOGLEVEL)


def get_dcs(config, scope):
    scheme, hostname, port = map(config.get('dcs', {}).get, ('scheme', 'hostname', 'port'))

    if scheme == 'etcd':
        return Etcd(name=scope, config={'scope': scope, 'host': '{}:{}'.format(hostname, port)})

    raise Exception('Can not find suitable configuration of distributed configuration store')


def post_patroni(member, endpoint, content, headers={'Content-Type': 'application/json'}):
    url = urlparse(member.api_url)
    logging.debug(url)
    r = requests.post('{}://{}/{}'.format(url.scheme, url.netloc, endpoint), headers=headers, data=json.dumps(content))
    return r


def print_output(columns, rows=[], alignment=None, format='pretty'):
    if format == 'pretty':
        t = PrettyTable(columns)
        for k, v in (alignment or {}).items():
            t.align[k] = v
        for r in rows:
            t.add_row(r)
        print(t)
        return

    if format == 'json':
        elements = list()
        for r in rows:
            elements.append(dict(zip(columns, r)))

        print(json.dumps(elements))


def watching(w, watch):
    if w and not watch:
        watch = 2
    if watch:
        click.clear()
    yield 0
    if watch:
        while True:
            time.sleep(watch)
            click.clear()
            yield 0


@cli.command('remove', help='Remove cluster from DCS')
@click.argument('cluster_name')
@option_config_file
@option_format
@option_dcs
def remove(config_file, cluster_name, format, dcs):
    config = load_config(config_file, dcs)
    dcs = get_dcs(config, cluster_name)
    cluster = dcs.get_cluster()

    output_members(cluster, format=format)

    if cluster.name is None:
        raise Exception("This does not seem to be a valid Patroni cluster")

    confirm = click.prompt('Please confirm the cluster name to remove', type=str)
    if confirm != cluster_name:
        raise Exception("Cluster names specified do not match")

    message = 'Yes I am aware'
    confirm = click.prompt('You are about to remove all information in DCS for {}, please type: "{}"'.format(
        cluster_name, message), type=str)
    if message != confirm:
        raise Exception('You did not exactly type "{}"'.format(message))

    if cluster.leader:
        confirm = click.prompt('This cluster currently is healthy. Please specify the master name to continue')
        if confirm != cluster.leader.name:
            raise Exception("You did not specify the current master of the cluster")

    if isinstance(dcs, Etcd):
        dcs.client.delete(dcs._base_path, recursive=True)
    else:
        raise Exception("We have not implemented this for DCS of type {}", type(dcs))


def wait_for_master(dcs, timeout=30):
    t_stop = time.time() + timeout
    timeout /= 2

    while time.time() < t_stop:
        dcs.watch(timeout)
        cluster = dcs.get_cluster()

        if cluster.leader and cluster.leader.member.data['role'] == 'master':
            return cluster

    raise Exception('Timeout occured')


@cli.command('failover', help='Failover to a replica')
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
    config = load_config(config_file, dcs)
    dcs = get_dcs(config, cluster_name)
    cluster = dcs.get_cluster()

    if cluster.leader is None:
        raise Exception('This cluster has no master')

    if master is None:
        if force:
            master = cluster.leader.member.name
        else:
            master = click.prompt('Master', type=str, default=cluster.leader.member.name)

    if cluster.leader.member.name != master:
        raise Exception('Member {} is not the leader of cluster {}'.format(master, cluster_name))

    candidate_names = [str(m.name) for m in cluster.members if m.name != master]
    candidate_names.sort()

    if candidate is None and not force:
        candidate = click.prompt('Candidate '+str(candidate_names), type=str, default='')

    if candidate and candidate not in candidate_names:
        raise Exception('Member {} does not exist in cluster {}'.format(candidate, cluster_name))

    # By now we have established that the leader exists and the candidate exists
    click.echo('Current cluster topology')
    output_members(dcs.get_cluster(), name=cluster_name)

    if not force:
        a = click.confirm('Are you sure you want to failover cluster {}, demoting current master {}?'.format(
            cluster_name, master))
        if not a:
            raise Exception('Aborting failover')

    failover_value = '{}:{}'.format(master, (candidate or ''))

    t_started = time.time()
    try:
        r = post_patroni(cluster.leader.member, 'failover', {'leader': master, 'candidate': (candidate or '')})
        if r.status_code == 200:
            logging.debug(r)
            logging.debug(r.text)
            cluster = dcs.get_cluster()
            click.echo(timestamp()+' Failing over to new leader: {}'.format(cluster.leader.member.name))
        else:
            click.echo('Failover failed, details: {}, {}'.format(r.status_code, r.text))
            return
    except:
        logging.exception(r)
        logging.warning('Failing over to DCS')
        click.echo(timestamp()+' Could not failover using Patroni api, falling back to DCS')
        dcs.set_failover_value(failover_value)
        click.echo(timestamp()+' Initialized failover from master {}'.format(master))

    # The failover process should within a minute update the failover key, we will keep watching it until it changes
    # or we timeout
    cluster = wait_for_master(dcs, timeout=60)
    click.echo(timestamp()+' Failover completed in {:0.1f} seconds, new leader is {}'.format(
        time.time() - t_started, str(cluster.leader.member.name)))
    output_members(cluster, name=cluster_name)


def output_members(cluster, name=None, format='pretty'):
    rows = []
    logging.debug(cluster)
    leader_name = None
    if cluster.leader:
        leader_name = cluster.leader.member.name

    # Mainly for consistent pretty printing and watching we sort the output
    cluster.members.sort(key=lambda x: x.name)
    for m in cluster.members:
        logging.debug(m)

        leader = ''
        if m.name == leader_name:
            leader = '*'
            role = m.data['role']
        else:
            role = 'replica'

        rows.append([name, m.name, role, leader])

    print_output(['Cluster', 'Member', 'Role', 'Leader'], rows, {'Cluster': 'l', 'Member': 'l', 'Role': 'l'}, format)


@cli.command('list', help='List the Patroni members for a given Patroni')
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


def timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


@cli.command('configure', help='Create configuration file')
@click.option('--config-file', '-c', help='Configuration file', prompt='Configuration file', default=CONFIG_FILE_PATH)
@click.option('--dcs', '-d', help='The DCS connect url', prompt='DCS connect url', default='etcd://localhost:4001')
@click.option('--namespace', '-n', help='The namespace', prompt='Namespace', default='/service/')
def configure(config_file, dcs, namespace):
    config = dict()
    config['dcs_api'] = str(dcs)
    config['namespace'] = str(namespace)
    store_config(config, config_file)
