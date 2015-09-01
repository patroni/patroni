from __future__ import absolute_import
import etcd
import logging
import os
import random
import requests
import socket
import time
import urllib3

from dns.exception import DNSException
from dns import resolver
from helpers.dcs import AbstractDCS, Cluster, DCSError, Leader, Member, parse_connection_string
from helpers.utils import sleep
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class EtcdError(DCSError):
    pass


class Client(etcd.Client):

    def __init__(self, config):
        super(Client, self).__init__(read_timeout=5)
        self._config = config
        self._load_machines_cache()
        self._allow_reconnect = True

    @property
    def machines(self):
        """Original `machines` method(property) of `etcd.Client` class raise exception
        when it failed to get list of etcd cluster members. This method is being called
        only when request failed on one of the etcd members during `api_execute` call.
        For us it's more important to execute original request rather then get new
        topology of etcd cluster. So we will catch this exception and return valid list
        of machines with setting flag `self._update_machines_cache` to `!True`.
        Later, during next `api_execute` call we will forcefully update machines_cache"""
        try:
            ret = super(Client, self).machines
            random.shuffle(ret)
            return ret
        except etcd.EtcdException:
            if self._update_machines_cache:  # We are updating machines_cache
                raise  # This exception is fatal, we should re-raise it.
            self._update_machines_cache = True
            return [self._base_uri]

    def api_execute(self, path, method, **kwargs):
        # Update machines_cache if previous attempt of update has failed
        self._update_machines_cache and self._load_machines_cache()
        return super(Client, self).api_execute(path, method, **kwargs)

    @staticmethod
    def get_srv_record(host):
        try:
            return [(str(r.target).rstrip('.'), r.port) for r in resolver.query('_etcd-server._tcp.' + host, 'SRV')]
        except DNSException:
            logger.exception('Can not resolve SRV for %s', host)
        return []

    # try to workarond bug in python-etcd: https://github.com/jplana/python-etcd/issues/81
    def _result_from_response(self, response):
        response.data.decode('utf-8')
        return super(Client, self)._result_from_response(response)

    def _get_machines_cache_from_srv(self, discovery_srv):
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""

        ret = []
        for host, port in self.get_srv_record(discovery_srv):
            url = '{}://{}:{}/members'.format(self._protocol, host, port)
            try:
                response = requests.get(url)
                if response.ok:
                    for member in response.json():
                        ret.extend(member['clientURLs'])
                    break
            except RequestException:
                logger.exception('GET %s', url)
        return list(set(ret))

    def _get_machines_cache_from_dns(self, addr):
        """One host might be resolved into multiple ip addresses. We will make list out of it"""

        ret = []
        host, port = addr.split(':')
        try:
            for r in set(socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)):
                ret.append('{}://{}:{}'.format(self._protocol, r[4][0], r[4][1]))
        except socket.error:
            logger.exception('Can not resolve %s', host)
        return list(set(ret)) if ret else ['{}://{}:{}'.format(self._protocol, host, port)]

    def _load_machines_cache(self):
        """This method should fill up `_machines_cache` from scratch.
        It could happen only in two cases:
        1. During class initialization
        2. When all etcd members failed"""

        self._update_machines_cache = True

        if 'discovery_srv' not in self._config and 'host' not in self._config:
            raise Exception('Neither discovery_srv nor host are defined in etcd section of config')

        self._machines_cache = []

        if 'discovery_srv' in self._config:
            self._machines_cache = self._get_machines_cache_from_srv(self._config['discovery_srv'])

        if not self._machines_cache and 'host' in self._config:
            self._machines_cache = self._get_machines_cache_from_dns(self._config['host'])

        # Can not bootstrap list of etcd-cluster members, giving up
        if not self._machines_cache:
            raise etcd.EtcdException

        # After filling up initial list of machines_cache we should ask etcd-cluster about actual list
        self._base_uri = self._machines_cache.pop(0)
        self._machines_cache = self.machines
        self._base_uri in self._machines_cache and self._machines_cache.remove(self._base_uri)

        self._update_machines_cache = False


def catch_etcd_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return not func(*args, **kwargs) is None
        except etcd.EtcdException:
            return False
    return wrapper


class Etcd(AbstractDCS):

    def __init__(self, name, config):
        super(Etcd, self).__init__(name, config)
        self.ttl = config['ttl']
        self.member_ttl = config.get('member_ttl', 3600)
        self.client = self.get_etcd_client(config)
        self.cluster = None

    def get_etcd_client(self, config):
        client = None
        while not client:
            try:
                client = Client(config)
            except etcd.EtcdException:
                logger.info('waiting on etcd')
                sleep(5)
        return client

    @staticmethod
    def member(node):
        conn_url, api_url = parse_connection_string(node.value)
        return Member(node.modifiedIndex, os.path.basename(node.key), conn_url, api_url, node.expiration, node.ttl)

    def get_cluster(self):
        try:
            result = self.client.read(self.client_path(''), recursive=True)
            nodes = {os.path.relpath(node.key, result.key): node for node in result.leaves}

            # get initialize flag
            initialize = bool(nodes.get('initialize', False))

            # get last leader operation
            last_leader_operation = nodes.get('optime/leader', None)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation.value)

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith('members/') and len(k.split('/')) == 2]

            # get leader
            leader = nodes.get('leader', None)
            if leader:
                member = Member(-1, leader.value, None, None, None, None)
                member = ([m for m in members if m.name == leader.value] or [member])[0]
                leader = Leader(leader.modifiedIndex, leader.expiration, leader.ttl, member)

            self.cluster = Cluster(initialize, leader, last_leader_operation, members)
            return self.cluster
        except etcd.EtcdKeyNotFound:
            return Cluster(False, None, None, [])
        except:
            logger.exception('get_cluster')

        self.cluster = None
        raise EtcdError('Etcd is not responding properly')

    @catch_etcd_errors
    def touch_member(self, connection_string, ttl=None):
        return self.client.set(self.client_path('/members/' + self._name), connection_string, ttl or self.member_ttl)

    @catch_etcd_errors
    def take_leader(self):
        return self.client.set(self.client_path('/leader'), self._name, self.ttl)

    @catch_etcd_errors
    def attempt_to_acquire_leader(self):
        ret = self.client.write(self.client_path('/leader'), self._name, ttl=self.ttl, prevExist=False)
        ret or logger.info('Could not take out TTL lock')
        return ret

    @catch_etcd_errors
    def write_leader_optime(self, state_handler):
        return self.client.set(self.client_path('/optime/leader'), state_handler.last_operation())

    @catch_etcd_errors
    def update_leader(self, state_handler):
        ret = self.client.test_and_set(self.client_path('/leader'), self._name, self._name, self.ttl)
        ret and self.write_leader_optime(state_handler)
        return ret

    @catch_etcd_errors
    def race(self, path):
        return self.client.write(self.client_path(path), self._name, prevExist=False)

    @catch_etcd_errors
    def delete_leader(self):
        return self.client.delete(self.client_path('/leader'), prevValue=self._name)

    def sleep(self, timeout):
        # watch on leader key changes if it is defined and current node is not lock owner
        if self.cluster and self.cluster.leader and self.cluster.leader.member.name != self._name:
            end_time = time.time() + timeout
            index = self.cluster.leader.index

            while index and timeout >= 1:  # when timeout is too small urllib3 doesn't have enough time to connect
                try:
                    res = self.client.watch(self.client_path('/leader'), index=index + 1, timeout=timeout)
                    if res.action not in ['set', 'compareAndSwap'] or res.value != self.cluster.leader.member.name:
                        return
                    index = res.modifiedIndex
                except urllib3.exceptions.TimeoutError:
                    self.client.http.clear()
                    return
                except etcd.EtcdException:
                    index = None

                timeout = end_time - time.time()

        timeout > 0 and super(Etcd, self).sleep(timeout)
