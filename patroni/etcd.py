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
from patroni.dcs import AbstractDCS, Cluster, Failover, Leader, Member
from patroni.exceptions import DCSError
from patroni.utils import Retry, RetryFailedError, sleep
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
        try:
            return super(Client, self).api_execute(path, method, **kwargs)
        except etcd.EtcdConnectionFailed:
            self._update_machines_cache = True
            raise

    @staticmethod
    def get_srv_record(host):
        try:
            return [(str(r.target).rstrip('.'), r.port) for r in resolver.query('_etcd-server._tcp.' + host, 'SRV')]
        except DNSException:
            logger.exception('Can not resolve SRV for %s', host)
        return []

    # try to workarond bug in python-etcd: https://github.com/jplana/python-etcd/issues/81
    def _result_from_response(self, response):
        try:
            response.data.decode('utf-8')
        except urllib3.exceptions.TimeoutError:
            raise
        except Exception as e:
            raise etcd.EtcdException('Unable to decode server response: %s' % e)
        return super(Client, self)._result_from_response(response)

    def _get_machines_cache_from_srv(self, discovery_srv):
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""

        ret = []
        for host, port in self.get_srv_record(discovery_srv):
            url = '{}://{}:{}/members'.format(self._protocol, host, port)
            try:
                response = requests.get(url, timeout=5)
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
        except (RetryFailedError, etcd.EtcdException):
            return False
    return wrapper


class Etcd(AbstractDCS):

    def __init__(self, name, config):
        super(Etcd, self).__init__(name, config)
        self.ttl = config['ttl']
        self._retry = Retry(deadline=10, max_delay=1, max_tries=-1,
                            retry_exceptions=(etcd.EtcdConnectionFailed,
                                              etcd.EtcdLeaderElectionInProgress,
                                              etcd.EtcdWatcherCleared,
                                              etcd.EtcdEventIndexCleared))
        self.client = self.get_etcd_client(config)

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

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
        return Member.from_node(node.modifiedIndex, os.path.basename(node.key), node.ttl, node.value)

    def _load_cluster(self):
        try:
            result = self.retry(self.client.read, self.client_path(''), recursive=True)
            nodes = {os.path.relpath(node.key, result.key): node for node in result.leaves}

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE, None)
            initialize = initialize and initialize.value

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME, None)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation.value)

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER, None)
            if leader:
                member = Member(-1, leader.value, None, {})
                member = ([m for m in members if m.name == leader.value] or [member])[0]
                leader = Leader(leader.modifiedIndex, leader.ttl, member)

            # failover key
            failover = nodes.get(self._FAILOVER, None)
            if failover:
                failover = Failover.from_node(failover.modifiedIndex, failover.value)

            self._cluster = Cluster(initialize, leader, last_leader_operation, members, failover)
        except etcd.EtcdKeyNotFound:
            self._cluster = Cluster(False, None, None, [], None)
        except:
            logger.exception('get_cluster')
            raise EtcdError('Etcd is not responding properly')

    @catch_etcd_errors
    def touch_member(self, connection_string, ttl=None):
        return self.retry(self.client.set, self.member_path, connection_string, ttl or self.ttl)

    @catch_etcd_errors
    def take_leader(self):
        return self.retry(self.client.set, self.leader_path, self._name, self.ttl)

    def attempt_to_acquire_leader(self):
        try:
            return bool(self.retry(self.client.write, self.leader_path, self._name, ttl=self.ttl, prevExist=False))
        except etcd.EtcdAlreadyExist:
            logger.info('Could not take out TTL lock')
        except (RetryFailedError, etcd.EtcdException):
            pass
        return False

    @catch_etcd_errors
    def set_failover_value(self, value, index=None):
        return self.client.write(self.failover_path, value, prevIndex=index or 0)

    @catch_etcd_errors
    def write_leader_optime(self, last_operation):
        return self.client.set(self.leader_optime_path, last_operation)

    @catch_etcd_errors
    def update_leader(self):
        return self.retry(self.client.test_and_set, self.leader_path, self._name, self._name, self.ttl)

    @catch_etcd_errors
    def initialize(self, create_new=True, sysid=""):
        return self.retry(self.client.write, self.initialize_path, sysid, prevExist=(not create_new))

    @catch_etcd_errors
    def delete_leader(self):
        return self.client.delete(self.leader_path, prevValue=self._name)

    @catch_etcd_errors
    def cancel_initialization(self):
        return self.retry(self.client.delete, self.initialize_path)

    def watch(self, timeout):
        cluster = self.cluster
        # watch on leader key changes if it is defined and current node is not lock owner
        if cluster and cluster.leader and cluster.leader.name != self._name:
            end_time = time.time() + timeout
            index = cluster.leader.index

            while index and timeout >= 1:  # when timeout is too small urllib3 doesn't have enough time to connect
                try:
                    self.client.watch(self.leader_path, index=index + 1, timeout=timeout + 0.5)
                    # Synchronous work of all cluster members with etcd is less expensive
                    # than reestablishing http connection every time from every replica.
                    return True
                except urllib3.exceptions.TimeoutError:
                    self.client.http.clear()
                    return False
                except etcd.EtcdException:
                    logging.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Etcd, self).watch(timeout)
        finally:
            self.event.clear()
