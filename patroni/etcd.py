from __future__ import absolute_import
import etcd
import logging
import os
import random
import requests
import socket
import time

from dns.exception import DNSException
from dns import resolver
from patroni.dcs import AbstractDCS, Cluster, Failover, Leader, Member
from patroni.exceptions import DCSError
from patroni.utils import Retry, RetryFailedError, sleep
from urllib3.exceptions import HTTPError, ReadTimeoutError
from requests.exceptions import RequestException
from six.moves.http_client import HTTPException

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

    def _do_http_request(self, request_executor, method, url, fields=None, **kwargs):
        try:
            response = request_executor(method, url, fields=fields, **kwargs)
            response.data.decode('utf-8')
            self._check_cluster_id(response)
        except (HTTPError, HTTPException, socket.error, socket.timeout) as e:
            if (isinstance(fields, dict) and fields.get("wait") == "true" and
                    isinstance(e, ReadTimeoutError)):
                logger.debug("Watch timed out.")
                raise etcd.EtcdWatchTimedOut("Watch timed out: {0}".format(e), cause=e)
            logger.error("Request to server %s failed: %r", self._base_uri, e)
            logger.info("Reconnection allowed, looking for another server.")
            self._base_uri = self._next_server(cause=e)
            response = False
        return response

    def api_execute(self, path, method, params=None, timeout=None):
        if not path.startswith('/'):
            raise ValueError('Path does not start with /')

        if timeout is None:
            timeout = self.read_timeout

        if timeout == 0:
            timeout = None

        kwargs = {'timeout': timeout, 'fields': params, 'redirect': self.allow_redirect,
                  'headers': self._get_headers(), 'preload_content': False}

        if method in [self._MGET, self._MDELETE]:
            request_executor = self.http.request
        elif method in [self._MPUT, self._MPOST]:
            request_executor = self.http.request_encode_body
            kwargs['encode_multipart'] = False
        else:
            raise etcd.EtcdException('HTTP method {0} not supported'.format(method))

        # Update machines_cache if previous attempt of update has failed
        if self._update_machines_cache:
            self._load_machines_cache()

        response = False

        try:
            while not response:
                response = self._do_http_request(request_executor, method, self._base_uri + path, **kwargs)

                if response is False and not self._use_proxies:
                    self._machines_cache = self.machines
                    self._machines_cache.remove(self._base_uri)
            return self._handle_server_response(response)
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

    def _get_machines_cache_from_srv(self, discovery_srv):
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""

        ret = []
        for host, port in self.get_srv_record(discovery_srv):
            url = '{0}://{1}:{2}/members'.format(self._protocol, host, port)
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
                ret.append('{0}://{1}:{2}'.format(self._protocol, r[4][0], r[4][1]))
        except socket.error:
            logger.exception('Can not resolve %s', host)
        return list(set(ret)) if ret else ['{0}://{1}:{2}'.format(self._protocol, host, port)]

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

        if self._base_uri in self._machines_cache:
            self._machines_cache.remove(self._base_uri)

        self._update_machines_cache = False


def catch_etcd_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs) is not None
        except (RetryFailedError, etcd.EtcdException):
            return False
        except:
            logger.exception("")
            raise EtcdError("unexpected error")

    return wrapper


class Etcd(AbstractDCS):

    def __init__(self, name, config):
        super(Etcd, self).__init__(name, config)
        self.ttl = config.get('ttl', 30)
        self._retry = Retry(deadline=10, max_delay=1, max_tries=-1,
                            retry_exceptions=(etcd.EtcdConnectionFailed,
                                              etcd.EtcdLeaderElectionInProgress,
                                              etcd.EtcdWatcherCleared,
                                              etcd.EtcdEventIndexCleared))
        self._client = self.get_etcd_client(config)

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    @staticmethod
    def get_etcd_client(config):
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
            result = self.retry(self._client.read, self.client_path(''), recursive=True)
            nodes = {os.path.relpath(node.key, result.key): node for node in result.leaves}

            # get initialize flag
            initialize = nodes.get(self._INITIALIZE)
            initialize = initialize and initialize.value

            # get last leader operation
            last_leader_operation = nodes.get(self._LEADER_OPTIME)
            last_leader_operation = 0 if last_leader_operation is None else int(last_leader_operation.value)

            # get list of members
            members = [self.member(n) for k, n in nodes.items() if k.startswith(self._MEMBERS) and k.count('/') == 1]

            # get leader
            leader = nodes.get(self._LEADER)
            if leader:
                member = Member(-1, leader.value, None, {})
                member = ([m for m in members if m.name == leader.value] or [member])[0]
                leader = Leader(leader.modifiedIndex, leader.ttl, member)

            # failover key
            failover = nodes.get(self._FAILOVER)
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
        return self.retry(self._client.set, self.member_path, connection_string, ttl or self.ttl)

    @catch_etcd_errors
    def take_leader(self):
        return self.retry(self._client.set, self.leader_path, self._name, self.ttl)

    def attempt_to_acquire_leader(self):
        try:
            return bool(self.retry(self._client.write, self.leader_path, self._name, ttl=self.ttl, prevExist=False))
        except etcd.EtcdAlreadyExist:
            logger.info('Could not take out TTL lock')
        except (RetryFailedError, etcd.EtcdException):
            pass
        return False

    @catch_etcd_errors
    def set_failover_value(self, value, index=None):
        return self._client.write(self.failover_path, value, prevIndex=index or 0)

    @catch_etcd_errors
    def write_leader_optime(self, last_operation):
        return self._client.set(self.leader_optime_path, last_operation)

    @catch_etcd_errors
    def update_leader(self):
        return self.retry(self._client.test_and_set, self.leader_path, self._name, self._name, self.ttl)

    @catch_etcd_errors
    def initialize(self, create_new=True, sysid=""):
        return self.retry(self._client.write, self.initialize_path, sysid, prevExist=(not create_new))

    @catch_etcd_errors
    def delete_leader(self):
        return self._client.delete(self.leader_path, prevValue=self._name)

    @catch_etcd_errors
    def cancel_initialization(self):
        return self.retry(self._client.delete, self.initialize_path)

    @catch_etcd_errors
    def delete_cluster(self):
        return self.retry(self._client.delete, self.client_path(''), recursive=True)

    def watch(self, timeout):
        cluster = self.cluster
        # watch on leader key changes if it is defined and current node is not lock owner
        if cluster and cluster.leader and cluster.leader.name != self._name and cluster.leader.index:
            end_time = time.time() + timeout

            while timeout >= 1:  # when timeout is too small urllib3 doesn't have enough time to connect
                try:
                    self._client.watch(self.leader_path, index=cluster.leader.index + 1, timeout=timeout + 0.5)
                    # Synchronous work of all cluster members with etcd is less expensive
                    # than reestablishing http connection every time from every replica.
                    return True
                except etcd.EtcdWatchTimedOut:
                    self._client.http.clear()
                    return False
                except etcd.EtcdException:
                    logging.exception('watch')

                timeout = end_time - time.time()

        try:
            return super(Etcd, self).watch(timeout)
        finally:
            self.event.clear()
