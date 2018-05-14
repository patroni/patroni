from __future__ import absolute_import
import etcd
import json
import logging
import os
import urllib3.util.connection
import random
import requests
import six
import socket
import time

from dns.exception import DNSException
from dns import resolver
from patroni.dcs import AbstractDCS, ClusterConfig, Cluster, Failover, Leader, Member, SyncState, TimelineHistory
from patroni.exceptions import DCSError
from patroni.utils import Retry, RetryFailedError, split_host_port
from urllib3.exceptions import HTTPError, ReadTimeoutError
from requests.exceptions import RequestException
from six.moves.queue import Queue
from six.moves.http_client import HTTPException
from six.moves.urllib_parse import urlparse
from threading import Thread

logger = logging.getLogger(__name__)


def uri(protocol, host, port, endpoint=''):
    return '{0}://{1}:{2}{3}'.format(protocol, host, port, endpoint)


class EtcdError(DCSError):
    pass


class DnsCachingResolver(Thread):

    def __init__(self, cache_time=600.0, cache_fail_time=30.0):
        super(DnsCachingResolver, self).__init__()
        self._cache = {}
        self._cache_time = cache_time
        self._cache_fail_time = cache_fail_time
        self._resolve_queue = Queue()
        self.daemon = True
        self.start()

    def run(self):
        while True:
            (host, port), attempt = self._resolve_queue.get()
            response = self._do_resolve(host, port)
            if response:
                self._cache[(host, port)] = (time.time(), response)
            else:
                if attempt < 10:
                    self.resolve_async(host, port, attempt + 1)
                    time.sleep(1)

    def resolve(self, host, port):
        current_time = time.time()
        cached_time, response = self._cache.get((host, port), (0, []))
        time_passed = current_time - cached_time
        if time_passed > self._cache_time or (not response and time_passed > self._cache_fail_time):
            new_response = self._do_resolve(host, port)
            if new_response:
                self._cache[(host, port)] = (current_time, new_response)
                response = new_response
        return response

    def resolve_async(self, host, port, attempt=0):
        self._resolve_queue.put(((host, port), attempt))

    @staticmethod
    def _do_resolve(host, port):
        try:
            return socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        except socket.gaierror:
            logger.warning('failed to resolve host %s', host)
            return []


class Client(etcd.Client):

    def __init__(self, config, dns_resolver, cache_ttl=300):
        self._dns_resolver = dns_resolver
        self.set_machines_cache_ttl(cache_ttl)
        self._machines_cache_updated = 0
        args = {p: config.get(p) for p in ('host', 'port', 'protocol', 'use_proxies', 'username', 'password',
                                           'cert', 'ca_cert') if config.get(p)}
        super(Client, self).__init__(read_timeout=config['retry_timeout'], **args)
        self._config = config
        self._load_machines_cache()
        self._allow_reconnect = not self._use_proxies

    def _build_request_parameters(self):
        kwargs = {'headers': self._get_headers(), 'redirect': self.allow_redirect}

        # calculate the number of retries and timeout *per node*
        # actual number of retries depends on the number of nodes
        etcd_nodes = len(self._machines_cache) + 1
        kwargs['retries'] = 0 if etcd_nodes > 3 else (1 if etcd_nodes > 1 else 2)

        # if etcd_nodes > 3:
        #     kwargs.update({'retries': 0, 'timeout': float(self.read_timeout)/etcd_nodes})
        # elif etcd_nodes > 1:
        #     kwargs.update({'retries': 1, 'timeout': self.read_timeout/2.0/etcd_nodes})
        # else:
        #     kwargs.update({'retries': 2, 'timeout': self.read_timeout/3.0})
        kwargs['timeout'] = self.read_timeout/float(kwargs['retries'] + 1)/etcd_nodes
        return kwargs

    def set_machines_cache_ttl(self, cache_ttl):
        self._machines_cache_ttl = cache_ttl

    @property
    def machines(self):
        """Original `machines` method(property) of `etcd.Client` class raise exception
        when it failed to get list of etcd cluster members. This method is being called
        only when request failed on one of the etcd members during `api_execute` call.
        For us it's more important to execute original request rather then get new topology
        of etcd cluster. So we will catch this exception and return empty list of machines.
        Later, during next `api_execute` call we will forcefully update machines_cache.

        Also this method implements the same timeout-retry logic as `api_execute`, because
        the original method was retrying 2 times with the `read_timeout` on each node."""

        kwargs = self._build_request_parameters()

        while True:
            try:
                response = self.http.request(self._MGET, self._base_uri + self.version_prefix + '/machines', **kwargs)
                machines = [n.strip() for n in self._handle_server_response(response).data.decode('utf-8').split(',')]
                logger.debug("Retrieved list of machines: %s", machines)
                random.shuffle(machines)
                for url in machines:
                    r = urlparse(url)
                    port = r.port or (443 if r.scheme == 'https' else 80)
                    self._dns_resolver.resolve_async(r.hostname, port)
                return machines
            except Exception as e:
                # We can't get the list of machines, if one server is in the
                # machines cache, try on it
                logger.error("Failed to get list of machines from %s%s: %r", self._base_uri, self.version_prefix, e)
                if self._machines_cache:
                    self._base_uri = self._machines_cache.pop(0)
                    logger.info("Retrying on %s", self._base_uri)
                elif self._update_machines_cache:
                    raise etcd.EtcdException("Could not get the list of servers, "
                                             "maybe you provided the wrong "
                                             "host(s) to connect to?")
                else:
                    return []

    def set_read_timeout(self, timeout):
        self._read_timeout = timeout

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

        kwargs = {'fields': params, 'preload_content': False}

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
        elif time.time() - self._machines_cache_updated > self._machines_cache_ttl:
            self._machines_cache = self.machines
            if self._base_uri in self._machines_cache:
                self._machines_cache.remove(self._base_uri)
            self._machines_cache_updated = time.time()

        kwargs.update(self._build_request_parameters())

        if timeout is not None:
            kwargs.update({'retries': 0, 'timeout': timeout})

        response = False

        try:
            some_request_failed = False
            while not response:
                response = self._do_http_request(request_executor, method, self._base_uri + path, **kwargs)

                if response is False:
                    some_request_failed = True
            if some_request_failed and not self._use_proxies:
                self._machines_cache = self.machines
                if self._base_uri in self._machines_cache:
                    self._machines_cache.remove(self._base_uri)
        except etcd.EtcdConnectionFailed as e:
            if isinstance(e, etcd.EtcdWatchTimedOut) and self._machines_cache:
                self._base_uri = self._next_server()
            else:
                self._update_machines_cache = True
            if not response:
                raise
        return self._handle_server_response(response)

    @staticmethod
    def get_srv_record(host):
        try:
            return [(r.target.to_text(True), r.port) for r in resolver.query(host, 'SRV')]
        except DNSException:
            return []

    def _get_machines_cache_from_srv(self, srv):
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""

        ret = []
        for r in ['-client-ssl', '-client', '-ssl', '', '-server-ssl', '-server']:
            protocol = 'https' if '-ssl' in r else 'http'
            endpoint = '/members' if '-server' in r else ''
            for host, port in self.get_srv_record('_etcd{0}._tcp.{1}'.format(r, srv)):
                url = uri(protocol, host, port, endpoint)
                if endpoint:
                    try:
                        response = requests.get(url, timeout=self.read_timeout, verify=False)
                        if response.ok:
                            for member in response.json():
                                ret.extend(member['clientURLs'])
                            break
                    except RequestException:
                        logger.exception('GET %s', url)
                else:
                    ret.append(url)
            if ret:
                self._protocol = protocol
                break
        else:
            logger.warning('Can not resolve SRV for %s', srv)
        return list(set(ret))

    def _get_machines_cache_from_dns(self, host, port):
        """One host might be resolved into multiple ip addresses. We will make list out of it"""
        if self.protocol == 'http':
            ret = []
            for af, _, _, _, sa in self._dns_resolver.resolve(host, port):
                host, port = sa[:2]
                if af == socket.AF_INET6:
                    host = '[{0}]'.format(host)
                ret.append(uri(self.protocol, host, port))
            if ret:
                return list(set(ret))
        return [uri(self.protocol, host, port)]

    def _load_machines_cache(self):
        """This method should fill up `_machines_cache` from scratch.
        It could happen only in two cases:
        1. During class initialization
        2. When all etcd members failed"""

        self._update_machines_cache = True

        if 'srv' not in self._config and 'host' not in self._config and 'hosts' not in self._config:
            raise Exception('Neither srv, hosts, host nor url are defined in etcd section of config')

        if self._use_proxies:
            self._machines_cache = [uri(self.protocol, self._config['host'], self._config['port'])]
        else:
            self._machines_cache = []

            if 'srv' in self._config:
                self._machines_cache = self._get_machines_cache_from_srv(self._config['srv'])

            if not self._machines_cache and 'hosts' in self._config:
                self._machines_cache = list(self._config['hosts'])

            if not self._machines_cache and 'host' in self._config:
                self._machines_cache = self._get_machines_cache_from_dns(self._config['host'], self._config['port'])

        # Can not bootstrap list of etcd-cluster members, giving up
        if not self._machines_cache:
            raise etcd.EtcdException

        # After filling up initial list of machines_cache we should ask etcd-cluster about actual list
        self._base_uri = self._next_server()
        self._machines_cache = self.machines

        if self._base_uri in self._machines_cache:
            self._machines_cache.remove(self._base_uri)

        self._update_machines_cache = False
        self._machines_cache_updated = time.time()


class Etcd(AbstractDCS):

    def __init__(self, config):
        super(Etcd, self).__init__(config)
        self._ttl = int(config.get('ttl') or 30)
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(etcd.EtcdLeaderElectionInProgress,
                                              etcd.EtcdWatcherCleared,
                                              etcd.EtcdEventIndexCleared))
        self._client = self.get_etcd_client(config)
        self.__do_not_watch = False
        self._has_failed = False

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    def _handle_exception(self, e, name='', do_sleep=False, raise_ex=None):
        if not self._has_failed:
            logger.exception(name)
        else:
            logger.error(e)
            if do_sleep:
                time.sleep(1)
        self._has_failed = True
        if isinstance(raise_ex, Exception):
            raise raise_ex

    def catch_etcd_errors(func):
        def wrapper(self, *args, **kwargs):
            try:
                retval = func(self, *args, **kwargs) is not None
                self._has_failed = False
                return retval
            except (RetryFailedError, etcd.EtcdException) as e:
                self._handle_exception(e)
                return False
            except Exception as e:
                self._handle_exception(e, raise_ex=EtcdError('unexpected error'))

        return wrapper

    @staticmethod
    def get_etcd_client(config):
        if 'proxy' in config:
            config['use_proxies'] = True
            config['url'] = config['proxy']

        if 'url' in config:
            r = urlparse(config['url'])
            config.update({'protocol': r.scheme, 'host': r.hostname, 'port': r.port or 2379,
                           'username': r.username, 'password': r.password})
        elif 'hosts' in config:
            hosts = config.pop('hosts')
            default_port = config.pop('port', 2379)
            protocol = config.get('protocol', 'http')

            if isinstance(hosts, six.string_types):
                hosts = hosts.split(',')

            config['hosts'] = []
            for value in hosts:
                if isinstance(value, six.string_types):
                    config['hosts'].append(uri(protocol, *split_host_port(value, default_port)))
        elif 'host' in config:
            host, port = split_host_port(config['host'], 2379)
            config['host'] = host
            if 'port' not in config:
                config['port'] = int(port)

        if config.get('cacert'):
            config['ca_cert'] = config.pop('cacert')

        if config.get('key') and config.get('cert'):
            config['cert'] = (config['cert'], config['key'])

        for p in ('discovery_srv', 'srv_domain'):
            if p in config:
                config['srv'] = config.pop(p)

        dns_resolver = DnsCachingResolver()

        def create_connection_patched(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                                      source_address=None, socket_options=None):
            host, port = address
            if host.startswith('['):
                host = host.strip('[]')
            err = None
            for af, socktype, proto, _, sa in dns_resolver.resolve(host, port):
                sock = None
                try:
                    sock = socket.socket(af, socktype, proto)
                    if socket_options:
                        for opt in socket_options:
                            sock.setsockopt(*opt)
                    if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                        sock.settimeout(timeout)
                    if source_address:
                        sock.bind(source_address)
                    sock.connect(sa)
                    return sock

                except socket.error as e:
                    err = e
                    if sock is not None:
                        sock.close()
                        sock = None

            if err is not None:
                raise err

            raise socket.error("getaddrinfo returns an empty list")

        urllib3.util.connection.create_connection = create_connection_patched

        client = None
        while not client:
            try:
                client = Client(config, dns_resolver)
            except etcd.EtcdException:
                logger.info('waiting on etcd')
                time.sleep(5)
        return client

    def set_ttl(self, ttl):
        ttl = int(ttl)
        self.__do_not_watch = self._ttl != ttl
        self._ttl = ttl
        self._client.set_machines_cache_ttl(ttl*10)

    def set_retry_timeout(self, retry_timeout):
        self._retry.deadline = retry_timeout
        self._client.set_read_timeout(retry_timeout)

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

            # get global dynamic configuration
            config = nodes.get(self._CONFIG)
            config = config and ClusterConfig.from_node(config.modifiedIndex, config.value)

            # get timeline history
            history = nodes.get(self._HISTORY)
            history = history and TimelineHistory.from_node(history.modifiedIndex, history.value)

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
                index = result.etcd_index if result.etcd_index > leader.modifiedIndex else leader.modifiedIndex + 1
                leader = Leader(index, leader.ttl, member)

            # failover key
            failover = nodes.get(self._FAILOVER)
            if failover:
                failover = Failover.from_node(failover.modifiedIndex, failover.value)

            # get synchronization state
            sync = nodes.get(self._SYNC)
            sync = SyncState.from_node(sync and sync.modifiedIndex, sync and sync.value)

            self._cluster = Cluster(initialize, config, leader, last_leader_operation, members, failover, sync, history)
        except etcd.EtcdKeyNotFound:
            self._cluster = Cluster(None, None, None, None, [], None, None, None)
        except Exception as e:
            self._handle_exception(e, 'get_cluster', raise_ex=EtcdError('Etcd is not responding properly'))
        self._has_failed = False

    @catch_etcd_errors
    def touch_member(self, data, ttl=None, permanent=False):
        data = json.dumps(data, separators=(',', ':'))
        return self.retry(self._client.set, self.member_path, data, None if permanent else ttl or self._ttl)

    @catch_etcd_errors
    def take_leader(self):
        return self.retry(self._client.set, self.leader_path, self._name, self._ttl)

    def attempt_to_acquire_leader(self, permanent=False):
        try:
            return bool(self.retry(self._client.write,
                                   self.leader_path,
                                   self._name,
                                   ttl=None if permanent else self._ttl,
                                   prevExist=False))
        except etcd.EtcdAlreadyExist:
            logger.info('Could not take out TTL lock')
        except (RetryFailedError, etcd.EtcdException):
            pass
        return False

    @catch_etcd_errors
    def set_failover_value(self, value, index=None):
        return self._client.write(self.failover_path, value, prevIndex=index or 0)

    @catch_etcd_errors
    def set_config_value(self, value, index=None):
        return self._client.write(self.config_path, value, prevIndex=index or 0)

    @catch_etcd_errors
    def _write_leader_optime(self, last_operation):
        return self._client.set(self.leader_optime_path, last_operation)

    @catch_etcd_errors
    def _update_leader(self):
        return self.retry(self._client.test_and_set, self.leader_path, self._name, self._name, self._ttl)

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

    @catch_etcd_errors
    def set_history_value(self, value):
        return self._client.write(self.history_path, value)

    @catch_etcd_errors
    def set_sync_state_value(self, value, index=None):
        return self.retry(self._client.write, self.sync_path, value, prevIndex=index or 0)

    @catch_etcd_errors
    def delete_sync_state(self, index=None):
        return self.retry(self._client.delete, self.sync_path, prevIndex=index or 0)

    def watch(self, leader_index, timeout):
        if self.__do_not_watch:
            self.__do_not_watch = False
            return True

        if leader_index:
            end_time = time.time() + timeout

            while timeout >= 1:  # when timeout is too small urllib3 doesn't have enough time to connect
                try:
                    self._client.watch(self.leader_path, index=leader_index, timeout=timeout + 0.5)
                    self._has_failed = False
                    # Synchronous work of all cluster members with etcd is less expensive
                    # than reestablishing http connection every time from every replica.
                    return True
                except etcd.EtcdWatchTimedOut:
                    self._client.http.clear()
                    self._has_failed = False
                    return False
                except (etcd.EtcdEventIndexCleared, etcd.EtcdWatcherCleared):  # Watch failed
                    self._has_failed = False
                    return True  # leave the loop, because watch with the same parameters will fail anyway
                except etcd.EtcdException as e:
                    self._handle_exception(e, 'watch', True)

                timeout = end_time - time.time()

        try:
            return super(Etcd, self).watch(None, timeout)
        finally:
            self.event.clear()
