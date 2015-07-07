import logging
import random
import requests
import socket

from dns.exception import DNSException
from dns import resolver
from helpers.dcs import AbstractDCS, Cluster, DCSError, Member, parse_connection_string
from helpers.utils import sleep
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class EtcdError(DCSError):
    pass


class CurrentLeaderError(EtcdError):
    pass


class EtcdConnectionFailed(EtcdError):
    pass


class Client:

    API_VERSION = 'v2'

    def __init__(self, config):
        self._config = config
        self.timeout = 5
        self._base_uri = None
        self._members_cache = []
        self.load_members()

    def client_url(self, path):
        return self._base_uri + path

    def _next_server(self):
        self._base_uri = None
        try:
            self._base_uri = self._members_cache.pop()
        except IndexError:
            logger.error('Members cache is empty, can not retry.')
            raise EtcdConnectionFailed('No more members in the cluster')
        else:
            logger.info('Selected new etcd server %s', self._base_uri)

    def _get(self, path):
        response = None
        while response is None:
            uri = self.client_url(path)
            try:
                logger.info('GET %s', uri)
                response = requests.get(uri, timeout=self.timeout)
            except RequestException:
                self._next_server()

        logger.debug([response.status_code, response.content])
        try:
            return response.json(), response.status_code
        except (TypeError, ValueError):
            raise EtcdError('Bad response from %s: %s' % (uri, response.content))

    @staticmethod
    def get_srv_record(host):
        try:
            return [(str(r.target).rstrip('.'), r.port) for r in resolver.query('_etcd-server._tcp.' + host, 'SRV')]
        except DNSException:
            logger.exception('Can not resolve SRV for %s', host)
        return []

    @staticmethod
    def get_peers_urls_from_dns(host):
        return ['http://{}:{}'.format(h, p) for h, p in Client.get_srv_record(host)]

    @staticmethod
    def get_client_urls_from_dns(addr):
        host, port = addr.split(':')
        ret = []
        try:
            for r in set(socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)):
                ret.append('http://{}:{}/{}'.format(r[4][0], r[4][1], Client.API_VERSION))
        except socket.error:
            logger.exception('Can not resolve %s', host)
        return list(set(ret)) if ret else ['http://{}:{}/{}'.format(host, port, Client.API_VERSION)]

    def load_members(self):
        load_from_srv = False
        if not self._base_uri:
            if 'discovery_srv' not in self._config and 'host' not in self._config:
                raise Exception('Neither discovery_srv nor host are defined in etcd section of config')

            if 'discovery_srv' in self._config:
                load_from_srv = True
                self._members_cache = self.get_peers_urls_from_dns(self._config['discovery_srv'])

            if not self._members_cache and 'host' in self._config:
                load_from_srv = False
                self._members_cache = self.get_client_urls_from_dns(self._config['host'])

            self._next_server()

        response, status_code = self._get('/members')
        if status_code != 200:
            self._base_uri = None
            raise EtcdError('Got response with code=%s from %s' % (status_code, self._base_uri))

        members_cache = []
        try:
            for member in response if load_from_srv else response['members']:
                members_cache.extend([m + '/' + self.API_VERSION for m in member['clientURLs']])
        except:
            self._base_uri = None
            raise EtcdError('Got invalid response from %s: %s' % (self._base_uri, response))

        self._members_cache = list(set(members_cache))
        random.shuffle(self._members_cache)
        if load_from_srv:
            self._next_server()
        else:
            try:
                self._members_cache.remove(self._base_uri)
            except ValueError:
                pass

    def get(self, path):
        if not self._base_uri:
            self.load_members()
        old_base_uri = self._base_uri
        try:
            return self._get(path)
        finally:
            if self._base_uri != old_base_uri:
                try:
                    self.load_members()
                except EtcdError:
                    logger.exception('load_members')

    def put(self, path, **data):
        if not self._base_uri:
            self.load_members()
        old_base_uri = self._base_uri
        response = None
        while response is None:
            uri = self.client_url(path)
            try:
                logger.info('PUT %s', uri)
                response = requests.put(uri, timeout=self.timeout, data=data)
            except RequestException:
                logger.exception('PUT %s data=%s', uri, data)
                self._next_server()

        if self._base_uri != old_base_uri:
            try:
                self.load_members()
            except EtcdError:
                logger.exception('load_members')
        if response.status_code in [200, 201, 202, 204]:
            return True
        logger.error('Unexpected response: %s %s', response.status_code, response.content)
        return False

    def delete(self, path):
        if not self._base_uri:
            self.load_members()
        old_base_uri = self._base_uri
        response = None
        while response is None:
            uri = self.client_url(path)
            try:
                logger.info('DELETE %s', uri)
                response = requests.delete(uri, timeout=self.timeout)
            except RequestException:
                logger.exception('DELETE %s', uri)
                self._next_server()

        if self._base_uri != old_base_uri:
            try:
                self.load_members()
            except EtcdError:
                logger.exception('load_members')
        if response.status_code in [200, 202, 204]:
            return True
        logger.error('Unexpected response: %s %s', response.status_code, response.content)
        return False


class Etcd(AbstractDCS):

    def __init__(self, name, config):
        super(Etcd, self).__init__(name, config)
        self.ttl = config['ttl']
        self.member_ttl = config.get('member_ttl', 3600)
        self.client = self.get_etcd_client(config)

    def get_etcd_client(self, config):
        client = None
        while not client:
            try:
                client = Client(config)
            except EtcdError:
                logger.info('waiting on etcd')
                sleep(5)
        return client

    def client_path(self, path):
        return '/keys' + super(Etcd, self).client_path(path)

    def get_client_path(self, path):
        return self.client.get(self.client_path(path))

    def put_client_path(self, path, **data):
        return self.client.put(self.client_path(path), **data)

    def delete_client_path(self, path):
        try:
            return self.client.delete(self.client_path(path))
        except EtcdConnectionFailed:
            return False

    @staticmethod
    def find_node(node, key):
        """
        >>> Etcd.find_node({}, None)
        >>> Etcd.find_node({'dir': True, 'nodes': [], 'key': '/test/'}, 'test')
        """
        if not node.get('dir', False):
            return None
        key = node['key'] + key
        for n in node['nodes']:
            if n['key'] == key:
                return n
        return None

    @staticmethod
    def member(node):
        conn_url, api_url = parse_connection_string(node['value'])
        expiration = node.get('expiration', None)
        ttl = node.get('ttl', None)
        return Member(node['modifiedIndex'], node['key'].split('/')[-1], conn_url, api_url, expiration, ttl)

    def get_cluster(self):
        try:
            response, status_code = self.get_client_path('?recursive=true')
            if status_code == 200:
                node = self.find_node(response['node'], '/initialize')
                initialize = True if node else False
                # get list of members
                node = self.find_node(response['node'], '/members') or {'nodes': []}
                members = [self.member(n) for n in node['nodes']]

                # get last leader operation
                last_leader_operation = 0
                node = self.find_node(response['node'], '/optime')
                if node:
                    node = self.find_node(node, '/leader')
                    if node:
                        last_leader_operation = int(node['value'])

                # get leader
                leader = None
                node = self.find_node(response['node'], '/leader')
                if node:
                    for m in members:
                        if m.name == node['value']:
                            leader = m
                            break
                    if not leader:
                        leader = Member(-1, node['value'], None, None, None, None)

                return Cluster(initialize, leader, last_leader_operation, members)
            elif status_code == 404:
                return Cluster(False, None, None, [])
        except:
            logger.exception('get_cluster')

        raise EtcdError('Etcd is not responding properly')

    def touch_member(self, connection_string, ttl=None):
        try:
            return self.put_client_path('/members/' + self._name, value=connection_string, ttl=ttl or self.member_ttl)
        except EtcdError:
            return False

    def take_leader(self):
        try:
            return self.put_client_path('/leader', value=self._name, ttl=self.ttl)
        except EtcdError:
            return False

    def attempt_to_acquire_leader(self):
        try:
            ret = self.put_client_path('/leader', value=self._name, ttl=self.ttl, prevExist=False)
            ret or logger.info('Could not take out TTL lock')
            return ret
        except EtcdError:
            return False

    def update_leader(self, state_handler):
        if self.put_client_path('/leader', value=state_handler.name, ttl=self.ttl, prevValue=state_handler.name):
            try:
                self.put_client_path('/optime/leader', value=state_handler.last_operation())
            except EtcdError:
                pass
            return True
        return False

    def race(self, path):
        try:
            return self.put_client_path(path, value=self._name, prevExist=False)
        except EtcdError:
            return False

    def delete_leader(self):
        return self.delete_client_path('/leader?prevValue=' + self._name)
