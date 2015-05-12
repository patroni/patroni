import logging
import requests
import time

from collections import namedtuple
from helpers.errors import CurrentLeaderError, EtcdError

logger = logging.getLogger(__name__)


class Member(namedtuple('Member', 'hostname,address')):

    pass


class Cluster(namedtuple('Cluster', 'leader,members')):

    pass


class Etcd:

    def __init__(self, config):
        self.ttl = config['ttl']
        self.base_client_url = 'http://{host}/v2/keys/service/{scope}'.format(**config)
        self.postgres_cluster = None

    def get_client_path(self, path, max_attempts=1):
        attempts = 0
        response = None

        while True:
            ex = None
            try:
                response = requests.get(self.client_url(path))
                if response.status_code == 200:
                    break
            except Exception, e:
                logger.exception('get_client_path')
                ex = e

            attempts += 1
            if attempts < max_attempts:
                logger.info('Failed to return %s, trying again. (%s of %s)', path, attempts, max_attempts)
                time.sleep(3)
            elif ex:
                raise ex

        return response.json(), response.status_code

    def put_client_path(self, path, **data):
        try:
            response = requests.put(self.client_url(path), data=data)
            return response.status_code in [200, 201]
        except:
            logger.exception('PUT %s data=%s', path, data)
            return False

    def client_url(self, path):
        return self.base_client_url + path

    @staticmethod
    def find_node(node, key):
        if not node['dir']:
            return None
        key = node['key'] + key
        for n in node['nodes']:
            if n['key'] == key:
                return n
        return None

    def get_cluster(self):
        try:
            response, status_code = self.get_client_path('?recursive=true')
            if status_code == 200:
                leader = None
                members = self.find_node(response['node'], '/members')
                members = [Member(n['key'].split('/')[-1], n['value']) for n in members['nodes']] if members else []

                leader_node = self.find_node(response['node'], '/leader')
                if leader_node:
                    for m in members:
                        if m.hostname == leader_node['value']:
                            leader = m
                            break
                    if not leader:
                        leader = Member(leader['value'], None)
                return Cluster(leader, members)
            elif status_code == 404:
                return Cluster(None, [])
        except:
            logger.exception('get_cluster')

        raise EtcdError('Etcd is not responding properly')

    def current_leader(self):
        try:
            cluster = self.get_cluster()
            if not cluster.leader or not cluster.leader.address:
                return None
            return cluster.leader
        except:
            raise CurrentLeaderError("Etcd is not responding properly")

    def touch_member(self, member, connection_string):
        return self.put_client_path('/members/' + member, value=connection_string)

    def take_leader(self, value):
        return self.put_client_path('/leader', value=value, ttl=self.ttl)

    def attempt_to_acquire_leader(self, value):
        ret = self.put_client_path('/leader', value=value, ttl=self.ttl, prevExist=False)
        ret or logger.info('Could not take out TTL lock')
        return ret

    def update_leader(self, value):
        return self.put_client_path('/leader', value=value, ttl=self.ttl, prevValue=value)

    def race(self, path, value):
        return self.put_client_path(path, value=value, prevExist=False)
