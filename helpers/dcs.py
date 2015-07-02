import sys

from collections import namedtuple
from helpers.utils import calculate_ttl

if sys.hexversion >= 0x03000000:
    from urllib.parse import urlparse, urlunparse, parse_qsl
else:
    from urlparse import urlparse, urlunparse, parse_qsl


class Member(namedtuple('Member', 'name,conn_url,api_url,expiration,ttl')):

    @staticmethod
    def fromNode(node):
        scheme, netloc, path, params, query, fragment = urlparse(node['value'])
        conn_url = urlunparse((scheme, netloc, path, params, '', fragment))
        api_url = ([v for n, v in parse_qsl(query) if n == 'application_name'] or [None])[0]
        expiration = node.get('expiration', None)
        ttl = node.get('ttl', None)
        return Member(node['key'].split('/')[-1], conn_url, api_url, expiration, ttl)

    def real_ttl(self):
        return calculate_ttl(self.expiration) or -1


class Cluster(namedtuple('Cluster', 'initialize,leader,last_leader_operation,members')):

    def is_unlocked(self):
        return not (self.leader and self.leader.name)


