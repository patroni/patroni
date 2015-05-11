import urllib2
import json
import time
import logging

from helpers.errors import CurrentLeaderError
from urllib import urlencode

logger = logging.getLogger(__name__)


class Etcd:

    def __init__(self, config):
        self.ttl = config['ttl']
        self.base_client_url = 'http://{host}/v2/keys/service/{scope}'.format(**config)

    def get_client_path(self, path, max_attempts=1):
        attempts = 0
        response = None

        while True:
            try:
                response = urllib2.urlopen(self.client_url(path)).read()
                break
            except (urllib2.HTTPError, urllib2.URLError) as e:
                attempts += 1
                if attempts < max_attempts:
                    logger.info('Failed to return %s, trying again. (%s of %s)', path, attempts, max_attempts)
                    time.sleep(3)
                else:
                    raise e
        try:
            return json.loads(response)
        except ValueError:
            return response

    def put_client_path(self, path, data):
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(self.client_url(path), data=urlencode(data).replace("false", "False"))
        request.get_method = lambda: 'PUT'
        opener.open(request)

    def client_url(self, path):
        return self.base_client_url + path

    def current_leader(self):
        try:
            hostname = self.get_client_path('/leader')['node']['value']
            address = self.get_client_path('/members/' + hostname)['node']['value']

            return {'hostname': hostname, 'address': address}
        except urllib2.HTTPError as e:
            if e.code == 404:
                return None
            raise CurrentLeaderError("Etcd is not responding properly")

    def members(self):
        try:
            members = []

            r = self.get_client_path("/members?recursive=true")
            for node in r["node"]["nodes"]:
                members.append({"hostname": node["key"].split('/')[-1], "address": node["value"]})

            return members
        except urllib2.HTTPError as e:
            if e.code == 404:
                return None
            raise CurrentLeaderError("Etcd is not responding properly")

    def touch_member(self, member, connection_string):
        self.put_client_path('/members/' + member, {"value": connection_string})

    def take_leader(self, value):
        return self.put_client_path("/leader", {"value": value, "ttl": self.ttl}) is None

    def attempt_to_acquire_leader(self, value):
        try:
            return self.put_client_path("/leader", {"value": value, "ttl": self.ttl, "prevExist": False}) is None
        except urllib2.HTTPError as e:
            if e.code == 412:
                logger.info('Could not take out TTL lock: %s', e)
            return False

    def update_leader(self, value):
        try:
            self.put_client_path("/leader", {"value": value, "ttl": self.ttl, "prevValue": value})
            return True
        except urllib2.HTTPError:
            logger.error("Error updating TTL on ETCD for primary.")
            return False

    def leader_unlocked(self):
        try:
            self.get_client_path("/leader")
            return False
        except urllib2.HTTPError as e:
            if e.code == 404:
                return True
            return False
        except ValueError as e:
            return False

    def am_i_leader(self, value):
        # try:
            reponse = self.get_client_path("/leader")
            logger.info('Lock owner: %s; I am %s', reponse["node"]["value"], value)
            return reponse["node"]["value"] == value
        # except Exception as e:
            # return False

    def race(self, path, value):
        try:
            return self.put_client_path(path, {"prevExist": False, "value": value}) is None
        except urllib2.HTTPError:
            return False
