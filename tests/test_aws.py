import unittest
import requests
import boto.ec2
from collections import namedtuple
from helpers.aws import AWSConnection
from requests.exceptions import RequestException
import yaml


class MockEc2Connection:

    def __init__(self, error=False):
        self.error = error

    def get_all_volumes(self, filters):
        if self.error:
            raise Exception("get_all_volumes")
        oid = namedtuple('Volume', 'id')
        return [oid(id='a'), oid(id='b')]

    def create_tags(self, objects, tags):
        if self.error or len(objects) == 0:
            raise Exception("create_tags")
        return True


class TestAWSConnection(unittest.TestCase):

    def __init__(self, method_name='runTest'):
        super(TestAWSConnection, self).__init__(method_name)

    def set_error(self):
        self.error = True

    def set_ok(self):
        self.error = False

    def boto_ec2_connect_to_region(self, region):
        return MockEc2Connection(self.error)

    def requests_get(self, url, **kwargs):
        if self.error:
            raise RequestException("foo")
        result = namedtuple('Request', 'ok content')
        result.ok = True
        if url.split('/')[-1] == 'document':
            result.content = '{\n  "instanceId" : "012345",\n "region" : "eu-west-1"\n}'
        else:
            result.content = 'foo'
        return result

    def setUp(self):
        self.error = False
        requests.get = self.requests_get
        boto.ec2.connect_to_region = self.boto_ec2_connect_to_region
        self.config_string = """
loop_wait: 10
restapi:
  listen: 0.0.0.0:8008
  connect_address: 127.0.0.1:5432
etcd:
  scope: test
  ttl: 30
  host: 127.0.0.1:8080
postgresql:
  name: postgresql_foo
  listen: 0.0.0.0:5432
  connect_address: 127.0.0.1:5432
  data_dir: /home/postgres/pgdata/data
  replication:
    username: standby
    password: standby
    network: 0.0.0.0/0
  superuser:
    password: zalando
  admin:
    username: admin
    password: admin
  parameters:
    archive_mode: "on"
    wal_level: hot_standby
    max_wal_senders: 5
    wal_keep_segments: 8
    archive_timeout: 1800s
    max_replication_slots: 5
    hot_standby: "on"
    ssl: "on"
"""
        self.conn = AWSConnection(yaml.load(self.config_string))

    def test_aws_available(self):
        self.assertTrue(self.conn.aws_available())

    def test_on_role_change(self):
        self.assertTrue(self.conn._tag_ebs('master'))
        self.assertTrue(self.conn._tag_ec2('master'))
        self.assertTrue(self.conn.on_role_change('master'))

    def test_non_aws(self):
        self.set_error()
        conn = AWSConnection(yaml.load(self.config_string))
        self.assertFalse(conn.aws_available())
        self.assertFalse(conn._tag_ebs('master'))
        self.assertFalse(conn._tag_ec2('master'))

    def test_aws_tag_ebs_error(self):
        self.set_error()
        self.assertFalse(self.conn._tag_ebs("master"))

    def test_aws_tag_ec2_error(self):
        self.set_error()
        self.assertFalse(self.conn._tag_ec2("master"))
