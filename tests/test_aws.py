import boto.ec2
import requests
import sys
import unittest

from mock import Mock, patch
from collections import namedtuple
from patroni.scripts.aws import AWSConnection, main as _main
from requests.exceptions import RequestException


class MockEc2Connection(object):

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


class MockResponse(object):

    def __init__(self, content):
        self.content = content
        self.ok = True

    def json(self):
        return self.content


class TestAWSConnection(unittest.TestCase):

    def boto_ec2_connect_to_region(self, region):
        return MockEc2Connection(self.error)

    def requests_get(self, url, **kwargs):
        if self.error:
            raise RequestException("foo")
        result = namedtuple('Request', 'ok content')
        result.ok = True
        if url.split('/')[-1] == 'document' and not self.json_error:
            result = {"instanceId": "012345", "region": "eu-west-1"}
        else:
            result = 'foo'
        return MockResponse(result)

    def setUp(self):
        self.error = False
        self.json_error = False
        requests.get = self.requests_get
        boto.ec2.connect_to_region = self.boto_ec2_connect_to_region
        self.conn = AWSConnection('test')

    def test_aws_available(self):
        self.assertTrue(self.conn.aws_available())

    def test_on_role_change(self):
        self.assertTrue(self.conn._tag_ebs('master'))
        self.assertTrue(self.conn._tag_ec2('master'))
        self.assertTrue(self.conn.on_role_change('master'))

    def test_non_aws(self):
        self.error = True
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())
        self.assertFalse(conn._tag_ebs('master'))
        self.assertFalse(conn._tag_ec2('master'))

    def test_aws_bizare_response(self):
        self.json_error = True
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())

    def test_aws_tag_ebs_error(self):
        self.error = True
        self.assertFalse(self.conn._tag_ebs("master"))

    def test_aws_tag_ec2_error(self):
        self.error = True
        self.assertFalse(self.conn._tag_ec2("master"))

    @patch('sys.exit', Mock())
    def test_main(self):
        self.assertIsNone(_main())
        sys.argv = ['aws.py', 'on_start', 'replica', 'foo']
        self.assertIsNone(_main())
