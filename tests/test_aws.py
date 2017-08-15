import boto.ec2
import sys
import unittest

from mock import Mock, patch
from collections import namedtuple
from patroni.scripts.aws import AWSConnection, main as _main
from requests.exceptions import RequestException


class MockEc2Connection(object):

    @staticmethod
    def get_all_volumes(*args, **kwargs):
        oid = namedtuple('Volume', 'id')
        return [oid(id='a'), oid(id='b')]

    @staticmethod
    def create_tags(objects, *args, **kwargs):
        if len(objects) == 0:
            raise boto.exception.BotoServerError(503, 'Service Unavailable', 'Request limit exceeded')
        return True


class MockResponse(object):
    ok = True

    def __init__(self, content):
        self.content = content

    def json(self):
        return self.content


def requests_get(url, **kwargs):
    if url.split('/')[-1] == 'document':
        result = {"instanceId": "012345", "region": "eu-west-1"}
    else:
        result = 'foo'
    return MockResponse(result)


@patch('boto.ec2.connect_to_region', Mock(return_value=MockEc2Connection()))
class TestAWSConnection(unittest.TestCase):

    @patch('requests.get', requests_get)
    def setUp(self):
        self.conn = AWSConnection('test')

    def test_on_role_change(self):
        self.assertTrue(self.conn.on_role_change('master'))
        with patch.object(MockEc2Connection, 'get_all_volumes', Mock(return_value=[])):
            self.conn._retry.max_tries = 1
            self.assertFalse(self.conn.on_role_change('master'))

    @patch('requests.get', Mock(side_effect=RequestException('foo')))
    def test_non_aws(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.on_role_change("master"))

    @patch('requests.get', Mock(return_value=MockResponse('foo')))
    def test_aws_bizare_response(self):
        conn = AWSConnection('test')
        self.assertFalse(conn.aws_available())

    @patch('requests.get', requests_get)
    @patch('sys.exit', Mock())
    def test_main(self):
        self.assertIsNone(_main())
        sys.argv = ['aws.py', 'on_start', 'replica', 'foo']
        self.assertIsNone(_main())
