#!/usr/bin/env python

import logging
import requests
from requests.exceptions import RequestException
import sys
import boto.ec2

from patroni.utils import Retry, RetryFailedError

logger = logging.getLogger(__name__)


class AWSConnection(object):

    def __init__(self, cluster_name):
        self.available = False
        self.cluster_name = cluster_name if cluster_name is not None else 'unknown'
        self._retry = Retry(deadline=300, max_delay=30, max_tries=-1, retry_exceptions=(boto.exception.StandardError,))
        try:
            # get the instance id
            r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=2.1)
        except RequestException:
            logger.error('cannot query AWS meta-data')
            return

        if r.ok:
            try:
                content = r.json()
                self.instance_id = content['instanceId']
                self.region = content['region']
            except Exception:
                logger.exception('unable to fetch instance id and region from AWS meta-data')
                return
            self.available = True

    def retry(self, *args, **kwargs):
        return self._retry.copy()(*args, **kwargs)

    def aws_available(self):
        return self.available

    def _tag_ebs(self, conn, role):
        """ set tags, carrying the cluster name, instance role and instance id for the EBS storage """
        tags = {'Name': 'spilo_' + self.cluster_name, 'Role': role, 'Instance': self.instance_id}
        volumes = conn.get_all_volumes(filters={'attachment.instance-id': self.instance_id})
        conn.create_tags([v.id for v in volumes], tags)

    def _tag_ec2(self, conn, role):
        """ tag the current EC2 instance with a cluster role """
        tags = {'Role': role}
        conn.create_tags([self.instance_id], tags)

    def on_role_change(self, new_role):
        if not self.available:
            return False
        try:
            conn = self.retry(boto.ec2.connect_to_region, self.region)
            self.retry(self._tag_ec2, conn, new_role)
            self.retry(self._tag_ebs, conn, new_role)
        except RetryFailedError:
            logger.warning("Unable to communicate to AWS "
                           "when setting tags for the EC2 instance {0} "
                           "and attached EBS volumes".format(self.instance_id))
            return False
        return True


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    if len(sys.argv) == 4 and sys.argv[1] in ('on_start', 'on_stop', 'on_role_change'):
        AWSConnection(cluster_name=sys.argv[3]).on_role_change(sys.argv[2])
    else:
        sys.exit("Usage: {0} action role name".format(sys.argv[0]))


if __name__ == '__main__':
    main()
