#!/usr/bin/env python

import json
import logging
import sys

from typing import Any, Optional

import boto3

from botocore.exceptions import ClientError
from botocore.utils import IMDSFetcher

from ..utils import Retry, RetryFailedError

logger = logging.getLogger(__name__)


class AWSConnection(object):

    def __init__(self, cluster_name: Optional[str]) -> None:
        self.available = False
        self.cluster_name = cluster_name if cluster_name is not None else 'unknown'
        self._retry = Retry(deadline=300, max_delay=30, max_tries=-1, retry_exceptions=ClientError)
        try:
            # get the instance id
            fetcher = IMDSFetcher(timeout=2.1)
            token = fetcher._fetch_metadata_token()
            r = fetcher._get_request("/latest/dynamic/instance-identity/document", None, token)
        except Exception:
            logger.error('cannot query AWS meta-data')
            return

        if r.status_code < 400:
            try:
                content = json.loads(r.text)
                self.instance_id = content['instanceId']
                self.region = content['region']
            except Exception:
                logger.exception('unable to fetch instance id and region from AWS meta-data')
                return
            self.available = True

    def retry(self, *args: Any, **kwargs: Any) -> Any:
        return self._retry.copy()(*args, **kwargs)

    def aws_available(self) -> bool:
        return self.available

    def _tag_ebs(self, conn: Any, role: str) -> None:
        """ set tags, carrying the cluster name, instance role and instance id for the EBS storage """
        tags = [{'Key': 'Name', 'Value': 'spilo_' + self.cluster_name},
                {'Key': 'Role', 'Value': role},
                {'Key': 'Instance', 'Value': self.instance_id}]
        volumes = conn.volumes.filter(Filters=[{'Name': 'attachment.instance-id', 'Values': [self.instance_id]}])
        conn.create_tags(Resources=[v.id for v in volumes], Tags=tags)

    def _tag_ec2(self, conn: Any, role: str) -> None:
        """ tag the current EC2 instance with a cluster role """
        tags = [{'Key': 'Role', 'Value': role}]
        conn.create_tags(Resources=[self.instance_id], Tags=tags)

    def on_role_change(self, new_role: str) -> bool:
        if not self.available:
            return False
        try:
            conn = boto3.resource('ec2', region_name=self.region)  # type: ignore
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
