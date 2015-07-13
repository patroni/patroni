import logging
import requests
from requests.exceptions import RequestException
import sys
import boto.ec2

logger = logging.getLogger(__name__)


class AWSConnection:
    def __init__(self, config):
        self.available = False
        self.config = config

        if 'cluster_name' in config:
            self.cluster_name = config.get('cluster_name')
        elif 'etcd' in config and isinstance(config['etcd'], dict):
            self.cluster_name = config['etcd'].get('scope', 'unknown')
        else:
            self.cluster_name = 'unknown'
        try:
            # get the instance id
            r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=0.1)
        except RequestException:
            logger.info("cannot query AWS meta-data")
            return
        if r.ok:
            try:
                content = r.json()
                self.instance_id = content['instanceId']
                self.region = content['region']
            except Exception as e:
                logger.info('unable to fetch instance id and region from AWS meta-data: {}'.format(e))
                return
            self.available = True

    def aws_available(self):
        return self.available

    def _tag_ebs(self, role):
        """ set tags, carrying the cluster name, instance role and instance id for the EBS storage """
        if not self.available:
            return False

        tags = {'Name': 'spilo_'+self.cluster_name, 'Role': role, 'Instance': self.instance_id}
        try:
            conn = boto.ec2.connect_to_region(self.region)
            volumes = conn.get_all_volumes(filters={'attachment.instance-id': self.instance_id})
            conn.create_tags([v.id for v in volumes], tags)
        except Exception as e:
            logger.info('could not set tags for EBS storage devices attached: {}'.format(e))
            return False
        return True

    def _tag_ec2(self, role):
        """ tag the current EC2 instance with a cluster role """
        if not self.available:
            return False
        tags = {'Role': role}
        try:
            conn = boto.ec2.connect_to_region(self.region)
            conn.create_tags([self.instance_id], tags)
        except Exception as e:
            logger.info("could not set tags for EC2 instance {}: {}".format(self.instance_id, e))
            return False
        return True

    def on_role_change(self, new_role):
        ret = self._tag_ec2(new_role)
        return self._tag_ebs(new_role) and ret


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print ("Usage: {0} action role name".format(sys.argv[0]))
        return 1
    action, is_master, name = sys.argv[1:]
    if action in ('on_start', 'on_stop', 'on_role_change'):
        aws = AWSConnection({'cluster_name': name})
        aws.on_role_change('master' if is_master else 'replica')
        return 0
    return 2
