#!/usr/bin/env python

import logging
import os
import socket
import sys
import time

from kubernetes import client as k8s_client, config as k8s_config
from urllib3.exceptions import HTTPError
from six.moves.http_client import HTTPException

logger = logging.getLogger(__name__)


class CoreV1Api(k8s_client.CoreV1Api):

    def retry(func):
        def wrapped(*args, **kwargs):
            count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (HTTPException, HTTPError, socket.error, socket.timeout):
                    if count >= 10:
                        raise
                    logger.info('Throttling API requests...')
                    time.sleep(2 ** count * 0.5)
                    count += 1
        return wrapped

    @retry
    def patch_namespaced_endpoints(self, *args, **kwargs):
        return super(CoreV1Api, self).patch_namespaced_endpoints(*args, **kwargs)


def patch_master_endpoint(api, namespace, cluster):
    addresses = [k8s_client.V1EndpointAddress(ip=os.environ['POD_IP'])]
    ports = [k8s_client.V1EndpointPort(port=5432)]
    subsets = [k8s_client.V1EndpointSubset(addresses=addresses, ports=ports)]
    body = k8s_client.V1Endpoints(subsets=subsets)
    return api.patch_namespaced_endpoints(cluster, namespace, body)


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    if len(sys.argv) != 4 or sys.argv[1] not in ('on_start', 'on_stop', 'on_role_change'):
        sys.exit('Usage: %s <action> <role> <cluster_name>', sys.argv[0])

    action, role, cluster = sys.argv[1:4]

    k8s_config.load_incluster_config()
    k8s_api = CoreV1Api()

    namespace = os.environ['KUBERNETES_NAMESPACE']

    if role == 'master' and action in ('on_start', 'on_role_change'):
        patch_master_endpoint(k8s_api, namespace, cluster)


if __name__ == '__main__':
    main()
