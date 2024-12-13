import abc
import json
import logging
from datetime import datetime
from threading import Thread, Event
import time
import six

from .dcs import Member
from .dcs.kubernetes import catch_kubernetes_errors, Kubernetes
from .exceptions import DCSError

import kubernetes

logger = logging.getLogger(__name__)

@six.add_metaclass(abc.ABCMeta)
class AbstractSiteController(object):
    # Set whether we are relying on this controller for providing standby config
    is_active = False

    def start(self):
        pass

    def shutdown(self):
        pass

    def get_active_standby_config(self):
        """Returns currently active configuration for standby leader"""

    def is_leader_site(self):
        return self.get_active_standby_config() is None

    def resolve_leader(self):
        """Try to become leader, update active config correspondingly.

        Return error when unable to resolve"""
        return None

    def heartbeat(self):
        """"Notify multisite mechanism that this site has a properly operating cluster mechanism.

        Need to send out an async lease update. If that fails to complete within safety margin of ttl running
        out then we need to update site config
        """

    def release(self):
        pass

    def status(self):
        pass

    def should_failover(self):
        return False

    def on_shutdown(self, checkpoint_location):
        pass

class SingleSiteController(AbstractSiteController):
    """Do nothing controller for single site operation."""
    def status(self):
        return "Leader"

class MultisiteController(Thread, AbstractSiteController):
    is_active = True

    def __init__(self, config, on_change=None):
        super().__init__()
        self.stop_requested = False
        self.on_change = on_change

        msconfig = config['multisite']

        from .dcs import get_dcs

        # Multisite configuration inherits values from main configuration
        inherited_keys = ['name', 'scope', 'namespace', 'loop_wait', 'ttl', 'retry_timeout']
        for key in inherited_keys:
            if key not in msconfig and key in config:
                msconfig[key] = config[key]

        msconfig.setdefault('observe_interval', config.get('loop_wait'))

        # TODO: fetch default host/port from postgresql section
        if 'host' not in msconfig or 'port' not in msconfig:
            raise Exception("Missing host or port from multisite configuration")

        # Disable etcd3 lease ownership detection warning
        msconfig['multisite'] = True

        self.config = msconfig

        self.name = msconfig['name']
        self.dcs = get_dcs(msconfig)

        if msconfig.get('update_crd'):
            self._state_updater = KubernetesStateManagement(msconfig.get('update_crd'),
                                                            msconfig.get('crd_uid'),
                                                            reporter=self.name, #  Use pod name?
                                                            crd_api=msconfig.get('crd_api', 'acid.zalan.do/v1'))
        else:
            self._state_updater = None

        self.switchover_timeout = msconfig.get('switchover_timeout', 300)

        self._heartbeat = Event()
        self._standby_config = None
        self._leader_resolved = Event()
        self._has_leader = False
        self._release = False
        self._status = None
        self._failover_target = None
        self._failover_timeout = None

        self.site_switches = None

        self._dcs_error = None

    def status(self):
        return "Leader" if self._has_leader else "Standby"

    def get_active_standby_config(self):
        return self._standby_config

    def resolve_leader(self):
        """Try to become leader, update active config correspondingly.

        Must be called from Patroni main thread. After a successful return get_active_standby_config() will
        return a value corresponding to a multisite status that was active after start of the call.

        Returns error message encountered when unable to resolve leader status."""
        self._leader_resolved.clear()
        self._heartbeat.set()
        self._leader_resolved.wait()
        return self._dcs_error

    def heartbeat(self):
        """Notify multisite mechanism that this site has a properly operating cluster mechanism.

        Need to send out an async lease update. If that fails to complete within safety margin of ttl running
        out then we need to demote.
        """
        logger.info("Triggering multisite hearbeat")
        self._heartbeat.set()

    def release(self):
        self._release = True
        self._heartbeat.set()

    def should_failover(self):
        return self._failover_target is not None and self._failover_target != self.name

    def on_shutdown(self, checkpoint_location):
        """ Called when shutdown for multisite failover has completed.
        """
        # TODO: check if we replicated everything to standby site
        self.release()

    def _disconnected_operation(self):
        self._standby_config = {'restore_command': 'false'}

    def _set_standby_config(self, other: Member):
        logger.info(f"Multisite replicate from {other}")
        # TODO: add support for replication slots
        try:
            old_conf, self._standby_config = self._standby_config, {
                'host': other.data['host'],
                'port': other.data['port'],
                'create_replica_methods': ['basebackup'],
            }
        except KeyError:
            old_conf = self._standby_config
            self._disconnected_operation()

        if old_conf != self._standby_config:
            logger.info(f"Setting standby configuration to: {self._standby_config}")
        return old_conf != self._standby_config

    def _check_transition(self, leader, note=None):
        if self._has_leader != leader:
            logger.info("State transition")
            self._has_leader = leader
            if self.on_change:
                self.on_change()
        if self._state_updater and self._status != leader:
            self._state_updater.state_transition('Leader' if leader else 'Standby', note)
            self._status = leader


    def _resolve_multisite_leader(self):
        logger.info("Running multisite consensus.")
        try:
            # Refresh the latest known state
            cluster = self.dcs.get_cluster()
            self._dcs_error = None

            if not cluster.has_member(self.name):
                self.touch_member()

            if cluster.is_unlocked():
                if self._release:
                    self._release = False
                    self._disconnected_operation()
                    return
                if self._failover_target and self._failover_timeout > time.time():
                    logger.info("Waiting for multisite failover to complete")
                    self._disconnected_operation()
                    return
                # Became leader of unlocked cluster
                if self.dcs.attempt_to_acquire_leader():
                    logger.info("Became multisite leader")
                    self._standby_config = None
                    self._check_transition(leader=True, note="Acquired multisite leader status")
                    if cluster.failover and cluster.failover.target_site and cluster.failover.target_site == self.name:
                        logger.info("Cleaning up multisite failover key after acquiring leader status")
                        self.dcs.manual_failover('', '')
                # Failed to become leader, maybe someone else acquired lock, maybe we just failed
                else:
                    logger.info("Failed to acquire multisite lock")
                    # Non-working standby config while we are resolving who to connect to
                    self._disconnected_operation()
                    self._check_transition(leader=False, note="Lost multisite leader status")
                    # Try to get new leader
                    cluster = self.dcs.get_cluster(force=True)
                    if cluster.leader and cluster.leader.name != self.name:
                        self._set_standby_config(cluster.leader.member)
            else:
                # There is a leader cluster
                lock_owner = cluster.leader and cluster.leader.name
                # The leader is us
                if lock_owner == self.name:
                    logger.info("Multisite has leader and it is us")
                    if self._release:
                        logger.info("Releasing multisite leader status")
                        self.dcs.delete_leader(cluster.leader)
                        self._release = False
                        self._disconnected_operation()
                        self._check_transition(leader=False, note="Released multisite leader status on request")
                        return
                    if self.dcs.update_leader(cluster, None):
                        logger.info("Updated multisite leader lease")
                        # Make sure we are disabled from standby mode
                        self._standby_config = None
                        self._check_transition(leader=True, note="Already have multisite leader status")
                        self._check_for_failover(cluster)
                    else:
                        logger.error("Failed to update multisite leader status")
                        self._disconnected_operation()
                        self._check_transition(leader=False, note="Failed to update multisite leader status")
                # Current leader is someone else
                else:
                    logger.info(f"Multisite has leader and it is {lock_owner}")
                    self._release = False
                    # Failover successful or someone else took over
                    if self._failover_target is not None:
                        self._failover_target = None
                        self._failover_timeout = None
                    if self._set_standby_config(cluster.leader.member):
                        # Wake up anyway to notice that we need to replicate from new leader. For the other case
                        # _check_transition() handles the wake.
                        if not self._has_leader:
                            self.on_change()
                        note = f"Lost leader lock to {lock_owner}" if self._has_leader else f"Current leader {lock_owner}"
                        self._check_transition(leader=False, note=note)

        except DCSError as e:
            logger.error(f"Error accessing multisite DCS: {e}")
            self._dcs_error = 'Multi site DCS cannot be reached'
            if self._has_leader:
                self._disconnected_operation()
                self._has_leader = False
                self.on_change()
                if self._state_updater:
                    self._state_updater.state_transition('Standby', 'Unable to access multisite DCS')
        else:
            try:
                self._update_history(cluster)
                self.touch_member()
            except DCSError as e:
                pass

    def _observe_leader(self):
        """
        Observe multisite state and make sure

        """
        try:
            cluster = self.dcs.get_cluster()

            if cluster.is_unlocked():
                logger.info("Multisite has no leader")
                self._disconnected_operation()
            else:
                # There is a leader cluster
                lock_owner = cluster.leader and cluster.leader.name
                # The leader is us
                if lock_owner == self.name:
                    logger.info("Multisite leader is us")
                    self._standby_config = None
                else:
                    logger.info(f"Multisite leader is {lock_owner}")
                    self._set_standby_config(cluster.leader.member)
        except DCSError as e:
            # On replicas we need to know the multisite status only for rewinding.
            logger.warning(f"Error accessing multisite DCS: {e}")

    def _update_history(self, cluster):
        if cluster.history and cluster.history.lines and isinstance(cluster.history.lines[0], dict):
            self.site_switches = cluster.history.lines[0].get('switches')

        if self._has_leader:
            if cluster.history and cluster.history.lines and isinstance(cluster.history.lines, dict):
                history_state = cluster.history.lines
                if history_state.get('last_leader') != self.name:
                    new_state = [{'last_leader': self.name, 'switches': history_state.get('switches', 0) + 1}]
                    self.dcs.set_history_value(json.dumps(new_state))
            else:
                self.dcs.set_history_value(json.dumps([{'last_leader': self.name, 'switches': 0}]))

    def _check_for_failover(self, cluster):
        if cluster.failover and cluster.failover.target_site:
            if cluster.failover.target_site == self.name:
                logger.info("Cleaning up failover key targeting us")
                self.dcs.manual_failover('', '')
            elif not any(m.name == cluster.failover.target_site for m in cluster.members):
                logger.info(f"Multisite failover target {cluster.failover.target_site} is not registered")
            else:
                if self._failover_target != cluster.failover.target_site:
                    logger.info(f"Initiating multisite failover to {cluster.failover.target_site}")
                    self._failover_timeout = time.time() + self.switchover_timeout
                    # TODO: need to set timeout in DCS for more than two sites to avoid wrong site taking over
                self._failover_target = cluster.failover.target_site
        else:
            self._failover_target = None
            self._failover_timeout = None

    def touch_member(self):
        data = {
            'host': self.config['host'],
            'port': self.config['port'],
        }
        logger.info(f"Touching member {self.name} with {data!r}")
        self.dcs.touch_member(data)

    def run(self):
        self._observe_leader()
        while not self._heartbeat.wait(self.config['observe_interval']):
            # Keep track of who is the leader even when we are not the primary node to be able to rewind from them
            self._observe_leader()
        while not self.stop_requested:
            self._resolve_multisite_leader()
            self._heartbeat.clear()
            self._leader_resolved.set()
            if self._state_updater:
                self._state_updater.store_updates()
            while not self._heartbeat.wait(self.config['observe_interval']):
                self._observe_leader()

    def shutdown(self):
        self.stop_requested = True
        self._heartbeat.set()
        self.join()


class KubernetesStateManagement:
    def __init__(self, crd_name, crd_uid, reporter, crd_api):
        self.crd_namespace, self.crd_name = (['default'] + crd_name.rsplit('.', 1))[-2:]
        self.crd_uid = crd_uid
        self.reporter = reporter
        self.crd_api_group, self.crd_api_version = crd_api.rsplit('/', 1)

        # TODO: handle config loading when main DCS is not Kubernetes based
        #apiclient = k8s_client.ApiClient(False)
        kubernetes.config.load_incluster_config()
        apiclient = kubernetes.client.ApiClient()
        self._customobj_api = kubernetes.client.CustomObjectsApi(apiclient)
        self._events_api = kubernetes.client.EventsV1Api(apiclient)

        self._status_update = None
        self._event_obj = None

    def state_transition(self, new_state, note):
        self._status_update = {"status": {"Multisite": new_state}}

        failover_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        reason = 'Promote' if new_state == 'Leader' else 'Demote'
        if note is None:
            note = 'Acquired multisite leader' if new_state == 'Leader' else 'Became a standby cluster'

        self._event_obj = kubernetes.client.EventsV1Event(
            action='Failover',
            event_time=failover_time,
            type="Normal",
            reporting_controller="patroni",
            reporting_instance=self.reporter,
            regarding=kubernetes.client.V1ObjectReference(
                api_version="acid.zalan.do/v1",
                kind="postgresql",
                name=self.crd_name,
                namespace=self.crd_namespace,
                uid=self.crd_uid,
            ),
            reason=reason, note=note,
            metadata=kubernetes.client.V1ObjectMeta(namespace=self.crd_namespace, generate_name=self.crd_name)
        )

    def store_updates(self):
        try:
            if self._status_update:
                self.update_crd_state(self._status_update)
                self._status_update = None
            if self._event_obj:
                self.create_failover_event(self._event_obj)
                self._event_obj = None
        except Exception as e:
            logger.warning("Unable to store Kubernetes status update: %s", e)

    @catch_kubernetes_errors
    def update_crd_state(self, update):
        self._customobj_api.patch_namespaced_custom_object_status(self.crd_api_group, self.crd_api_version, self.crd_namespace,
                                                    'postgresqls', self.crd_name + '/status', update,
                                                     field_manager='patroni')

        return True

    def create_failover_event(self, event):
        self._events_api.create_namespaced_event(self.crd_namespace, event)
