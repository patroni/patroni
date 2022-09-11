import abc
import logging
from threading import Thread, Event
import six

from .dcs import Member
from .exceptions import DCSError

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


class SingleSiteController(AbstractSiteController):
    """Do nothing controller for single site operation."""


class MultisiteController(Thread, AbstractSiteController):
    is_active = True

    def __init__(self, config, on_change=None):
        super().__init__()
        self.stop_requested = False
        self.on_change = on_change

        msconfig = config['multisite']

        from .dcs import get_dcs
        inherited_keys = ['name', 'scope', 'namespace', 'loop_wait', 'ttl', 'retry_timeout']
        for key in inherited_keys:
            if key not in msconfig and key in config:
                msconfig[key] = config[key]

        # TODO: fetch default host/port from postgresql section
        if 'host' not in msconfig or 'port' not in msconfig:
            raise Exception("Missing host or port from multisite configuration")

        self.config = msconfig

        self.name = msconfig['name']
        self.dcs = get_dcs(msconfig)

        self._heartbeat = Event()
        self._standby_config = None
        self._leader_resolved = Event()
        self._has_leader = False
        self._release = False

        self._dcs_error = None

    def get_active_standby_config(self):
        return self._standby_config

    def resolve_leader(self):
        """Try to become leader, update active config correspondingly.

        Return error when unable to res"""
        self._leader_resolved.clear()
        self._heartbeat.set()
        self._leader_resolved.wait()
        return self._dcs_error

    def heartbeat(self):
        """"Notify multisite mechanism that this site has a properly operating cluster mechanism.

        Need to send out an async lease update. If that fails to complete within safety margin of ttl running
        out then we need to
        """
        logger.info("Triggering multisite hearbeat")
        self._heartbeat.set()

    def release(self):
        self._release = True
        self._heartbeat.set()

    def _disconnected_operation(self):
        self._standby_config = {'restore_command': 'false'}

    def _set_standby_config(self, other: Member):
        logger.info(f"Multisite replicate from {other}")
        # TODO: add support for replication slots
        old_conf, self._standby_config = self._standby_config, {
            'host': other.data['host'],
            'port': other.data['port'],
        }
        if old_conf != self._standby_config:
            logger.info(f"Setting standby configuration to: {self._standby_config}")
        return old_conf != self._standby_config

    def _resolve_multisite_leader(self):
        logger.info("Running multisite consensus.")
        try:
            cluster = self.dcs.get_cluster()
            self._dcs_error = None

            if not cluster.has_member(self.name):
                self.touch_member()

            if cluster.is_unlocked():
                if self._release:
                    self._release = False
                    return
                # Became leader of unlocked cluster
                if self.dcs.attempt_to_acquire_leader():
                    logger.info("Became multisite leader")
                    self._standby_config = None
                    if not self._has_leader:
                        self._has_leader = True
                        self.on_change()
                # Failed to become leader, maybe someone else acquired lock, maybe we just failed
                else:
                    logger.info("Failed to acquire multisite lock")
                    # Non-working standby config while we are resolving who to connect to
                    self._standby_config = self.DISCONNECTED_STANDBY_CONFIG
                    if self._has_leader:
                        # If we had considered ourselves to be the leader, notify main loop to reconfigure ASAP
                        self._has_leader = False
                        self.on_change()
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
                        self.dcs.delete_leader()
                        self._release = False
                        return
                    if self.dcs.update_leader(None):
                        logger.info("Updated multisite leader lease")
                        # Make sure we are disabled from standby mode
                        self._standby_config = None
                        if not self._has_leader:
                            self._has_leader = True
                            self.on_change()
                    else:
                        logger.error("Failed to update multiste leader status")
                        self._disconnected_operation()
                        if self._has_leader:
                            self._has_leader = False
                            self.on_change()
                # Current leader is someone else
                else:
                    logger.info(f"Multisite has leader and it is {lock_owner}")
                    self._release = False
                    if self._set_standby_config(cluster.leader.member):
                        if self._has_leader:
                            self._has_leader = False
                        self.on_change()
        except DCSError as e:
            logger.error(f"Error accessing multisite DCS: {e}")
            self._dcs_error = 'Multi site DCS cannot be reached'
            if self._has_leader:
                self._disconnected_operation()
                self._has_leader = False
                self.on_change()
        else:
            try:
                self.touch_member()
            except DCSError as e:
                pass

    def touch_member(self):
        data = {
            'host': self.config['host'],
            'port': self.config['port'],
        }
        logger.info(f"Touching member {self.name} with {data!r}")
        self.dcs.touch_member(data)

    def run(self):
        self._heartbeat.wait()
        while not self.stop_requested:
            self._resolve_multisite_leader()
            self._heartbeat.clear()
            self._leader_resolved.set()
            self._heartbeat.wait()

    def shutdown(self):
        self.stop_requested = True
        self._heartbeat.set()
        self.join()
