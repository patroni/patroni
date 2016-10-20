import abc
import logging
import platform
import six
import sys

from patroni.exceptions import WatchdogError

__all__ = ['WatchdogError', 'Watchdog']

logger = logging.getLogger(__name__)

MODE_REQUIRED = 'required'    # Will not run if a watchdog is not available
MODE_AUTOMATIC = 'automatic'  # Will use a watchdog if one is available
MODE_OFF = 'off'              # Will not try to use a watchdog


def parse_mode(mode):
    if mode is False:
        return MODE_OFF
    mode = mode.lower()
    if mode in ['require', 'required']:
        return MODE_REQUIRED
    elif mode in ['auto', 'automatic']:
        return MODE_AUTOMATIC
    else:
        if mode not in ['off', 'disable', 'disabled']:
            logger.warning("Watchdog mode {0} not recognized, disabling watchdog".format(mode))
        return MODE_OFF


class Watchdog(object):
    """Facade to dynamically manage watchdog implementations and handle config changes."""
    def __init__(self, config):
        self.ttl = config['ttl']
        self.loop_wait = config['loop_wait']
        self.mode = parse_mode(config['watchdog'].get('mode', 'automatic'))
        self.config = config

        if self.mode == MODE_OFF:
            self.impl = NullWatchdog()
        else:
            self.impl = self._get_impl()
            if self.mode == MODE_REQUIRED and isinstance(self.impl, NullWatchdog):
                logger.error("Configuration requires a watchdog, but watchdog is not supported on this platform.")
                sys.exit(1)

    def activate(self):
        """Activates the watchdog device with suitable timeouts. While watchdog is active keepalive needs
        to be called every time loop_wait expires.
        """
        desired_timeout = int(self.ttl // 2)
        slack = desired_timeout - self.loop_wait
        if slack < 0:
            logger.warning('Watchdog not supported because leader TTL {0} is less than 2x loop_wait {1}'
                           .format(self.ttl, self.loop_wait))
            self.impl = NullWatchdog()

        try:
            self.impl.open()
        except WatchdogError as e:
            logger.warning("Could not activate %s: %s", self.impl.describe(), e)
            self.impl = NullWatchdog()

        if self.impl.is_running and not self.impl.can_be_disabled:
            logger.warning("Watchdog implementation can't be disabled."
                           " Watchdog will trigger after Patroni is shut down.")

        if self.impl.has_set_timeout():
            self.impl.set_timeout(desired_timeout)

        # Safety checks for watchdog implementations that don't support configurable timeouts
        actual_timeout = self.impl.get_timeout()
        if self.impl.is_running and actual_timeout < self.loop_wait:
            logger.error('loop_wait of {0} seconds is too long for watchdog {1} second timeout'
                         .format(self.loop_wait, actual_timeout))
            if self.impl.can_be_disabled:
                logger.info('Disabling watchdog due to unsafe timeout.')
                self.impl.close()
                self.impl = NullWatchdog()

        if not self.impl.is_running or actual_timeout > desired_timeout:
            if self.mode == MODE_REQUIRED:
                logger.error("Configuration requires watchdog, but a safe watchdog timeout {0} could"
                             " not be configured. Watchdog timeout is {1}.".format(desired_timeout, actual_timeout))
                sys.exit(1)
            else:
                logger.warning("Watchdog timeout {0} seconds does not ensure safe termination within {1} seconds"
                               .format(actual_timeout, desired_timeout))

        if self.is_running:
            logger.info("{0} activated with {1} second timeout, timing slack {2} seconds"
                        .format(self.impl.describe(), actual_timeout, slack))
        else:
            if self.mode == MODE_REQUIRED:
                logger.error("Configuration requires watchdog, but watchdog could not be activated")
                sys.exit(1)

    def disable(self):
        try:
            if self.impl.is_running and not self.impl.can_be_disabled:
                # Give sysadmin some extra time to clean stuff up.
                self.impl.keepalive()
                logger.warning("Watchdog implementation can't be disabled. System will reboot after "
                               "{0} seconds when watchdog times out.".format(self.impl.get_timeout()))
            self.impl.close()
        except WatchdogError as e:
            logger.error("Error while disabling watchdog: %s", e)

    def keepalive(self):
        try:
            self.impl.keepalive()
        except WatchdogError as e:
            logger.error("Error while disabling watchdog: %s", e)

    def _get_impl(self):
        if self.mode not in ['automatic', 'required', 'require']:
            return NullWatchdog()

        if platform.system() == 'Linux':
            from patroni.watchdog.linux import LinuxWatchdogDevice
            return LinuxWatchdogDevice.from_config(self.config['watchdog'])
        else:
            return NullWatchdog()

    @property
    def is_running(self):
        return self.impl.is_running


@six.add_metaclass(abc.ABCMeta)
class WatchdogBase(object):
    """A watchdog object when opened requires periodic calls to keepalive.
    When keepalive is not called within a timeout the system will be terminated."""

    @property
    def is_running(self):
        """Returns True when watchdog is activated and capable of performing it's task."""
        return False

    @property
    def can_be_disabled(self):
        """Returns True when watchdog will be disabled by calling close(). Some watchdog devices
        will keep running no matter what once activated. May raise WatchdogError if called without
        calling open() first."""
        return True

    @abc.abstractmethod
    def open(self):
        """Open watchdog device.

        When watchdog is opened keepalive must be called. Returns nothing on success
        or raises WatchdogError if the device could not be opened."""

    @abc.abstractmethod
    def close(self):
        """Gracefully close watchdog device."""

    @abc.abstractmethod
    def keepalive(self):
        """Resets the watchdog timer.

        Watchdog must be open when keepalive is called."""

    @abc.abstractmethod
    def get_timeout(self):
        """Returns the current keepalive timeout in effect."""

    def has_set_timeout(self):
        """Returns True if setting a timeout is supported."""
        return False

    def set_timeout(self, timeout):
        """Set the watchdog timer timeout.

        :param timeout: watchdog timeout in seconds"""
        raise WatchdogError("Setting timeout is not supported on {0}".format(self.describe()))

    def describe(self):
        """Human readable name for this device"""
        return self.__class__.__name__

    @classmethod
    def from_config(cls, config):
        return cls()


class NullWatchdog(WatchdogBase):
    """Null implementation when watchdog is not supported."""
    def open(self):
        return

    def close(self):
        return

    def keepalive(self):
        return

    def get_timeout(self):
        # A big enough number to not matter
        return 1000000000
