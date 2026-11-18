import abc
import logging
import platform
import sys

from threading import RLock
from typing import Any, Callable, Dict, Optional, Union

from ..config import Config
from ..exceptions import WatchdogError

__all__ = ['WatchdogError', 'Watchdog']

logger = logging.getLogger(__name__)

MODE_REQUIRED = 'required'    # Will not run if a watchdog is not available
MODE_AUTOMATIC = 'automatic'  # Will use a watchdog if one is available
MODE_OFF = 'off'              # Will not try to use a watchdog


def parse_mode(mode: Union[bool, str]) -> str:
    if mode is False:
        return MODE_OFF
    mode = str(mode).lower()
    if mode in ['require', 'required']:
        return MODE_REQUIRED
    elif mode in ['auto', 'automatic']:
        return MODE_AUTOMATIC
    else:
        if mode not in ['off', 'disable', 'disabled']:
            logger.warning("Watchdog mode %s not recognized, disabling watchdog", mode)
        return MODE_OFF


def synchronized(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(self: 'Watchdog', *args: Any, **kwargs: Any) -> Any:
        with self.lock:
            return func(self, *args, **kwargs)
    return wrapped


class WatchdogConfig(object):
    """Helper to contain a snapshot of configuration"""
    def __init__(self, config: Config) -> None:
        watchdog_config = config.get("watchdog") or {'mode': 'automatic'}

        self.mode = parse_mode(watchdog_config.get('mode', 'automatic'))
        self.ttl = config['ttl']
        self.loop_wait = config['loop_wait']
        self.safety_margin = watchdog_config.get('safety_margin', 5)
        self.driver = watchdog_config.get('driver', 'default')
        self.fallback = str(watchdog_config.get('fallback', 'none')).lower()
        if self.fallback not in ['none', 'thread']:
            logger.warning("Watchdog fallback %s not recognized", self.fallback)
            self.fallback = 'none'
        self.driver_config = dict((k, v) for k, v in watchdog_config.items()
                                  if k not in ['mode', 'safety_margin', 'driver', 'fallback'])

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, WatchdogConfig) and \
            all(getattr(self, attr) == getattr(other, attr) for attr in
                ['mode', 'ttl', 'loop_wait', 'safety_margin', 'driver', 'driver_config', 'fallback'])

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def get_impl(self) -> 'WatchdogBase':
        if self.driver == 'testing':  # pragma: no cover
            from patroni.watchdog.linux import TestingWatchdogDevice
            return TestingWatchdogDevice.from_config(self.driver_config)
        elif platform.system() == 'Linux' and self.driver == 'default':
            from patroni.watchdog.linux import LinuxWatchdogDevice
            return LinuxWatchdogDevice.from_config(self.driver_config)
        else:
            return NullWatchdog()

    @property
    def timeout(self) -> int:
        if self.safety_margin == -1:
            return int(self.ttl // 2)
        else:
            return self.ttl - self.safety_margin

    @property
    def timing_slack(self) -> int:
        return self.timeout - self.loop_wait


class Watchdog(object):
    """Facade to dynamically manage watchdog implementations and handle config changes.

    When device activation fails automatic mode can optionally fall back to a watchdog thread. To avoid log spam
    device activation will only be retried when watchdog configuration is changed."""
    def __init__(self, config: Config, on_timeout: Optional[Callable[[], None]] = None) -> None:
        self.config = WatchdogConfig(config)
        self.active_config: WatchdogConfig = self.config
        self.lock = RLock()
        self.active = False
        self._impl_pending = False
        # Activation in required mode failed because of a too long timeout of the device, see is_healthy
        self._unsafe_timeout = False

        if self.config.mode == MODE_OFF:
            self.impl = NullWatchdog()
        else:
            self.impl = self.config.get_impl()
            if self.config.mode == MODE_REQUIRED and self.impl.is_null:
                logger.error("Configuration requires a watchdog, but watchdog is not supported on this platform.")
                sys.exit(1)

        from patroni.watchdog.thread import ThreadWatchdog
        try:
            self._fallback: WatchdogBase = ThreadWatchdog(self.config.timeout, on_timeout)
        except WatchdogError as e:
            logger.warning("Could not initialize watchdog thread: %s", e)
            self._fallback = NullWatchdog()

    @synchronized
    def reload_config(self, config: Config) -> None:
        self.config = WatchdogConfig(config)
        self._unsafe_timeout = False
        # Turning a watchdog off can always be done immediately
        if self.config.mode == MODE_OFF:
            if self.active:
                self._close_impl()
            self.active_config = self.config
            self.impl = NullWatchdog()
            self._impl_pending = False
        elif self.config.fallback != 'thread' and self.impl is self._fallback:
            self._close_impl()
            self.impl = NullWatchdog()
        # If watchdog is not active we can apply config immediately to show any warnings early. Otherwise we need to
        # delay until next time a keepalive is sent so timeout matches up with leader key update. Until then the armed
        # timeout still protects the leader key, see needs_immediate_keepalive.
        if self.active:
            return
        if self._impl_pending or self.config.mode != self.active_config.mode or \
                self.config.driver != self.active_config.driver or \
                self.config.driver_config != self.active_config.driver_config or \
                self.config.fallback != self.active_config.fallback:
            self.impl = self.config.get_impl()
            self._impl_pending = False
        self.active_config = self.config

    @synchronized
    def activate(self) -> bool:
        """Activates the watchdog device with suitable timeouts. While watchdog is active keepalive needs
        to be called every time loop_wait expires.

        :returns False if a safe watchdog could not be configured, but is required.
        """
        self.active = True
        if self._impl_pending or self.config.mode != self.active_config.mode or \
                self.config.fallback != self.active_config.fallback and self.impl.is_null:
            if not self._replace_impl():
                return self.config.mode != MODE_REQUIRED
        if self._activate():
            return True
        # The HA loop gives up the leader key, so keepalive() won't apply configuration changes anymore. Unless a
        # running watchdog has to be disabled on demotion, reload_config() must apply them, or the node stays unhealthy.
        self.active = self.impl.is_running
        return False

    def _replace_impl(self) -> bool:
        """Replace the implementation, unless the current one could not be confirmed as disabled."""
        if not self._try_disable_impl():
            logger.warning("Keeping %s because it could not be disabled", self.impl.describe())
            self._impl_pending = True
            return False
        self.impl = self.config.get_impl()
        self._impl_pending = False
        return True

    def _activate(self) -> bool:
        self.active_config = self.config

        if self.config.timing_slack < 0:
            logger.warning('Watchdog not supported because leader TTL %s is less than 2x loop_wait %s',
                           self.config.ttl, self.config.loop_wait)
            self.impl = NullWatchdog()
            # Reacquire the configured device once a configuration change fixes the timing.
            self._impl_pending = True

        try:
            self.impl.open()
            actual_timeout = self._set_timeout()
            if self.impl.is_running and actual_timeout < self.config.loop_wait and \
                    self._disable_unsafe_timeout(actual_timeout):
                self.impl = NullWatchdog()
                actual_timeout = None
        except WatchdogError as e:
            log = logger.warning if self.config.mode == MODE_REQUIRED else logger.debug
            log("Could not activate %s: %s", self.impl.describe(), e)
            if not self._try_disable_impl():
                return self.config.mode != MODE_REQUIRED
            self.impl = NullWatchdog()
            actual_timeout = self.impl.get_timeout()

        actual_timeout = self._activate_fallback(actual_timeout)

        if self.impl.is_running and not self.impl.can_be_disabled:
            logger.warning("Watchdog implementation can't be disabled."
                           " Watchdog will trigger after Patroni loses leader key.")

        if not self.impl.is_running or actual_timeout and actual_timeout > self.config.timeout:
            if self.config.mode == MODE_REQUIRED:
                if self.impl.is_null:
                    logger.error("Configuration requires watchdog, but watchdog could not be configured.")
                else:
                    logger.error("Configuration requires watchdog, but a safe watchdog timeout %s could"
                                 " not be configured. Watchdog timeout is %s.", self.config.timeout, actual_timeout)
                    # Otherwise the node would keep winning leader races only to give up the leader key again
                    self._unsafe_timeout = True
                return False
            if not self.impl.is_null:
                logger.warning("Watchdog timeout %s seconds does not ensure safe termination within %s seconds",
                               actual_timeout, self.config.timeout)

        if self.is_running:
            logger.info("%s activated with %s second timeout, timing slack %s seconds",
                        self.impl.describe(), actual_timeout, self.config.timing_slack)
        elif self.config.mode == MODE_REQUIRED:
            logger.error("Configuration requires watchdog, but watchdog could not be activated")
            return False

        return True

    def _activate_fallback(self, actual_timeout: Optional[int]) -> Optional[int]:
        if self.config.mode != MODE_AUTOMATIC or self.config.fallback != 'thread' or self.config.timing_slack < 0:
            return actual_timeout
        if self.impl.is_running:
            if actual_timeout is not None and self.config.loop_wait <= actual_timeout <= self.config.timeout:
                return actual_timeout
            # An explicit disable doesn't depend on magic close support; a device that can't be stopped stays armed.
            if not self._try_disable_impl():
                return actual_timeout
        self.impl = self._fallback
        try:
            actual_timeout = self._set_timeout()
            self.impl.open()
            if self.impl.is_running:
                logger.warning("Watchdog device is unavailable or unusable, using thread watchdog fallback")
        except WatchdogError as e:
            logger.warning("Could not activate %s: %s", self.impl.describe(), e)
            self.impl = NullWatchdog()
            actual_timeout = self.impl.get_timeout()
        return actual_timeout

    def _set_timeout(self) -> int:
        if self.impl.has_set_timeout():
            self.impl.set_timeout(self.config.timeout)
        return self.impl.get_timeout()

    def _disable_unsafe_timeout(self, actual_timeout: int) -> bool:
        """Stop an unsafe timer, allowing magic close if explicit disabling is unsupported."""
        logger.error('loop_wait of %s seconds is too long for watchdog %s second timeout',
                     self.config.loop_wait, actual_timeout)
        if not self.impl.can_be_disabled:
            return False
        logger.info('Disabling watchdog due to unsafe timeout.')
        try:
            self.impl.disable()
        except WatchdogError as e:
            logger.warning('%s, closing watchdog device instead', e)
            self.impl.close()
        return True

    @synchronized
    def disable(self) -> None:
        self._close_impl()
        self.active = False

    def wait_for_thread_expiry(self) -> None:
        """Unlike a device, the watchdog thread can't outlive Patroni, so block while it is armed or firing."""
        from patroni.watchdog.thread import ThreadWatchdog
        if isinstance(self._fallback, ThreadWatchdog):
            self._fallback.wait()

    def _try_disable_impl(self) -> bool:
        """Try to disable the implementation and report whether it is no longer running."""
        try:
            self.impl.disable()
        except WatchdogError as e:
            logger.error("Error while disabling watchdog: %s", e)
        return not self.impl.is_running

    def _close_impl(self) -> None:
        """Close the implementation without requiring confirmation that its timer stopped."""
        try:
            if self.impl.is_running and not self.impl.can_be_disabled:
                # Give sysadmin some extra time to clean stuff up.
                self.impl.keepalive()
                logger.warning("Watchdog implementation can't be disabled. System will reboot after "
                               "%s seconds when watchdog times out.", self.impl.get_timeout())
        except WatchdogError as e:
            logger.error("Error while disabling watchdog: %s", e)
        try:
            self.impl.close()
        except WatchdogError as e:
            logger.error("Error while disabling watchdog: %s", e)

    def _apply_config_changes(self) -> None:
        if self.config.timing_slack < 0:
            # No timeout is safe with the new timing, see _activate(). In required mode the HA loop gives up the leader
            # key, see activate().
            self._close_impl()
            self.active = self._activate()
            return
        device_changed = self.config.driver != self.active_config.driver or \
            self.config.driver_config != self.active_config.driver_config
        mode_requires_replacement = self.config.mode != self.active_config.mode and \
            (self.impl is self._fallback or not self.impl.is_running)
        fallback_requires_replacement = self.config.fallback != self.active_config.fallback and self.impl.is_null
        needs_replacement = self._impl_pending or device_changed or mode_requires_replacement or \
            fallback_requires_replacement

        if needs_replacement and self._replace_impl():
            self._activate()
        timeout_changed = self.config.timeout != self.active_config.timeout
        if not self.impl.is_null and (timeout_changed or self.config.loop_wait != self.active_config.loop_wait):
            actual_timeout = self._set_timeout() if timeout_changed else self.impl.get_timeout()
            # Same check as on activation, a device might not accept a timeout that covers the new loop_wait.
            if self.impl.is_running and actual_timeout < self.config.loop_wait and \
                    self._disable_unsafe_timeout(actual_timeout):
                self.impl = NullWatchdog()
            elif timeout_changed and self.is_running:
                logger.info("%s updated with %s second timeout, timing slack %s seconds",
                            self.impl.describe(), actual_timeout, self.config.timing_slack)
        if self.config.mode == MODE_AUTOMATIC and self.config.fallback == 'thread':
            self._activate_fallback(self.impl.get_timeout())
        self.active_config = self.config

    @synchronized
    def keepalive(self) -> None:
        if not self.active:
            return
        try:
            self.impl.keepalive()
            if self.config != self.active_config:
                self._apply_config_changes()
        except WatchdogError as e:
            logger.error("Error while sending keepalive: %s", e)
            self._activate_fallback(None)

    @property
    @synchronized
    def is_running(self) -> bool:
        return self.impl.is_running

    @property
    @synchronized
    def needs_immediate_keepalive(self) -> bool:
        """The timeout armed for the previous configuration would expire before the HA loop wakes up after the new
        ``loop_wait``. Only the keepalive following the leader key update with the new TTL may apply the new timeout,
        so it has to be sent right away."""
        if not self.active or not self.impl.is_running:
            return False
        try:
            # The actual timeout, because a device might not have accepted the configured one.
            return self.impl.get_timeout() < self.config.loop_wait
        except WatchdogError:
            return True

    @property
    @synchronized
    def is_healthy(self) -> bool:
        if self.config.mode != MODE_REQUIRED:
            return True
        return self.config.timing_slack >= 0 and self.config.mode == self.active_config.mode and \
            not self._unsafe_timeout and self.impl.is_healthy


class WatchdogBase(abc.ABC):
    """A watchdog object when opened requires periodic calls to keepalive.
    When keepalive is not called within a timeout the system will be terminated."""
    is_null = False

    @property
    def is_running(self) -> bool:
        """Returns True when watchdog is activated and capable of performing it's task."""
        return False

    @property
    def is_healthy(self) -> bool:
        """Returns False when calling open() is known to fail."""
        return False

    @property
    def can_be_disabled(self) -> bool:
        """Whether the implementation supports disabling; this does not guarantee success.

        Use disable() to confirm the timer stopped before replacing it. May raise WatchdogError
        if called without calling open() first."""
        return True

    @abc.abstractmethod
    def open(self) -> None:
        """Open watchdog device.

        When watchdog is opened keepalive must be called. Returns nothing on success
        or raises WatchdogError if the device could not be opened."""

    @abc.abstractmethod
    def close(self) -> None:
        """Gracefully close watchdog device."""

    def disable(self) -> None:
        """Confirm the watchdog stopped and close it; retain an armed device on failure."""
        self.close()

    @abc.abstractmethod
    def keepalive(self) -> None:
        """Resets the watchdog timer.

        Watchdog must be open when keepalive is called."""

    @abc.abstractmethod
    def get_timeout(self) -> int:
        """Returns the current keepalive timeout in effect."""

    def has_set_timeout(self) -> bool:
        """Returns True if setting a timeout is supported."""
        return False

    def set_timeout(self, timeout: int) -> None:
        """Set the watchdog timer timeout.

        :param timeout: watchdog timeout in seconds"""
        raise WatchdogError("Setting timeout is not supported on {0}".format(self.describe()))

    def describe(self) -> str:
        """Human readable name for this device"""
        return self.__class__.__name__

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'WatchdogBase':
        return cls()


class NullWatchdog(WatchdogBase):
    """Null implementation when watchdog is not supported."""
    is_null = True

    def open(self) -> None:
        return

    def close(self) -> None:
        return

    def keepalive(self) -> None:
        return

    def get_timeout(self) -> int:
        # A big enough number to not matter
        return 1000000000
