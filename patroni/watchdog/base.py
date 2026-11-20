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

# The HA loop doesn't run more often than with the minimal loop_wait, see Watchdog.keepalive_interval
MIN_KEEPALIVE_INTERVAL = 1


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
        self.retry_timeout = config.get('retry_timeout', 10)
        self.safety_margin = watchdog_config.get('safety_margin', 5)
        self.driver = watchdog_config.get('driver', 'default')
        self.driver_config = dict((k, v) for k, v in watchdog_config.items()
                                  if k not in ['mode', 'safety_margin', 'driver'])

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, WatchdogConfig) and \
            all(getattr(self, attr) == getattr(other, attr) for attr in
                ['mode', 'ttl', 'loop_wait', 'retry_timeout', 'safety_margin', 'driver', 'driver_config'])

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

    When activation fails underlying implementation will be switched to a Null implementation. To avoid log spam
    activation will only be retried when watchdog configuration is changed."""
    def __init__(self, config: Config) -> None:
        self.config = WatchdogConfig(config)
        self.active_config: WatchdogConfig = self.config
        self.lock = RLock()
        self.active = False
        # The last activation or configuration change resulted in a watchdog that is safe to lead with, see is_activated
        self._activated = False
        self._impl_pending = False
        # The last known timeout of the open implementation, see keepalive_interval
        self._timeout: Optional[int] = None

        if self.config.mode == MODE_OFF:
            self.impl = NullWatchdog()
        else:
            self.impl = self.config.get_impl()
            if self.config.mode == MODE_REQUIRED and self.impl.is_null:
                logger.error("Configuration requires a watchdog, but watchdog is not supported on this platform.")
                sys.exit(1)
        # Activation in required mode failed because of a too long timeout of the device, see is_healthy
        self._unsafe_timeout = False

    @synchronized
    def reload_config(self, config: Config) -> None:
        self.config = WatchdogConfig(config)
        self._unsafe_timeout = False
        # Turning a watchdog off can always be done immediately
        if self.config.mode == MODE_OFF:
            # Also if it isn't active, a device might be still open after closing it failed
            self._close_impl()
            self.active_config = self.config
            self.impl = NullWatchdog()
            self._impl_pending = False
        # If watchdog is not active we can apply config immediately to show any warnings early. Otherwise we need to
        # delay until next time a keepalive is sent so timeout matches up with leader key update. Until then the armed
        # timeout still protects the leader key, see keepalive_interval.
        if self.active:
            return
        self._apply_inactive_config()

    def _device_changed(self) -> bool:
        return self.config.driver != self.active_config.driver or \
            self.config.driver_config != self.active_config.driver_config

    def _impl_outdated(self) -> bool:
        """The configuration requires a new implementation, but replacing it might have been deferred."""
        return self._impl_pending or self.config.mode != self.active_config.mode or self._device_changed()

    def _apply_inactive_config(self) -> None:
        """Apply the configuration while the watchdog is inactive, the implementation is opened on activation."""
        if self._impl_outdated():
            if self.impl.is_running:
                # Closing it failed, so the next activation has to disable it before replacing it
                self._impl_pending = True
            else:
                self.impl = self.config.get_impl()
                self._impl_pending = False
        self.active_config = self.config

    @synchronized
    def activate(self) -> bool:
        """Activates the watchdog device with suitable timeouts. While watchdog is active keepalive needs
        to be called every time loop_wait expires.

        :returns False if a safe watchdog could not be configured, but is required.
        """
        if self.active and self.impl.is_running:
            # It might have been retained because it could not be disabled, then it must not be opened again
            return self._activated
        self.active = True
        # A configuration change might have been deferred to a keepalive that was never sent. The HA loop doesn't
        # disable a watchdog that isn't running, so it might still be active after losing the leader key. A device
        # might be still open after closing it failed, but nobody fed it since then.
        if (self._impl_outdated() or self.impl.is_running) and not self._replace_impl():
            self._activated = self._keep_impl()
            return self._activated
        self._activated = self._activate()
        if not self._activated:
            # The HA loop gives up the leader key, so keepalive() won't apply configuration changes anymore. Unless a
            # running watchdog has to be disabled on demotion, reload_config() must apply them, or the node stays
            # unhealthy.
            self.active = self.impl.is_running
        return self._activated

    def _replace_impl(self) -> bool:
        """Replace the implementation, unless the current one could not be confirmed as disabled."""
        if not self._try_disable_impl():
            logger.warning("Keeping %s because it could not be disabled", self.impl.describe())
            self._impl_pending = True
            return False
        self.impl = self.config.get_impl()
        self._impl_pending = False
        return True

    def _keep_impl(self) -> bool:
        """Keep a running implementation that could not be disabled, it still has to be fed.

        In required mode it isn't considered safe to lead with. Otherwise the configured timeout is applied to it,
        because it might still have the one it was armed with.

        :returns: ``False`` if a safe watchdog is required, see is_activated.
        """
        if self.config.mode == MODE_REQUIRED:
            return False
        try:
            self._update_timeout()
        except WatchdogError as e:
            logger.error("Error while applying watchdog configuration: %s", e)
        return True

    def _activate(self) -> bool:
        self.active_config = self.config

        if self.config.timing_slack < 0:
            logger.warning('Watchdog not supported because leader TTL %s is less than 2x loop_wait %s',
                           self.config.ttl, self.config.loop_wait)
            self.impl = NullWatchdog()
            # Reacquire the configured device once a configuration change fixes the timing.
            self._impl_pending = True

        # Opening a new descriptor, e.g. after the device was replaced
        self._timeout = None
        try:
            self.impl.open()
            actual_timeout = self._set_timeout()
        except WatchdogError as e:
            log = logger.warning if self.config.mode == MODE_REQUIRED else logger.debug
            log("Could not activate %s: %s", self.impl.describe(), e)
            # The device is armed if open() succeeded, but _set_timeout() failed
            if not self._try_disable_impl():
                logger.warning("Keeping %s because it could not be disabled", self.impl.describe())
                return self._keep_impl()
            self.impl = NullWatchdog()
            actual_timeout = self.impl.get_timeout()

        if self.impl.is_running and not self.impl.can_be_disabled:
            logger.warning("Watchdog implementation can't be disabled."
                           " Watchdog will trigger after Patroni loses leader key.")

        if not self._check_timeout(actual_timeout):
            return False

        if self.is_running:
            logger.info("%s activated with %s second timeout, timing slack %s seconds",
                        self.impl.describe(), actual_timeout, self.config.timing_slack)
            self._check_keepalive_interval(actual_timeout)
        elif self.config.mode == MODE_REQUIRED:
            logger.error("Configuration requires watchdog, but watchdog could not be activated")
            return False

        return True

    def _check_timeout(self, actual_timeout: int) -> bool:
        """Check whether the implementation ensures safe termination within the configured timeout.

        :returns: ``False`` if a safe watchdog is required, but could not be configured.
        """
        if self._is_safe_timeout(actual_timeout):
            return True
        too_short = self.impl.is_running and self._is_too_short(actual_timeout)
        if too_short:
            logger.warning("Watchdog timeout of %s seconds is too short, the HA loop would have to run every %s"
                           " seconds", actual_timeout, self._keepalive_interval(actual_timeout))
        if self.config.mode == MODE_REQUIRED:
            if self.impl.is_null:
                logger.error("Configuration requires watchdog, but watchdog could not be configured.")
            else:
                logger.error("Configuration requires watchdog, but a safe watchdog timeout %s could"
                             " not be configured. Watchdog timeout is %s.", self.config.timeout, actual_timeout)
                # Otherwise the node would keep winning leader races only to give up the leader key again
                self._unsafe_timeout = True
            return False
        if too_short:
            # Like after a failed activation, it's acquired again only after the mode or device configuration changed
            if self._try_disable_impl():
                self.impl = NullWatchdog()
            else:
                logger.warning("Keeping %s because it could not be disabled", self.impl.describe())
        elif not self.impl.is_null:
            logger.warning("Watchdog timeout %s seconds does not ensure safe termination within %s seconds",
                           actual_timeout, self.config.timeout)
        return True

    def _is_safe_timeout(self, timeout: Optional[int]) -> bool:
        """The running implementation ensures safe termination within the configured timeout, and can be fed in time."""
        # A driver might also round the requested timeout down to zero, e.g. PC87413 counts in whole minutes
        return self.impl.is_running and timeout is not None and 0 < timeout <= self.config.timeout \
            and not self._is_too_short(timeout)

    def _is_too_short(self, timeout: int) -> bool:
        """The HA loop would have to run more often than every ``MIN_KEEPALIVE_INTERVAL`` seconds."""
        interval = self._keepalive_interval(timeout)
        return interval is not None and interval < MIN_KEEPALIVE_INTERVAL

    def _set_timeout(self) -> int:
        if self.impl.has_set_timeout():
            # The device might not accept the requested timeout as is, but returns the one it actually set
            self._timeout = self.impl.set_timeout(self.config.timeout)
            return self._timeout
        return self._get_timeout()

    def _get_timeout(self) -> int:
        self._timeout = self.impl.get_timeout()
        return self._timeout

    def _keepalive_interval(self, timeout: int) -> Optional[float]:
        """See keepalive_interval."""
        interval = max(timeout - self.config.retry_timeout, timeout / 2.0)
        # Running the HA loop more often doesn't help with a timeout of zero, see _check_timeout()
        return interval if 0 < interval < self.config.loop_wait else None

    def _check_keepalive_interval(self, actual_timeout: int) -> None:
        """A device might not accept a timeout that leaves the HA loop enough time, see keepalive_interval."""
        interval = self._keepalive_interval(actual_timeout)
        if interval is not None:
            logger.warning('Watchdog timeout of %s seconds does not leave retry_timeout of %s seconds after loop_wait'
                           ' of %s seconds, running the HA loop every %s seconds', actual_timeout,
                           self.config.retry_timeout, self.config.loop_wait, interval)

    @synchronized
    def disable(self) -> None:
        self._close_impl()
        self.active = False
        self._activated = False
        # Changes deferred to the next keepalive would be lost otherwise
        self._apply_inactive_config()

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
            self._apply_unsafe_timing()
            self.active_config = self.config
            return
        mode_changed = self.config.mode != self.active_config.mode
        needs_replacement = self._impl_pending or self._device_changed() or mode_changed and not self.impl.is_running

        if needs_replacement and self._replace_impl():
            self._activated = self._activate()
            return
        if self.impl.is_running:
            # Also a device that was retained because it could not be disabled
            self._activated = self._update_timeout()
        self.active_config = self.config

    def _update_timeout(self) -> bool:
        """Apply the configuration to the running implementation, see _activate()."""
        timeout_changed = self.config.timeout != self.active_config.timeout
        previous_timeout = self._timeout
        # Also if the configured timeout didn't change, a retained device might still have the one it was armed with
        actual_timeout = self._set_timeout()
        if timeout_changed or actual_timeout != previous_timeout:
            logger.info("%s updated with %s second timeout, timing slack %s seconds",
                        self.impl.describe(), actual_timeout, self.config.timing_slack)
        # Same checks as on activation, e.g. a device might not accept a timeout that covers the new loop_wait.
        if not self._check_timeout(actual_timeout):
            return False
        if self.impl.is_running:
            self._check_keepalive_interval(actual_timeout)
        return True

    def _apply_unsafe_timing(self) -> None:
        """No timeout is safe with the new timing, see _activate().

        The armed timer must be kept fed until it is confirmed as disabled, or in required mode until the HA loop
        demotes the primary. Closing the device with the magic character can't confirm that: if the driver doesn't
        support disabling the timer, it might have been loaded with ``nowayout`` without a way to detect it.
        """
        logger.warning('Watchdog not supported because leader TTL %s is less than 2x loop_wait %s',
                       self.config.ttl, self.config.loop_wait)
        if self._impl_outdated():
            self._impl_pending = True
        if self.config.mode == MODE_REQUIRED:
            logger.error("Configuration requires watchdog, but watchdog could not be configured.")
            self._activated = False
        elif self.impl.is_running and not self._try_disable_impl():
            logger.warning("Keeping %s because it could not be disabled", self.impl.describe())
        else:
            self.impl = NullWatchdog()
            # Reacquire the configured device once a configuration change fixes the timing.
            self._impl_pending = True

    @synchronized
    def keepalive(self) -> None:
        if not self.active:
            return
        try:
            self.impl.keepalive()
        except WatchdogError as e:
            logger.error("Error while sending keepalive: %s", e)
        # Even if the keepalive failed, e.g. because the device was removed and has to be replaced
        if self.config != self.active_config:
            try:
                self._apply_config_changes()
            except WatchdogError as e:
                logger.error("Error while applying watchdog configuration: %s", e)
                # It is retried with the next keepalive, until then the armed timeout might be safe enough
                self._activated = self.config.mode != MODE_REQUIRED or self._is_safe_timeout(self._timeout)

    @property
    @synchronized
    def is_running(self) -> bool:
        """The implementation is armed and has to be fed, even if it isn't safe to lead with, see is_activated."""
        return self.impl.is_running

    @property
    @synchronized
    def is_activated(self) -> bool:
        """A running watchdog was activated and is safe to lead with. Otherwise activate() has to be called, e.g. to
        apply a deferred configuration change or to open a device again after it was removed."""
        return self.active and self.impl.is_running and self._activated

    @property
    @synchronized
    def keepalive_interval(self) -> Optional[float]:
        """Seconds between HA loop runs needed to feed the watchdog in time, if that is shorter than ``loop_wait``.

        The HA loop has to complete before the watchdog expires, including a DCS request that takes up to
        ``retry_timeout``. A device might not accept a timeout that leaves that much time after ``loop_wait``. Or the
        timeout armed for the previous configuration stays in effect until the keepalive following the leader key
        update with the new TTL applies the new one. Also a watchdog that isn't safe to lead with has to be fed until it
        is disabled.

        :returns: the actual timeout minus ``retry_timeout``, but at least half of the timeout, if that is positive and
                  shorter than ``loop_wait``, otherwise ``None``.
        """
        if not self.active or not self.impl.is_running:
            return None
        try:
            # The actual timeout, because a device might not have accepted the configured one.
            timeout = self._get_timeout()
        except WatchdogError:
            if not self.impl.is_running or self._timeout is None:
                # E.g. the device was removed, then running the HA loop more often doesn't help
                return None
            # E.g. a transient I/O error, the descriptor is still open and the last known timeout still armed
            timeout = self._timeout
        return self._keepalive_interval(timeout)

    @property
    @synchronized
    def is_healthy(self) -> bool:
        if self.config.mode != MODE_REQUIRED:
            return True
        # The implementation the next activation would use, see activate()
        impl = self.config.get_impl() if self._impl_outdated() else self.impl
        return self.config.timing_slack >= 0 and not self._unsafe_timeout and impl.is_healthy


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

    def set_timeout(self, timeout: int) -> int:
        """Set the watchdog timer timeout.

        :param timeout: watchdog timeout in seconds

        :returns: the timeout accepted by the device, which might differ from the requested one."""
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
