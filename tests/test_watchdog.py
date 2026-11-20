import ctypes
import errno
import os
import sys
import unittest

from unittest.mock import call, Mock, patch, PropertyMock

import patroni.watchdog.linux as linuxwd

from patroni.watchdog import Watchdog, WatchdogError
from patroni.watchdog.base import NullWatchdog
from patroni.watchdog.linux import LinuxWatchdogDevice


class MockDevice:
    def __init__(self, fd, filename, flag):
        self.fd = fd
        self.filename = filename
        self.flag = flag
        self.timeout = 60
        self.open = True
        self.armed = True
        self.nowayout = False
        self.writes = []


mock_devices = [None]


def mock_open(filename, flag):
    fd = len(mock_devices)
    mock_devices.append(MockDevice(fd, filename, flag))
    return fd


def mock_ioctl(fd, op, arg=None, mutate_flag=False):
    assert 0 < fd < len(mock_devices)
    dev = mock_devices[fd]
    sys.stderr.write("Ioctl %d %d %r\n" % (fd, op, arg))
    if op == linuxwd.WDIOC_GETSUPPORT:
        sys.stderr.write("Get support\n")
        assert (mutate_flag is True)
        arg.options = sum(map(linuxwd.WDIOF.get, ['SETTIMEOUT', 'KEEPALIVEPING']))
        arg.identity = (ctypes.c_ubyte * 32)(*map(ord, 'Mock Watchdog'))
    elif op == linuxwd.WDIOC_GETTIMEOUT:
        arg.value = dev.timeout
    elif op == linuxwd.WDIOC_SETTIMEOUT:
        sys.stderr.write("Set timeout called with %s\n" % arg.value)
        assert 0 < arg.value < 65535
        # Like Linux drivers, return the timeout that was set
        arg.value = dev.timeout = arg.value - 1
    elif op == linuxwd.WDIOC_SETOPTIONS:
        assert arg.value == linuxwd.WDIOS['DISABLECARD']
        if dev.nowayout:
            raise OSError(errno.EBUSY, 'nowayout')
        dev.armed = False
    else:
        raise Exception("Unknown op %d", op)
    return 0


def mock_write(fd, string):
    assert 0 < fd < len(mock_devices)
    assert len(string) == 1
    assert mock_devices[fd].open
    mock_devices[fd].writes.append(string)


def mock_close(fd):
    assert 0 < fd < len(mock_devices)
    assert mock_devices[fd].open
    if not mock_devices[fd].nowayout and mock_devices[fd].writes[-1:] == [b'V']:
        mock_devices[fd].armed = False
    mock_devices[fd].open = False


def mock_legacy_ioctl(fd, op, arg=None, mutate_flag=False):
    if op == linuxwd.WDIOC_SETOPTIONS:
        raise OSError(errno.ENOTTY, 'Inappropriate ioctl for device')
    return mock_ioctl(fd, op, arg, mutate_flag)


def mock_fixed_timeout_ioctl(fd, op, arg=None, mutate_flag=False):
    ret = mock_ioctl(fd, op, arg, mutate_flag)
    if op == linuxwd.WDIOC_GETSUPPORT:
        arg.options &= ~linuxwd.WDIOF['SETTIMEOUT']
    return ret


def mock_nowayout_ioctl(fd, *args):
    mock_devices[fd].nowayout = True
    return mock_ioctl(fd, *args)


def mock_nowayout_open(filename, flag):
    fd = mock_open(filename, flag)
    mock_devices[fd].nowayout = True
    return fd


def mock_rounding_ioctl(rounding):
    """Models drivers that set a timeout supported by the hardware instead of the requested one."""
    def ioctl(fd, op, arg=None, mutate_flag=False):
        if op == linuxwd.WDIOC_SETTIMEOUT:
            arg.value = mock_devices[fd].timeout = rounding(arg.value)
            return 0
        return mock_ioctl(fd, op, arg, mutate_flag)
    return ioctl


@unittest.skipIf(os.name == 'nt', "Windows not supported")
@patch('os.open', mock_open)
@patch('os.write', mock_write)
@patch('os.close', mock_close)
@patch('fcntl.ioctl', mock_ioctl)
class TestWatchdog(unittest.TestCase):
    def setUp(self):
        mock_devices[:] = [None]

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_timeout_shorter_than_loop_wait(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'safety_margin': -1}})
        try:
            with self.assertLogs('patroni.watchdog.base', 'WARNING'):
                self.assertTrue(watchdog.activate())
            # the device accepted only 14 seconds, it stays armed and the HA loop runs more often instead
            self.assertTrue(watchdog.is_activated)
            self.assertEqual(watchdog.keepalive_interval, 7)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(return_value=16))
    def test_timeout_does_not_ensure_safe_termination(self):
        Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'auto', 'safety_margin': -1}}).activate()
        self.assertEqual(len(mock_devices), 2)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=60))
    def test_required_timeout_too_long(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}}
        watchdog = Watchdog(config)
        self.assertFalse(watchdog.activate())
        # stays armed until the HA loop disables it, e.g. on the safepoint of the demotion
        self.assertTrue(watchdog.is_running)
        watchdog.disable()
        self.assertFalse(watchdog.is_healthy)
        watchdog.reload_config(config)
        self.assertTrue(watchdog.is_healthy)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_timeout_rounded_down_to_zero(self):
        # E.g. the PC87413 driver counts in whole minutes
        ioctl = mock_rounding_ioctl(lambda timeout: timeout // 60 * 60)
        for mode, expected in (('automatic', True), ('required', False)):
            with self.subTest(mode=mode), patch('fcntl.ioctl', ioctl):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode}})
                try:
                    self.assertEqual(watchdog.activate(), expected)
                    self.assertEqual(mock_devices[-1].timeout, 0)
                    self.assertEqual(watchdog.is_activated, expected)
                    self.assertEqual(watchdog.is_healthy, expected)
                    # Running the HA loop more often doesn't help
                    self.assertIsNone(watchdog.keepalive_interval)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(Watchdog, 'is_running', PropertyMock(return_value=False))
    def test_watchdog_not_activated(self):
        self.assertFalse(Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}}).activate())

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_running', PropertyMock(return_value=False))
    def test_watchdog_activate(self):
        with patch.object(LinuxWatchdogDevice, 'open', Mock(side_effect=WatchdogError(''))):
            self.assertTrue(Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'auto'}}).activate())
        self.assertFalse(Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}}).activate())

    @patch('platform.system', Mock(return_value='Linux'))
    def test_basic_operation(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        watchdog.activate()

        self.assertEqual(len(mock_devices), 2)
        device = mock_devices[-1]
        self.assertTrue(device.open)

        self.assertEqual(device.timeout, 24)

        watchdog.keepalive()
        self.assertEqual(len(device.writes), 1)

        watchdog.impl._fd, fd = None, watchdog.impl._fd
        watchdog.keepalive()
        self.assertEqual(len(device.writes), 1)
        watchdog.impl._fd = fd

        watchdog.disable()
        self.assertFalse(device.open)
        self.assertEqual(device.writes[-1], b'V')

    @patch('platform.system', Mock(return_value='Linux'))
    def test_activation_failure_closes_device(self):
        # get_timeout() is only used if the device doesn't support setting the timeout
        for method, ioctl in (('get_support', mock_ioctl), ('set_timeout', mock_ioctl),
                              ('get_timeout', mock_fixed_timeout_ioctl)):
            with self.subTest(method=method), patch('fcntl.ioctl', ioctl), \
                    patch.object(LinuxWatchdogDevice, method, Mock(side_effect=WatchdogError('failed'))):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'automatic'}})
                self.assertTrue(watchdog.activate())
                self.assertFalse(watchdog.is_running)
                # open() armed the device, it must be closed with the magic character
                self.assertFalse(mock_devices[-1].open)
                self.assertEqual(mock_devices[-1].writes[-1], b'V')

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(side_effect=WatchdogError('EIO')))
    def test_set_timeout_is_not_read_back(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        try:
            # The device returns the timeout it actually set
            self.assertTrue(watchdog.activate())
            self.assertTrue(watchdog.is_activated)
            self.assertEqual(mock_devices[-1].timeout, 24)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    def test_timeout_too_short(self):
        # The HA loop would have to run every half a second, but it doesn't run more often than every second
        too_short = mock_rounding_ioctl(lambda timeout: 1)
        for mode, nowayout, reload in (('required', False, False), ('required', False, True),
                                       ('automatic', False, False), ('automatic', False, True),
                                       ('automatic', True, False)):
            with self.subTest(mode=mode, nowayout=nowayout, reload=reload), \
                    patch('os.open', mock_nowayout_open if nowayout else mock_open):
                config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode}}
                watchdog = Watchdog(config)
                try:
                    if reload:
                        self.assertTrue(watchdog.activate())
                        watchdog.reload_config(dict(config, ttl=40))
                        with patch('fcntl.ioctl', too_short):
                            watchdog.keepalive()
                    else:
                        with patch('fcntl.ioctl', too_short):
                            self.assertEqual(watchdog.activate(), mode == 'automatic')
                    device = mock_devices[-1]
                    self.assertEqual(device.timeout, 1)
                    if mode == 'required':
                        self.assertFalse(watchdog.is_activated)
                        self.assertFalse(watchdog.is_healthy)
                        # it still has to be fed until it is disabled on demotion
                        self.assertTrue(device.armed)
                        self.assertEqual(watchdog.keepalive_interval, 0.5)
                    elif nowayout:
                        self.assertTrue(watchdog.is_activated)
                        self.assertTrue(device.armed)
                        self.assertEqual(watchdog.keepalive_interval, 0.5)
                    else:
                        self.assertFalse(device.open)
                        self.assertFalse(device.armed)
                        self.assertFalse(watchdog.is_running)
                        # it isn't acquired again on every activation
                        self.assertTrue(watchdog.activate())
                        self.assertIs(mock_devices[-1], device)
                finally:
                    watchdog.disable()

    def test_invalid_timings(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 20, 'watchdog': {'mode': 'automatic', 'safety_margin': -1}})
        watchdog.activate()
        self.assertEqual(len(mock_devices), 1)
        self.assertFalse(watchdog.is_running)

    def test_parse_mode(self):
        with patch('patroni.watchdog.base.logger.warning', new_callable=Mock()) as warning_mock:
            watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'bad'}})
            self.assertEqual(watchdog.config.mode, 'off')
            warning_mock.assert_called_once()

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_unsupported_platform(self):
        self.assertRaises(SystemExit, Watchdog, {'ttl': 30, 'loop_wait': 10,
                                                 'watchdog': {'mode': 'required', 'driver': 'bad'}})

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(return_value=5))
    def test_set_timeout_keeps_impl(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        impl = watchdog.impl
        impl.open()
        self.assertEqual(watchdog._set_timeout(), 5)
        self.assertIs(watchdog.impl, impl)
        self.assertTrue(mock_devices[-1].open)
        self.assertTrue(mock_devices[-1].armed)
        watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_timeout_change_without_device(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'automatic', 'driver': 'none'}}
        watchdog = Watchdog(config)
        self.assertTrue(watchdog.activate())
        watchdog.reload_config(dict(config, ttl=40))
        with patch('patroni.watchdog.base.logger.error') as mock_error:
            watchdog.keepalive()
        mock_error.assert_not_called()
        self.assertEqual(watchdog.active_config, watchdog.config)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    def test_timeout_change_with_fixed_timeout_device(self):
        for mode in ('automatic', 'required'):
            with self.subTest(mode=mode):
                config = {'ttl': 90, 'loop_wait': 10, 'watchdog': {'mode': mode}}
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    impl = watchdog.impl
                    with patch.object(impl, 'set_timeout',
                                      side_effect=WatchdogError('unsupported')) as mock_set_timeout:
                        watchdog.reload_config(dict(config, ttl=100))
                        with patch('patroni.watchdog.base.logger.error') as mock_error:
                            watchdog.keepalive()
                        mock_set_timeout.assert_not_called()
                        mock_error.assert_not_called()
                    self.assertIs(watchdog.impl, impl)
                    self.assertEqual(impl.get_timeout(), 60)
                    self.assertTrue(watchdog.is_running)
                    self.assertEqual(watchdog.active_config, watchdog.config)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(side_effect=WatchdogError('failed')))
    def test_activation_error_retains_device_when_disable_fails(self):
        for mode, expected in (('automatic', True), ('required', False)):
            with self.subTest(mode=mode), patch('fcntl.ioctl', mock_nowayout_ioctl):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode}})
                impl = watchdog.impl
                self.assertEqual(watchdog.activate(), expected)
                self.assertIs(watchdog.impl, impl)
                self.assertTrue(watchdog.active)
                self.assertTrue(mock_devices[-1].open)
                self.assertTrue(mock_devices[-1].armed)
                watchdog.keepalive()
                self.assertEqual(mock_devices[-1].writes[-1], b'1')

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    def test_mode_change_keeps_running_device(self):
        for initial_mode, mode, expected in (('required', 'automatic', True), ('automatic', 'required', False)):
            with self.subTest(mode=mode):
                watchdog = Watchdog({'ttl': 90, 'loop_wait': 10, 'watchdog': {'mode': initial_mode}})
                try:
                    self.assertTrue(watchdog.activate())
                    impl = watchdog.impl
                    device = mock_devices[-1]
                    # the fixed 60 second timeout is too long for the new ttl
                    watchdog.reload_config({'ttl': 60, 'loop_wait': 10, 'watchdog': {'mode': mode}})
                    with patch.object(LinuxWatchdogDevice, 'disable') as mock_disable:
                        watchdog.keepalive()
                        self.assertEqual(watchdog.is_activated, expected)
                        # the running device is neither opened again nor replaced
                        self.assertEqual(watchdog.activate(), expected)
                        mock_disable.assert_not_called()
                    self.assertIs(watchdog.impl, impl)
                    self.assertIs(mock_devices[-1], device)
                    self.assertTrue(device.open)
                    self.assertTrue(device.armed)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_failed_replacement_is_not_activated(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}}
        watchdog = Watchdog(config)
        try:
            self.assertTrue(watchdog.activate())
            self.assertTrue(watchdog.is_activated)
            device = mock_devices[-1]
            watchdog.reload_config(dict(config, watchdog={'mode': 'required', 'device': '/dev/other'}))
            # the new device rejects the timeout and can't be stopped, so it stays armed with its default timeout
            with patch('os.open', mock_nowayout_open), \
                    patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(side_effect=WatchdogError('rejected'))):
                watchdog.keepalive()
            self.assertFalse(device.open)
            new_device = mock_devices[-1]
            self.assertEqual(new_device.filename, '/dev/other')
            self.assertTrue(new_device.open)
            self.assertTrue(new_device.armed)
            # it isn't safe to lead with it, but it must be fed until the HA loop demotes the primary
            self.assertTrue(watchdog.is_running)
            self.assertFalse(watchdog.is_activated)
            self.assertFalse(watchdog.activate())
            self.assertIs(mock_devices[-1], new_device)
            watchdog.keepalive()
            self.assertEqual(new_device.writes[-1], b'1')
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_removed_device_is_replaced(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}}
        for leader in (True, False):
            with self.subTest(leader=leader):
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    watchdog.reload_config(dict(config, watchdog={'mode': 'required', 'device': '/dev/other'}))
                    with patch('os.write', Mock(side_effect=OSError(errno.ENODEV, 'No such device'))):
                        if leader:
                            # the failed keepalive doesn't prevent the replacement
                            watchdog.keepalive()
                        else:
                            watchdog.disable()
                    self.assertFalse(device.open)
                    if not leader:
                        self.assertFalse(watchdog.is_running)
                        self.assertTrue(watchdog.activate())
                    self.assertTrue(watchdog.is_activated)
                    self.assertEqual(mock_devices[-1].filename, '/dev/other')
                    self.assertTrue(mock_devices[-1].open)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_removed_device_is_activated_again(self):
        for mode, available in (('automatic', False), ('required', True), ('required', False)):
            with self.subTest(mode=mode, available=available):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode}})
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    with patch('os.write', Mock(side_effect=OSError(errno.ENODEV, 'No such device'))):
                        watchdog.keepalive()
                    self.assertFalse(device.open)
                    self.assertFalse(watchdog.is_activated)
                    with patch('os.open', mock_open if available else Mock(side_effect=OSError(errno.ENOENT, ''))):
                        self.assertEqual(watchdog.activate(), available or mode == 'automatic')
                    self.assertEqual(watchdog.is_running, available)
                    self.assertEqual(watchdog.is_activated, available)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_removed_device_is_replaced_at_the_same_path(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        try:
            with patch('fcntl.ioctl', mock_fixed_timeout_ioctl), \
                    patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=20)):
                self.assertTrue(watchdog.activate())
            with patch('os.write', Mock(side_effect=OSError(errno.ENODEV, 'No such device'))):
                watchdog.keepalive()
            # another device, that supports setting the timeout, appeared at the same path
            self.assertTrue(watchdog.activate())
            self.assertEqual(mock_devices[-1].timeout, 24)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_activate_replaces_device_left_open(self):
        for nowayout in (False, True):
            with self.subTest(nowayout=nowayout):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
                try:
                    with patch('os.open', mock_nowayout_open if nowayout else mock_open):
                        self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    with patch('os.write', Mock(side_effect=OSError(errno.EIO, 'write failed'))):
                        watchdog.disable()
                    # nobody fed it since then, it has to be disabled before it is opened again
                    self.assertTrue(watchdog.is_running)
                    self.assertFalse(watchdog.is_activated)
                    self.assertEqual(watchdog.activate(), not nowayout)
                    self.assertEqual(device.open, nowayout)
                    self.assertEqual(device.armed, nowayout)
                    self.assertEqual(mock_devices[-1] is device, nowayout)
                    self.assertTrue(mock_devices[-1].open)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    def test_failed_timeout_update(self):
        config = {'ttl': 30, 'loop_wait': 10}
        for mode, ttl in (('automatic', 20), ('required', 20), ('required', 40)):
            with self.subTest(mode=mode, ttl=ttl):
                config = dict(config, watchdog={'mode': mode})
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    watchdog.reload_config(dict(config, ttl=ttl))
                    with patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(side_effect=WatchdogError(''))):
                        watchdog.keepalive()
                    # the previous timeout ensures termination before the longer TTL expires, but not the shorter one
                    self.assertTrue(watchdog.is_running)
                    self.assertEqual(watchdog.is_activated, mode == 'automatic' or ttl > 30)
                    # the error might be transient, the node isn't excluded from leader races after the demotion
                    self.assertTrue(watchdog.is_healthy)
                    # the update is retried with the next keepalive
                    watchdog.keepalive()
                    self.assertTrue(watchdog.is_activated)
                    self.assertEqual(mock_devices[-1].timeout, ttl - 6)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_keepalive_applies_config_when_device_is_retained(self):
        with patch('fcntl.ioctl', mock_nowayout_ioctl):
            config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'automatic'}}
            watchdog = Watchdog(config)
            self.assertTrue(watchdog.activate())
            impl = watchdog.impl
            device = mock_devices[-1]

            # A mode change keeps a running device without trying to disable it
            config['watchdog']['mode'] = 'required'
            watchdog.reload_config(config)
            with patch.object(LinuxWatchdogDevice, 'disable') as mock_disable:
                watchdog.keepalive()
                mock_disable.assert_not_called()
            self.assertIs(watchdog.impl, impl)
            self.assertEqual(len(mock_devices), 2)

            # A failed device replacement is attempted only once
            config['watchdog']['device'] = '/dev/other'
            watchdog.reload_config(config)
            with patch.object(LinuxWatchdogDevice, 'disable', side_effect=WatchdogError('busy')) as mock_disable:
                watchdog.keepalive()
                watchdog.keepalive()
                mock_disable.assert_called_once()
            self.assertIs(watchdog.impl, impl)

            # Timeout changes are still applied to the retained device
            config['ttl'] = 20
            watchdog.reload_config(config)
            watchdog.keepalive()
            self.assertIs(watchdog.impl, impl)
            self.assertEqual(device.timeout, 14)
            self.assertTrue(device.open)
            self.assertTrue(device.armed)

            # The pending device change is applied on the next activation
            watchdog.disable()
            self.assertTrue(watchdog.activate())
            self.assertEqual(mock_devices[-1].filename, '/dev/other')
            self.assertTrue(mock_devices[-1].open)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_unsafe_timing_reload_with_device(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'safety_margin': 12}}
        for demoted in (False, True):
            with self.subTest(demoted=demoted):
                mock_devices[:] = [None]
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    # No timeout is safe with the new TTL, the device must not stay armed after the keepalive that
                    # follows the leader key update with the new TTL. Until then the old timeout protects it.
                    watchdog.reload_config(dict(config, ttl=20))
                    self.assertTrue(device.armed)
                    self.assertEqual(watchdog.keepalive_interval, 8.5)
                    watchdog.keepalive()
                    self.assertFalse(device.open)
                    self.assertFalse(device.armed)
                    self.assertFalse(watchdog.is_running)
                    # the device is acquired again once the timing is fixed
                    if demoted:
                        watchdog.disable()
                        watchdog.reload_config(config)
                        self.assertTrue(watchdog.activate())
                    else:
                        watchdog.reload_config(config)
                        watchdog.keepalive()
                    self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)
                    self.assertTrue(mock_devices[-1].armed)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_unsafe_timing_reload_keeps_device_that_cannot_be_disabled(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'safety_margin': 12}}
        # A legacy driver doesn't support disabling the timer, then it can't be detected whether it was loaded with
        # nowayout and closing the device with the magic character doesn't confirm that the timer stopped
        for ioctl, nowayout in ((mock_ioctl, True), (mock_legacy_ioctl, False), (mock_legacy_ioctl, True)):
            with self.subTest(ioctl=ioctl.__name__, nowayout=nowayout), patch('fcntl.ioctl', ioctl), \
                    patch('os.open', mock_nowayout_open if nowayout else mock_open):
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    watchdog.reload_config(dict(config, ttl=20))
                    watchdog.keepalive()
                    # closing it would leave the timer armed without anybody feeding it
                    self.assertTrue(device.open)
                    self.assertTrue(device.armed)
                    self.assertEqual(device.timeout, 17)
                    self.assertTrue(watchdog.is_activated)
                    writes = len(device.writes)
                    watchdog.keepalive()
                    self.assertEqual(device.writes[writes:], [b'1'])
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_retained_device_timeout_is_applied(self):
        config = {'ttl': 30, 'loop_wait': 10}
        with patch('os.open', mock_nowayout_open):
            watchdog = Watchdog(config)
            try:
                # Setting the timeout fails, e.g. with a transient error, and the timer can't be stopped anymore
                with patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(side_effect=WatchdogError('EIO'))):
                    self.assertTrue(watchdog.activate())
                device = mock_devices[-1]
                self.assertEqual(device.timeout, 60)
                # It is applied even if the configured timeout didn't change, otherwise the primary would be demoted
                watchdog.reload_config(dict(config, watchdog={'mode': 'required'}))
                watchdog.keepalive()
                self.assertEqual(device.timeout, 24)
                self.assertTrue(watchdog.is_activated)
            finally:
                watchdog.disable()

            watchdog = Watchdog(config)
            try:
                self.assertTrue(watchdog.activate())
                device = mock_devices[-1]
                # The device is left open because closing it failed, and the TTL is changed since then
                with patch('os.write', Mock(side_effect=OSError(errno.EIO, 'write failed'))):
                    watchdog.disable()
                watchdog.reload_config(dict(config, ttl=20))
                self.assertTrue(watchdog.activate())
                self.assertIs(mock_devices[-1], device)
                self.assertEqual(device.timeout, 14)
            finally:
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    def test_required_unsafe_timing_reload_demotes(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required', 'safety_margin': 12}}
        watchdog = Watchdog(config)
        try:
            self.assertTrue(watchdog.activate())
            device = mock_devices[-1]
            other = {'mode': 'required', 'safety_margin': 12, 'device': '/dev/other'}
            watchdog.reload_config(dict(config, ttl=20, watchdog=other))
            watchdog.keepalive()
            # The HA loop demotes the primary, possibly after a promotion that is still in progress. The armed device
            # protects the leader key until PostgreSQL reaches the safepoint of the demotion.
            self.assertFalse(watchdog.is_activated)
            self.assertFalse(watchdog.activate())
            self.assertIs(mock_devices[-1], device)
            self.assertTrue(device.armed)
            writes = len(device.writes)
            watchdog.keepalive()
            self.assertEqual(device.writes[writes:], [b'1'])
            self.assertFalse(watchdog.is_healthy)
            watchdog.disable()
            self.assertFalse(device.armed)
            # only the leader sends keepalives, so the new device is acquired on reload
            watchdog.reload_config(dict(config, watchdog=other))
            self.assertTrue(watchdog.is_healthy)
            self.assertTrue(watchdog.activate())
            self.assertEqual(mock_devices[-1].filename, '/dev/other')
            self.assertTrue(mock_devices[-1].armed)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(side_effect=WatchdogError('')))
    def test_keepalive_interval_get_timeout_error(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        watchdog.active = True
        with patch.object(LinuxWatchdogDevice, 'is_running', PropertyMock(return_value=True)):
            self.assertIsNone(watchdog.keepalive_interval)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_keepalive_interval_retained_on_get_timeout_error(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15})
        try:
            self.assertTrue(watchdog.activate())
            self.assertEqual(watchdog.keepalive_interval, 14)
            # The descriptor is still open, so the last known timeout is still armed
            with patch('fcntl.ioctl', Mock(side_effect=OSError(errno.EIO, 'Input/output error'))):
                self.assertEqual(watchdog.keepalive_interval, 14)
            self.assertTrue(watchdog.is_running)
            # The descriptor is released once the device is removed
            with patch('fcntl.ioctl', Mock(side_effect=OSError(errno.ENODEV, 'No such device'))):
                self.assertIsNone(watchdog.keepalive_interval)
            self.assertFalse(watchdog.is_running)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_keepalive_interval_retains_accepted_timeout(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        try:
            self.assertTrue(watchdog.activate())
            watchdog.reload_config({'ttl': 124, 'loop_wait': 80})
            # The device accepts only 60 of the requested 119 seconds, but reading the timeout back fails
            with patch('fcntl.ioctl', mock_rounding_ioctl(lambda timeout: min(timeout, 60))), \
                    patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(side_effect=WatchdogError('EIO'))):
                watchdog.keepalive()
                self.assertEqual(mock_devices[-1].timeout, 60)
                self.assertEqual(watchdog.keepalive_interval, 50)
        finally:
            watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    def test_keepalive_interval_leaves_retry_timeout(self):
        for timeout, loop_wait, retry_timeout, interval in ((15, 15, 3, 12), (15, 15, 10, 7.5), (13, 10, 3, None)):
            with self.subTest(timeout=timeout, loop_wait=loop_wait, retry_timeout=retry_timeout):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': loop_wait, 'retry_timeout': retry_timeout})
                try:
                    with patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=timeout)):
                        with patch('patroni.watchdog.base.logger.warning') as mock_warning:
                            self.assertTrue(watchdog.activate())
                        self.assertEqual(mock_warning.called, interval is not None)
                        self.assertEqual(watchdog.keepalive_interval, interval)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=15))
    def test_retry_timeout_change(self):
        config = {'ttl': 30, 'loop_wait': 10, 'retry_timeout': 3}
        watchdog = Watchdog(config)
        try:
            self.assertTrue(watchdog.activate())
            self.assertIsNone(watchdog.keepalive_interval)
            watchdog.reload_config(dict(config, retry_timeout=10))
            with self.assertLogs('patroni.watchdog.base', 'WARNING'):
                watchdog.keepalive()
            self.assertEqual(watchdog.keepalive_interval, 7.5)
        finally:
            watchdog.disable()

    def test_exceptions(self):
        wd = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'bad'}})
        wd.impl.close = wd.impl.keepalive = Mock(side_effect=WatchdogError(''))
        self.assertTrue(wd.activate())
        self.assertIsNone(wd.keepalive())
        self.assertIsNone(wd.disable())

    @patch('platform.system', Mock(return_value='Linux'))
    def test_config_reload(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        self.assertTrue(watchdog.activate())
        self.assertTrue(watchdog.is_running)

        watchdog.reload_config({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'off'}})
        self.assertFalse(watchdog.is_running)

        watchdog.reload_config({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        self.assertFalse(watchdog.is_running)
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)

        watchdog.disable()
        watchdog.reload_config({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'driver': 'unknown'}})
        self.assertFalse(watchdog.is_healthy)

        self.assertFalse(watchdog.activate())
        # the HA loop gives up the leader key, so the next activation picks up the reloaded configuration
        watchdog.reload_config({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        self.assertFalse(watchdog.is_running)
        watchdog.keepalive()
        self.assertFalse(watchdog.is_running)
        self.assertTrue(watchdog.activate())
        self.assertTrue(watchdog.is_running)

        watchdog.reload_config({'ttl': 60, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)
        self.assertEqual(watchdog.config.timeout, 60 - 5)

        watchdog.reload_config({'ttl': 60, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'safety_margin': -1}})
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)
        self.assertEqual(watchdog.config.timeout, 60 // 2)

        impl = watchdog.impl
        watchdog.reload_config({'ttl': 60, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'driver': 'unknown'}})
        self.assertIs(watchdog.impl, impl)
        watchdog.keepalive()
        self.assertFalse(impl.is_running)
        self.assertIsInstance(watchdog.impl, NullWatchdog)
        self.assertFalse(watchdog.is_running)
        self.assertIsNone(watchdog.keepalive_interval)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    def test_reacquire_after_unsafe_timing(self):
        config = {'ttl': 20, 'loop_wait': 10, 'watchdog': {'safety_margin': 12}}
        for mode in ('automatic', 'required'):
            with self.subTest(mode=mode):
                config['watchdog']['mode'] = mode
                watchdog = Watchdog(config)
                self.assertEqual(watchdog.activate(), mode == 'automatic')
                self.assertFalse(watchdog.is_running)
                self.assertEqual(watchdog.is_healthy, mode == 'automatic')
                # automatic mode keeps the leader key, required mode gives it up
                watchdog.reload_config(dict(config, ttl=30))
                if mode == 'automatic':
                    watchdog.keepalive()
                else:
                    self.assertTrue(watchdog.is_healthy)
                    self.assertTrue(watchdog.activate())
                self.assertTrue(watchdog.is_running)
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_mode_off_after_unsafe_timing(self):
        config = {'ttl': 20, 'loop_wait': 10, 'watchdog': {'mode': 'automatic', 'safety_margin': 12}}
        for leader in (True, False):
            with self.subTest(leader=leader):
                watchdog = Watchdog(config)
                self.assertTrue(watchdog.activate())
                if not leader:
                    watchdog.disable()
                # the device must not be acquired anymore once the watchdog is turned off
                watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off'}})
                self.assertTrue(watchdog.activate())
                self.assertFalse(watchdog.is_running)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_mode_off_closes_device_left_open(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        self.assertTrue(watchdog.activate())
        device = mock_devices[-1]
        with patch('os.write', Mock(side_effect=OSError(errno.EIO, 'write failed'))):
            watchdog.disable()
        self.assertTrue(device.armed)
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off'}})
        self.assertFalse(device.open)
        self.assertFalse(device.armed)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=True))
    def test_reacquire_after_mode_change(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off'}})
        self.assertTrue(watchdog.activate())
        watchdog.disable()
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        self.assertTrue(watchdog.is_healthy)
        self.assertTrue(watchdog.activate())
        self.assertTrue(watchdog.is_running)
        watchdog.disable()

        # the change was deferred to a keepalive, but the watchdog was disabled first
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off'}})
        self.assertTrue(watchdog.activate())
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        watchdog.disable()
        self.assertTrue(watchdog.activate())
        self.assertTrue(watchdog.is_running)
        watchdog.disable()

        # The HA loop doesn't disable a watchdog that isn't running when losing the leader key, so it stays active
        # and the change is deferred to a keepalive. It must not keep the node out of the leader race.
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off'}})
        self.assertTrue(watchdog.activate())
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        self.assertTrue(watchdog.is_healthy)
        with patch.object(LinuxWatchdogDevice, 'is_healthy', PropertyMock(return_value=False)):
            self.assertFalse(watchdog.is_healthy)
        with patch('os.open', Mock(side_effect=OSError(errno.EBUSY, 'Device or resource busy'))):
            self.assertFalse(watchdog.activate())
        # A failed activation is retried only after a configuration change
        self.assertFalse(watchdog.is_healthy)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_disable_applies_deferred_device_change(self):
        for close_error in (None, OSError(errno.EIO, 'write failed')):
            with self.subTest(close_error=close_error):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
                self.assertTrue(watchdog.activate())
                impl, device = watchdog.impl, mock_devices[-1]
                # deferred to the next keepalive, but the HA loop disables the watchdog first
                watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'device': '/dev/other'}})
                with patch('os.write', Mock(side_effect=close_error or mock_write)):
                    watchdog.disable()
                if close_error:
                    # the device is still open, so the next activation has to disable it before replacing it
                    self.assertIs(watchdog.impl, impl)
                    self.assertTrue(device.armed)
                else:
                    self.assertEqual(watchdog.impl.device, '/dev/other')
                self.assertTrue(watchdog.activate())
                self.assertFalse(device.open)
                self.assertFalse(device.armed)
                self.assertEqual(mock_devices[-1].filename, '/dev/other')
                self.assertTrue(watchdog.is_running)
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_activate_applies_deferred_device_change(self):
        for watchdog_config in ({'driver': 'none'}, {'device': '/dev/missing'}):
            with self.subTest(watchdog_config=watchdog_config):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': watchdog_config})
                with patch('os.open', Mock(side_effect=OSError(errno.ENOENT, 'No such file or directory'))):
                    self.assertTrue(watchdog.activate())
                self.assertFalse(watchdog.is_running)
                # The HA loop doesn't disable a watchdog that isn't running when losing the leader key, so it stays
                # active and the fixed configuration is deferred to a keepalive. Winning the leader race activates the
                # watchdog before the next keepalive.
                watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {}})
                self.assertTrue(watchdog.activate())
                self.assertEqual(mock_devices[-1].filename, '/dev/watchdog')
                self.assertTrue(watchdog.is_running)
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_timing_reload(self):
        watchdog = Watchdog({'ttl': 35, 'loop_wait': 10, 'watchdog': {'safety_margin': 10}})
        self.assertTrue(watchdog.activate())
        device = mock_devices[-1]
        self.assertIsNone(watchdog.keepalive_interval)
        # the new configuration is safe, but the armed timeout doesn't leave retry_timeout after the new loop_wait
        watchdog.reload_config({'ttl': 60, 'loop_wait': 25, 'watchdog': {'safety_margin': 10}})
        self.assertEqual(watchdog.keepalive_interval, 14)
        watchdog.keepalive()
        self.assertEqual(device.timeout, 49)
        self.assertIsNone(watchdog.keepalive_interval)

        # no timeout is safe with the new timing, the device must not stay armed
        watchdog.reload_config({'ttl': 30, 'loop_wait': 25, 'watchdog': {'safety_margin': 10}})
        watchdog.keepalive()
        self.assertFalse(device.open)
        self.assertEqual(device.writes[-1], b'V')
        self.assertFalse(watchdog.is_running)
        with patch('patroni.watchdog.base.logger.error') as mock_error:
            watchdog.reload_config({'ttl': 60, 'loop_wait': 25, 'watchdog': {'safety_margin': 10}})
            watchdog.keepalive()
        mock_error.assert_not_called()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=12))
    def test_timing_reload_with_fixed_timeout_device(self):
        for mode in ('automatic', 'required'):
            with self.subTest(mode=mode):
                config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode}}
                watchdog = Watchdog(config)
                try:
                    # The device doesn't accept a timeout that leaves retry_timeout after loop_wait, the HA loop runs
                    # every half of the timeout instead.
                    with self.assertLogs('patroni.watchdog.base', 'WARNING'):
                        self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    self.assertEqual(watchdog.keepalive_interval, 6)
                    # Neither does it accept one that covers the new loop_wait. Disabling it would leave the primary
                    # unprotected, or a timer that can't be stopped unfed. It stays armed and the HA loop keeps running
                    # more often instead.
                    watchdog.reload_config(dict(config, loop_wait=15))
                    self.assertEqual(watchdog.keepalive_interval, 6)
                    with patch.object(LinuxWatchdogDevice, 'disable') as mock_disable, \
                            self.assertLogs('patroni.watchdog.base', 'WARNING'):
                        watchdog.keepalive()
                    mock_disable.assert_not_called()
                    self.assertTrue(watchdog.is_activated)
                    self.assertEqual(watchdog.keepalive_interval, 6)
                    watchdog.reload_config(config)
                    watchdog.keepalive()
                    self.assertTrue(watchdog.is_activated)
                    self.assertEqual(watchdog.keepalive_interval, 6)
                    self.assertIs(mock_devices[-1], device)
                    self.assertTrue(device.open)
                    self.assertTrue(device.armed)
                finally:
                    watchdog.disable()


class TestNullWatchdog(unittest.TestCase):

    def test_basics(self):
        watchdog = NullWatchdog()
        self.assertTrue(watchdog.can_be_disabled)
        self.assertRaises(WatchdogError, watchdog.set_timeout, 1)
        self.assertEqual(watchdog.describe(), 'NullWatchdog')
        self.assertIsInstance(NullWatchdog.from_config({}), NullWatchdog)


@unittest.skipIf(os.name == 'nt', "Windows not supported")
class TestLinuxWatchdogDevice(unittest.TestCase):

    def setUp(self):
        self.impl = LinuxWatchdogDevice.from_config({})

    @patch('os.open', Mock(return_value=3))
    @patch('os.write', Mock(side_effect=OSError))
    @patch('fcntl.ioctl', Mock(return_value=0))
    def test_basics(self):
        self.impl.open()
        try:
            if self.impl.get_support().has_foo:
                self.assertFail()
        except Exception as e:
            self.assertTrue(isinstance(e, AttributeError))
        self.assertRaises(WatchdogError, self.impl.close)
        self.assertRaises(WatchdogError, self.impl.keepalive)
        self.assertRaises(WatchdogError, self.impl.set_timeout, -1)

    @patch('os.open', Mock(return_value=3))
    @patch('fcntl.ioctl', Mock(side_effect=OSError))
    def test__ioctl(self):
        self.assertRaises(WatchdogError, self.impl.get_support)
        self.impl.open()
        self.assertRaises(WatchdogError, self.impl.get_support)

    def test_is_healthy(self):
        self.assertFalse(self.impl.is_healthy)

    @patch('os.open', Mock(return_value=3))
    def test_disable(self):
        manager = Mock()
        with patch('os.write', manager.write), patch('fcntl.ioctl', manager.ioctl), patch('os.close', manager.close):
            self.impl.open()
            self.impl.disable()
        # Some legacy drivers restart a stopped timer on write
        self.assertEqual([name for name, _, _ in manager.mock_calls], ['write', 'ioctl', 'close'])
        manager.write.assert_called_once_with(3, b'V')
        self.assertFalse(self.impl.is_running)

    @patch('os.open', Mock(return_value=3))
    @patch('os.write')
    @patch('os.close')
    @patch('fcntl.ioctl')
    def test_disable_rejected(self, mock_ioctl, mock_close, mock_write):
        for error, message in ((errno.EBUSY, 'Could not disable'), (errno.EINVAL, 'Could not disable'),
                               (errno.ENOTTY, "doesn't support disabling")):
            with self.subTest(error=error):
                mock_write.reset_mock()
                mock_ioctl.side_effect = OSError(error, 'cannot disable')
                self.impl.open()
                with self.assertRaises(WatchdogError) as ctx:
                    self.impl.disable()
                self.assertIn(message, str(ctx.exception))
                self.assertTrue(self.impl.is_running)
                # the magic character is revoked, so the timer won't stop if Patroni exits
                self.assertEqual(mock_write.call_args_list, [call(3, b'V'), call(3, b'1')])
                mock_close.assert_not_called()

    @patch('os.open', Mock(return_value=3))
    @patch('os.write')
    @patch('os.close', side_effect=OSError(errno.EBADF, 'close failed'))
    @patch('fcntl.ioctl', Mock(side_effect=OSError(errno.ENODEV, 'No such device')))
    def test_disable_device_removed(self, mock_close, mock_write):
        self.impl.open()
        self.assertRaises(WatchdogError, self.impl.disable)
        self.assertFalse(self.impl.is_running)
        mock_write.assert_called_once_with(3, b'V')
        mock_close.assert_called_once_with(3)

    @patch('os.open', Mock(return_value=3))
    @patch('fcntl.ioctl')
    def test_disable_close_error(self, mock_ioctl):
        for write_error in (None, OSError(errno.EIO, 'write failed')):
            with self.subTest(write_error=write_error), patch('os.write', side_effect=write_error), \
                    patch('os.close', side_effect=OSError(errno.EIO, 'close failed')) as mock_close:
                mock_ioctl.reset_mock()
                self.impl.open()
                self.assertRaises(WatchdogError, self.impl.disable)
                # the timer might be still running if writing the magic character failed
                self.assertEqual(self.impl.is_running, bool(write_error))
                self.assertEqual(mock_ioctl.called, not write_error)
                self.assertEqual(mock_close.called, not write_error)

    @patch('os.open', Mock(return_value=3))
    @patch('os.write', Mock(side_effect=OSError(errno.ENODEV, 'No such device')))
    @patch('fcntl.ioctl', Mock(side_effect=OSError(errno.ENODEV, 'No such device')))
    def test_device_removed(self):
        for method in ('close', 'keepalive', 'get_timeout'):
            with self.subTest(method=method), patch('os.close') as mock_close:
                self.impl.open()
                self.assertRaises(WatchdogError, getattr(self.impl, method))
                self.assertFalse(self.impl.is_running)
                mock_close.assert_called_once_with(3)

    @patch('os.open', Mock(return_value=3))
    @patch('os.write', Mock())
    @patch('os.close', Mock(side_effect=OSError(errno.EIO, 'close failed')))
    def test_close_error(self):
        self.impl.open()
        self.assertRaises(WatchdogError, self.impl.close)
        # the descriptor is released anyway, and its number might be reused
        self.assertFalse(self.impl.is_running)

    @patch('os.open', Mock(return_value=3))
    @patch('fcntl.ioctl', Mock(side_effect=OSError))
    def test_error_handling(self):
        self.impl.open()
        self.assertRaises(WatchdogError, self.impl.get_timeout)
        self.assertRaises(WatchdogError, self.impl.set_timeout, 10)
        # We still try to output a reasonable string even if getting info errors
        self.assertEqual(self.impl.describe(), "Linux watchdog device")

    @patch('os.open', Mock(side_effect=OSError))
    def test_open(self):
        self.assertRaises(WatchdogError, self.impl.open)
