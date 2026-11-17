import ctypes
import errno
import os
import signal
import sys
import unittest

from unittest.mock import call, Mock, patch, PropertyMock

import patroni.watchdog.linux as linuxwd

from patroni.watchdog import Watchdog, WatchdogError
from patroni.watchdog.base import NullWatchdog, WatchdogConfig
from patroni.watchdog.linux import LinuxWatchdogDevice
from patroni.watchdog.thread import ThreadWatchdog


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
        dev.timeout = arg.value - 1
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


def mock_nowayout_ioctl(fd, *args):
    mock_devices[fd].nowayout = True
    return mock_ioctl(fd, *args)


@unittest.skipIf(os.name == 'nt', "Windows not supported")
@patch('os.open', mock_open)
@patch('os.write', mock_write)
@patch('os.close', mock_close)
@patch('fcntl.ioctl', mock_ioctl)
class TestWatchdog(unittest.TestCase):
    def setUp(self):
        mock_devices[:] = [None]
        thread_start = patch('patroni.watchdog.thread.Thread.start')
        thread_start.start()
        self.addCleanup(thread_start.stop)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_unsafe_timeout_disable_watchdog_and_exit(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'safety_margin': -1}})
        self.assertEqual(watchdog.activate(), False)
        self.assertEqual(watchdog.is_running, False)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=16))
    def test_timeout_does_not_ensure_safe_termination(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'auto', 'safety_margin': -1}})
        watchdog.activate()
        self.assertEqual(watchdog.impl.get_timeout(), 16)
        self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)
        self.assertEqual(len(mock_devices), 2)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(Watchdog, 'is_running', PropertyMock(return_value=False))
    def test_watchdog_not_activated(self):
        self.assertFalse(Watchdog({
            'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required', 'fallback': 'thread'}
        }).activate())

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'is_running', PropertyMock(return_value=False))
    def test_watchdog_activate(self):
        with patch.object(LinuxWatchdogDevice, 'open', Mock(side_effect=WatchdogError(''))):
            watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'auto'}})
            self.addCleanup(watchdog.disable)
            self.assertTrue(watchdog.activate())
            self.assertFalse(watchdog.is_running)
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

    def test_invalid_timings(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 20,
                            'watchdog': {'mode': 'automatic', 'safety_margin': -1, 'fallback': 'thread'}})
        watchdog.activate()
        self.assertEqual(len(mock_devices), 1)
        self.assertFalse(watchdog.is_running)

    def test_parse_mode(self):
        with patch('patroni.watchdog.base.logger.warning', new_callable=Mock()) as warning_mock:
            watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'bad'}})
            self.assertEqual(watchdog.config.mode, 'off')
            warning_mock.assert_called_once()

    def test_parse_fallback(self):
        default = WatchdogConfig({'ttl': 30, 'loop_wait': 10})
        self.assertEqual(default.fallback, 'none')
        for value in ('none', 'thread', 'THREAD', None, 'invalid', True):
            with self.subTest(value=value), patch('patroni.watchdog.base.logger.warning') as warning_mock:
                config = WatchdogConfig({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': value}})
                self.assertEqual(config.driver_config, {})
                if value in ('thread', 'THREAD'):
                    self.assertEqual(config.fallback, 'thread')
                    self.assertNotEqual(config, default)
                else:
                    self.assertEqual(config, default)
                if value in ('invalid', True):
                    warning_mock.assert_called_once()
                else:
                    warning_mock.assert_not_called()

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_unsupported_platform(self):
        self.assertRaises(SystemExit, Watchdog, {'ttl': 30, 'loop_wait': 10,
                                                 'watchdog': {'mode': 'required', 'driver': 'bad'}})

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_automatic_no_fallback(self):
        for config in ({}, {'watchdog': {'mode': 'automatic'}}, {'watchdog': {'fallback': 'none'}}):
            with self.subTest(config=config):
                watchdog = Watchdog(dict(ttl=30, loop_wait=10, **config))
                self.addCleanup(watchdog.disable)
                self.assertTrue(watchdog.activate())
                self.assertFalse(watchdog.is_running)
                self.assertFalse(watchdog._fallback.is_running)

    @patch('platform.system', Mock(return_value='Unknown'))
    @patch('patroni.watchdog.thread.Thread')
    def test_automatic_fallback(self, mock_thread):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.addCleanup(watchdog.disable)
        mock_thread.return_value.start.assert_called_once()
        self.assertFalse(watchdog.is_running)
        with patch('patroni.watchdog.base.logger.warning') as warning_mock:
            self.assertTrue(watchdog.activate())
            warning_mock.assert_called_once_with(
                "Watchdog device is unavailable or unusable, using thread watchdog fallback")
            warning_mock.reset_mock()
            watchdog.keepalive()
            warning_mock.assert_not_called()
        self.assertTrue(watchdog.is_running)
        self.assertIsInstance(watchdog.impl, ThreadWatchdog)
        self.assertEqual(watchdog.impl.get_timeout(), 25)
        watchdog.disable()
        self.assertTrue(watchdog.activate())
        mock_thread.assert_called_once()
        mock_thread.return_value.start.assert_called_once()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch('patroni.watchdog.thread.Thread')
    def test_thread_started_in_other_modes(self, mock_thread):
        for mode in ('off', 'required'):
            with self.subTest(mode=mode):
                mock_thread.reset_mock()
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10,
                                    'watchdog': {'mode': mode, 'fallback': 'thread'}})
                self.addCleanup(watchdog.disable)
                fallback = watchdog._fallback
                mock_thread.return_value.start.assert_called_once()
                self.assertFalse(fallback.is_running)
                self.assertTrue(watchdog.activate())
                self.assertFalse(fallback.is_running)
                watchdog.reload_config({'ttl': 60, 'loop_wait': 10,
                                        'watchdog': {'driver': 'unknown', 'fallback': 'thread'}})
                watchdog.keepalive()
                self.assertIs(watchdog.impl, fallback)
                self.assertEqual(watchdog.impl.get_timeout(), 55)
                mock_thread.assert_called_once()
                mock_thread.return_value.start.assert_called_once()
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_automatic_device_available(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.assertTrue(watchdog.activate())
        self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_automatic_device_failure(self):
        for operation in ('open', 'get_support', 'set_timeout'):
            with self.subTest(operation=operation), \
                    patch.object(LinuxWatchdogDevice, operation, side_effect=WatchdogError('failed')):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                self.addCleanup(watchdog.disable)
                self.assertTrue(watchdog.activate())
                self.assertIsInstance(watchdog.impl, ThreadWatchdog)
                if operation != 'open':
                    self.assertFalse(mock_devices[-1].open)
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    def test_automatic_unsafe_timeout(self):
        # the mock device does not support magic close, but an explicit disable stops it
        for timeout in (5, 60):
            with self.subTest(timeout=timeout), patch.object(LinuxWatchdogDevice, 'get_timeout', return_value=timeout):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                try:
                    self.assertTrue(watchdog.activate())
                    self.assertIsInstance(watchdog.impl, ThreadWatchdog)
                    self.assertEqual(watchdog.impl.get_timeout(), 25)
                    self.assertFalse(mock_devices[-1].open)
                    self.assertFalse(mock_devices[-1].armed)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=5))
    def test_set_timeout_keeps_impl(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        impl = watchdog.impl
        impl.open()
        self.assertEqual(watchdog._set_timeout(), 5)
        self.assertIs(watchdog.impl, impl)
        self.assertTrue(mock_devices[-1].open)
        self.assertTrue(mock_devices[-1].armed)
        watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=5))
    def test_unsafe_timeout_falls_back_to_magic_close(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'automatic'}})
        with patch('fcntl.ioctl', mock_legacy_ioctl), self.assertLogs('patroni.watchdog.base', 'WARNING'):
            self.assertTrue(watchdog.activate())
        self.assertIsInstance(watchdog.impl, NullWatchdog)
        self.assertFalse(mock_devices[-1].open)
        self.assertFalse(mock_devices[-1].armed)

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
        for mode, fallback in (('automatic', 'none'), ('automatic', 'thread'), ('required', 'none')):
            with self.subTest(mode=mode, fallback=fallback):
                config = {'ttl': 90, 'loop_wait': 10, 'watchdog': {'mode': mode, 'fallback': fallback}}
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
                    self.assertFalse(watchdog._fallback.is_running)
                    self.assertEqual(watchdog.active_config, watchdog.config)
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=25))
    def test_fallback_rechecks_timeout_on_reload(self):
        for ttl, fallback in ((40, 'thread'), (20, 'none')):
            with self.subTest(ttl=ttl, fallback=fallback):
                watchdog = Watchdog({'ttl': ttl, 'loop_wait': 5, 'watchdog': {'fallback': fallback}})
                self.assertTrue(watchdog.activate())
                device = mock_devices[-1]
                watchdog.reload_config({'ttl': 20, 'loop_wait': 5, 'watchdog': {'fallback': 'thread'}})
                watchdog.keepalive()
                self.assertIsInstance(watchdog.impl, ThreadWatchdog)
                self.assertEqual(watchdog.impl.get_timeout(), 15)
                self.assertFalse(device.open)
                self.assertFalse(device.armed)
                self.assertEqual(device.writes[-1], b'V')
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_fallback_retains_nowayout_device(self):
        # a timeout shorter than loop_wait is covered by test_unsafe_timeout_falls_back_to_magic_close
        for timeout in (25, 60):
            with self.subTest(timeout=timeout), \
                    patch.object(LinuxWatchdogDevice, 'get_timeout', return_value=timeout), \
                    patch('fcntl.ioctl', mock_nowayout_ioctl):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                self.assertTrue(watchdog.activate())
                device = mock_devices[-1]
                self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)
                self.assertTrue(device.open)
                self.assertTrue(device.armed)
                self.assertFalse(watchdog._fallback.is_running)
                watchdog.keepalive()
                self.assertEqual(device.writes[-1], b'1')
                impl = watchdog.impl
                watchdog.reload_config({
                    'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread', 'device': '/dev/other'}
                })
                watchdog.keepalive()
                self.assertIs(watchdog.impl, impl)
                self.assertTrue(device.open)
                self.assertTrue(device.armed)
                self.assertFalse(watchdog._fallback.is_running)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    @patch.object(LinuxWatchdogDevice, 'set_timeout', Mock(side_effect=WatchdogError('failed')))
    def test_activation_error_retains_device_when_disable_fails(self):
        for mode, expected in (('automatic', True), ('required', False)):
            with self.subTest(mode=mode), patch('fcntl.ioctl', mock_nowayout_ioctl):
                watchdog = Watchdog({
                    'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode, 'fallback': 'thread'}
                })
                impl = watchdog.impl
                self.assertEqual(watchdog.activate(), expected)
                self.assertIs(watchdog.impl, impl)
                self.assertTrue(watchdog.active)
                self.assertTrue(mock_devices[-1].open)
                self.assertTrue(mock_devices[-1].armed)
                self.assertFalse(watchdog._fallback.is_running)
                watchdog.keepalive()
                self.assertEqual(mock_devices[-1].writes[-1], b'1')

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_mode_change_retains_device_when_disable_fails(self):
        for initial_mode, mode, expected in (('required', 'automatic', True), ('automatic', 'required', False)):
            with self.subTest(mode=mode), patch('fcntl.ioctl', mock_nowayout_ioctl):
                watchdog = Watchdog({
                    'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': initial_mode, 'fallback': 'thread'}
                })
                self.assertTrue(watchdog.activate())
                impl = watchdog.impl
                device = mock_devices[-1]
                watchdog.reload_config({
                    'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': mode, 'fallback': 'thread'}
                })
                self.assertEqual(watchdog.activate(), expected)
                self.assertIs(watchdog.impl, impl)
                self.assertTrue(watchdog.active)
                self.assertTrue(device.open)
                self.assertTrue(device.armed)
                self.assertFalse(watchdog._fallback.is_running)
                writes = len(device.writes)
                watchdog.keepalive()
                self.assertEqual(device.writes[writes:], [b'1'])

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_keepalive_applies_config_when_device_is_retained(self):
        with patch('fcntl.ioctl', mock_nowayout_ioctl):
            config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'automatic', 'fallback': 'thread'}}
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
    @patch.object(LinuxWatchdogDevice, 'keepalive', Mock(side_effect=WatchdogError('failed')))
    def test_automatic_keepalive_failure(self):
        on_timeout = Mock()
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}}, on_timeout)
        self.assertTrue(watchdog.activate())
        with patch.object(LinuxWatchdogDevice, 'disable', Mock(side_effect=WatchdogError('failed'))):
            watchdog.keepalive()
        self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)
        self.assertTrue(mock_devices[-1].open)
        watchdog.keepalive()
        self.assertIsInstance(watchdog.impl, ThreadWatchdog)
        self.assertIs(watchdog.impl._on_timeout, on_timeout)
        self.assertFalse(mock_devices[-1].open)
        self.assertFalse(mock_devices[-1].armed)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_keepalive_failure_without_fallback(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        self.assertTrue(watchdog.activate())
        device = watchdog.impl
        with patch.object(LinuxWatchdogDevice, 'keepalive', side_effect=WatchdogError('failed')):
            watchdog.keepalive()
            self.assertIs(watchdog.impl, device)
            self.assertTrue(mock_devices[-1].open)
            self.assertFalse(watchdog._fallback.is_running)
            watchdog.disable()
        self.assertFalse(mock_devices[-1].open)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_keepalive_failure_device_removed(self):
        def unsupported_ioctl(fd, op, *args):
            if op == linuxwd.WDIOC_GETSUPPORT:
                raise OSError(errno.ENOTTY, 'Inappropriate ioctl for device')
            return mock_legacy_ioctl(fd, op, *args)

        # a device that can be neither configured nor disabled is kept without a cached support query result
        for ioctl in (mock_ioctl, unsupported_ioctl):
            with self.subTest(ioctl=ioctl.__name__):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                with patch('fcntl.ioctl', ioctl):
                    self.assertTrue(watchdog.activate())
                self.assertIsInstance(watchdog.impl, LinuxWatchdogDevice)
                removed = OSError(errno.ENODEV, 'No such device')
                with patch('os.write', side_effect=removed), patch('fcntl.ioctl', side_effect=removed):
                    watchdog.keepalive()
                self.assertIsInstance(watchdog.impl, ThreadWatchdog)
                self.assertTrue(watchdog.is_running)
                self.assertFalse(mock_devices[-1].open)
                watchdog.disable()

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_fallback_reload(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        self.addCleanup(watchdog.disable)
        fallback = watchdog._fallback
        self.assertTrue(watchdog.activate())
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.assertFalse(watchdog.is_running)
        watchdog.keepalive()
        self.assertIs(watchdog.impl, fallback)
        self.assertTrue(watchdog.is_running)
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'none'}})
        self.assertFalse(watchdog.is_running)
        self.assertFalse(fallback.is_running)
        watchdog.keepalive()
        self.assertFalse(watchdog.is_running)
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.assertTrue(watchdog.activate())
        self.assertIs(watchdog.impl, fallback)
        watchdog.disable()
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10})
        self.assertTrue(watchdog.activate())
        self.assertFalse(watchdog.is_running)
        self.assertFalse(fallback.is_running)

    @patch('platform.system', Mock(return_value='Linux'))
    def test_fallback_reload_with_device(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        self.assertTrue(watchdog.activate())
        device = watchdog.impl
        for fallback in ('thread', 'none'):
            watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': fallback}})
            watchdog.keepalive()
            self.assertIs(watchdog.impl, device)
            self.assertTrue(mock_devices[-1].open)
            self.assertEqual(len(mock_devices), 2)
            self.assertFalse(watchdog._fallback.is_running)

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_fallback_unsafe_timeout_reload(self):
        for config in ({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread', 'safety_margin': 25}},
                       {'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread', 'safety_margin': 30}},
                       {'ttl': 30, 'loop_wait': 26, 'watchdog': {'fallback': 'thread'}}):
            with self.subTest(config=config):
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                self.assertTrue(watchdog.activate())
                fallback = watchdog.impl
                self.assertIsInstance(fallback, ThreadWatchdog)
                watchdog.reload_config(config)
                # the next HA loop sleep uses the new loop_wait, so waiting for keepalive would be too late
                self.assertFalse(fallback.is_running)
                watchdog.keepalive()
                self.assertFalse(fallback.is_running)
                self.assertFalse(watchdog.is_running)
                watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                self.assertTrue(watchdog.activate())
                self.assertIs(watchdog.impl, fallback)
                self.assertEqual(fallback.get_timeout(), 25)

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_fallback_longer_loop_wait_reload(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread', 'safety_margin': 10}})
        self.addCleanup(watchdog.disable)
        self.assertTrue(watchdog.activate())
        self.assertFalse(watchdog.needs_immediate_keepalive)
        # the new configuration is safe, but the next HA loop sleep is longer than the armed 20 second timeout
        watchdog.reload_config({'ttl': 60, 'loop_wait': 25, 'watchdog': {'fallback': 'thread', 'safety_margin': 10}})
        self.assertTrue(watchdog.is_running)
        self.assertEqual(watchdog.impl.get_timeout(), 20)
        self.assertTrue(watchdog.needs_immediate_keepalive)
        watchdog.keepalive()
        self.assertEqual(watchdog.impl.get_timeout(), 50)
        self.assertFalse(watchdog.needs_immediate_keepalive)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'can_be_disabled', PropertyMock(return_value=True))
    def test_unsafe_timing_reload_with_device(self):
        config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'safety_margin': 12}}
        for fallback, demoted in (('none', False), ('thread', False), ('none', True)):
            with self.subTest(fallback=fallback, demoted=demoted):
                mock_devices[:] = [None]
                config['watchdog']['fallback'] = fallback
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    # 8 second timeout is shorter than loop_wait, the device must not stay armed
                    watchdog.reload_config(dict(config, ttl=20))
                    self.assertFalse(device.open)
                    self.assertFalse(device.armed)
                    watchdog.keepalive()
                    self.assertFalse(watchdog.is_running)
                    self.assertFalse(watchdog._fallback.is_running)
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
    @patch.object(LinuxWatchdogDevice, 'has_set_timeout', Mock(return_value=False))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=15))
    def test_longer_loop_wait_reload_with_fixed_timeout_device(self):
        for fallback in ('none', 'thread'):
            with self.subTest(fallback=fallback):
                config = {'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': fallback}}
                watchdog = Watchdog(config)
                try:
                    self.assertTrue(watchdog.activate())
                    device = mock_devices[-1]
                    watchdog.reload_config(dict(config, loop_wait=18))
                    self.assertTrue(watchdog.needs_immediate_keepalive)
                    with self.assertLogs('patroni.watchdog.base', 'ERROR'):
                        watchdog.keepalive()
                    self.assertFalse(device.armed)
                    self.assertEqual(watchdog.is_running, fallback == 'thread')
                    self.assertEqual(watchdog._fallback.is_running, fallback == 'thread')
                finally:
                    watchdog.disable()

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(side_effect=WatchdogError('')))
    def test_needs_immediate_keepalive_get_timeout_error(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10})
        watchdog.active = True
        with patch.object(LinuxWatchdogDevice, 'is_running', PropertyMock(return_value=True)):
            self.assertTrue(watchdog.needs_immediate_keepalive)

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_automatic_config_reload(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.addCleanup(watchdog.disable)
        self.assertTrue(watchdog.activate())
        watchdog.reload_config({'ttl': 60, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        self.assertEqual(watchdog.impl.get_timeout(), 25)
        watchdog.keepalive()
        self.assertEqual(watchdog.impl.get_timeout(), 55)

        old_thread = watchdog.impl._thread
        watchdog.reload_config({
            'ttl': 60, 'loop_wait': 10, 'watchdog': {'device': '/dev/other', 'fallback': 'thread'}
        })
        watchdog.keepalive()
        self.assertIs(watchdog.impl._thread, old_thread)
        self.assertTrue(watchdog.is_running)

        watchdog.reload_config({
            'ttl': 60, 'loop_wait': 10, 'watchdog': {'mode': 'required', 'fallback': 'thread'}
        })
        self.assertFalse(watchdog.is_healthy)
        self.assertFalse(watchdog.activate())
        watchdog.keepalive()
        self.assertFalse(watchdog.is_running)
        self.assertFalse(watchdog.is_healthy)
        self.assertFalse(watchdog.activate())

        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        watchdog.keepalive()
        self.assertIsInstance(watchdog.impl, ThreadWatchdog)
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'off', 'fallback': 'thread'}})
        self.assertFalse(watchdog.is_running)
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)
        self.assertIs(watchdog.impl._thread, old_thread)

        watchdog.disable()
        watchdog.reload_config({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})
        self.assertFalse(watchdog.activate())

    @patch('platform.system', Mock(return_value='Unknown'))
    @patch('patroni.watchdog.thread.Thread.start', side_effect=RuntimeError('no threads'))
    def test_automatic_thread_start_failure(self, mock_start):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
        mock_start.assert_called_once()
        self.assertTrue(watchdog.activate())
        watchdog.disable()
        self.assertTrue(watchdog.activate())
        self.assertFalse(watchdog.is_running)
        mock_start.assert_called_once()

    @patch('platform.system', Mock(return_value='Unknown'))
    def test_automatic_fallback_activation_failure(self):
        for operation in ('set_timeout', 'open'):
            error = WatchdogError('failed')
            with self.subTest(operation=operation), \
                    patch.object(ThreadWatchdog, operation, side_effect=error) as mock_operation, \
                    patch('patroni.watchdog.base.logger.warning') as mock_warning:
                watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'fallback': 'thread'}})
                self.addCleanup(watchdog.disable)
                self.assertTrue(watchdog.activate())
                mock_operation.assert_called_once()
                mock_warning.assert_called_once_with('Could not activate %s: %s', 'ThreadWatchdog', error)
                self.assertIsInstance(watchdog.impl, NullWatchdog)
                self.assertFalse(watchdog.is_running)
                self.assertFalse(watchdog._fallback.is_running)

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
        watchdog.reload_config({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        self.assertFalse(watchdog.is_running)
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)

        watchdog.reload_config({'ttl': 60, 'loop_wait': 15, 'watchdog': {'mode': 'required'}})
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)
        self.assertEqual(watchdog.config.timeout, 60 - 5)

        watchdog.reload_config({'ttl': 60, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'safety_margin': -1}})
        watchdog.keepalive()
        self.assertTrue(watchdog.is_running)
        self.assertEqual(watchdog.config.timeout, 60 // 2)


class TestThreadWatchdog(unittest.TestCase):

    def setUp(self):
        self.clock = patch('patroni.watchdog.thread.time.monotonic', return_value=100).start()
        self.thread = patch('patroni.watchdog.thread.Thread').start()
        self.exit = patch('patroni.watchdog.thread.os._exit').start()
        self.guard = patch('patroni.watchdog.thread.faulthandler.dump_traceback_later').start()
        self.alarm = patch('patroni.watchdog.thread.signal.alarm', create=True).start()
        self.sys = patch('patroni.watchdog.thread.sys').start()
        self.sys.stderr.fileno.return_value = 2
        self.addCleanup(patch.stopall)
        self.on_timeout = Mock()
        self.watchdog = ThreadWatchdog(25, self.on_timeout)
        self.addCleanup(self.watchdog.close)

    def test_lifecycle(self):
        self.assertTrue(self.watchdog.is_healthy)
        self.assertTrue(self.watchdog.can_be_disabled)
        self.assertTrue(self.watchdog.has_set_timeout())
        self.assertFalse(self.watchdog.is_running)
        self.thread.assert_called_once_with(target=self.watchdog._run, name='watchdog', daemon=True)
        self.thread.return_value.start.assert_called_once()
        self.watchdog.open()
        self.watchdog.open()
        self.thread.assert_called_once_with(target=self.watchdog._run, name='watchdog', daemon=True)
        self.thread.return_value.start.assert_called_once()
        self.assertTrue(self.watchdog.is_running)
        self.assertEqual(self.watchdog._deadline, 125)
        self.clock.return_value = 110
        self.watchdog.keepalive()
        self.assertEqual(self.watchdog._deadline, 135)
        self.watchdog.close()
        self.thread.return_value.join.assert_not_called()
        self.assertFalse(self.watchdog.is_running)
        self.watchdog.keepalive()
        with patch.object(self.watchdog._condition, 'wait', side_effect=RuntimeError('idle')) as mock_wait:
            self.assertRaises(RuntimeError, self.watchdog._run)
        mock_wait.assert_called_once_with()
        self.on_timeout.assert_not_called()
        self.exit.assert_not_called()
        self.clock.return_value = 200
        self.watchdog.open()
        self.assertEqual(self.watchdog._deadline, 225)
        self.thread.assert_called_once()
        self.thread.return_value.start.assert_called_once()

    def test_waits_when_disarmed(self):
        def expire():
            self.clock.return_value = 125

        wakeups = iter((self.watchdog.open, self.watchdog.close, self.watchdog.open, expire))

        def wakeup(timeout=None):
            next(wakeups)()

        with patch.object(self.watchdog._condition, 'wait', side_effect=wakeup) as mock_wait, \
                patch('patroni.watchdog.thread.time.sleep', side_effect=wakeup) as mock_sleep:
            self.watchdog._run()
        self.assertEqual(mock_wait.call_args_list, [call(), call()])
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(1)])
        self.on_timeout.assert_called_once()
        self.exit.assert_called_once_with(1)

    def test_timeout(self):
        actions = Mock()
        actions.attach_mock(self.guard, 'guard')
        actions.attach_mock(self.alarm, 'alarm')
        actions.attach_mock(self.on_timeout, 'shutdown')
        actions.attach_mock(self.exit, 'exit')
        self.watchdog.open()
        self.clock.side_effect = [110, 124.5, 125]
        with patch('patroni.watchdog.thread.time.sleep') as mock_sleep:
            self.watchdog._run()
        self.assertEqual(mock_sleep.call_args_list, [call(1), call(0.5)])
        # the alarm can't block, so it is armed first; a file descriptor stops faulthandler from flushing sys.stderr
        expected = ([call.alarm(10)] if os.name != 'nt' else []) + \
            [call.guard(5, exit=True, file=2), call.shutdown(), call.exit(1)]
        self.assertEqual(actions.mock_calls, expected)
        self.assertFalse(self.watchdog.is_running)

    def test_late_keepalive(self):
        self.watchdog.open()
        self.clock.return_value = 125
        self.watchdog.keepalive()
        self.watchdog.set_timeout(100)
        self.assertEqual(self.watchdog._deadline, 125)
        self.watchdog._run()
        self.on_timeout.assert_called_once()
        self.exit.assert_called_once_with(1)

    def test_set_timeout(self):
        self.watchdog.open()
        self.clock.return_value = 110
        self.watchdog.keepalive()
        self.clock.return_value = 115
        self.watchdog.set_timeout(40)
        self.assertEqual(self.watchdog.get_timeout(), 40)
        self.assertEqual(self.watchdog._deadline, 150)
        self.watchdog.set_timeout(5)
        self.watchdog._run()
        self.on_timeout.assert_called_once()
        self.exit.assert_called_once_with(1)
        self.watchdog.close()
        self.watchdog.set_timeout(30)
        self.assertFalse(self.watchdog.is_running)

    def test_shutdown_error(self):
        self.watchdog.open()
        self.clock.return_value = 125
        self.sys.stderr = None
        self.on_timeout.side_effect = RuntimeError('shutdown failed')
        with self.assertLogs('patroni.watchdog.thread', 'WARNING') as logs:
            self.watchdog._run()
        self.assertEqual([r.levelname for r in logs.records], ['WARNING', 'ERROR'])
        self.guard.assert_not_called()
        if os.name != 'nt':
            self.alarm.assert_called_once_with(10)
        self.on_timeout.assert_called_once()
        self.exit.assert_called_once_with(1)

    def test_no_callback(self):
        self.watchdog._on_timeout = None
        self.watchdog.open()
        self.clock.return_value = 125
        self.watchdog._run()
        self.exit.assert_called_once_with(1)

    def test_wait(self):
        with patch.object(self.watchdog._condition, 'wait', side_effect=self.watchdog.close) as mock_wait:
            self.watchdog.wait()
            mock_wait.assert_not_called()
            self.watchdog.open()
            with self.assertLogs('patroni.watchdog.thread', 'WARNING'):
                self.watchdog.wait()
            mock_wait.assert_called_once_with()
            # the timeout handler is still running once the deadline is cleared
            self.watchdog.open()
            self.clock.return_value = 125
            self.watchdog._run()
            mock_wait.side_effect = RuntimeError('still blocked')
            self.assertRaises(RuntimeError, self.watchdog.wait)

    def test_wait_ignores_interrupt(self):
        def wait():
            if mock_wait.call_count == 1:
                raise KeyboardInterrupt
            self.watchdog.close()

        self.watchdog.open()
        with patch.object(self.watchdog._condition, 'wait', side_effect=wait) as mock_wait, \
                self.assertLogs('patroni.watchdog.thread', 'WARNING') as logs:
            self.watchdog.wait()
        self.assertEqual(mock_wait.call_count, 2)
        self.assertIn('Ignoring interrupt', logs.output[-1])

    @unittest.skipIf(os.name == 'nt', 'SIGALRM is not available on Windows')
    @patch('patroni.watchdog.thread.signal.signal')
    @patch('patroni.watchdog.thread.signal.getsignal')
    def test_reset_ignored_alarm(self, mock_getsignal, mock_signal):
        mock_getsignal.return_value = signal.SIG_IGN
        ThreadWatchdog(25)
        for handler in (signal.SIG_DFL, Mock()):
            mock_getsignal.return_value = handler
            self.watchdog.open()
            self.watchdog.close()
        mock_getsignal.return_value = signal.SIG_IGN
        with patch('patroni.watchdog.thread.current_thread', Mock()):
            self.watchdog.open()
        self.watchdog.close()
        mock_signal.assert_not_called()
        self.watchdog.open()
        mock_signal.assert_called_once_with(signal.SIGALRM, signal.SIG_DFL)

    def test_start_failure(self):
        self.thread.return_value.start.side_effect = RuntimeError('no threads')
        self.assertRaises(WatchdogError, ThreadWatchdog, 25)
        self.assertFalse(self.watchdog.is_running)


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
    @patch('os.write')
    @patch('os.close')
    @patch('fcntl.ioctl')
    def test_disable_rejected(self, mock_ioctl, mock_close, mock_write):
        for error in (errno.EBUSY, errno.ENOTTY):
            with self.subTest(error=error):
                mock_ioctl.side_effect = OSError(error, 'cannot disable')
                self.impl.open()
                self.assertRaises(WatchdogError, self.impl.disable)
                self.assertTrue(self.impl.is_running)
                mock_write.assert_not_called()
                mock_close.assert_not_called()

    @patch('os.open', Mock(return_value=3))
    @patch('os.write')
    @patch('os.close', side_effect=OSError(errno.EBADF, 'close failed'))
    @patch('fcntl.ioctl', Mock(side_effect=OSError(errno.ENODEV, 'No such device')))
    def test_disable_device_removed(self, mock_close, mock_write):
        self.impl.open()
        self.assertRaises(WatchdogError, self.impl.disable)
        self.assertFalse(self.impl.is_running)
        mock_write.assert_not_called()
        mock_close.assert_called_once_with(3)

    @patch('os.open', Mock(return_value=3))
    @patch('fcntl.ioctl', Mock(return_value=0))
    def test_disable_close_error(self):
        for write_error in (None, OSError(errno.EIO, 'write failed')):
            with self.subTest(write_error=write_error), patch('os.write', side_effect=write_error) as mock_write, \
                    patch('os.close', side_effect=OSError(errno.EIO, 'close failed')) as mock_close:
                self.impl.open()
                self.assertRaises(WatchdogError, self.impl.disable)
                self.assertFalse(self.impl.is_running)
                mock_write.assert_called_once_with(3, b'V')
                mock_close.assert_called_once_with(3)

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
