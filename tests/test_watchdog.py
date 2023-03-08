import ctypes
import patroni.watchdog.linux as linuxwd
import sys
import unittest
import os

from mock import patch, Mock, PropertyMock
from patroni.watchdog import Watchdog, WatchdogError
from patroni.watchdog.base import NullWatchdog
from patroni.watchdog.linux import LinuxWatchdogDevice


class MockDevice(object):
    def __init__(self, fd, filename, flag):
        self.fd = fd
        self.filename = filename
        self.flag = flag
        self.timeout = 60
        self.open = True
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
        arg.identity = (ctypes.c_ubyte*32)(*map(ord, 'Mock Watchdog'))
    elif op == linuxwd.WDIOC_GETTIMEOUT:
        arg.value = dev.timeout
    elif op == linuxwd.WDIOC_SETTIMEOUT:
        sys.stderr.write("Set timeout called with %s\n" % arg.value)
        assert 0 < arg.value < 65535
        dev.timeout = arg.value - 1
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
    mock_devices[fd].open = False


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
    def test_unsafe_timeout_disable_watchdog_and_exit(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'required', 'safety_margin': -1}})
        self.assertEqual(watchdog.activate(), False)
        self.assertEqual(watchdog.is_running, False)

    @patch('platform.system', Mock(return_value='Linux'))
    @patch.object(LinuxWatchdogDevice, 'get_timeout', Mock(return_value=16))
    def test_timeout_does_not_ensure_safe_termination(self):
        Watchdog({'ttl': 30, 'loop_wait': 15, 'watchdog': {'mode': 'auto', 'safety_margin': -1}}).activate()
        self.assertEqual(len(mock_devices), 2)

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

        watchdog.disable()
        self.assertFalse(device.open)
        self.assertEqual(device.writes[-1], b'V')

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
