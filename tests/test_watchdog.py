import ctypes
import patroni.watchdog.linux as linuxwd
import platform
import sys
import unittest

from mock import patch
from patroni.watchdog import Watchdog


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
        assert(mutate_flag is True)
        arg.options = sum(map(linuxwd.WDIOF.get, ['SETTIMEOUT', 'KEEPALIVEPING', 'MAGICCLOSE']))
        arg.identity = (ctypes.c_ubyte*32)(*map(ord, 'Mock Watchdog'))
    elif op == linuxwd.WDIOC_GETTIMEOUT:
        arg.value = dev.timeout
    elif op == linuxwd.WDIOC_SETTIMEOUT:
        sys.stderr.write("Set timeout called with %s\n" % arg.value)
        assert 0 < arg.value < 65535
        dev.timeout = arg.value
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


@patch('os.open', mock_open)
@patch('os.write', mock_write)
@patch('os.close', mock_close)
@patch('fcntl.ioctl', mock_ioctl)
class TestWatchdog(unittest.TestCase):
    def setUp(self):
        mock_devices[:] = [None]

    def test_basic_operation(self):
        if platform.system() != 'Linux':
            return

        watchdog = Watchdog({'ttl': 30, 'loop_wait': 10, 'watchdog': {'mode': 'required'}})

        watchdog.activate()
        self.assertEquals(len(mock_devices), 2)
        device = mock_devices[-1]
        self.assertTrue(device.open)

        self.assertEquals(device.timeout, 15)

        watchdog.keepalive()
        self.assertEquals(len(device.writes), 1)

        watchdog.disable()
        self.assertFalse(device.open)
        self.assertEquals(device.writes[-1], b'V')

    def test_invalid_timings(self):
        watchdog = Watchdog({'ttl': 30, 'loop_wait': 20, 'watchdog': {'mode': 'automatic'}})
        watchdog.activate()
        self.assertEquals(len(mock_devices), 1)
        self.assertFalse(watchdog.is_running)
