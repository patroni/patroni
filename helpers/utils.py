import os
import signal
import sys
import time

received_sigchld = False


def lsn_to_bytes(value):
    """
    >>> lsn_to_bytes('1/66000060')
    6006243424
    >>> lsn_to_bytes('j/66000060')
    0
    """
    try:
        e = value.split('/')
        if len(e) == 2 and len(e[0]) > 0 and len(e[1]) > 0:
            return (int(e[0], 16) << 32) | int(e[1], 16)
    except ValueError:
        pass
    return 0


def bytes_to_lsn(value):
    """
    >>> bytes_to_lsn(6006243424)
    '1/66000060'
    """
    id = value >> 32
    off = value & 0xffffffff
    return '%x/%x' % (id, off)


def sigterm_handler(signo, stack_frame):
    sys.exit()


def sigchld_handler(signo, stack_frame):
    global received_sigchld
    received_sigchld = True
    try:
        while True:
            ret = os.waitpid(-1, os.WNOHANG)
            if ret == (0, 0):
                break
    except OSError:
        pass


def sleep(interval):
    global received_sigchld
    current_time = time.time()
    end_time = current_time + interval
    while current_time < end_time:
        received_sigchld = False
        time.sleep(end_time - current_time)
        if not received_sigchld:  # we will ignore only sigchld
            break
        current_time = time.time()
    received_sigchld = False


def setup_signal_handlers():
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)
