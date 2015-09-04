import datetime
import os
import re
import signal
import sys
import time

interrupted_sleep = False
reap_children = False

_DATE_TIME_RE = re.compile(r'''^
(?P<year>\d{4})\-(?P<month>\d{2})\-(?P<day>\d{2})  # date
T
(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\.(?P<microsecond>\d{6})  # time
\d*Z$''', re.X)


def parse_datetime(time_str):
    """
    >>> parse_datetime('2015-06-10T12:56:30.552539016Z')
    datetime.datetime(2015, 6, 10, 12, 56, 30, 552539)
    >>> parse_datetime('2015-06-10 12:56:30.552539016Z')
    """
    m = _DATE_TIME_RE.match(time_str)
    if not m:
        return None
    p = dict((n, int(m.group(n))) for n in 'year month day hour minute second microsecond'.split(' '))
    return datetime.datetime(**p)


def calculate_ttl(expiration):
    """
    >>> calculate_ttl(None)
    >>> calculate_ttl('2015-06-10 12:56:30.552539016Z')
    """
    if not expiration:
        return None
    expiration = parse_datetime(expiration)
    if not expiration:
        return None
    now = datetime.datetime.utcnow()
    return int((expiration - now).total_seconds())


def sigterm_handler(signo, stack_frame):
    sys.exit()


def sigchld_handler(signo, stack_frame):
    global interrupted_sleep, reap_children
    reap_children = interrupted_sleep = True


def sleep(interval):
    global interrupted_sleep
    current_time = time.time()
    end_time = current_time + interval
    while current_time < end_time:
        interrupted_sleep = False
        time.sleep(end_time - current_time)
        if not interrupted_sleep:  # we will ignore only sigchld
            break
        current_time = time.time()
    interrupted_sleep = False


def setup_signal_handlers():
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)


def reap_children():
    global reap_children
    if reap_children:
        try:
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
        except OSError:
            pass
        finally:
            reap_children = False
