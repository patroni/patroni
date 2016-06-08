import datetime
import os
import random
import signal
import six
import sys
import time
import pytz
import dateutil.parser

from patroni.exceptions import PatroniException

__ignore_sigterm = False
__interrupted_sleep = False
__reap_children = False


def calculate_ttl(expiration):
    """
    >>> calculate_ttl(None)
    >>> calculate_ttl('2015-06-10 12:56:30.552539016Z') < 0
    True
    >>> calculate_ttl('2015-06-10T12:56:30.552539016Z') < 0
    True
    >>> calculate_ttl('fail-06-10T12:56:30.552539016Z')
    """
    if not expiration:
        return None
    try:
        expiration = dateutil.parser.parse(expiration)
    except (ValueError, TypeError):
        return None
    now = datetime.datetime.now(pytz.utc)
    return int((expiration - now).total_seconds())


def deep_compare(obj1, obj2):
    """
    >>> deep_compare({'1': None}, {})
    False
    >>> deep_compare({'1': {}}, {'1': None})
    False
    >>> deep_compare({'1': [1]}, {'1': [2]})
    False
    >>> deep_compare({'1': 2}, {'1': '2'})
    True
    >>> deep_compare({'1': {'2': [3, 4]}}, {'1': {'2': [3, 4]}})
    True
    """

    if set(list(obj1.keys())) != set(list(obj2.keys())):  # Objects have different sets of keys
        return False

    for key, value in obj1.items():
        if isinstance(value, dict):
            if not (isinstance(obj2[key], dict) and deep_compare(value, obj2[key])):
                return False
        elif str(value) != str(obj2[key]):
            return False
    return True


def patch_config(config, data):
    """recursively 'patch' `config` with `data`
    :returns: `!True` if the `config` was changed"""
    is_changed = False
    for name, value in data.items():
        if value is None:
            if config.pop(name, None) is not None:
                is_changed = True
        elif name in config:
            if isinstance(value, dict):
                if patch_config(config[name], value):
                    is_changed = True
            elif str(config[name]) != str(value):
                config[name] = value
                is_changed = True
        else:
            config[name] = value
            is_changed = True
    return is_changed


def parse_bool(value):
    """
    >>> parse_bool(1)
    True
    >>> parse_bool('off')
    False
    >>> parse_bool('foo')
    """
    value = str(value).lower()
    if value in ('on', 'true', 'yes', '1'):
        return True
    if value in ('off', 'false', 'no', '0'):
        return False


def split_int_unit(value, strict=True):
    value = str(value)
    l = len(value) - 1
    while l >= 0 and not value[l].isdigit():
        l -= 1
    unit = value[l + 1:].strip()
    try:
        value = int(value[:l + 1], 0) if six.PY3 else long(value[:l + 1], 0)
    except ValueError:
        value = None if strict else 1
    return (value, unit)


def parse_int(value, base_unit=None):
    """
    >>> parse_int('1') == 1
    True
    >>> parse_int(' 0x400 MB ', '16384kB') == 64
    True
    >>> parse_int('1MB', 'kB') == 1024
    True
    >>> parse_int('1000 ms', 's') == 1
    True
    >>> parse_int('1GB', 'MB') is None
    True
    """

    convert = {
        'kB': {'kB': 1, 'MB': 1024, 'GB': 1024 * 1024, 'TB': 1024 * 1024 * 1024},
        'ms': {'ms': 1, 's': 1000, 'min': 1000 * 60, 'h': 1000 * 60 * 60, 'd': 1000 * 60 * 60 * 24},
        's': {'ms': -1000, 's': 1, 'min': 60, 'h': 60 * 60, 'd': 60 * 60 * 24},
        'min': {'ms': -1000 * 60, 's': -60, 'min': 1, 'h': 60, 'd': 60 * 24}
    }

    value, unit = split_int_unit(value)
    if value is not None:
        if not unit:
            return value

        if base_unit and base_unit not in convert:
            base_value, base_unit = split_int_unit(base_unit, False)
        else:
            base_value = 1
        if base_unit in convert and unit in convert[base_unit]:
            multiplier = convert[base_unit][unit]
            if multiplier < 0:
                value /= -multiplier
            else:
                value *= multiplier
            return int(value/base_value)


def compare_values(vartype, unit, old_value, new_value):
    """
    >>> compare_values('enum', None, 'remote_write', 'REMOTE_WRITE')
    True
    >>> compare_values('real', None, '1.23', 1.23)
    True
    """

    # if the integer or bool new_value is not correct this function will return False
    if vartype == 'bool':
        old_value = parse_bool(old_value)
        new_value = parse_bool(new_value)
    elif vartype == 'integer':
        old_value = parse_int(old_value)
        new_value = parse_int(new_value, unit)
    elif vartype == 'enum':
        return str(old_value).lower() == str(new_value).lower()
    else:  # ('string', 'real')
        return str(old_value) == str(new_value)
    return old_value is not None and new_value is not None and old_value == new_value


def set_ignore_sigterm(value=True):
    global __ignore_sigterm
    __ignore_sigterm = value


def sigterm_handler(signo, stack_frame):
    if not __ignore_sigterm:
        set_ignore_sigterm()
        sys.exit()


def sigchld_handler(signo, stack_frame):
    global __interrupted_sleep, __reap_children
    __reap_children = __interrupted_sleep = True


def sleep(interval):
    global __interrupted_sleep
    current_time = time.time()
    end_time = current_time + interval
    while current_time < end_time:
        __interrupted_sleep = False
        time.sleep(end_time - current_time)
        if not __interrupted_sleep:  # we will ignore only sigchld
            break
        current_time = time.time()
    __interrupted_sleep = False


def setup_signal_handlers():
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGCHLD, sigchld_handler)


def reap_children():
    global __reap_children
    if __reap_children:
        try:
            while True:
                ret = os.waitpid(-1, os.WNOHANG)
                if ret == (0, 0):
                    break
        except OSError:
            pass
        finally:
            __reap_children = False


class RetryFailedError(PatroniException):

    """Raised when retrying an operation ultimately failed, after retrying the maximum number of attempts."""


class Retry(object):

    """Helper for retrying a method in the face of retry-able exceptions"""

    def __init__(self, max_tries=1, delay=0.1, backoff=2, max_jitter=0.8, max_delay=3600,
                 sleep_func=sleep, deadline=None, retry_exceptions=PatroniException):
        """Create a :class:`Retry` instance for retrying function calls

        :param max_tries: How many times to retry the command. -1 means infinite tries.
        :param delay: Initial delay between retry attempts.
        :param backoff: Backoff multiplier between retry attempts. Defaults to 2 for exponential backoff.
        :param max_jitter: Additional max jitter period to wait between retry attempts to avoid slamming the server.
        :param max_delay: Maximum delay in seconds, regardless of other backoff settings. Defaults to one hour.
        :param retry_exceptions: single exception or tuple"""

        self.max_tries = max_tries
        self.delay = delay
        self.backoff = backoff
        self.max_jitter = int(max_jitter * 100)
        self.max_delay = float(max_delay)
        self._attempts = 0
        self._cur_delay = delay
        self.deadline = deadline
        self._cur_stoptime = None
        self.sleep_func = sleep_func
        self.retry_exceptions = retry_exceptions

    def reset(self):
        """Reset the attempt counter"""
        self._attempts = 0
        self._cur_delay = self.delay
        self._cur_stoptime = None

    def copy(self):
        """Return a clone of this retry manager"""
        return Retry(max_tries=self.max_tries, delay=self.delay, backoff=self.backoff,
                     max_jitter=self.max_jitter / 100.0, max_delay=self.max_delay, sleep_func=self.sleep_func,
                     deadline=self.deadline, retry_exceptions=self.retry_exceptions)

    def __call__(self, func, *args, **kwargs):
        """Call a function with arguments until it completes without throwing a `retry_exceptions`

        :param func: Function to call
        :param args: Positional arguments to call the function with
        :params kwargs: Keyword arguments to call the function with

        The function will be called until it doesn't throw one of the retryable exceptions"""
        self.reset()

        while True:
            try:
                if self.deadline is not None and self._cur_stoptime is None:
                    self._cur_stoptime = time.time() + self.deadline
                return func(*args, **kwargs)
            except self.retry_exceptions:
                # Note: max_tries == -1 means infinite tries.
                if self._attempts == self.max_tries:
                    raise RetryFailedError("Too many retry attempts")
                self._attempts += 1
                sleeptime = self._cur_delay + (random.randint(0, self.max_jitter) / 100.0)

                if self._cur_stoptime is not None and time.time() + sleeptime >= self._cur_stoptime:
                    raise RetryFailedError("Exceeded retry deadline")
                else:
                    self.sleep_func(sleeptime)
                self._cur_delay = min(self._cur_delay * self.backoff, self.max_delay)
