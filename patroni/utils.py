import logging
import random
import re
import time

from dateutil import tz
from patroni.exceptions import PatroniException

tzutc = tz.tzutc()

logger = logging.getLogger(__name__)

OCT_RE = re.compile(r'^[-+]?0[0-7]*')
DEC_RE = re.compile(r'^[-+]?(0|[1-9][0-9]*)')
HEX_RE = re.compile(r'^[-+]?0x[0-9a-fA-F]+')
DBL_RE = re.compile(r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?')


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
                if isinstance(config[name], dict):
                    if patch_config(config[name], value):
                        is_changed = True
                else:
                    config[name] = value
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


def strtol(value, strict=True):
    """As most as possible close equivalent of strtol(3) function (with base=0),
       used by postgres to parse parameter values.
    >>> strtol(0) == (0, '')
    True
    >>> strtol(1) == (1, '')
    True
    >>> strtol(9) == (9, '')
    True
    >>> strtol(' +0x400MB') == (1024, 'MB')
    True
    >>> strtol(' -070d') == (-56, 'd')
    True
    >>> strtol(' d ') == (None, 'd')
    True
    >>> strtol(' 1 d ') == (1, ' d')
    True
    >>> strtol('9s', False) == (9, 's')
    True
    >>> strtol(' s ', False) == (1, 's')
    True
    """
    value = str(value).strip()
    for regex, base in ((HEX_RE, 16), (OCT_RE, 8), (DEC_RE, 10)):
        match = regex.match(value)
        if match:
            end = match.end()
            return int(value[:end], base), value[end:]
    return (None if strict else 1), value


def strtod(value):
    """As most as possible close equivalent of strtod(3) function used by postgres to parse parameter values.
    >>> strtod(' A ') == (None, 'A')
    True
    """
    value = str(value).strip()
    match = DBL_RE.match(value)
    if match:
        end = match.end()
        return float(value[:end]), value[end:]
    return None, value


def rint(value):
    """
    >>> rint(0.5) == 0
    True
    >>> rint(0.501) == 1
    True
    >>> rint(1.5) == 2
    True
    """

    ret = round(value)
    return 2.0 * round(value / 2.0) if abs(ret - value) == 0.5 else ret


def convert_to_base_unit(value, unit, base_unit):
    convert = {
        'B': {'B': 1, 'kB': 1024, 'MB': 1024 * 1024, 'GB': 1024 * 1024 * 1024, 'TB': 1024 * 1024 * 1024 * 1024},
        'kB': {'B': 1.0 / 1024, 'kB': 1, 'MB': 1024, 'GB': 1024 * 1024, 'TB': 1024 * 1024 * 1024},
        'MB': {'B': 1.0 / (1024 * 1024), 'kB': 1.0 / 1024, 'MB': 1, 'GB': 1024, 'TB': 1024 * 1024},
        'ms': {'us': 1.0 / 1000, 'ms': 1, 's': 1000, 'min': 1000 * 60, 'h': 1000 * 60 * 60, 'd': 1000 * 60 * 60 * 24},
        's': {'us': 1.0 / (1000 * 1000), 'ms': 1.0 / 1000, 's': 1, 'min': 60, 'h': 60 * 60, 'd': 60 * 60 * 24},
        'min': {'us': 1.0 / (1000 * 1000 * 60), 'ms': 1.0 / (1000 * 60), 's': 1.0 / 60, 'min': 1, 'h': 60, 'd': 60 * 24}
    }

    round_order = {
        'TB': 'GB', 'GB': 'MB', 'MB': 'kB', 'kB': 'B',
        'd': 'h', 'h': 'min', 'min': 's', 's': 'ms', 'ms': 'us'
    }

    if base_unit and base_unit not in convert:
        base_value, base_unit = strtol(base_unit, False)
    else:
        base_value = 1

    if base_unit in convert and unit in convert[base_unit]:
        value *= convert[base_unit][unit] / float(base_value)

        if unit in round_order:
            multiplier = convert[base_unit][round_order[unit]]
            value = rint(value / float(multiplier)) * multiplier

        return value


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
    >>> parse_int('1TB', 'GB') is None
    True
    >>> parse_int(0) == 0
    True
    >>> parse_int('6GB', '16MB') == 384
    True
    >>> parse_int('4097.4kB', 'kB') == 4097
    True
    >>> parse_int('4097.5kB', 'kB') == 4098
    True
    """

    val, unit = strtol(value)
    if val is None and unit.startswith('.') or unit and unit[0] in ('.', 'e', 'E'):
        val, unit = strtod(value)

    if val is not None:
        unit = unit.strip()
        if not unit:
            return int(rint(val))

        val = convert_to_base_unit(val, unit, base_unit)
        if val is not None:
            return int(rint(val))


def parse_real(value, base_unit=None):
    """
    >>> parse_real(' +0.0005 ') == 0.0005
    True
    >>> parse_real('0.0005ms', 'ms') == 0.0
    True
    >>> parse_real('0.00051ms', 'ms') == 0.001
    True
    """
    val, unit = strtod(value)

    if val is not None:
        unit = unit.strip()
        if not unit:
            return val

        return convert_to_base_unit(val, unit, base_unit)


def compare_values(vartype, unit, old_value, new_value):
    """
    >>> compare_values('enum', None, 'remote_write', 'REMOTE_WRITE')
    True
    >>> compare_values('real', None, '1e-06', 0.000001)
    True
    """

    converters = {
        'bool': lambda v1, v2: parse_bool(v1),
        'integer': parse_int,
        'real': parse_real,
        'enum': lambda v1, v2: str(v1).lower(),
        'string': lambda v1, v2: str(v1)
    }

    convert = converters.get(vartype) or converters['string']
    old_value = convert(old_value, None)
    new_value = convert(new_value, unit)

    return old_value is not None and new_value is not None and old_value == new_value


def _sleep(interval):
    time.sleep(interval)


class RetryFailedError(PatroniException):

    """Raised when retrying an operation ultimately failed, after retrying the maximum number of attempts."""


class Retry(object):

    """Helper for retrying a method in the face of retry-able exceptions"""

    def __init__(self, max_tries=1, delay=0.1, backoff=2, max_jitter=0.8, max_delay=3600,
                 sleep_func=_sleep, deadline=None, retry_exceptions=PatroniException):
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
            except self.retry_exceptions as e:
                # Note: max_tries == -1 means infinite tries.
                if self._attempts == self.max_tries:
                    logger.warning('Retry got exception: %s', e)
                    raise RetryFailedError("Too many retry attempts")
                self._attempts += 1
                sleeptime = self._cur_delay + (random.randint(0, self.max_jitter) / 100.0)

                if self._cur_stoptime is not None and time.time() + sleeptime >= self._cur_stoptime:
                    logger.warning('Retry got exception: %s', e)
                    raise RetryFailedError("Exceeded retry deadline")
                logger.debug('Retry got exception: %s', e)
                self.sleep_func(sleeptime)
                self._cur_delay = min(self._cur_delay * self.backoff, self.max_delay)


def polling_loop(timeout, interval=1):
    """Returns an iterator that returns values until timeout has passed. Timeout is measured from start of iteration."""
    start_time = time.time()
    iteration = 0
    end_time = start_time + timeout
    while time.time() < end_time:
        yield iteration
        iteration += 1
        time.sleep(interval)


def split_host_port(value, default_port):
    t = value.rsplit(':', 1)
    t.append(default_port)
    return t[0], int(t[1])
