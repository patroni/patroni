"""Utilitary objects and functions that can be used throughout Patroni code.

:var tzutc: UTC time zone info object.
:var logger: logger of this module.
:var USER_AGENT: identifies the Patroni version, Python version, and the underlying platform.
:var OCT_RE: regular expression to match octal numbers, signed or unsigned.
:var DEC_RE: regular expression to match decimal numbers, signed or unsigned.
:var HEX_RE: regular expression to match hex strings, signed or unsigned.
:var DBL_RE: regular expression to match double precision numbers, signed or unsigned. Matches scientific notation too.
:var WHITESPACE_RE: regular expression to match whitespace characters
"""
import errno
import itertools
import logging
import os
import platform
import random
import re
import socket
import subprocess
import sys
import tempfile
import time

from collections import OrderedDict
from json import JSONDecoder
from shlex import split
from typing import Any, Callable, cast, Dict, Iterator, List, Optional, Tuple, Type, TYPE_CHECKING, Union

from dateutil import tz
from urllib3.response import HTTPResponse

from .exceptions import PatroniException
from .version import __version__

if TYPE_CHECKING:  # pragma: no cover
    from .dcs import Cluster

tzutc = tz.tzutc()

logger = logging.getLogger(__name__)

USER_AGENT = 'Patroni/{0} Python/{1} {2}'.format(__version__, platform.python_version(), platform.system())
OCT_RE = re.compile(r'^[-+]?0[0-7]*')
DEC_RE = re.compile(r'^[-+]?(0|[1-9][0-9]*)')
HEX_RE = re.compile(r'^[-+]?0x[0-9a-fA-F]+')
DBL_RE = re.compile(r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?')
WHITESPACE_RE = re.compile(r'[ \t\n\r]*', re.VERBOSE | re.MULTILINE | re.DOTALL)


def get_conversion_table(base_unit: str) -> Dict[str, Dict[str, Union[int, float]]]:
    """Get conversion table for the specified base unit.

    If no conversion table exists for the passed unit, return an empty :class:`OrderedDict`.

    :param base_unit: unit to choose the conversion table for.

    :returns: :class:`OrderedDict` object.
    """
    memory_unit_conversion_table: Dict[str, Dict[str, Union[int, float]]] = OrderedDict([
        ('TB', {'B': 1024**4, 'kB': 1024**3, 'MB': 1024**2}),
        ('GB', {'B': 1024**3, 'kB': 1024**2, 'MB': 1024}),
        ('MB', {'B': 1024**2, 'kB': 1024, 'MB': 1}),
        ('kB', {'B': 1024, 'kB': 1, 'MB': 1024**-1}),
        ('B', {'B': 1, 'kB': 1024**-1, 'MB': 1024**-2})
    ])
    time_unit_conversion_table: Dict[str, Dict[str, Union[int, float]]] = OrderedDict([
        ('d', {'ms': 1000 * 60**2 * 24, 's': 60**2 * 24, 'min': 60 * 24}),
        ('h', {'ms': 1000 * 60**2, 's': 60**2, 'min': 60}),
        ('min', {'ms': 1000 * 60, 's': 60, 'min': 1}),
        ('s', {'ms': 1000, 's': 1, 'min': 60**-1}),
        ('ms', {'ms': 1, 's': 1000**-1, 'min': 1 / (1000 * 60)}),
        ('us', {'ms': 1000**-1, 's': 1000**-2, 'min': 1 / (1000**2 * 60)})
    ])
    if base_unit in ('B', 'kB', 'MB'):
        return memory_unit_conversion_table
    elif base_unit in ('ms', 's', 'min'):
        return time_unit_conversion_table
    return OrderedDict()


def deep_compare(obj1: Dict[Any, Any], obj2: Dict[Any, Any]) -> bool:
    """Recursively compare two dictionaries to check if they are equal in terms of keys and values.

    .. note::
        Values are compared based on their string representation.

    :param obj1: dictionary to be compared with *obj2*.
    :param obj2: dictionary to be compared with *obj1*.

    :returns: ``True`` if all keys and values match between the two dictionaries.

    :Example:

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
            if not (isinstance(obj2[key], dict) and deep_compare(cast(Dict[Any, Any], value), obj2[key])):
                return False
        elif str(value) != str(obj2[key]):
            return False
    return True


def patch_config(config: Dict[Any, Any], data: Dict[Any, Any]) -> bool:
    """Update and append to dictionary *config* from overrides in *data*.

    .. note::

        * If the value of a given key in *data* is ``None``, then the key is removed from *config*;
        * If a key is present in *data* but not in *config*, the key with the corresponding value is added to *config*
        * For keys that are present on both sides it will compare the string representation of the corresponding values,
          if the comparison doesn't match override the value

    :param config: configuration to be patched.
    :param data: new configuration values to patch *config* with.

    :returns: ``True`` if *config* was changed.
    """
    is_changed = False
    for name, value in data.items():
        if value is None:
            if config.pop(name, None) is not None:
                is_changed = True
        elif name in config:
            if isinstance(value, dict):
                if isinstance(config[name], dict):
                    if patch_config(config[name], cast(Dict[Any, Any], value)):
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


def parse_bool(value: Any) -> Optional[bool]:
    """Parse a given value to a :class:`bool` object.

    .. note::

        The parsing is case-insensitive, and takes into consideration these values:
            * ``on``, ``true``, ``yes``, and ``1`` as ``True``.
            * ``off``, ``false``, ``no``, and ``0`` as ``False``.

    :param value: value to be parsed to :class:`bool`.

    :returns: the parsed value. If not able to parse, returns ``None``.

    :Example:

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


def strtol(value: Any, strict: Optional[bool] = True) -> Tuple[Optional[int], str]:
    """Extract the long integer part from the beginning of a string that represents a configuration value.

    As most as possible close equivalent of ``strtol(3)`` C function (with base=0), which is used by postgres to parse
    parameter values.

    Takes into consideration numbers represented either as hex, octal or decimal formats.

    :param value: any value from which we want to extract a long integer.
    :param strict: dictates how the first item in the returning tuple is set when :func:`strtol` is not able to find a
        long integer in *value*. If *strict* is ``True``, then the first item will be ``None``, else it will be ``1``.

    :returns: the first item is the extracted long integer from *value*, and the second item is the remaining string of
        *value*. If not able to match a long integer in *value*, then the first item will be either ``None`` or ``1``
        (depending on *strict* argument), and the second item will be the original *value*.

    :Example:

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


def strtod(value: Any) -> Tuple[Optional[float], str]:
    """Extract the double precision part from the beginning of a string that reprensents a configuration value.

    As most as possible close equivalent of ``strtod(3)`` C function, which is used by postgres to parse parameter
    values.

    :param value: any value from which we want to extract a double precision.

    :returns: the first item is the extracted double precision from *value*, and the second item is the remaining
        string of *value*. If not able to match a double precision in *value*, then the first item will be ``None``,
        and the second item will be the original *value*.

    :Example:

        >>> strtod(' A ') == (None, 'A')
        True

        >>> strtod('1 A ') == (1.0, ' A')
        True

        >>> strtod('1.5A') == (1.5, 'A')
        True

        >>> strtod('8.325e-10A B C') == (8.325e-10, 'A B C')
        True
    """
    value = str(value).strip()
    match = DBL_RE.match(value)
    if match:
        end = match.end()
        return float(value[:end]), value[end:]
    return None, value


def convert_to_base_unit(value: Union[int, float], unit: str, base_unit: Optional[str]) -> Union[int, float, None]:
    """Convert *value* as a *unit* of compute information or time to *base_unit*.

    :param value: value to be converted to the base unit.
    :param unit: unit of *value*. Accepts these units (case sensitive):

            * For space: ``B``, ``kB``, ``MB``, ``GB``, or ``TB``;
            * For time: ``d``, ``h``, ``min``, ``s``, ``ms``, or ``us``.

    :param base_unit: target unit in the conversion. May contain the target unit with an associated value, e.g
        ``512MB``. Accepts these units (case sensitive):

            * For space: ``B``, ``kB``, or ``MB``;
            * For time: ``ms``, ``s``, or ``min``.

    :returns: *value* in *unit* converted to *base_unit*. Returns ``None`` if *unit* or *base_unit* is invalid.

    :Example:

        >>> convert_to_base_unit(1, 'GB', '256MB')
        4

        >>> convert_to_base_unit(1, 'GB', 'MB')
        1024

        >>> convert_to_base_unit(1, 'gB', '512MB') is None
        True

        >>> convert_to_base_unit(1, 'GB', '512 MB') is None
        True
    """
    base_value, base_unit = strtol(base_unit, False)
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(base_value, int)

    convert_tbl = get_conversion_table(base_unit)
    # {'TB': 'GB', 'GB': 'MB', ...}
    round_order = dict(zip(convert_tbl, itertools.islice(convert_tbl, 1, None)))
    if unit in convert_tbl and base_unit in convert_tbl[unit]:
        value *= convert_tbl[unit][base_unit] / float(base_value)
        if unit in round_order:
            multiplier = convert_tbl[round_order[unit]][base_unit]
            value = round(value / float(multiplier)) * multiplier
        return value


def convert_int_from_base_unit(base_value: int, base_unit: Optional[str]) -> Optional[str]:
    """Convert an integer value in some base unit to a human-friendly unit.

    The output unit is chosen so that it's the greatest unit that can represent
    the value without loss.

    :param base_value: value to be converted from a base unit
    :param base_unit: unit of *value*. Should be one of the base units (case sensitive):

            * For space: ``B``, ``kB``, ``MB``;
            * For time: ``ms``, ``s``, ``min``.

    :returns: :class:`str` value representing *base_value* converted from *base_unit* to the greatest
        possible human-friendly unit, or ``None`` if conversion failed.

    :Example:

        >>> convert_int_from_base_unit(1024, 'kB')
        '1MB'

        >>> convert_int_from_base_unit(1025, 'kB')
        '1025kB'

        >>> convert_int_from_base_unit(4, '256MB')
        '1GB'

        >>> convert_int_from_base_unit(4, '256 MB') is None
        True

        >>> convert_int_from_base_unit(1024, 'KB') is None
        True
    """
    base_value_mult, base_unit = strtol(base_unit, False)
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(base_value_mult, int)
    base_value *= base_value_mult

    convert_tbl = get_conversion_table(base_unit)
    for unit in convert_tbl:
        multiplier = convert_tbl[unit][base_unit]
        if multiplier <= 1.0 or base_value % multiplier == 0:
            return str(round(base_value / multiplier)) + unit


def convert_real_from_base_unit(base_value: float, base_unit: Optional[str]) -> Optional[str]:
    """Convert an floating-point value in some base unit to a human-friendly unit.

    Same as :func:`convert_int_from_base_unit`, except we have to do the math a bit differently,
    and there's a possibility that we don't find any exact divisor.

    :param base_value: value to be converted from a base unit
    :param base_unit: unit of *value*. Should be one of the base units (case sensitive):

            * For space: ``B``, ``kB``, ``MB``;
            * For time: ``ms``, ``s``, ``min``.

    :returns: :class:`str` value representing *base_value* converted from *base_unit* to the greatest
        possible human-friendly unit, or ``None`` if conversion failed.

    :Example:

        >>> convert_real_from_base_unit(5, 'ms')
        '5ms'

        >>> convert_real_from_base_unit(2.5, 'ms')
        '2500us'

        >>> convert_real_from_base_unit(4.0, '256MB')
        '1GB'

        >>> convert_real_from_base_unit(4.0, '256 MB') is None
        True
    """
    base_value_mult, base_unit = strtol(base_unit, False)
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(base_value_mult, int)
    base_value *= base_value_mult

    result = None
    convert_tbl = get_conversion_table(base_unit)
    for unit in convert_tbl:
        value = base_value / convert_tbl[unit][base_unit]
        result = f'{value:g}{unit}'
        if value > 0 and abs((round(value) / value) - 1.0) <= 1e-8:
            break
    return result


def maybe_convert_from_base_unit(base_value: str, vartype: str, base_unit: Optional[str]) -> str:
    """Try to convert integer or real value in a base unit to a human-readable unit.

    Value is passed as a string. If parsing or subsequent conversion fails, the original
    value is returned.

    :param base_value: value to be converted from a base unit.
    :param vartype: the target type to parse *base_value* before converting (``integer``
        or ``real`` is expected, any other type results in return value being equal to the
        *base_value* string).
    :param base_unit: unit of *value*. Should be one of the base units (case sensitive):

            * For space: ``B``, ``kB``, ``MB``;
            * For time: ``ms``, ``s``, ``min``.

    :returns: :class:`str` value representing *base_value* converted from *base_unit* to the greatest
        possible human-friendly unit, or *base_value* string if conversion failed.

    :Example:

        >>> maybe_convert_from_base_unit('5', 'integer', 'ms')
        '5ms'

        >>> maybe_convert_from_base_unit('4.2', 'real', 'ms')
        '4200us'

        >>> maybe_convert_from_base_unit('on', 'bool', None)
        'on'

        >>> maybe_convert_from_base_unit('', 'integer', '256MB')
        ''
    """
    converters: Dict[str, Tuple[Callable[[str, Optional[str]], Union[int, float, str, None]],
                                Callable[[Any, Optional[str]], Optional[str]]]] = {
        'integer': (parse_int, convert_int_from_base_unit),
        'real': (parse_real, convert_real_from_base_unit),
        'default': (lambda v, _: v, lambda v, _: v)
    }
    parser, converter = converters.get(vartype, converters['default'])
    parsed_value = parser(base_value, None)
    if parsed_value:
        return converter(parsed_value, base_unit) or base_value
    return base_value


def parse_int(value: Any, base_unit: Optional[str] = None) -> Optional[int]:
    """Parse *value* as an :class:`int`.

    :param value: any value that can be handled either by :func:`strtol` or :func:`strtod`. If *value* contains a
        unit, then *base_unit* must be given.
    :param base_unit: an optional base unit to convert *value* through :func:`convert_to_base_unit`. Not used if
        *value* does not contain a unit.

    :returns: the parsed value, if able to parse. Otherwise returns ``None``.

    :Example:

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

        >>> parse_int(50, None) == 50
        True

        >>> parse_int("51", None) == 51
        True

        >>> parse_int("nonsense", None) == None
        True

        >>> parse_int("nonsense", "kB") == None
        True

        >>> parse_int("nonsense") == None
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
            return round(val)

        val = convert_to_base_unit(val, unit, base_unit)
        if val is not None:
            return round(val)


def parse_real(value: Any, base_unit: Optional[str] = None) -> Optional[float]:
    """Parse *value* as a :class:`float`.

    :param value: any value that can be handled by :func:`strtod`. If *value* contains a unit, then *base_unit* must
        be given.
    :param base_unit: an optional base unit to convert *value* through :func:`convert_to_base_unit`. Not used if
        *value* does not contain a unit.

    :returns: the parsed value, if able to parse. Otherwise returns ``None``.

    :Example:

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


def compare_values(vartype: str, unit: Optional[str], settings_value: Any, config_value: Any) -> bool:
    """Check if the value from ``pg_settings`` and from Patroni config are equivalent after parsing them as *vartype*.

    :param vartype: the target type to parse *settings_value* and *config_value* before comparing them.
        Accepts any among of the following (case sensitive):

        * ``bool``: parse values using :func:`parse_bool`; or
        * ``integer``: parse values using :func:`parse_int`; or
        * ``real``: parse values using :func:`parse_real`; or
        * ``enum``: parse values as lowercase strings; or
        * ``string``: parse values as strings. This one is used by default if no valid value is passed as *vartype*.
    :param unit: base unit to be used as argument when calling :func:`parse_int` or :func:`parse_real`
        for *config_value*.
    :param settings_value: value to be compared with *config_value*.
    :param config_value: value to be compared with *settings_value*.

    :returns: ``True`` if *settings_value* is equivalent to *config_value* when both are parsed as *vartype*.

    :Example:

        >>> compare_values('enum', None, 'remote_write', 'REMOTE_WRITE')
        True

        >>> compare_values('string', None, 'remote_write', 'REMOTE_WRITE')
        False

        >>> compare_values('real', None, '1e-06', 0.000001)
        True

        >>> compare_values('integer', 'MB', '6GB', '6GB')
        False

        >>> compare_values('integer', None, '6GB', '6GB')
        False

        >>> compare_values('integer', '16384kB', '64', ' 0x400 MB ')
        True

        >>> compare_values('integer', '2MB', 524288, '1TB')
        True

        >>> compare_values('integer', 'MB', 1048576, '1TB')
        True

        >>> compare_values('integer', 'kB', 4098, '4097.5kB')
        True
    """
    converters: Dict[str, Callable[[str, Optional[str]], Union[None, bool, int, float, str]]] = {
        'bool': lambda v1, v2: parse_bool(v1),
        'integer': parse_int,
        'real': parse_real,
        'enum': lambda v1, v2: str(v1).lower(),
        'string': lambda v1, v2: str(v1)
    }

    converter = converters.get(vartype) or converters['string']
    old_converted = converter(settings_value, None)
    new_converted = converter(config_value, unit)

    return old_converted is not None and new_converted is not None and old_converted == new_converted


def _sleep(interval: Union[int, float]) -> None:
    """Wrap :func:`~time.sleep`.

    :param interval: Delay execution for a given number of seconds. The argument may be a floating point number for
        subsecond precision.
    """
    time.sleep(interval)


def read_stripped(file_path: str) -> Iterator[str]:
    """Iterate over stripped lines in the given file.

    :param file_path: path to the file to read from

    :yields: each line from the given file stripped
    """
    with open(file_path) as f:
        for line in f:
            yield line.strip()


class RetryFailedError(PatroniException):
    """Maximum number of attempts exhausted in retry operation."""


class Retry(object):
    """Helper for retrying a method in the face of retryable exceptions.

    :ivar max_tries: how many times to retry the command.
    :ivar delay: initial delay between retry attempts.
    :ivar backoff: backoff multiplier between retry attempts.
    :ivar max_jitter: additional max jitter period to wait between retry attempts to avoid slamming the server.
    :ivar max_delay: maximum delay in seconds, regardless of other backoff settings.
    :ivar sleep_func: function used to introduce artificial delays.
    :ivar deadline: timeout for operation retries.
    :ivar retry_exceptions: single exception or tuple
    """

    def __init__(self, max_tries: Optional[int] = 1, delay: float = 0.1, backoff: int = 2,
                 max_jitter: float = 0.8, max_delay: int = 3600,
                 sleep_func: Callable[[Union[int, float]], None] = _sleep,
                 deadline: Optional[Union[int, float]] = None,
                 retry_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = PatroniException) -> None:
        """Create a :class:`Retry` instance for retrying function calls.

        :param max_tries: how many times to retry the command. ``-1`` means infinite tries.
        :param delay: initial delay between retry attempts.
        :param backoff: backoff multiplier between retry attempts. Defaults to ``2`` for exponential backoff.
        :param max_jitter: additional max jitter period to wait between retry attempts to avoid slamming the server.
        :param max_delay: maximum delay in seconds, regardless of other backoff settings.
        :param sleep_func: function used to introduce artificial delays.
        :param deadline: timeout for operation retries.
        :param retry_exceptions: single exception or tuple
        """
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

    def reset(self) -> None:
        """Reset the attempt counter, delay and stop time."""
        self._attempts = 0
        self._cur_delay = self.delay
        self._cur_stoptime = None

    def copy(self) -> 'Retry':
        """Return a clone of this retry manager."""
        return Retry(max_tries=self.max_tries, delay=self.delay, backoff=self.backoff,
                     max_jitter=self.max_jitter / 100.0, max_delay=int(self.max_delay), sleep_func=self.sleep_func,
                     deadline=self.deadline, retry_exceptions=self.retry_exceptions)

    @property
    def sleeptime(self) -> float:
        """Get next cycle sleep time.

        It is based on the current delay plus a number up to ``max_jitter``.
        """
        return self._cur_delay + (random.randint(0, self.max_jitter) / 100.0)

    def update_delay(self) -> None:
        """Set next cycle delay.

        It will be the minimum value between:

            * current delay with ``backoff``; or
            * ``max_delay``.
        """
        self._cur_delay = min(self._cur_delay * self.backoff, self.max_delay)

    @property
    def stoptime(self) -> float:
        """Get the current stop time."""
        return self._cur_stoptime or 0

    def ensure_deadline(self, timeout: float, raise_ex: Optional[Exception] = None) -> bool:
        """Calculates and checks the remaining deadline time.

        :param timeout: if the *deadline* is smaller than the provided *timeout* value raise *raise_ex* exception.
        :param raise_ex: the exception object that will be raised if the *deadline* is smaller than provided *timeout*.

        :returns: ``False`` if *deadline* is smaller than a provided *timeout* and *raise_ex* isn't set. Otherwise
            ``True``.

        :raises:
            :class:`Exception`: *raise_ex* if calculated deadline is smaller than provided *timeout*.
        """
        if self.stoptime - time.time() < timeout:
            if raise_ex:
                raise raise_ex
            return False
        return True

    def __call__(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Call a function *func* with arguments ``*args`` and ``*kwargs`` in a loop.

        *func* will be called until one of the following conditions is met:

            * It completes without throwing one of the configured ``retry_exceptions``; or
            * ``max_retries`` is exceeded.; or
            * ``deadline`` is exceeded.

        .. note::
            * It will set loop stop time based on ``deadline`` attribute.
            * It will adjust delay on each cycle.

        :param func: function to call.
        :param args: positional arguments to call *func* with.
        :params kwargs: keyword arguments to call *func* with.
        :raises:
            :class:`RetryFailedError`:
                * If ``max_tries`` is exceeded; or
                * If ``deadline`` is exceeded.
        """
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
                sleeptime = getattr(e, 'sleeptime', None)
                if not isinstance(sleeptime, (int, float)):
                    sleeptime = self.sleeptime

                if self._cur_stoptime is not None and time.time() + sleeptime >= self._cur_stoptime:
                    logger.warning('Retry got exception: %s', e)
                    raise RetryFailedError("Exceeded retry deadline")
                logger.debug('Retry got exception: %s', e)
                self.sleep_func(sleeptime)
                self.update_delay()


def polling_loop(timeout: Union[int, float], interval: Union[int, float] = 1) -> Iterator[int]:
    """Return an iterator that returns values every *interval* seconds until *timeout* has passed.

    .. note::
        Timeout is measured from start of iteration.

    :param timeout: for how long (in seconds) from now it should keep returning values.
    :param interval: for how long to sleep before returning a new value.

    :yields: current iteration counter, starting from ``0``.
    """
    start_time = time.time()
    iteration = 0
    end_time = start_time + timeout
    while time.time() < end_time:
        yield iteration
        iteration += 1
        time.sleep(float(interval))


def split_host_port(value: str, default_port: Optional[int]) -> Tuple[str, int]:
    """Extract host(s) and port from *value*.

    :param value: string from where host(s) and port will be extracted. Accepts either of these formats:

            * ``host:port``; or
            * ``host1,host2,...,hostn:port``.

        Each ``host`` portion of *value* can be either:

            * A FQDN; or
            * An IPv4 address; or
            * An IPv6 address, with or without square brackets.

    :param default_port: if no port can be found in *param*, use *default_port* instead.

    :returns: the first item is composed of a CSV list of hosts from *value*, and the second item is either the port
        from *value* or *default_port*.

    :Example:

        >>> split_host_port('127.0.0.1', 5432)
        ('127.0.0.1', 5432)

        >>> split_host_port('127.0.0.1:5400', 5432)
        ('127.0.0.1', 5400)

        >>> split_host_port('127.0.0.1,192.168.0.101:5400', 5432)
        ('127.0.0.1,192.168.0.101', 5400)

        >>> split_host_port('127.0.0.1,www.mydomain.com,[fe80:0:0:0:213:72ff:fe3c:21bf], 0:0:0:0:0:0:0:0:5400', 5432)
        ('127.0.0.1,www.mydomain.com,fe80:0:0:0:213:72ff:fe3c:21bf,0:0:0:0:0:0:0:0', 5400)
    """
    t = value.rsplit(':', 1)
    # If *value* contains ``:`` we consider it to be an IPv6 address, so we attempt to remove possible square brackets
    if ':' in t[0]:
        t[0] = ','.join([h.strip().strip('[]') for h in t[0].split(',')])
    t.append(str(default_port))
    return t[0], int(t[1])


def uri(proto: str, netloc: Union[List[str], Tuple[str, Union[int, str]], str], path: Optional[str] = '',
        user: Optional[str] = None) -> str:
    """Construct URI from given arguments.

    :param proto: the URI protocol.
    :param netloc: the URI host(s) and port. Can be specified in either way among

        * A :class:`list` or :class:`tuple`. The second item should be a port, and the first item should be composed of
            hosts in either of these formats:

            * ``host``; or.
            * ``host1,host2,...,hostn``.

        * A :class:`str` in either of these formats:

            * ``host:port``; or
            * ``host1,host2,...,hostn:port``.

        In all cases, each ``host`` portion of *netloc* can be either:

            * An FQDN; or
            * An IPv4 address; or
            * An IPv6 address, with or without square brackets.

    :param path: the URI path.
    :param user: the authenticating user, if any.

    :returns: constructed URI.
    """
    host, port = netloc if isinstance(netloc, (list, tuple)) else split_host_port(netloc, 0)
    # If ``host`` contains ``:`` we consider it to be an IPv6 address, so we add square brackets if they are missing
    if host and ':' in host and host[0] != '[' and host[-1] != ']':
        host = '[{0}]'.format(host)
    port = ':{0}'.format(port) if port else ''
    path = '/{0}'.format(path) if path and not path.startswith('/') else path
    user = '{0}@'.format(user) if user else ''
    return '{0}://{1}{2}{3}{4}'.format(proto, user, host, port, path)


def iter_response_objects(response: HTTPResponse) -> Iterator[Dict[str, Any]]:
    """Iterate over the chunks of a :class:`~urllib3.response.HTTPResponse` and yield each JSON document that is found.

    :param response: the HTTP response from which JSON documents will be retrieved.

    :yields: current JSON document.
    """
    prev = ''
    decoder = JSONDecoder()
    for chunk in response.read_chunked(decode_content=False):
        chunk = prev + chunk.decode('utf-8')

        length = len(chunk)
        # ``chunk`` is analyzed in parts. ``idx`` holds the position of the first character in the current part that is
        # neither space nor tab nor line-break, or in other words, the position in the ``chunk`` where it is likely
        # that a JSON document begins
        idx = WHITESPACE_RE.match(chunk, 0).end()  # pyright: ignore [reportOptionalMemberAccess]
        while idx < length:
            try:
                # Get a JSON document from the chunk. ``message`` is a dictionary representing the JSON document, and
                # ``idx`` becomes the position in the ``chunk`` where the retrieved JSON document ends
                message, idx = decoder.raw_decode(chunk, idx)
            except ValueError:  # malformed or incomplete JSON, unlikely to happen
                break
            else:
                yield message
                idx = WHITESPACE_RE.match(chunk, idx).end()  # pyright: ignore [reportOptionalMemberAccess]
        # It is not usual that a ``chunk`` would contain more than one JSON document, but we handle that just in case
        prev = chunk[idx:]


def cluster_as_json(cluster: 'Cluster') -> Dict[str, Any]:
    """Get a JSON representation of *cluster*.

    :param cluster: the :class:`~patroni.dcs.Cluster` object to be parsed as JSON.

    :returns: JSON representation of *cluster*.

    These are the possible keys in the returning object depending on the available information in *cluster*:

        * ``members``: list of members in the cluster. Each value is a :class:`dict` that may have the following keys:

            * ``name``: the name of the host (unique in the cluster). The ``members`` list is sorted by this key;
            * ``role``: ``leader``, ``standby_leader``, ``sync_standby``, ``quorum_standby``, or ``replica``;
            * ``state``: one of :class:`~patroni.postgresql.misc.PostgresqlState`;
            * ``api_url``: REST API URL based on ``restapi->connect_address`` configuration;
            * ``host``: PostgreSQL host based on ``postgresql->connect_address``;
            * ``port``: PostgreSQL port based on ``postgresql->connect_address``;
            * ``timeline``: PostgreSQL current timeline;
            * ``pending_restart``: ``True`` if PostgreSQL is pending to be restarted;
            * ``scheduled_restart``: scheduled restart timestamp, if any;
            * ``tags``: any tags that were set for this member;
            * ``lsn``: current WAL position. See :meth:`Postgresql._wal_position`
            * ``receive_lsn``: receive LSN (``pg_catalog.pg_last_(xlog|wal)_receive_(location|lsn)()``),
                if applicable;
            * ``replay_lsn``: replay LSN (``pg_catalog.pg_last_(xlog|wal)_replay_(location|lsn)()``),
                if applicable;
            * ``lag``: replication lag for ``lsn``, if applicable;
            * ``receive_lag``: lag of the receive LSN;
            * ``replay_lag``: lag of the replay LSN;

        * ``pause``: ``True`` if cluster is in maintenance mode;
        * ``scheduled_switchover``: if a switchover has been scheduled, then it contains this entry with these keys:

            * ``at``: timestamp when switchover was scheduled to occur;
            * ``from``: name of the member to be demoted;
            * ``to``: name of the member to be promoted.
    """
    from . import global_config
    from .postgresql.misc import format_lsn

    config = global_config.from_cluster(cluster)
    leader_name = cluster.leader.name if cluster.leader else None
    cluster_lsn = cluster.status.last_lsn

    ret: Dict[str, Any] = {'members': []}
    sync_role = 'quorum_standby' if config.is_quorum_commit_mode else 'sync_standby'
    for m in cluster.members:
        if m.name == leader_name:
            role = 'standby_leader' if config.is_standby_cluster else 'leader'
        elif config.is_synchronous_mode and cluster.sync.matches(m.name):
            role = sync_role
        else:
            role = 'replica'

        state = (m.data.get('replication_state', '') if role != 'leader' else '') or m.data.get('state', '')
        member = {'name': m.name, 'role': role, 'state': state, 'api_url': m.api_url}
        conn_kwargs = m.conn_kwargs()
        if conn_kwargs.get('host'):
            member['host'] = conn_kwargs['host']
            if conn_kwargs.get('port'):
                member['port'] = int(conn_kwargs['port'])
        optional_attributes = ('timeline', 'pending_restart', 'pending_restart_reason', 'scheduled_restart', 'tags')
        member.update({n: m.data[n] for n in optional_attributes if n in m.data})

        if m.name != leader_name:
            for location in ('receive_', 'replay_', ''):
                lsn_type, lag_type = f'{location}lsn', f'{location}lag'

                lsn = getattr(m, lsn_type)
                if not lsn:
                    member[lsn_type] = member[lag_type] = 'unknown'
                elif cluster_lsn >= lsn:
                    member[lag_type] = cluster_lsn - lsn
                    member[lsn_type] = format_lsn(lsn)
                else:
                    member[lag_type] = 0
                    member[lsn_type] = format_lsn(lsn)

        ret['members'].append(member)

    # sort members by name for consistency
    cmp: Callable[[Dict[str, Any]], bool] = lambda m: m['name']
    ret['members'].sort(key=cmp)
    if config.is_paused:
        ret['pause'] = True
    if cluster.failover and cluster.failover.scheduled_at:
        ret['scheduled_switchover'] = {'at': cluster.failover.scheduled_at.isoformat()}
        if cluster.failover.leader:
            ret['scheduled_switchover']['from'] = cluster.failover.leader
        if cluster.failover.candidate:
            ret['scheduled_switchover']['to'] = cluster.failover.candidate
    return ret


def is_subpath(d1: str, d2: str) -> bool:
    """Check if the file system path *d2* is contained within *d1* after resolving symbolic links.

    .. note::
        It will not check if the paths actually exist, it will only expand the paths and resolve any symbolic links
        that happen to be found.

    :param d1: path to a directory.
    :param d2: path to be checked if is within *d1*.

    :returns: ``True`` if *d1* is a subpath of *d2*.
    """
    real_d1 = os.path.realpath(d1) + os.path.sep
    real_d2 = os.path.realpath(os.path.join(real_d1, d2))
    return os.path.commonprefix([real_d1, real_d2 + os.path.sep]) == real_d1


def validate_directory(d: str, msg: str = "{} {}") -> None:
    """Ensure directory exists and is writable.

    .. note::
        If the directory does not exist, :func:`validate_directory` will attempt to create it.

    :param d: the directory to be checked.
    :param msg: a message to be thrown when raising :class:`~patroni.exceptions.PatroniException`, if any issue is
        faced. It must contain 2 placeholders to be used by :func:`format`:

            * The first placeholder will be replaced with path *d*;
            * The second placeholder will be replaced with the error condition.

    :raises:
        :class:`~patroni.exceptions.PatroniException`: if any issue is observed while validating *d*. Can be thrown if:

            * *d* did not exist, and :func:`validate_directory` was not able to create it; or
            * *d* is an existing directory, but Patroni is not able to write to that directory; or
            * *d* is an existing file, not a directory.
    """
    if not os.path.exists(d):
        try:
            os.makedirs(d)
        except OSError as e:
            logger.error(e)
            if e.errno != errno.EEXIST:
                raise PatroniException(msg.format(d, "couldn't create the directory"))
    elif os.path.isdir(d):
        try:
            fd, tmpfile = tempfile.mkstemp(dir=d)
            os.close(fd)
            os.remove(tmpfile)
        except OSError:
            raise PatroniException(msg.format(d, "the directory is not writable"))
    else:
        raise PatroniException(msg.format(d, "is not a directory"))


def data_directory_is_empty(data_dir: str) -> bool:
    """Check if a PostgreSQL data directory is empty.

    .. note::
        In non-Windows environments *data_dir* is also considered empty if it only contains hidden files and/or
        ``lost+found`` directory.

    :param data_dir: the PostgreSQL data directory to be checked.

    :returns: ``True`` if *data_dir* is empty.
    """
    if not os.path.exists(data_dir):
        return True
    return all(os.name != 'nt' and (n.startswith('.') or n == 'lost+found') for n in os.listdir(data_dir))


def apply_keepalive_limit(option: str, value: int) -> int:
    """
    Ensures provided *value* for keepalive *option* does not exceed the maximum allowed value for the current platform.

    :param option: The TCP keepalive option name. Possible values are:

            * ``TCP_USER_TIMEOUT``;
            * ``TCP_KEEPIDLE``;
            * ``TCP_KEEPINTVL``;
            * ``TCP_KEEPCNT``.

    :param value: The desired value for the keepalive option.

    :returns: maybe adjusted value.
    """
    max_of_options = {
        'linux': {'TCP_USER_TIMEOUT': 2147483647, 'TCP_KEEPIDLE': 32767, 'TCP_KEEPINTVL': 32767, 'TCP_KEEPCNT': 127},
        'darwin': {'TCP_KEEPIDLE': 4294967, 'TCP_KEEPINTVL': 4294967, 'TCP_KEEPCNT': 2147483647},
    }
    platform = 'linux' if sys.platform.startswith('linux') else sys.platform
    max_possible_value = max_of_options.get(platform, {}).get(option)
    if max_possible_value is not None and value > max_possible_value:
        logger.debug('%s changed from %d to %d.', option, value, max_possible_value)
        value = max_possible_value
    return value


def keepalive_intvl(timeout: int, idle: int, cnt: int = 3) -> int:
    """Calculate the value to be used as ``TCP_KEEPINTVL`` based on *timeout*, *idle*, and *cnt*.

    :param timeout: value for ``TCP_USER_TIMEOUT``.
    :param idle: value for ``TCP_KEEPIDLE``.
    :param cnt: value for ``TCP_KEEPCNT``.

    :returns: the value to be used as ``TCP_KEEPINTVL``.
    """
    intvl = max(1, int(float(timeout - idle) / cnt))
    return apply_keepalive_limit('TCP_KEEPINTVL', intvl)


def keepalive_socket_options(timeout: int, idle: int, cnt: int = 3) -> Iterator[Tuple[int, int, int]]:
    """Get all keepalive related options to be set in a socket.

    :param timeout: value for ``TCP_USER_TIMEOUT``.
    :param idle: value for ``TCP_KEEPIDLE``.
    :param cnt: value for ``TCP_KEEPCNT``.

    :yields: all keepalive related socket options to be set. The first item in the tuple is the protocol, the second
        item is the option, and the third item is the value to be used. The return values depend on the platform:

            * ``Windows``:
                * ``SO_KEEPALIVE``.
            * ``Linux``:
                * ``SO_KEEPALIVE``;
                * ``TCP_USER_TIMEOUT``;
                * ``TCP_KEEPIDLE``;
                * ``TCP_KEEPINTVL``;
                * ``TCP_KEEPCNT``.
            * ``MacOS``:
                * ``SO_KEEPALIVE``;
                * ``TCP_KEEPIDLE``;
                * ``TCP_KEEPINTVL``;
                * ``TCP_KEEPCNT``.
    """
    yield (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    if not (sys.platform.startswith('linux') or sys.platform.startswith('darwin')):
        return

    TCP_USER_TIMEOUT = getattr(socket, 'TCP_USER_TIMEOUT', None)
    if TCP_USER_TIMEOUT is not None:
        yield (socket.SOL_TCP, TCP_USER_TIMEOUT, apply_keepalive_limit('TCP_USER_TIMEOUT', int(timeout * 1000)))
    # The socket constants from MacOS netinet/tcp.h are not exported by python's
    # socket module, therefore we are using 0x10, 0x101, 0x102 constants.
    TCP_KEEPIDLE = getattr(socket, 'TCP_KEEPIDLE', 0x10 if sys.platform.startswith('darwin') else None)
    if TCP_KEEPIDLE is not None:
        idle = apply_keepalive_limit('TCP_KEEPIDLE', idle)
        yield (socket.IPPROTO_TCP, TCP_KEEPIDLE, idle)
    TCP_KEEPINTVL = getattr(socket, 'TCP_KEEPINTVL', 0x101 if sys.platform.startswith('darwin') else None)
    if TCP_KEEPINTVL is not None:
        intvl = keepalive_intvl(timeout, idle, cnt)
        yield (socket.IPPROTO_TCP, TCP_KEEPINTVL, intvl)
    TCP_KEEPCNT = getattr(socket, 'TCP_KEEPCNT', 0x102 if sys.platform.startswith('darwin') else None)
    if TCP_KEEPCNT is not None:
        cnt = apply_keepalive_limit('TCP_KEEPCNT', cnt)
        yield (socket.IPPROTO_TCP, TCP_KEEPCNT, cnt)


def enable_keepalive(sock: socket.socket, timeout: int, idle: int, cnt: int = 3) -> None:
    """Enable keepalive for *sock*.

    Will set socket options depending on the platform, as per return of :func:`keepalive_socket_options`.

    .. note::
        Value for ``TCP_KEEPINTVL`` will be calculated through :func:`keepalive_intvl` based on *timeout*, *idle*, and
        *cnt*.

    :param sock: the socket for which keepalive will be enabled.
    :param timeout: value for ``TCP_USER_TIMEOUT``.
    :param idle: value for ``TCP_KEEPIDLE``.
    :param cnt: value for ``TCP_KEEPCNT``.

    :returns: output of :func:`~socket.ioctl` if we are on Windows, nothing otherwise.
    """
    SIO_KEEPALIVE_VALS = getattr(socket, 'SIO_KEEPALIVE_VALS', None)
    if SIO_KEEPALIVE_VALS is not None:  # Windows
        intvl = keepalive_intvl(timeout, idle, cnt)
        sock.ioctl(SIO_KEEPALIVE_VALS, (1, idle * 1000, intvl * 1000))

    for opt in keepalive_socket_options(timeout, idle, cnt):
        sock.setsockopt(*opt)


def unquote(string: str) -> str:
    """Unquote a fully quoted *string*.

    :param string: The string to be checked for quoting.

    :returns: The string with quotes removed, if it is a fully quoted single string, or the original string if quoting
        is not detected, or unquoting was not possible.

    :Examples:

        A *string* with quotes will have those quotes removed

        >>> unquote('"a quoted string"')
        'a quoted string'

        A *string* with multiple quotes will be returned as is

        >>> unquote('"a multi" "quoted string"')
        '"a multi" "quoted string"'

        So will a *string* with unbalanced quotes

        >>> unquote('unbalanced "quoted string')
        'unbalanced "quoted string'
    """
    try:
        ret = split(string)
        ret = ret[0] if len(ret) == 1 else string
    except ValueError:
        ret = string
    return ret


def get_postgres_version(bin_dir: Optional[str] = None, bin_name: str = 'postgres') -> str:
    """Get full PostgreSQL version.

    It is based on the output of ``postgres --version``.

    :param bin_dir: path to the PostgreSQL binaries directory. If ``None`` or an empty string, it will use the first
                    *bin_name* binary that is found by the subprocess in the ``PATH``.
    :param bin_name: name of the postgres binary to call (``postgres`` by default)

    :returns: the PostgreSQL version.

    :raises:
        :exc:`~patroni.exceptions.PatroniException`: if the postgres binary call failed due to :exc:`OSError`.

    :Example:

        * Returns `9.6.24` for PostgreSQL 9.6.24
        * Returns `15.2` for PostgreSQL 15.2
    """
    if not bin_dir:
        binary = bin_name
    else:
        binary = os.path.join(bin_dir, bin_name)
    try:
        version = subprocess.check_output([binary, '--version']).decode()
    except OSError as e:
        raise PatroniException(f'Failed to get postgres version: {e}')
    version = re.match(r'^[^\s]+ [^\s]+ ((\d+)(\.\d+)*)', version)
    if TYPE_CHECKING:  # pragma: no cover
        assert version is not None
    version = version.groups()  # e.g., ('15.2', '15', '.2')
    major_version = int(version[1])
    dot_count = version[0].count('.')
    if major_version < 10 and dot_count < 2 or major_version >= 10 and dot_count < 1:
        return '.'.join((version[0], '0'))
    return version[0]


def get_major_version(bin_dir: Optional[str] = None, bin_name: str = 'postgres') -> str:
    """Get the major version of PostgreSQL.

    Like func:`get_postgres_version` but without minor version.

    :param bin_dir: path to the PostgreSQL binaries directory. If ``None`` or an empty string, it will use the first
                    *bin_name* binary that is found by the subprocess in the ``PATH``.
    :param bin_name: name of the postgres binary to call (``postgres`` by default)

    :returns: the PostgreSQL major version.

    :raises:
        :exc:`~patroni.exceptions.PatroniException`: if the postgres binary call failed due to :exc:`OSError`.

    :Example:

        * Returns `9.6` for PostgreSQL 9.6.24
        * Returns `15` for PostgreSQL 15.2
    """
    full_version = get_postgres_version(bin_dir, bin_name)
    return re.sub(r'\.\d+$', '', full_version)
