"""Define general variables and functions for :mod:`patroni`.

:var PATRONI_ENV_PREFIX: prefix for Patroni related configuration environment variables.
:var KUBERNETES_ENV_PREFIX: prefix for Kubernetes related configuration environment variables.
:var MIN_PSYCOPG2: minimum version of :mod:`psycopg2` required by Patroni to work.
"""

import sys

from typing import Any, Callable, Iterator, Tuple

PATRONI_ENV_PREFIX = 'PATRONI_'
KUBERNETES_ENV_PREFIX = 'KUBERNETES_'
MIN_PSYCOPG2 = (2, 5, 4)


def fatal(string: str, *args: Any) -> None:
    """Write a fatal message to stderr and exit with code ``1``.

    :param string: message to be written before exiting.
    """
    sys.exit('FATAL: ' + string.format(*args))


def parse_version(version: str) -> Tuple[int, ...]:
    """Convert *version* from human-readable format to tuple of integers.

    .. note::
        Designed for easy comparison of software versions in Python.

    :param version: human-readable software version, e.g. ``2.5.4``.

    :returns: tuple of *version* parts, each part as an integer.

    :Example:

        >>> parse_version('2.5.4')
        (2, 5, 4)
    """
    def _parse_version(version: str) -> Iterator[int]:
        """Yield each part of a human-readable version string as an integer.

        :param version: human-readable software version, e.g. ``2.5.4``.

        :yields: each part of *version* as an integer.

        :Example:

            >>> tuple(_parse_version('2.5.4'))
            (2, 5, 4)
        """
        for e in version.split('.'):
            try:
                yield int(e)
            except ValueError:
                break
    return tuple(_parse_version(version.split(' ')[0]))


def check_psycopg(_min_psycopg2: Tuple[int, ...] = MIN_PSYCOPG2,
                  _parse_version: Callable[[str], Tuple[int, ...]] = parse_version) -> None:
    """Ensure at least one among :mod:`psycopg2` or :mod:`psycopg` libraries are available in the environment.

    .. note::
        We pass ``MIN_PSYCOPG2`` and :func:`parse_version` as arguments to simplify usage of :func:`check_psycopg` from
        the ``setup.py``.

    .. note::
        Patroni chooses :mod:`psycopg2` over :mod:`psycopg`, if possible.

        If nothing meeting the requirements is found, then exit with a fatal message.

    :param _min_psycopg2: minimum required version in case :mod:`psycopg2` is chosen.
    :param _parse_version: function used to parse :mod:`psycopg2`/:mod:`psycopg` version into a comparable object.
    """
    min_psycopg2_str = '.'.join(map(str, _min_psycopg2))

    # try psycopg2
    try:
        from psycopg2 import __version__
        if _parse_version(__version__) >= _min_psycopg2:
            return
        version_str = __version__.split(' ')[0]
    except ImportError:
        version_str = None

    # try psycopg3
    try:
        from psycopg import __version__
    except ImportError:
        error = 'Patroni requires psycopg2>={0}, psycopg2-binary, or psycopg>=3.0'.format(min_psycopg2_str)
        if version_str is not None:
            error += ', but only psycopg2=={0} is available'.format(version_str)
        fatal(error)
