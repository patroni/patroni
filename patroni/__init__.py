import sys

PATRONI_ENV_PREFIX = 'PATRONI_'
KUBERNETES_ENV_PREFIX = 'KUBERNETES_'
MIN_PSYCOPG2 = (2, 5, 4)


def fatal(string, *args):
    sys.stderr.write('FATAL: ' + string.format(*args) + '\n')
    sys.exit(1)


def parse_version(version):
    def _parse_version(version):
        for e in version.split('.'):
            try:
                yield int(e)
            except ValueError:
                break
    return tuple(_parse_version(version.split(' ')[0]))


# We pass MIN_PSYCOPG2 and parse_version as arguments to simplify usage of check_psycopg from the setup.py
def check_psycopg(_min_psycopg2=MIN_PSYCOPG2, _parse_version=parse_version):
    min_psycopg2_str = '.'.join(map(str, _min_psycopg2))

    try:
        from psycopg2 import __version__
        if _parse_version(__version__) >= _min_psycopg2:
            return
        version_str = __version__.split(' ')[0]
    except ImportError:
        version_str = None

    try:
        from psycopg import __version__
    except ImportError:
        error = 'Patroni requires psycopg2>={0}, psycopg2-binary, or psycopg>=3.0'.format(min_psycopg2_str)
        if version_str:
            error += ', but only psycopg2=={0} is available'.format(version_str)
        fatal(error)
