import logging
import re

from patroni.exceptions import PostgresException

logger = logging.getLogger(__name__)


def postgres_version_to_int(pg_version):
    """Convert the server_version to integer

    >>> postgres_version_to_int('9.5.3')
    90503
    >>> postgres_version_to_int('9.3.13')
    90313
    >>> postgres_version_to_int('10.1')
    100001
    >>> postgres_version_to_int('10')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 10'
    >>> postgres_version_to_int('9.6')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 9.6'
    >>> postgres_version_to_int('a.b.c')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    PostgresException: 'Invalid PostgreSQL version: a.b.c'
    """

    try:
        components = list(map(int, pg_version.split('.')))
    except ValueError:
        raise PostgresException('Invalid PostgreSQL version: {0}'.format(pg_version))

    if len(components) < 2 or len(components) == 2 and components[0] < 10 or len(components) > 3:
        raise PostgresException('Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: {0}'.format(pg_version))

    if len(components) == 2:
        # new style verion numbers, i.e. 10.1 becomes 100001
        components.insert(1, 0)

    return int(''.join('{0:02d}'.format(c) for c in components))


def postgres_major_version_to_int(pg_version):
    """
    >>> postgres_major_version_to_int('10')
    100000
    >>> postgres_major_version_to_int('9.6')
    90600
    """
    return postgres_version_to_int(pg_version + '.0')


def parse_lsn(lsn):
    t = lsn.split('/')
    return int(t[0], 16) * 0x100000000 + int(t[1], 16)


def parse_history(data):
    for line in data.split('\n'):
        values = line.strip().split('\t')
        if len(values) == 3:
            try:
                values[0] = int(values[0])
                values[1] = parse_lsn(values[1])
                yield values
            except (IndexError, ValueError):
                logger.exception('Exception when parsing timeline history line "%s"', values)


def pairwise(seq):
    it = iter(list(seq)+[None])
    return zip(it, it)


sync_rep_parser_re = re.compile(r"""
           (?P<first> [fF][iI][rR][sS][tT] )
         | (?P<any> [aA][nN][yY] )
         | (?P<space> \s+ )
         | (?P<ident> [A-Za-z_][A-Za-z_0-9\$]* )
         | (?P<dquot> " (?: [^"]+ | "" )* " )
         | (?P<star> [*] )
         | (?P<num> \d+ )
         | (?P<comma> , )
         | (?P<parenstart> \( )
         | (?P<parenend> \) )
         | (?P<JUNK> . )
        """, re.X)


def parse_sync_standby_names(sync_standby_names):
    """Parse postgresql synchronous_standby_names to constituent parts.

    Returns dict with the following keys:
    * type: 'quorum'|'priority'
    * num: int
    * members: list[str]
    * has_star: bool - Present if true

    If the configuration value can not be parsed, raises a ValueError.
    """
    tokens = [(m.lastgroup, m.group(0), m.start())
              for m in sync_rep_parser_re.finditer(sync_standby_names)
              if m.lastgroup != 'space']
    if not tokens:
        return {'type': 'off', 'num': 0, 'members': []}

    if [t[0] for t in tokens[0:3]] == ['any', 'num', 'parenstart'] and tokens[-1][0] == 'parenend':
        result = {'type': 'quorum', 'num': int(tokens[1][1])}
        synclist = tokens[3:-1]
    elif [t[0] for t in tokens[0:3]] == ['first', 'num', 'parenstart'] and tokens[-1][0] == 'parenend':
        result = {'type': 'priority', 'num': int(tokens[1][1])}
        synclist = tokens[3:-1]
    elif [t[0] for t in tokens[0:2]] == ['num', 'parenstart'] and tokens[-1][0] == 'parenend':
        result = {'type': 'priority', 'num': int(tokens[0][1])}
        synclist = tokens[2:-1]
    else:
        result = {'type': 'priority', 'num': 1}
        synclist = tokens
    result['members'] = []
    for (a_type, a_value, a_pos), token_b in pairwise(synclist):
        if a_type in ['ident', 'num']:
            result['members'].append(a_value)
        elif a_type == 'star':
            result['members'].append(a_value)
            result['has_star'] = True
        elif a_type == 'dquot':
            result['members'].append(a_value[1:-1].replace('""', '"'))
        else:
            raise ValueError("Unparseable synchronous_standby_names value %r: Unexpected token %s %r at %d" %
                             (sync_standby_names, a_type, a_value, a_pos))

    return result
