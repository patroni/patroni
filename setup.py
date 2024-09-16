#!/usr/bin/env python

"""
    Setup file for patroni
"""

import glob
import inspect
import logging
import os
import sys

from setuptools import Command, find_packages, setup

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))

NAME = 'patroni'
MAIN_PACKAGE = NAME
DESCRIPTION = 'PostgreSQL High-Available orchestrator and CLI'
LICENSE = 'The MIT License'
URL = 'https://github.com/patroni/patroni'
AUTHOR = 'Alexander Kukushkin, Polina Bungina'
AUTHOR_EMAIL = 'akukushkin@microsoft.com, polina.bungina@zalando.de'
KEYWORDS = 'etcd governor patroni postgresql postgres ha haproxy confd' +\
    ' zookeeper exhibitor consul streaming replication kubernetes k8s'

EXTRAS_REQUIRE = {'aws': ['boto3'], 'etcd': ['python-etcd'], 'etcd3': ['python-etcd'],
                  'consul': ['python-consul'], 'exhibitor': ['kazoo'], 'zookeeper': ['kazoo'],
                  'kubernetes': [], 'raft': ['pysyncobj', 'cryptography'], 'jsonlogger': ['python-json-logger']}

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: MIT License',
    'Operating System :: MacOS',
    'Operating System :: POSIX :: Linux',
    'Operating System :: POSIX :: BSD :: FreeBSD',
    'Operating System :: Microsoft :: Windows',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = ['patroni = patroni.__main__:main',
                   'patronictl = patroni.ctl:ctl',
                   'patroni_raft_controller = patroni.raft_controller:main',
                   "patroni_wale_restore = patroni.scripts.wale_restore:main",
                   "patroni_aws = patroni.scripts.aws:main",
                   "patroni_barman = patroni.scripts.barman.cli:main"]


class _Command(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class _Lint(_Command):

    def package_modules(self):
        package_dirs = self.distribution.package_dir or {}
        for package in self.distribution.packages or []:
            if package in package_dirs:
                yield package_dirs[package]
            elif '' in package_dirs:
                yield os.path.join(package_dirs[''], package)
            else:
                yield package

    def package_directories(self):
        for module in self.package_modules():
            yield module.replace('.', os.path.sep)

    def aux_directories(self):
        for dir_name in ('tests', 'features'):
            yield dir_name
            for root, dirs, files in os.walk(dir_name):
                for name in dirs:
                    yield os.path.join(root, name)

    def dirs_to_check(self):
        yield from self.package_directories()
        yield from self.aux_directories()

    def files_to_check(self):
        for path in self.dirs_to_check():
            for python_file in glob.iglob(os.path.join(path, '*.py')):
                yield python_file

        for filename in self.distribution.py_modules or []:
            yield f'{filename}.py'

        yield 'setup.py'


class Flake8(_Lint):

    def run(self):
        from flake8.main.cli import main

        logging.getLogger().setLevel(logging.ERROR)
        raise SystemExit(main(list(self.files_to_check())))


class ISort(_Lint):

    def run(self):
        from isort import api

        wrong_sorted_files = False
        for python_file in self.files_to_check():
            try:
                if not api.check_file(python_file, settings_path=__location__, show_diff=True):
                    wrong_sorted_files = True
            except OSError as error:
                logging.warning('Unable to parse file %s due to %r', python_file, error)
                wrong_sorted_files = True
        if wrong_sorted_files:
            sys.exit(1)


class PyTest(_Command):

    def run(self):
        try:
            import pytest
        except Exception:
            raise RuntimeError('py.test is not installed, run: pip install pytest')

        logging.getLogger().setLevel(logging.WARNING)

        args = ['--verbose', 'tests', '--doctest-modules', MAIN_PACKAGE] +\
            ['-s' if logging.getLogger().getEffectiveLevel() < logging.WARNING else '--capture=fd'] +\
            ['--cov', MAIN_PACKAGE, '--cov-report', 'term-missing', '--cov-report', 'xml']

        errno = pytest.main(args=args)
        sys.exit(errno)


def read(fname):
    with open(os.path.join(__location__, fname), encoding='utf-8') as fd:
        return fd.read()


def get_versions():
    old_modules = sys.modules.copy()
    try:
        from patroni import MIN_PSYCOPG2, MIN_PSYCOPG3
        from patroni.version import __version__
        return __version__, MIN_PSYCOPG2, MIN_PSYCOPG3
    finally:
        sys.modules.clear()
        sys.modules.update(old_modules)


def main():
    logging.basicConfig(format='%(message)s', level=os.getenv('LOGLEVEL', logging.WARNING))

    install_requires = []
    for r in read('requirements.txt').split('\n'):
        r = r.strip()
        if r == '':
            continue
        extra = False
        for e, deps in EXTRAS_REQUIRE.items():
            for i, v in enumerate(deps):
                if r.startswith(v):
                    deps[i] = r
                    EXTRAS_REQUIRE[e] = deps
                    extra = True
        if not extra:
            install_requires.append(r)

    # Just for convenience, if someone wants to install dependencies for all extras
    EXTRAS_REQUIRE['all'] = list({e for extras in EXTRAS_REQUIRE.values() for e in extras})

    patroni_version, min_psycopg2, min_psycopg3 = get_versions()

    # Make it possible to specify psycopg dependency as extra
    for name, version in {'psycopg[binary]': min_psycopg3, 'psycopg2': min_psycopg2, 'psycopg2-binary': None}.items():
        EXTRAS_REQUIRE[name] = [name + ('>=' + '.'.join(map(str, version)) if version else '')]
    EXTRAS_REQUIRE['psycopg3'] = EXTRAS_REQUIRE.pop('psycopg[binary]')

    setup(
        name=NAME,
        version=patroni_version,
        url=URL,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        description=DESCRIPTION,
        license=LICENSE,
        keywords=KEYWORDS,
        long_description=read('README.rst'),
        classifiers=CLASSIFIERS,
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={MAIN_PACKAGE: [
            "postgresql/available_parameters/*.yml",
            "postgresql/available_parameters/*.yaml",
        ]},
        install_requires=install_requires,
        extras_require=EXTRAS_REQUIRE,
        cmdclass={'test': PyTest, 'flake8': Flake8, 'isort': ISort},
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    main()
