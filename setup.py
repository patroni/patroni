#!/usr/bin/env python

"""
    Setup file for patroni
"""

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
URL = 'https://github.com/zalando/patroni'
AUTHOR = 'Alexander Kukushkin, Polina Bungina'
AUTHOR_EMAIL = 'akukushkin@microsoft.com, polina.bungina@zalando.de'
KEYWORDS = 'etcd governor patroni postgresql postgres ha haproxy confd' +\
    ' zookeeper exhibitor consul streaming replication kubernetes k8s'

EXTRAS_REQUIRE = {'aws': ['boto3'], 'etcd': ['python-etcd'], 'etcd3': ['python-etcd'],
                  'consul': ['python-consul'], 'exhibitor': ['kazoo'], 'zookeeper': ['kazoo'],
                  'kubernetes': [], 'raft': ['pysyncobj', 'cryptography']}
COVERAGE_XML = True

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
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = ['patroni = patroni.__main__:main',
                   'patronictl = patroni.ctl:ctl',
                   'patroni_raft_controller = patroni.raft_controller:main',
                   "patroni_wale_restore = patroni.scripts.wale_restore:main",
                   "patroni_aws = patroni.scripts.aws:main"]


class _Command(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class Flake8(_Command):

    def package_files(self):
        seen_package_directories = ()
        directories = self.distribution.package_dir or {}
        empty_directory_exists = "" in directories
        packages = self.distribution.packages or []
        for package in packages:
            if package in directories:
                package_directory = directories[package]
            elif empty_directory_exists:
                package_directory = os.path.join(directories[""], package)
            else:
                package_directory = package

            if not package_directory.startswith(seen_package_directories):
                seen_package_directories += (package_directory + ".",)
                yield package_directory

    def targets(self):
        return [package for package in self.package_files()] + ['tests', 'setup.py']

    def run(self):
        from flake8.main.cli import main

        logging.getLogger().setLevel(logging.ERROR)
        main(self.targets())


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
    with open(os.path.join(__location__, fname)) as fd:
        return fd.read()


def setup_package(version):
    logging.basicConfig(format='%(message)s', level=os.getenv('LOGLEVEL', logging.WARNING))

    # Assemble additional setup commands
    cmdclass = {'test': PyTest, 'flake8': Flake8}

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
                    break
            if extra:
                break
        if not extra:
            install_requires.append(r)

    setup(
        name=NAME,
        version=version,
        url=URL,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        description=DESCRIPTION,
        license=LICENSE,
        keywords=KEYWORDS,
        long_description=read('README.rst'),
        classifiers=CLASSIFIERS,
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={MAIN_PACKAGE: ["*.json"]},
        install_requires=install_requires,
        extras_require=EXTRAS_REQUIRE,
        cmdclass=cmdclass,
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    old_modules = sys.modules.copy()
    try:
        from patroni import check_psycopg
        from patroni.version import __version__
    finally:
        sys.modules.clear()
        sys.modules.update(old_modules)

    check_psycopg()

    setup_package(__version__)
