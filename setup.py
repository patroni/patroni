#!/usr/bin/env python

"""
    Setup file for patroni
"""

import inspect
import os
import sys

from setuptools import Command, find_packages, setup

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))

NAME = 'patroni'
MAIN_PACKAGE = NAME
DESCRIPTION = 'PostgreSQL High-Available orchestrator and CLI'
LICENSE = 'The MIT License'
URL = 'https://github.com/zalando/patroni'
AUTHOR = 'Alexander Kukushkin, Dmitrii Dolgov, Oleksii Kliukin'
AUTHOR_EMAIL = 'alexander.kukushkin@zalando.de, dmitrii.dolgov@zalando.de, alexk@hintbits.com'
KEYWORDS = 'etcd governor patroni postgresql postgres ha haproxy confd' +\
    ' zookeeper exhibitor consul streaming replication kubernetes k8s'

EXTRAS_REQUIRE = {'aws': ['boto'], 'etcd': ['python-etcd'], 'etcd3': ['python-etcd'],
                  'consul': ['python-consul'], 'exhibitor': ['kazoo'], 'zookeeper': ['kazoo'],
                  'kubernetes': [], 'raft': ['pysyncobj', 'cryptography']}
COVERAGE_XML = True
COVERAGE_HTML = False

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
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = ['patroni = patroni:main',
                   'patronictl = patroni.ctl:ctl',
                   'patroni_raft_controller = patroni.raft_controller:main',
                   "patroni_wale_restore = patroni.scripts.wale_restore:main",
                   "patroni_aws = patroni.scripts.aws:main"]


class Flake8(Command):

    user_options = []

    def initialize_options(self):
        from flake8.main import application

        self.flake8 = application.Application()
        self.flake8.initialize([])

    def finalize_options(self):
        pass

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
        self.flake8.run_checks(self.targets())
        self.flake8.formatter.start()
        self.flake8.report_errors()
        self.flake8.report_statistics()
        self.flake8.report_benchmarks()
        self.flake8.formatter.stop()
        try:
            self.flake8.exit()
        except SystemExit as e:
            # Cause system exit only if exit code is not zero (terminates
            # other possibly remaining/pending setuptools commands).
            if e.code:
                raise


class PyTest(Command):

    user_options = [('cov=', None, 'Run coverage'), ('cov-xml=', None, 'Generate junit xml report'),
                    ('cov-html=', None, 'Generate junit html report')]

    def initialize_options(self):
        self.cov = []
        self.cov_xml = False
        self.cov_html = False

    def finalize_options(self):
        if self.cov_xml or self.cov_html:
            self.cov = ['--cov', MAIN_PACKAGE, '--cov-report', 'term-missing']
            if self.cov_xml:
                self.cov.extend(['--cov-report', 'xml'])
            if self.cov_html:
                self.cov.extend(['--cov-report', 'html'])

    def run_tests(self):
        try:
            import pytest
        except Exception:
            raise RuntimeError('py.test is not installed, run: pip install pytest')

        import logging
        silence = logging.WARNING
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=os.getenv('LOGLEVEL', silence))

        args = ['--verbose', 'tests', '--doctest-modules', MAIN_PACKAGE] +\
            ['-s' if logging.getLogger().getEffectiveLevel() < silence else '--capture=fd']
        if self.cov:
            args += self.cov

        errno = pytest.main(args=args)
        sys.exit(errno)

    def run(self):
        from pkg_resources import evaluate_marker

        requirements = set(self.distribution.install_requires + ['mock>=2.0.0', 'pytest-cov', 'pytest'])
        for k, v in self.distribution.extras_require.items():
            if not k.startswith(':') or evaluate_marker(k[1:]):
                requirements.update(v)

        self.distribution.fetch_build_eggs(list(requirements))
        self.run_tests()


def read(fname):
    with open(os.path.join(__location__, fname)) as fd:
        return fd.read()


def setup_package(version):
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

    command_options = {'test': {}}
    if COVERAGE_XML:
        command_options['test']['cov_xml'] = 'setup.py', True
    if COVERAGE_HTML:
        command_options['test']['cov_html'] = 'setup.py', True

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
        python_requires='>=2.7',
        install_requires=install_requires,
        extras_require=EXTRAS_REQUIRE,
        setup_requires='flake8',
        cmdclass=cmdclass,
        command_options=command_options,
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    old_modules = sys.modules.copy()
    try:
        from patroni import check_psycopg2, fatal, __version__
    finally:
        sys.modules.clear()
        sys.modules.update(old_modules)

    if sys.version_info < (2, 7, 0):
        fatal('Patroni needs to be run with Python 2.7+')
    check_psycopg2()

    setup_package(__version__)
