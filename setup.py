#!/usr/bin/env python

"""
    Setup file for patroni
"""

import inspect
import os
import sys

from setuptools.command.test import test as TestCommand
from setuptools import find_packages, setup

if sys.version_info < (2, 7, 0):
    sys.stderr.write('FATAL: patroni needs to be run with Python 2.7+\n')
    sys.exit(1)

__location__ = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))


def read_version(package):
    data = {}
    with open(os.path.join(package, 'version.py'), 'r') as fd:
        exec(fd.read(), data)
    return data['__version__']


NAME = 'patroni'
MAIN_PACKAGE = NAME
SCRIPTS = 'scripts'
VERSION = read_version(MAIN_PACKAGE)
DESCRIPTION = 'PostgreSQL High-Available orchestrator and CLI'
LICENSE = 'The MIT License'
URL = 'https://github.com/zalando/patroni'
AUTHOR = 'Alexander Kukushkin, Oleksii Kliukin, Feike Steenbergen'
AUTHOR_EMAIL = 'alexander.kukushkin@zalando.de, oleksii.kliukin@zalando.de, feike.steenbergen@zalando.de'
KEYWORDS = 'etcd governor patroni postgresql postgres ha haproxy confd zookeeper exhibitor consul streaming replication'

COVERAGE_XML = True
COVERAGE_HTML = False
JUNIT_XML = True

# Add here all kinds of additional classifiers as defined under
# https://pypi.python.org/pypi?%3Aaction=list_classifiers
CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: Implementation :: CPython',
]

CONSOLE_SCRIPTS = ['patroni = patroni:main',
                   'patronictl = patroni.ctl:ctl',
                   "patroni_wale_restore = patroni.scripts.wale_restore:main",
                   "patroni_aws = patroni.scripts.aws:main"]


class PyTest(TestCommand):

    user_options = [('cov=', None, 'Run coverage'), ('cov-xml=', None, 'Generate junit xml report'), ('cov-html=',
                    None, 'Generate junit html report'), ('junitxml=', None, 'Generate xml of test results')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.cov_xml = False
        self.cov_html = False
        self.junitxml = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        if self.cov_xml or self.cov_html:
            self.cov = ['--cov', MAIN_PACKAGE, '--cov-report', 'term-missing']
            if self.cov_xml:
                self.cov.extend(['--cov-report', 'xml'])
            if self.cov_html:
                self.cov.extend(['--cov-report', 'html'])
        if self.junitxml is not None:
            self.junitxml = ['--junitxml', self.junitxml]

    def run_tests(self):
        try:
            import pytest
        except Exception:
            raise RuntimeError('py.test is not installed, run: pip install pytest')
        params = {'args': self.test_args}
        if self.cov:
            params['args'] += self.cov
        if self.junitxml:
            params['args'] += self.junitxml
        params['args'] += ['--doctest-modules', MAIN_PACKAGE, '-vv']

        import logging
        silence = logging.WARNING
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=os.getenv('LOGLEVEL', silence))
        params['args'] += ['-s' if logging.getLogger().getEffectiveLevel() < silence else '--capture=fd']
        errno = pytest.main(**params)
        sys.exit(errno)


def get_install_requirements(path):
    content = open(os.path.join(__location__, path)).read()
    return [req for req in content.split('\n') if req != '']


def read(fname):
    return open(os.path.join(__location__, fname)).read()


def setup_package():
    # Assemble additional setup commands
    cmdclass = {'test': PyTest}

    # Some helper variables
    version = os.getenv('GO_PIPELINE_LABEL', VERSION)

    install_requires = []
    extras_require = {'aws': ['boto'], 'etcd': ['python-etcd'], 'consul': ['python-consul'],
                      'exhibitor': ['kazoo'], 'zookeeper': ['kazoo'], 'kubernetes': ['kubernetes']}

    for r in get_install_requirements('requirements.txt'):
        extra = False
        for e, v in extras_require.items():
            if r.startswith(v[0]):
                extras_require[e] = [r]
                extra = True
        if not extra:
            install_requires.append(r)

    command_options = {'test': {'test_suite': ('setup.py', 'tests')}}
    if JUNIT_XML:
        command_options['test']['junitxml'] = 'setup.py', 'junit.xml'
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
        test_suite='tests',
        packages=find_packages(exclude=['tests', 'tests.*']),
        package_data={MAIN_PACKAGE: ["*.json"]},
        install_requires=install_requires,
        extras_require=extras_require,
        cmdclass=cmdclass,
        tests_require=['flake8', 'mock>=2.0.0', 'pytest-cov', 'pytest'],
        command_options=command_options,
        entry_points={'console_scripts': CONSOLE_SCRIPTS},
    )


if __name__ == '__main__':
    setup_package()
