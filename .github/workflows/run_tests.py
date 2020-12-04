import os
import shutil
import subprocess
import sys
import tempfile


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')

    if what == 'all':
        flake8 = subprocess.call([sys.executable, 'setup.py', 'flake8'])
        test = subprocess.call([sys.executable, 'setup.py', 'test'])
        version = '.'.join(map(str, sys.version_info[:2]))
        shutil.move('.coverage', os.path.join(tempfile.gettempdir(), '.coverage.' + version))
        return flake8 | test
    elif what == 'combine':
        tmp = tempfile.gettempdir()
        for name in os.listdir(tmp):
            if name.startswith('.coverage.'):
                shutil.move(os.path.join(tmp, name), name)
        return subprocess.call([sys.executable, '-m', 'coverage', 'combine'])

    env = os.environ.copy()
    if sys.platform.startswith('linux'):
        version = {'etcd': '9.6', 'etcd3': '9.6', 'consul': 10, 'exhibitor': 11, 'kubernetes': 12, 'raft': 13}.get(what)
        path = '/usr/lib/postgresql/{0}/bin:.'.format(version)
        unbuffer = ['timeout', '600', 'unbuffer']
    else:
        path = os.path.abspath(os.path.join('pgsql', 'bin'))
        if sys.platform == 'darwin':
            path += ':.'
        unbuffer = []
    env['PATH'] = path + os.pathsep + env['PATH']
    env['DCS'] = what

    ret = subprocess.call(unbuffer + [sys.executable, '-m', 'behave'], env=env)

    if ret != 0:
        if subprocess.call('grep . features/output/*_failed/*postgres?.*', shell=True) != 0:
            subprocess.call('grep . features/output/*/*postgres?.*', shell=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
