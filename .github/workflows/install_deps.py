import inspect
import os
import shutil
import subprocess
import stat
import sys
import tarfile
import zipfile


def install_requirements(what):
    old_path = sys.path[:]
    w = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))
    sys.path.insert(0, os.path.dirname(os.path.dirname(w)))
    try:
        from setup import EXTRAS_REQUIRE, read
    finally:
        sys.path = old_path
    requirements = ['mock>=2.0.0', 'flake8', 'pytest', 'pytest-cov'] if what == 'all' else ['behave']
    requirements += ['coverage']
    # try to split tests between psycopg2 and psycopg3
    requirements += ['psycopg[binary]'] if sys.version_info >= (3, 8, 0) and\
        (sys.platform != 'darwin' or what == 'etcd3') else ['psycopg2-binary']
    for r in read('requirements.txt').split('\n'):
        r = r.strip()
        if r != '':
            extras = {e for e, v in EXTRAS_REQUIRE.items() if v and any(r.startswith(x) for x in v)}
            if not extras or what == 'all' or what in extras:
                requirements.append(r)

    subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
    subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'wheel'])
    r = subprocess.call([sys.executable, '-m', 'pip', 'install'] + requirements)
    s = subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'setuptools'])
    return s | r


def install_packages(what):
    from mapping import versions

    packages = {
        'zookeeper': ['zookeeper', 'zookeeper-bin', 'zookeeperd'],
        'consul': ['consul'],
    }
    packages['exhibitor'] = packages['zookeeper']
    packages = packages.get(what, [])
    ver = versions.get(what)
    if float(ver) >= 15:
        packages += ['postgresql-{0}-citus-12.1'.format(ver)]
    subprocess.call(['sudo', 'apt-get', 'update', '-y'])
    return subprocess.call(['sudo', 'apt-get', 'install', '-y', 'postgresql-' + ver, 'expect-dev'] + packages)


def get_file(url, name):
    try:
        from urllib.request import urlretrieve
    except ImportError:
        from urllib import urlretrieve

    print('Downloading ' + url)
    urlretrieve(url, name)


def untar(archive, name):
    with tarfile.open(archive) as tar:
        f = tar.extractfile(name)
        dest = os.path.basename(name)
        with open(dest, 'wb') as d:
            shutil.copyfileobj(f, d)
            return dest


def unzip(archive, name):
    with zipfile.ZipFile(archive, 'r') as z:
        name = z.extract(name)
        dest = os.path.basename(name)
        shutil.move(name, dest)
        return dest


def unzip_all(archive):
    print('Extracting ' + archive)
    with zipfile.ZipFile(archive, 'r') as z:
        z.extractall()


def chmod_755(name):
    os.chmod(name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
             | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


def unpack(archive, name):
    print('Extracting {0} from {1}'.format(name, archive))
    func = unzip if archive.endswith('.zip') else untar
    name = func(archive, name)
    chmod_755(name)
    return name


def install_etcd():
    version = os.environ.get('ETCDVERSION', '3.4.23')
    platform = {'linux2': 'linux', 'win32': 'windows', 'cygwin': 'windows'}.get(sys.platform, sys.platform)
    dirname = 'etcd-v{0}-{1}-amd64'.format(version, platform)
    ext = 'tar.gz' if platform == 'linux' else 'zip'
    name = '{0}.{1}'.format(dirname, ext)
    url = 'https://github.com/etcd-io/etcd/releases/download/v{0}/{1}'.format(version, name)
    get_file(url, name)
    ext = '.exe' if platform == 'windows' else ''
    return int(unpack(name, '{0}/etcd{1}'.format(dirname, ext)) is None)


def install_postgres():
    version = os.environ.get('PGVERSION', '16.1-1')
    platform = {'darwin': 'osx', 'win32': 'windows-x64', 'cygwin': 'windows-x64'}[sys.platform]
    if platform == 'osx':
        return subprocess.call(['brew', 'install', 'expect', 'postgresql@{0}'.format(version.split('.')[0])])
    name = 'postgresql-{0}-{1}-binaries.zip'.format(version, platform)
    get_file('http://get.enterprisedb.com/postgresql/' + name, name)
    unzip_all(name)
    bin_dir = os.path.join('pgsql', 'bin')
    for f in os.listdir(bin_dir):
        chmod_755(os.path.join(bin_dir, f))
    return subprocess.call(['pgsql/bin/postgres', '-V'])


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')

    if what != 'all':
        if sys.platform.startswith('linux'):
            r = install_packages(what)
        else:
            r = install_postgres()

        if r == 0 and what.startswith('etcd'):
            r = install_etcd()

        if r != 0:
            return r

    return install_requirements(what)


if __name__ == '__main__':
    sys.exit(main())
