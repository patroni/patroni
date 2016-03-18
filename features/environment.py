import abc
import etcd
import kazoo.client
import kazoo.exceptions
import os
import psycopg2
import shutil
import subprocess
import tempfile
import time
import yaml


class AbstractController(object):

    def __init__(self, name, work_directory, output_dir):
        self._name = name
        self._work_directory = work_directory
        self._output_dir = output_dir
        self._handle = None
        self._log = None

    @abc.abstractmethod
    def _is_running(self):
        """:returns: `True` if process is already running"""

    @abc.abstractmethod
    def _start(self):
        """start process"""

    def start(self):
        if self._is_running():
            return True

        self._log = open(os.path.join(self._output_dir, self._name + '.log'), 'a')
        self._handle = self._start()

        assert self._handle and self._handle.pid and self._handle.poll() is None,\
            "Process {0} is not running after being started".format(self._name)

    def stop(self, kill=False, timeout=15):
        """ terminate process and wipe out the temp work directory, but only if we actually started it"""
        term = False
        start_time = time.time()

        while self._is_running() and self._handle:
            if not kill:
                if not term:
                    self._handle.terminate()
                    term = False
            else:
                self._handle.kill()
            time.sleep(1)
            if not kill and time.time() - start_time > timeout:
                kill = True

        if self._log and not self._log.closed:
            self._log.close()


class PatroniController(AbstractController):
    PATRONI_CONFIG = '{}.yml'
    """ starts and stops individual patronis"""

    def __init__(self, dcs, name, work_directory, output_dir, tags=None):
        super(PatroniController, self).__init__('patroni_' + name, work_directory, output_dir)
        self._connstring = None
        self._config = self._make_patroni_test_config(name, dcs, tags)
        self._data_dir = os.path.join(work_directory, 'data')

        self._conn = None
        self._curs = None

    def write_label(self, content):
        with open(os.path.join(self._data_dir, 'label'), 'w') as f:
            f.write(content)

    def read_label(self):
        try:
            with open(os.path.join(self._data_dir, 'label'), 'r') as f:
                return f.read().strip()
        except IOError:
            pass
        return None

    def _is_running(self):
        return self._handle and self._handle.pid and self._handle.poll() is None

    def _start(self):
        return subprocess.Popen(['coverage', 'run', '--source=patroni', '-p', 'patroni.py', self._config],
                                stdout=self._log, stderr=subprocess.STDOUT, cwd=self._work_directory)

    def start(self, max_wait_limit):
        super(PatroniController, self).start()
        # wait while patroni is available for queries, but not more than 10 seconds.
        for _ in range(max_wait_limit):
            if self.query("SELECT 1", fail_ok=True) is not None:
                break
            time.sleep(1)
        else:
            assert False,\
                "Patroni instance is not available for queries after {0} seconds".format(max_wait_limit)

    def _make_patroni_test_config(self, name, dcs, tags):
        patroni_config_name = self.PATRONI_CONFIG.format(name)
        patroni_config_path = os.path.join(self._output_dir, patroni_config_name)

        with open(patroni_config_name) as f:
            config = yaml.load(f)

        self._connstring = self._make_connstring(config)

        config['postgresql'].update({'name': name, 'data_dir': 'data/' + name})
        config['postgresql']['parameters'].update({
            'logging_collector': 'on', 'log_destination': 'csvlog', 'log_directory': self._output_dir,
            'log_filename': name + '.log', 'log_statement': 'all', 'log_min_messages': 'debug1'})

        if tags:
            config['tags'] = tags

        if dcs != 'etcd':
            etcd = config.pop('etcd')
            if dcs == 'zookeeper':
                config['zookeeper'] = {'session_timeout': etcd['ttl'], 'reconnect_timeout': config['loop_wait'],
                                       'scope': etcd['scope'], 'exhibitor': {'hosts': ['127.0.0.1'], 'port': 8181}}

        with open(patroni_config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return patroni_config_path

    @staticmethod
    def _make_connstring(config):
        connstring = config['postgresql']['connect_address']
        if ':' in connstring:
            address, port = connstring.split(':')
        else:
            address = connstring
            port = '5432'
        return 'host={0} port={1} dbname=postgres user=postgres'.format(address, port)

    def _connection(self):
        if not self._conn or self._conn.closed != 0:
            self._conn = psycopg2.connect(self._connstring)
            self._conn.autocommit = True
        return self._conn

    def _cursor(self):
        if not self._curs or self._curs.closed or self._curs.connection.closed != 0:
            self._curs = self._connection().cursor()
        return self._curs

    def query(self, query, fail_ok=False):
        try:
            cursor = self._cursor()
            cursor.execute(query)
            return cursor
        except psycopg2.Error:
            if not fail_ok:
                raise
            return None

    def check_role_has_changed_to(self, new_role, timeout=10):
        bound_time = time.time() + timeout
        recovery_status = False if new_role == 'primary' else True
        role_has_changed = False
        while not role_has_changed:
            cur = self.query("SELECT pg_is_in_recovery()", fail_ok=True)
            if cur:
                row = cur.fetchone()
                if row and len(row) > 0 and row[0] == recovery_status:
                    role_has_changed = True
            if time.time() > bound_time:
                break
            time.sleep(1)
        return role_has_changed


class PatroniPoolController(object):

    def __init__(self):
        self._dcs = None
        self._output_dir = None
        self._patroni_path = None
        self._processes = {}
        self.create_and_set_output_directory('')

    @property
    def patroni_path(self):
        if self._patroni_path is None:
            cwd = os.path.realpath(__file__)
            while True:
                path, entry = os.path.split(cwd)
                cwd = path
                if entry == 'features' or cwd == '/':
                    break
            self._patroni_path = cwd
        return self._patroni_path

    @property
    def output_dir(self):
        return self._output_dir

    def start(self, pg_name, max_wait_limit=20, tags=None):
        if pg_name not in self._processes:
            self._processes[pg_name] = PatroniController(self.dcs, pg_name, self.patroni_path, self._output_dir, tags)
        self._processes[pg_name].start(max_wait_limit)

    def __getattr__(self, func):
        if func not in ['stop', 'query', 'write_label', 'read_label', 'check_role_has_changed_to']:
            raise AttributeError("PatroniPoolController instance has no attribute '{0}'".format(func))

        def wrapper(pg_name, *args, **kwargs):
            proc = self._processes.get(pg_name)
            if proc:
                return getattr(proc, func)(*args, **kwargs)
        return wrapper

    def stop_all(self):
        for ctl in self._processes.values():
            ctl.stop()
        self._processes.clear()

    def create_and_set_output_directory(self, feature_name):
        feature_dir = os.path.join(self.patroni_path, "features", "output", feature_name.replace(' ', '_'))
        if os.path.exists(feature_dir):
            shutil.rmtree(feature_dir)
        os.makedirs(feature_dir)
        self._output_dir = feature_dir

    @property
    def dcs(self):
        if self._dcs is None:
            self._dcs = os.environ.get('DCS', 'etcd')
            assert self._dcs in ['etcd', 'zookeeper'], 'Unsupported dcs: ' + self.dcs
        return self._dcs


class DcsController(AbstractController):

    _CLUSTER_NODE = 'service/batman'

    def __init__(self, name, output_dir):
        super(DcsController, self).__init__(name, tempfile.mkdtemp(), output_dir)

    def stop_and_remove_work_directory(self, kill=False, timeout=15):
        self.stop(kill, timeout)
        if self._work_directory:
            shutil.rmtree(self._work_directory)

    @abc.abstractmethod
    def query(self, key):
        """ query for a value of a given key """

    @abc.abstractmethod
    def cleanup_service_tree(self):
        """ clean all contents stored in the tree used for the tests """


class EtcdController(DcsController):

    """ handles all etcd related tasks, used for the tests setup and cleanup """

    def __init__(self, output_dir):
        super(EtcdController, self).__init__('etcd', output_dir)
        self._client = etcd.Client()

    def _start(self):
        return subprocess.Popen(["etcd", "--debug", "--data-dir", self._work_directory],
                                stdout=self._log, stderr=subprocess.STDOUT)

    def query(self, key):
        try:
            return self._client.get('/{0}/{1}'.format(self._CLUSTER_NODE, key)).value
        except etcd.EtcdKeyNotFound:
            return None

    def cleanup_service_tree(self):
        try:
            self._client.delete('/' + self._CLUSTER_NODE, recursive=True)
        except (etcd.EtcdKeyNotFound, etcd.EtcdConnectionFailed):
            pass
        except Exception as e:
            assert False, "exception when cleaning up etcd contents: {0}".format(e)

    def _is_running(self):
        # if etcd is running, but we didn't start it
        try:
            return bool(self._client.machines)
        except:
            return False


class ZooKeeperController(DcsController):

    """ handles all zookeeper related tasks, used for the tests setup and cleanup """

    def __init__(self, output_dir):
        super(ZooKeeperController, self).__init__('zookeeper', output_dir)
        self._client = kazoo.client.KazooClient()

    def _start(self):
        pass  # TODO: implement later

    def query(self, key):
        try:
            return self._client.get('/{0}/{1}'.format(self._CLUSTER_NODE, key))[0].decode('utf-8')
        except kazoo.exceptions.NoNodeError:
            return None

    def cleanup_service_tree(self):
        try:
            self._client.delete('/' + self._CLUSTER_NODE, recursive=True)
        except (kazoo.exceptions.NoNodeError):
            pass
        except Exception as e:
            assert False, "exception when cleaning up zookeeper contents: {0}".format(e)

    def _is_running(self):
        # if zookeeper is running, but we didn't start it
        if self._client.connected:
            return True
        try:
            self._client.start(1)
            return True
        except:
            return False


# actions to execute on start/stop of the tests and before running invidual features
def before_all(context):
    context.pctl = PatroniPoolController()
    if context.pctl.dcs == 'zookeeper':
        context.dcs_ctl = ZooKeeperController(context.pctl.output_dir)
    else:  # context.pctl.dcs == 'etcd'
        context.dcs_ctl = EtcdController(context.pctl.output_dir)
    context.dcs_ctl.start()
    try:
        context.dcs_ctl.cleanup_service_tree()
    except AssertionError:  # after_all handlers won't be executed in before_all
        context.dcs_ctl.stop_and_remove_work_directory()
        raise


def after_all(context):
    context.dcs_ctl.stop_and_remove_work_directory()
    subprocess.call(['coverage', 'combine'])
    subprocess.call(['coverage', 'report'])


def before_feature(context, feature):
    """ create per-feature output directory to collect Patroni and PostgreSQL logs """
    context.pctl.create_and_set_output_directory(feature.name)


def after_feature(context, feature):
    """ stop all Patronis, remove their data directory and cleanup the keys in etcd """
    context.pctl.stop_all()
    shutil.rmtree(os.path.join(context.pctl.patroni_path, 'data'))
    context.dcs_ctl.cleanup_service_tree()
