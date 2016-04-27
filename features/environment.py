import abc
import consul
import etcd
import kazoo.client
import kazoo.exceptions
import os
import psycopg2
import shutil
import six
import subprocess
import tempfile
import time
import yaml


@six.add_metaclass(abc.ABCMeta)
class AbstractController(object):

    def __init__(self, name, work_directory, output_dir):
        self._name = name
        self._work_directory = work_directory
        self._output_dir = output_dir
        self._handle = None
        self._log = None

    def _has_started(self):
        return self._handle and self._handle.pid and self._handle.poll() is None

    def _is_running(self):
        return self._has_started()

    @abc.abstractmethod
    def _is_accessible(self):
        """process is accessible for queries"""

    @abc.abstractmethod
    def _start(self):
        """start process"""

    def start(self, max_wait_limit=5):
        if self._is_running():
            return True

        self._log = open(os.path.join(self._output_dir, self._name + '.log'), 'a')
        self._handle = self._start()

        assert self._has_started(), "Process {0} is not running after being started".format(self._name)

        for _ in range(max_wait_limit):
            if self._is_accessible():
                break
            time.sleep(1)
        else:
            assert False,\
                "{0} instance is not available for queries after {1} seconds".format(self._name, max_wait_limit)

    def stop(self, kill=False, timeout=15):
        term = False
        start_time = time.time()

        while self._handle and self._is_running():
            if kill:
                self._handle.kill()
            elif not term:
                self._handle.terminate()
                term = True
            time.sleep(1)
            if not kill and time.time() - start_time > timeout:
                kill = True

        if self._log:
            self._log.close()


class PatroniController(AbstractController):
    __PORT = 5440
    PATRONI_CONFIG = '{}.yml'
    """ starts and stops individual patronis"""

    def __init__(self, dcs, name, work_directory, output_dir, tags=None):
        super(PatroniController, self).__init__('patroni_' + name, work_directory, output_dir)
        PatroniController.__PORT += 1
        self._data_dir = os.path.join(work_directory, 'data', name)
        self._connstring = None
        self._config = self._make_patroni_test_config(name, dcs, tags)

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
            return None

    def _start(self):
        return subprocess.Popen(['coverage', 'run', '--source=patroni', '-p', 'patroni.py', self._config],
                                stdout=self._log, stderr=subprocess.STDOUT, cwd=self._work_directory)

    def _is_accessible(self):
        return self.query("SELECT 1", fail_ok=True) is not None

    def _make_patroni_test_config(self, name, dcs, tags):
        patroni_config_name = self.PATRONI_CONFIG.format(name)
        patroni_config_path = os.path.join(self._output_dir, patroni_config_name)

        with open(patroni_config_name) as f:
            config = yaml.load(f)

        host = config['postgresql']['listen'].split(':')[0]

        config['postgresql']['listen'] = config['postgresql']['connect_address'] = '{0}:{1}'.format(host, self.__PORT)

        user = config['postgresql'].get('superuser', {})
        self._connkwargs = {k: user[n] for n, k in [('username', 'user'), ('password', 'password')] if n in user}
        self._connkwargs.update({'host': host, 'port': self.__PORT, 'database': 'postgres'})

        config['postgresql'].update({'name': name, 'data_dir': self._data_dir})
        config['postgresql']['parameters'].update({
            'logging_collector': 'on', 'log_destination': 'csvlog', 'log_directory': self._output_dir,
            'log_filename': name + '.log', 'log_statement': 'all', 'log_min_messages': 'debug1'})

        if tags:
            config['tags'] = tags

        if dcs != 'etcd':
            dcs_config = config.pop('etcd')
            dcs_config.pop('host')

            if dcs == 'consul':
                config[dcs] = dcs_config
            else:
                dcs_config.update({'session_timeout': dcs_config.pop('ttl'), 'reconnect_timeout': config['loop_wait']})
                if dcs == 'exhibitor':
                    dcs_config['exhibitor'] = {'hosts': ['127.0.0.1'], 'port': 8181}
                else:
                    dcs_config['hosts'] = ['127.0.0.1:2181']
                config['zookeeper'] = dcs_config

        with open(patroni_config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return patroni_config_path

    def _connection(self):
        if not self._conn or self._conn.closed != 0:
            self._conn = psycopg2.connect(**self._connkwargs)
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

    def check_role_has_changed_to(self, new_role, timeout=10):
        bound_time = time.time() + timeout
        recovery_status = new_role != 'primary'
        while time.time() < bound_time:
            cur = self.query("SELECT pg_is_in_recovery()", fail_ok=True)
            if cur:
                row = cur.fetchone()
                if row and row[0] == recovery_status:
                    return True
            time.sleep(1)
        return False


class AbstractDcsController(AbstractController):

    _CLUSTER_NODE = 'service/batman'

    def _is_accessible(self):
        return self._is_running()

    def stop_and_remove_work_directory(self, timeout=15):
        """ terminate process and wipe out the temp work directory, but only if we actually started it"""
        self.stop(timeout=timeout)
        if self._work_directory:
            shutil.rmtree(self._work_directory)

    @abc.abstractmethod
    def query(self, key):
        """ query for a value of a given key """

    @abc.abstractmethod
    def cleanup_service_tree(self):
        """ clean all contents stored in the tree used for the tests """


class ConsulController(AbstractDcsController):

    def __init__(self, output_dir):
        super(ConsulController, self).__init__('consul', tempfile.mkdtemp(), output_dir)
        self._client = consul.Consul()

    def _start(self):
        return subprocess.Popen(['consul', 'agent', '-server', '-bootstrap', '-advertise=127.0.0.1',
                                 '-data-dir', self._work_directory], stdout=self._log, stderr=subprocess.STDOUT)

    def _is_running(self):
        try:
            return bool(self._client.status.leader())
        except Exception:
            return False

    def query(self, key):
        _, value = self._client.kv.get('{0}/{1}'.format(self._CLUSTER_NODE, key))
        return value and value['Value'].decode('utf-8')

    def cleanup_service_tree(self):
        self._client.kv.delete(self._CLUSTER_NODE, recurse=True)


class EtcdController(AbstractDcsController):

    """ handles all etcd related tasks, used for the tests setup and cleanup """

    def __init__(self, output_dir):
        super(EtcdController, self).__init__('etcd', tempfile.mkdtemp(), output_dir)
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
            return
        except Exception as e:
            assert False, "exception when cleaning up etcd contents: {0}".format(e)

    def _is_running(self):
        # if etcd is running, but we didn't start it
        try:
            return bool(self._client.machines)
        except Exception:
            return False


class ZooKeeperController(AbstractDcsController):

    """ handles all zookeeper related tasks, used for the tests setup and cleanup """

    def __init__(self, output_dir):
        super(ZooKeeperController, self).__init__('zookeeper', None, output_dir)
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
            return
        except Exception as e:
            assert False, "exception when cleaning up zookeeper contents: {0}".format(e)

    def _is_running(self):
        # if zookeeper is running, but we didn't start it
        if self._client.connected:
            return True
        try:
            return self._client.start(1) or True
        except Exception:
            return False


class PatroniPoolController(object):

    KNOWN_DCS = {'consul': ConsulController, 'etcd': EtcdController,
                 'zookeeper': ZooKeeperController, 'exhibitor': ZooKeeperController}

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
                cwd, entry = os.path.split(cwd)
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
            return getattr(self._processes[pg_name], func)(*args, **kwargs)
        return wrapper

    def stop_all(self):
        for ctl in self._processes.values():
            ctl.stop()
        self._processes.clear()

    def create_and_set_output_directory(self, feature_name):
        feature_dir = os.path.join(self.patroni_path, 'features/output', feature_name.replace(' ', '_'))
        if os.path.exists(feature_dir):
            shutil.rmtree(feature_dir)
        os.makedirs(feature_dir)
        self._output_dir = feature_dir

    @property
    def dcs(self):
        if self._dcs is None:
            self._dcs = os.environ.get('DCS', 'etcd')
            assert self._dcs in self.KNOWN_DCS, 'Unsupported dcs: ' + self.dcs
        return self._dcs


# actions to execute on start/stop of the tests and before running invidual features
def before_all(context):
    context.pctl = PatroniPoolController()
    context.dcs_ctl = context.pctl.KNOWN_DCS[context.pctl.dcs](context.pctl.output_dir)
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
