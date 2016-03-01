from lettuce import world, before, after
import os.path
import psycopg2
import requests
import subprocess
import shutil
import tempfile
import time
import yaml


class PatroniController(object):
    PATRONI_CONFIG = '{}.yml'
    """ starts and stops individual patronis"""

    def __init__(self):
        self._output_dir = None
        self._patroni_path = None
        self._connections = {}
        self._config = {}
        self._connstring = {}
        self._cursors = {}
        self._log = {}
        self._processes = {}

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

    def start(self, pg_name, max_wait_limit=15):
        if not self._is_running(pg_name):
            if pg_name in self._processes:
                del self._processes[pg_name]
            cwd = self.patroni_path
            self._log[pg_name] = open(os.path.join(self._output_dir, 'patroni_{0}.log'.format(pg_name)), 'a')

            self._config[pg_name] = self._make_patroni_test_config(pg_name)

            p = subprocess.Popen(['python', 'patroni.py', self._config[pg_name]],
                                 stdout=self._log[pg_name], stderr=subprocess.STDOUT, cwd=cwd)
            if not (p and p.pid and p.poll() is None):
                assert False, "PostgreSQL {0} is not running after being started".format(pg_name)
            self._processes[pg_name] = p
        # wait while patroni is available for queries, but not more than 10 seconds.
        for _ in range(max_wait_limit):
            if self.query(pg_name, "SELECT 1", fail_ok=True) is not None:
                break
            time.sleep(1)
        else:
            assert False,\
                "Patroni instance is not available for queries after {0} seconds".format(max_wait_limit)

    def stop(self, pg_name, kill=False, timeout=15):
        start_time = time.time()
        while self._is_running(pg_name):
            if not kill:
                self._processes[pg_name].terminate()
            else:
                self._processes[pg_name].kill()
            time.sleep(1)
            if not kill and time.time() - start_time > timeout:
                kill = True
        if self._log.get('pg_name') and not self._log['pg_name'].closed:
            self._log[pg_name].close()
        if pg_name in self._processes:
            del self._processes[pg_name]

    def query(self, pg_name, query, fail_ok=False):
        try:
            cursor = self._cursor(pg_name)
            cursor.execute(query)
            return cursor
        except psycopg2.Error:
            if fail_ok:
                return None
            else:
                raise

    def check_role_has_changed_to(self, pg_name, new_role, timeout=10):
        bound_time = time.time() + timeout
        recovery_status = False if new_role == 'primary' else True
        role_has_changed = False
        while not role_has_changed:
            cur = self.query(pg_name, "SELECT pg_is_in_recovery()", fail_ok=True)
            if cur:
                row = cur.fetchone()
                if row and len(row) > 0 and row[0] == recovery_status:
                    role_has_changed = True
            if time.time() > bound_time:
                break
            time.sleep(1)
        return role_has_changed

    def stop_all(self):
        for patroni in self._processes.copy():
            self.stop(patroni)

    def create_and_set_output_directory(self, feature_name):
        feature_dir = os.path.join(pctl.patroni_path, "features", "output", feature_name.encode('utf-8').replace(' ', '_'))
        if os.path.exists(feature_dir):
            shutil.rmtree(feature_dir)
        os.makedirs(feature_dir)
        self._output_dir = feature_dir

    def _is_running(self, pg_name):
        return pg_name in self._processes and self._processes[pg_name].pid and (self._processes[pg_name].poll() is None)

    def _make_patroni_test_config(self, pg_name):
        patroni_config_name = PatroniController.PATRONI_CONFIG.format(pg_name)
        patroni_config_path = os.path.join(self._output_dir, patroni_config_name)

        with open(patroni_config_name) as f:
            config = yaml.load(f)
        postgresql = config['postgresql']
        postgresql['name'] = pg_name.encode('utf-8')
        postgresql_params = postgresql['parameters']
        postgresql_params['logging_collector'] = 'on'
        postgresql_params['log_destination'] = 'csvlog'
        postgresql_params['log_directory'] = self._output_dir
        postgresql_params['log_filename'] = '{0}.log'.format(pg_name)
        postgresql_params['log_statement'] = 'all'
        postgresql_params['log_min_messages'] = 'debug1'

        with open(patroni_config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return patroni_config_path

    def _make_connstring(self, pg_name):
        if pg_name in self._connstring:
            return self._connstring[pg_name]
        try:
            patroni_path = self.patroni_path
            with open(os.path.join(patroni_path, PatroniController.PATRONI_CONFIG.format(pg_name)), 'r') as f:
                config = yaml.load(f)
        except OSError:
            return None
        connstring = config['postgresql']['connect_address']
        if ':' in connstring:
            address, port = connstring.split(':')
        else:
            address = connstring
            port = '5432'
        user = "postgres"
        dbname = "postgres"
        self._connstring[pg_name] = "host={0} port={1} dbname={2} user={3}".format(address, port, dbname, user)
        return self._connstring[pg_name]

    def _connection(self, pg_name):
        if pg_name not in self._connections or self._connections[pg_name].closed:
            conn = psycopg2.connect(self._make_connstring(pg_name))
            conn.autocommit = True
            self._connections[pg_name] = conn
        return self._connections[pg_name]

    def _cursor(self, pg_name):
        if pg_name not in self._cursors or self._cursors[pg_name].closed:
            cursor = self._connection(pg_name).cursor()
            self._cursors[pg_name] = cursor
        return self._cursors[pg_name]


class EtcdController(object):
    """ handles all etcd related tasks, used for the tests setup and cleanup """
    ETCD_VERSION_URL = 'http://127.0.0.1:2379/version'
    ETCD_CLEANUP_URL = 'http://127.0.0.1:2379/v2/keys/service/batman?recursive=true'

    def __init__(self, log_directory):
        self.handle = None
        self.work_directory = None
        self.log_directory = log_directory
        self.log_file = None
        self.pid = None
        self.start_timeout = 5

    def start(self):
        """ start etcd if it's not already running """
        if self._is_running():
            return True
        self.work_directory = tempfile.mkdtemp()
        # etcd is running throughout the tests, no need to append to the log
        self.log_file = open(os.path.join(self.log_directory, "features", "output", 'etcd.log'), 'w')
        self.handle =\
            subprocess.Popen(["etcd", "--debug", "--data-dir", self.work_directory],
                             stdout=self.log_file, stderr=subprocess.STDOUT)
        start_time = time.time()
        while (not self._is_running()):
            if time.time() - start_time > self.start_timeout:
                assert False, "Failed to start etcd"
            time.sleep(1)
        return True

    def query(self, key):
        """ query etcd for a value of a given key """
        r = requests.get("http://127.0.0.1:2379/v2/keys/service/batman/{0}".format(key))
        if r.ok:
            content = r.json()
            if content:
                return content.get('node', {}).get('value', None)
        return None

    def stop_and_remove_work_directory(self, timeout=15):
        """ terminate etcd and wipe out the temp work directory, but only if we actually started it"""
        kill = False
        start_time = time.time()
        while self._is_running() and self.handle:
            if not kill:
                self.handle.terminate()
            else:
                self.handle.kill()
            time.sleep(1)
            if not kill and time.time() - start_time > timeout:
                kill = True
        self.handle = None
        if self.log_file and not self.log_file.closed:
            self.log_file.close()
        if self.work_directory:
            shutil.rmtree(self.work_directory)
            self.work_directory = None

    @staticmethod
    def cleanup_service_tree():
        """ clean all contents stored in the tree used for the tests """
        r = None
        try:
            r = requests.delete(EtcdController.ETCD_CLEANUP_URL)
            if r and not r.ok:
                assert False,\
                    "request to cleanup the etcd contents was not successfull: status code {0}".format(r.status_code)
        except requests.exceptions.RequestException as e:
            assert False, "exception when cleaning up etcd contents: {0}".format(e)

    def _is_running(self):
        # if etcd is running, but we didn't start it
        try:
            r = requests.get(EtcdController.ETCD_VERSION_URL)
            running = (r and r.ok and 'etcdserver' in r.content)
        except requests.ConnectionError:
            running = False
        return running


pctl = PatroniController()
etcd_ctl = EtcdController(pctl.patroni_path)
# export pctl to manage patroni from scenario files
world.pctl = pctl
world.etcd_ctl = etcd_ctl


# actions to execute on start/stop of the tests and before running invidual features
@before.all
def start_etcd():
    etcd_ctl.start()
    try:
        etcd_ctl.cleanup_service_tree()
    except AssertionError:  # after.all handlers won't be executed in before.all
        etcd_ctl.stop_and_remove_work_directory()
        raise


@after.all
def stop_etcd(*args, **kwargs):
    etcd_ctl.stop_and_remove_work_directory()


@before.each_feature
def make_test_output_dir(feature):
    """ create per-feature output directory to collect Patroni and PostgreSQL logs """
    pctl.create_and_set_output_directory(feature.name)


@after.each_feature
def cleanup(*args, **kwargs):
    """ stop all Patronis, remove their data directory and cleanup the keys in etcd """
    pctl.stop_all()
    shutil.rmtree(os.path.join(pctl.patroni_path, 'data'))
    etcd_ctl.cleanup_service_tree()
