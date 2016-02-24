from lettuce import *
import os.path
import psycopg2
import requests
import subprocess
import shutil
import tempfile
import time
import yaml


ETCD_VERSION_URL = 'http://127.0.0.1:2379/version'
ETCD_CLEANUP_URL = 'http://127.0.0.1:2379/v2/keys/service/batman?recursive=true'
PATRONI_CONFIG = '{}.yml'
etcd_handle = None
etcd_dir = None
pctl = None


@world.absorb
class PatroniController(object):
    """ starts and stops individual patronis"""

    def __init__(self):
        self.processes = {}
        self._patroni_path = None
        self.connstring = {}
        self.connections = {}
        self.cursors = {}
        self.log = {}
        self.config = {}
        self.availability_check_time_limit = 10
        self.output_dir = None

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

    def patroni_is_running(self, pg_name):
        return pg_name in self.processes and self.processes[pg_name].pid and (self.processes[pg_name].poll() is None)

    def stop_patroni(self, pg_name):
        while self.patroni_is_running(pg_name):
            self.processes[pg_name].terminate()
            time.sleep(1)
        self.log.get('pg_name') and self.log[pg_name].close()
        del self.processes[pg_name]

    def make_patroni_test_config(self, pg_name, output_dir):
        patroni_config_name = PATRONI_CONFIG.format(pg_name)
        patroni_config_path = os.path.join(output_dir, patroni_config_name)

        with open(patroni_config_name) as f:
            config = yaml.load(f)
        postgresql = config['postgresql']['parameters']
        postgresql['logging_collector'] = 'on'
        postgresql['log_destination'] = 'csvlog'
        postgresql['log_directory'] = output_dir
        postgresql['log_filename'] = '{0}.log'.format(pg_name)
        postgresql['log_statement'] = 'all'
        postgresql['log_min_messages'] = 'debug1'

        with open(patroni_config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        return patroni_config_path

    def start_patroni(self, pg_name):
        if not self.patroni_is_running(pg_name):
            if pg_name in self.processes:
                del self.processes[pg_name]
            cwd = self.patroni_path
            self.log[pg_name] = open(os.path.join(self.output_dir, 'patroni_{0}.log'.format(pg_name)), 'a')

            self.config[pg_name] = self.make_patroni_test_config(pg_name, self.output_dir)

            p = subprocess.Popen(['python', 'patroni.py', self.config[pg_name]],
                                 stdout=self.log[pg_name], stderr=subprocess.STDOUT, cwd=cwd)
            if not (p and p.pid and p.poll() is None):
                assert False, "PostgreSQL {0} is not running after being started".format(pg_name)
            self.processes[pg_name] = p
        # wait while patroni is available for queries, but not more than 10 seconds.
        for tick in range(self.availability_check_time_limit):
            if self.query(pg_name, "SELECT 1", fail_ok=True) is not None:
                break
            time.sleep(1)
        else:
            assert False,\
                "Patroni instance is not available for queries after {0} seconds".format(self.availability_check_time_limit)

    def make_connstring(self, pg_name):
        if pg_name in self.connstring:
            return self.connstring[pg_name]
        try:
            patroni_path = self.patroni_path
            with open(os.path.join(patroni_path, world.PATRONI_CONFIG.format(pg_name)), 'r') as f:
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
        self.connstring[pg_name] = "host={0} port={1} dbname={2} user={3}".format(address, port, dbname, user)
        return self.connstring[pg_name]

    def connection(self, pg_name):
        if pg_name not in self.connections or self.connections[pg_name].closed:
            conn = psycopg2.connect(self.make_connstring(pg_name))
            conn.autocommit = True
            self.connections[pg_name] = conn
        return self.connections[pg_name]

    def cursor(self, pg_name):
        if pg_name not in self.cursors or self.cursors[pg_name].closed:
            cursor = self.connection(pg_name).cursor()
            self.cursors[pg_name] = cursor
        return self.cursors[pg_name]

    def query(self, pg_name, query, fail_ok=False):
        try:
            cursor = self.cursor(pg_name)
            cursor.execute(query)
            return cursor
        except psycopg2.Error:
            if fail_ok:
                return None
            else:
                raise

    def check_role_has_changed_to(self, pg_name, new_role, timeout=10):
        bound_time = time.time() + timeout
        current_role = 't' if new_role == 'primary' else 'f'
        role_has_changed = False
        while not role_has_changed:
            cur = self.query(pg_name, "SELECT pg_is_in_recovery()", fail_ok=True)
            if cur:
                row = cur.fetchone()
                if row and len(row) > 0 and row[0] != current_role:
                    role_has_changed = True
            if time.time() > bound_time:
                break
        return role_has_changed

    def stop_all(self):
        for patroni in self.processes.copy():
            self.stop_patroni(patroni)

pctl = PatroniController()
world.pctl = pctl
world.patroni_path = pctl.patroni_path
world.PATRONI_CONFIG = PATRONI_CONFIG


def etcd_is_running():
    # if we have already started etcd
    if etcd_handle and etcd_handle.pid and (etcd_handle.poll() is None):
        return True
    # if etcd is running, but we didn't start it
    try:
        r = requests.get(ETCD_VERSION_URL)
        if r and r.ok and 'etcdserver' in r.content:
            return True
    except requests.ConnectionError:
        pass
    return False


@before.all
def start_etcd():
    if not etcd_is_running():
        global etcd_handle
        global etcd_dir
        etcd_dir = tempfile.mkdtemp()
        etcd_handle = subprocess.Popen(["etcd", "--data-dir", etcd_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not etcd_is_running():
            assert False, "Failed to start etcd"


@after.all
def stop_etcd(total):
    global etcd_handle
    global etcd_dir
    if etcd_is_running() and etcd_handle:
        etcd_handle.terminate()
        etcd_handle = None
        shutil.rmtree(etcd_dir)
        etcd_dir = None


@before.each_feature
def make_test_output_dir(feature):
    feature_dir = os.path.join(pctl.patroni_path, "features", "output", feature.name.encode('utf-8').replace(' ', '_'))
    if os.path.exists(feature_dir):
        shutil.rmtree(feature_dir)
    os.makedirs(feature_dir)
    pctl.output_dir = feature_dir


def patroni_cleanup_all():
    pctl.stop_all()
    # remove the data directory
    shutil.rmtree(os.path.join(pctl.patroni_path, 'data'))


def etcd_cleanup():
    try:
        r = requests.delete(ETCD_CLEANUP_URL)
        if not r.ok:
            raise Exception('{}'.format(r.reason))
    except Exception as e:
        assert False, "Unable to cleanup etcd: {0}".format(e)


@after.each_feature
def cleanup(scenario):
    patroni_cleanup_all()
    etcd_cleanup()
