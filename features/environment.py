import abc
import datetime
import glob
import os
import json
import psutil
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import yaml

import patroni.psycopg as psycopg

from http.server import BaseHTTPRequestHandler, HTTPServer
from patroni.request import PatroniRequest


class AbstractController(abc.ABC):

    def __init__(self, context, name, work_directory, output_dir):
        self._context = context
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

        max_wait_limit *= self._context.timeout_multiplier
        for _ in range(max_wait_limit):
            assert self._has_started(), "Process {0} is not running after being started".format(self._name)
            if self._is_accessible():
                break
            time.sleep(1)
        else:
            assert False, \
                "{0} instance is not available for queries after {1} seconds".format(self._name, max_wait_limit)

    def stop(self, kill=False, timeout=15, _=False):
        term = False
        start_time = time.time()

        timeout *= self._context.timeout_multiplier
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

    def cancel_background(self):
        pass


class PatroniController(AbstractController):
    __PORT = 5360
    PATRONI_CONFIG = '{}.yml'
    """ starts and stops individual patronis"""

    def __init__(self, context, name, work_directory, output_dir, custom_config=None):
        super(PatroniController, self).__init__(context, 'patroni_' + name, work_directory, output_dir)
        PatroniController.__PORT += 1
        self._data_dir = os.path.join(work_directory, 'data', name)
        self._connstring = None
        if custom_config and 'watchdog' in custom_config:
            self.watchdog = WatchdogMonitor(name, work_directory, output_dir)
            custom_config['watchdog'] = {'driver': 'testing', 'device': self.watchdog.fifo_path, 'mode': 'required'}
        else:
            self.watchdog = None

        self._scope = (custom_config or {}).get('scope', 'batman')
        self._citus_group = (custom_config or {}).get('citus', {}).get('group')
        self._config = self._make_patroni_test_config(name, custom_config)
        self._closables = []

        self._conn = None
        self._curs = None

    def write_label(self, content):
        with open(os.path.join(self._data_dir, 'label'), 'w') as f:
            f.write(content)

    def read_label(self, label):
        try:
            with open(os.path.join(self._data_dir, label), 'r') as f:
                return f.read().strip()
        except IOError:
            return None

    @staticmethod
    def recursive_update(dst, src):
        for k, v in src.items():
            if k in dst and isinstance(dst[k], dict):
                PatroniController.recursive_update(dst[k], v)
            else:
                dst[k] = v

    def update_config(self, custom_config):
        with open(self._config) as r:
            config = yaml.safe_load(r)
            self.recursive_update(config, custom_config)
            with open(self._config, 'w') as w:
                yaml.safe_dump(config, w, default_flow_style=False)
        self._scope = config.get('scope', 'batman')

    def add_tag_to_config(self, tag, value):
        self.update_config({'tags': {tag: value}})

    def _start(self):
        if self.watchdog:
            self.watchdog.start()
        env = os.environ.copy()
        if isinstance(self._context.dcs_ctl, KubernetesController):
            self._context.dcs_ctl.create_pod(self._name[8:], self._scope, self._citus_group)
            env['PATRONI_KUBERNETES_POD_IP'] = '10.0.0.' + self._name[-1]
        if os.name == 'nt':
            env['BEHAVE_DEBUG'] = 'true'
        patroni = subprocess.Popen([sys.executable, '-m', 'coverage', 'run',
                                   '--source=patroni', '-p', 'patroni.py', self._config], env=env,
                                   stdout=self._log, stderr=subprocess.STDOUT, cwd=self._work_directory)
        if os.name == 'nt':
            patroni.terminate = self.terminate
        return patroni

    def terminate(self):
        try:
            self._context.request_executor.request('POST', self._restapi_url + '/sigterm')
        except Exception:
            pass

    def stop(self, kill=False, timeout=15, postgres=False):
        if postgres:
            mode = 'i' if kill else 'f'
            return subprocess.call(['pg_ctl', '-D', self._data_dir, 'stop', '-m' + mode, '-w'])
        super(PatroniController, self).stop(kill, timeout)
        if isinstance(self._context.dcs_ctl, KubernetesController) and not kill:
            self._context.dcs_ctl.delete_pod(self._name[8:])
        if self.watchdog:
            self.watchdog.stop()

    def _is_accessible(self):
        cursor = self.query("SELECT 1", fail_ok=True)
        if cursor is not None:
            cursor.execute("SET synchronous_commit TO 'local'")
            return True

    def _make_patroni_test_config(self, name, custom_config):
        patroni_config_name = self.PATRONI_CONFIG.format(name)
        patroni_config_path = os.path.join(self._output_dir, patroni_config_name)

        with open('postgres0.yml') as f:
            config = yaml.safe_load(f)
            config.pop('etcd', None)

        raft_port = os.environ.get('RAFT_PORT')
        # If patroni_raft_controller is suspended two Patroni members is enough to get a quorum,
        # therefore we don't want Patroni to join as a voting member when testing dcs_failsafe_mode.
        if raft_port and not self._output_dir.endswith('dcs_failsafe_mode'):
            os.environ['RAFT_PORT'] = str(int(raft_port) + 1)
            config['raft'] = {'data_dir': self._output_dir, 'self_addr': 'localhost:' + os.environ['RAFT_PORT']}

        host = config['restapi']['listen'].rsplit(':', 1)[0]
        config['restapi']['listen'] = config['restapi']['connect_address'] = '{}:{}'.format(host, 8008 + int(name[-1]))

        host = config['postgresql']['listen'].rsplit(':', 1)[0]
        config['postgresql']['listen'] = config['postgresql']['connect_address'] = '{0}:{1}'.format(host, self.__PORT)

        config['name'] = name
        config['postgresql']['data_dir'] = self._data_dir.replace('\\', '/')
        config['postgresql']['basebackup'] = [{'checkpoint': 'fast'}]
        config['postgresql']['callbacks'] = {
            'on_role_change': '{0} features/callback2.py {1}'.format(self._context.pctl.PYTHON, name)}
        config['postgresql']['use_unix_socket'] = os.name != 'nt'  # windows doesn't yet support unix-domain sockets
        config['postgresql']['use_unix_socket_repl'] = os.name != 'nt'
        config['postgresql']['pgpass'] = os.path.join(tempfile.gettempdir(), 'pgpass_' + name).replace('\\', '/')
        config['postgresql']['parameters'].update({
            'logging_collector': 'on', 'log_destination': 'csvlog',
            'log_directory': self._output_dir.replace('\\', '/'),
            'log_filename': name + '.log', 'log_statement': 'all', 'log_min_messages': 'debug1',
            'shared_buffers': '1MB', 'unix_socket_directories': tempfile.gettempdir().replace('\\', '/')})
        config['postgresql']['pg_hba'] = [
            'local all all trust',
            'local replication all trust',
            'host replication replicator all md5',
            'host all all all md5'
        ]

        if self._context.postgres_supports_ssl and self._context.certfile:
            config['postgresql']['parameters'].update({
                'ssl': 'on',
                'ssl_ca_file': self._context.certfile.replace('\\', '/'),
                'ssl_cert_file': self._context.certfile.replace('\\', '/'),
                'ssl_key_file': self._context.keyfile.replace('\\', '/')
            })
            for user in config['postgresql'].get('authentication').keys():
                config['postgresql'].get('authentication', {}).get(user, {}).update({
                    'sslmode': 'verify-ca',
                    'sslrootcert': self._context.certfile,
                    'sslcert': self._context.certfile,
                    'sslkey': self._context.keyfile
                })
            for i, line in enumerate(list(config['postgresql']['pg_hba'])):
                if line.endswith('md5'):
                    # we want to verify client cert first and than password
                    config['postgresql']['pg_hba'][i] = 'hostssl' + line[4:] + ' clientcert=verify-ca'

        if 'bootstrap' in config:
            config['bootstrap']['post_bootstrap'] = 'psql -w -c "SELECT 1"'
            if 'initdb' in config['bootstrap']:
                config['bootstrap']['initdb'].extend([{'auth': 'md5'}, {'auth-host': 'md5'}])

        if custom_config is not None:
            self.recursive_update(config, custom_config)

        self.recursive_update(config, {
            'log': {
                'format': '%(asctime)s %(levelname)s [%(pathname)s:%(lineno)d - %(funcName)s]: %(message)s',
                'loggers': {'patroni.postgresql.callback_executor': 'DEBUG'}
            },
            'bootstrap': {
                'dcs': {
                    'loop_wait': 2,
                    'postgresql': {
                        'parameters': {
                            'wal_keep_segments': 100,
                            'archive_mode': 'on',
                            'archive_command':
                                (PatroniPoolController.ARCHIVE_RESTORE_SCRIPT
                                 + ' --mode archive '
                                 + '--dirname {} --filename %f --pathname %p').format(
                                     os.path.join(self._work_directory, 'data',
                                                  f'wal_archive{str(self._citus_group or "")}')).replace('\\', '/'),
                            'restore_command':
                                (PatroniPoolController.ARCHIVE_RESTORE_SCRIPT
                                 + ' --mode restore '
                                 + '--dirname {} --filename %f --pathname %p').format(
                                     os.path.join(self._work_directory, 'data',
                                                  f'wal_archive{str(self._citus_group or "")}')).replace('\\', '/')
                        }
                    }
                }
            }
        })
        if config['postgresql'].get('callbacks', {}).get('on_role_change'):
            config['postgresql']['callbacks']['on_role_change'] += ' ' + str(self.__PORT)

        with open(patroni_config_path, 'w') as f:
            yaml.safe_dump(config, f, default_flow_style=False)

        self._connkwargs = config['postgresql'].get('authentication', config['postgresql']).get('superuser', {})
        self._connkwargs.update({'host': host, 'port': self.__PORT, 'dbname': 'postgres',
                                 'user': self._connkwargs.pop('username', None)})

        self._replication = config['postgresql'].get('authentication', config['postgresql']).get('replication', {})
        self._replication.update({'host': host, 'port': self.__PORT, 'user': self._replication.pop('username', None)})
        self._restapi_url = 'http://{0}'.format(config['restapi']['connect_address'])
        if self._context.certfile:
            self._restapi_url = self._restapi_url.replace('http://', 'https://')

        return patroni_config_path

    def _connection(self):
        if not self._conn or self._conn.closed != 0:
            self._conn = psycopg.connect(**self._connkwargs)
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
        except psycopg.Error:
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

    def get_watchdog(self):
        return self.watchdog

    def _get_pid(self):
        try:
            pidfile = os.path.join(self._data_dir, 'postmaster.pid')
            if not os.path.exists(pidfile):
                return None
            return int(open(pidfile).readline().strip())
        except Exception:
            return None

    def patroni_hang(self, timeout):
        hang = ProcessHang(self._handle.pid, timeout)
        self._closables.append(hang)
        hang.start()

    def cancel_background(self):
        for obj in self._closables:
            obj.close()
        self._closables = []

    @property
    def backup_source(self):
        def escape(value):
            return re.sub(r'([\'\\ ])', r'\\\1', str(value))

        return ' '.join('{0}={1}'.format(k, escape(v)) for k, v in self._replication.items())

    def backup(self, dest=os.path.join('data', 'basebackup')):
        subprocess.call(PatroniPoolController.BACKUP_SCRIPT + ['--walmethod=none',
                        '--datadir=' + os.path.join(self._work_directory, dest),
                        '--dbname=' + self.backup_source])

    def read_patroni_log(self, level):
        try:
            with open(str(os.path.join(self._output_dir or '', self._name + ".log"))) as f:
                return [line for line in f.readlines() if line[24:24 + len(level)] == level]
        except IOError:
            return []


class ProcessHang(object):

    """A background thread implementing a cancelable process hang via SIGSTOP."""

    def __init__(self, pid, timeout):
        self._cancelled = threading.Event()
        self._thread = threading.Thread(target=self.run)
        self.pid = pid
        self.timeout = timeout

    def start(self):
        self._thread.start()

    def run(self):
        os.kill(self.pid, signal.SIGSTOP)
        try:
            self._cancelled.wait(self.timeout)
        finally:
            os.kill(self.pid, signal.SIGCONT)

    def close(self):
        self._cancelled.set()
        self._thread.join()


class AbstractDcsController(AbstractController):

    _CLUSTER_NODE = '/service/{0}'

    def __init__(self, context, mktemp=True):
        work_directory = mktemp and tempfile.mkdtemp() or None
        self._paused = False
        super(AbstractDcsController, self).__init__(context, self.name(), work_directory, context.pctl.output_dir)

    def _is_accessible(self):
        return self._is_running()

    def stop(self, kill=False, timeout=15):
        """ terminate process and wipe out the temp work directory, but only if we actually started it"""
        super(AbstractDcsController, self).stop(kill=kill, timeout=timeout)
        if self._work_directory:
            shutil.rmtree(self._work_directory)

    def path(self, key=None, scope='batman', group=None):
        citus_group = '/{0}'.format(group) if group is not None else ''
        return self._CLUSTER_NODE.format(scope) + citus_group + (key and '/' + key or '')

    def start_outage(self):
        if not self._paused and self._handle:
            self._handle.suspend()
            self._paused = True

    def stop_outage(self):
        if self._paused and self._handle:
            self._handle.resume()
            self._paused = False

    @abc.abstractmethod
    def query(self, key, scope='batman', group=None):
        """ query for a value of a given key """

    @abc.abstractmethod
    def cleanup_service_tree(self):
        """ clean all contents stored in the tree used for the tests """

    @classmethod
    def get_subclasses(cls):
        for subclass in cls.__subclasses__():
            for subsubclass in subclass.get_subclasses():
                yield subsubclass
            yield subclass

    @classmethod
    def name(cls):
        return cls.__name__[:-10].lower()


class ConsulController(AbstractDcsController):

    def __init__(self, context):
        super(ConsulController, self).__init__(context)
        os.environ['PATRONI_CONSUL_HOST'] = 'localhost:8500'
        os.environ['PATRONI_CONSUL_REGISTER_SERVICE'] = 'on'
        self._config_file = None

        import consul
        self._client = consul.Consul()

    def _start(self):
        self._config_file = self._work_directory + '.json'
        with open(self._config_file, 'wb') as f:
            f.write(b'{"session_ttl_min":"5s","server":true,"bootstrap":true,"advertise_addr":"127.0.0.1"}')
        return psutil.Popen(['consul', 'agent', '-config-file', self._config_file, '-data-dir',
                             self._work_directory], stdout=self._log, stderr=subprocess.STDOUT)

    def stop(self, kill=False, timeout=15):
        super(ConsulController, self).stop(kill=kill, timeout=timeout)
        if self._config_file:
            os.unlink(self._config_file)

    def _is_running(self):
        try:
            return bool(self._client.status.leader())
        except Exception:
            return False

    def path(self, key=None, scope='batman', group=None):
        return super(ConsulController, self).path(key, scope, group)[1:]

    def query(self, key, scope='batman', group=None):
        _, value = self._client.kv.get(self.path(key, scope, group))
        return value and value['Value'].decode('utf-8')

    def cleanup_service_tree(self):
        self._client.kv.delete(self.path(scope=''), recurse=True)

    def start(self, max_wait_limit=15):
        super(ConsulController, self).start(max_wait_limit)


class AbstractEtcdController(AbstractDcsController):

    """ handles all etcd related tasks, used for the tests setup and cleanup """

    def __init__(self, context, client_cls):
        super(AbstractEtcdController, self).__init__(context)
        self._client_cls = client_cls

    def _start(self):
        return psutil.Popen(["etcd", "--enable-v2=true", "--data-dir", self._work_directory],
                            stdout=self._log, stderr=subprocess.STDOUT)

    def _is_running(self):
        from patroni.dcs.etcd import DnsCachingResolver
        # if etcd is running, but we didn't start it
        try:
            self._client = self._client_cls({'host': 'localhost', 'port': 2379, 'retry_timeout': 30,
                                             'patronictl': 1}, DnsCachingResolver())
            return True
        except Exception:
            return False


class EtcdController(AbstractEtcdController):

    def __init__(self, context):
        from patroni.dcs.etcd import EtcdClient
        super(EtcdController, self).__init__(context, EtcdClient)
        os.environ['PATRONI_ETCD_HOST'] = 'localhost:2379'

    def query(self, key, scope='batman', group=None):
        import etcd
        try:
            return self._client.get(self.path(key, scope, group)).value
        except etcd.EtcdKeyNotFound:
            return None

    def cleanup_service_tree(self):
        import etcd
        try:
            self._client.delete(self.path(scope=''), recursive=True)
        except (etcd.EtcdKeyNotFound, etcd.EtcdConnectionFailed):
            return
        except Exception as e:
            assert False, "exception when cleaning up etcd contents: {0}".format(e)


class Etcd3Controller(AbstractEtcdController):

    def __init__(self, context):
        from patroni.dcs.etcd3 import Etcd3Client
        super(Etcd3Controller, self).__init__(context, Etcd3Client)
        os.environ['PATRONI_ETCD3_HOST'] = 'localhost:2379'

    def query(self, key, scope='batman', group=None):
        import base64
        response = self._client.range(self.path(key, scope, group))
        for k in response.get('kvs', []):
            return base64.b64decode(k['value']).decode('utf-8') if 'value' in k else None

    def cleanup_service_tree(self):
        try:
            self._client.deleteprefix(self.path(scope=''))
        except Exception as e:
            assert False, "exception when cleaning up etcd contents: {0}".format(e)


class AbstractExternalDcsController(AbstractDcsController):

    def __init__(self, context, mktemp=True):
        super(AbstractExternalDcsController, self).__init__(context, mktemp)
        self._wrapper = ['sudo']

    def _start(self):
        return self._external_pid

    def start_outage(self):
        if not self._paused:
            subprocess.call(self._wrapper + ['kill', '-SIGSTOP', self._external_pid])
            self._paused = True

    def stop_outage(self):
        if self._paused:
            subprocess.call(self._wrapper + ['kill', '-SIGCONT', self._external_pid])
            self._paused = False

    def _has_started(self):
        return True

    @abc.abstractmethod
    def process_name():
        """process name to search with pgrep"""

    def _is_running(self):
        if not self._handle:
            self._external_pid = subprocess.check_output(['pgrep', '-nf', self.process_name()]).decode('utf-8').strip()
            return False
        return True

    def stop(self):
        pass


class KubernetesController(AbstractExternalDcsController):

    def __init__(self, context):
        super(KubernetesController, self).__init__(context)
        self._namespace = 'default'
        self._labels = {"application": "patroni"}
        self._label_selector = ','.join('{0}={1}'.format(k, v) for k, v in self._labels.items())
        os.environ['PATRONI_KUBERNETES_LABELS'] = json.dumps(self._labels)
        os.environ['PATRONI_KUBERNETES_USE_ENDPOINTS'] = 'true'
        os.environ.setdefault('PATRONI_KUBERNETES_BYPASS_API_SERVICE', 'true')

        from patroni.dcs.kubernetes import k8s_client, k8s_config
        k8s_config.load_kube_config(context=os.environ.setdefault('PATRONI_KUBERNETES_CONTEXT', 'kind-kind'))
        self._client = k8s_client
        self._api = self._client.CoreV1Api()

    def process_name(self):
        return "localkube"

    def _is_running(self):
        if not self._handle:
            context = os.environ.get('PATRONI_KUBERNETES_CONTEXT')
            if context.startswith('kind-'):
                container = '{0}-control-plane'.format(context[5:])
                api_process = 'kube-apiserver'
            elif context.startswith('k3d-'):
                container = '{0}-server-0'.format(context)
                api_process = 'k3s server'
            else:
                return super(KubernetesController, self)._is_running()
            try:
                docker = 'docker'
                with open(os.devnull, 'w') as null:
                    if subprocess.call([docker, 'info'], stdout=null, stderr=null) != 0:
                        raise Exception
            except Exception:
                docker = 'podman'
                with open(os.devnull, 'w') as null:
                    if subprocess.call([docker, 'info'], stdout=null, stderr=null) != 0:
                        raise Exception
            self._wrapper = [docker, 'exec', container]
            self._external_pid = subprocess.check_output(self._wrapper + ['pidof', api_process]).decode('utf-8').strip()
            return False
        return True

    def create_pod(self, name, scope, group=None):
        self.delete_pod(name)
        labels = self._labels.copy()
        labels['cluster-name'] = scope
        if group is not None:
            labels['citus-group'] = str(group)
        metadata = self._client.V1ObjectMeta(namespace=self._namespace, name=name, labels=labels)
        spec = self._client.V1PodSpec(containers=[self._client.V1Container(name=name, image='empty')])
        body = self._client.V1Pod(metadata=metadata, spec=spec)
        self._api.create_namespaced_pod(self._namespace, body)

    def delete_pod(self, name):
        try:
            self._api.delete_namespaced_pod(name, self._namespace, body=self._client.V1DeleteOptions())
        except Exception:
            pass
        while True:
            try:
                self._api.read_namespaced_pod(name, self._namespace)
            except Exception:
                break

    def query(self, key, scope='batman', group=None):
        if key.startswith('members/'):
            pod = self._api.read_namespaced_pod(key[8:], self._namespace)
            return (pod.metadata.annotations or {}).get('status', '')
        else:
            try:
                if group is not None:
                    scope = '{0}-{1}'.format(scope, group)
                rkey = 'leader' if key in ('status', 'failsafe') else key
                ep = scope + {'leader': '', 'history': '-config', 'initialize': '-config'}.get(rkey, '-' + rkey)
                e = self._api.read_namespaced_endpoints(ep, self._namespace)
                if key not in ('sync', 'status', 'failsafe'):
                    return e.metadata.annotations[key]
                else:
                    return json.dumps(e.metadata.annotations)
            except Exception:
                return None

    def cleanup_service_tree(self):
        try:
            self._api.delete_collection_namespaced_pod(self._namespace, label_selector=self._label_selector)
        except Exception:
            pass
        try:
            self._api.delete_collection_namespaced_endpoints(self._namespace, label_selector=self._label_selector)
        except Exception:
            pass

        while True:
            result = self._api.list_namespaced_pod(self._namespace, label_selector=self._label_selector)
            if len(result.items) < 1:
                break


class ZooKeeperController(AbstractExternalDcsController):

    """ handles all zookeeper related tasks, used for the tests setup and cleanup """

    def __init__(self, context, export_env=True):
        super(ZooKeeperController, self).__init__(context, False)
        if export_env:
            os.environ['PATRONI_ZOOKEEPER_HOSTS'] = "'localhost:2181'"

        import kazoo.client
        self._client = kazoo.client.KazooClient()

    def process_name(self):
        return "java .*zookeeper"

    def query(self, key, scope='batman', group=None):
        import kazoo.exceptions
        try:
            return self._client.get(self.path(key, scope, group))[0].decode('utf-8')
        except kazoo.exceptions.NoNodeError:
            return None

    def cleanup_service_tree(self):
        import kazoo.exceptions
        try:
            self._client.delete(self.path(scope=''), recursive=True)
        except (kazoo.exceptions.NoNodeError):
            return
        except Exception as e:
            assert False, "exception when cleaning up zookeeper contents: {0}".format(e)

    def _is_running(self):
        if not super(ZooKeeperController, self)._is_running():
            return False

        # if zookeeper is running, but we didn't start it
        if self._client.connected:
            return True
        try:
            return self._client.start(1) or True
        except Exception:
            return False


class MockExhibitor(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"servers":["127.0.0.1"],"port":2181}')

    def log_message(self, fmt, *args):
        pass


class ExhibitorController(ZooKeeperController):

    def __init__(self, context):
        super(ExhibitorController, self).__init__(context, False)
        port = 8181
        exhibitor = HTTPServer(('', port), MockExhibitor)
        exhibitor.daemon_thread = True
        exhibitor_thread = threading.Thread(target=exhibitor.serve_forever)
        exhibitor_thread.daemon = True
        exhibitor_thread.start()
        os.environ.update({'PATRONI_EXHIBITOR_HOSTS': 'localhost', 'PATRONI_EXHIBITOR_PORT': str(port)})


class RaftController(AbstractDcsController):

    CONTROLLER_ADDR = 'localhost:1234'
    PASSWORD = '12345'

    def __init__(self, context):
        super(RaftController, self).__init__(context)
        os.environ.update(PATRONI_RAFT_PARTNER_ADDRS="'" + self.CONTROLLER_ADDR + "'",
                          PATRONI_RAFT_PASSWORD=self.PASSWORD, RAFT_PORT='1234')
        self._raft = None

    def _start(self):
        env = os.environ.copy()
        del env['PATRONI_RAFT_PARTNER_ADDRS']
        env['PATRONI_RAFT_SELF_ADDR'] = self.CONTROLLER_ADDR
        env['PATRONI_RAFT_DATA_DIR'] = self._work_directory
        return psutil.Popen([sys.executable, '-m', 'coverage', 'run',
                             '--source=patroni', '-p', 'patroni_raft_controller.py'],
                            stdout=self._log, stderr=subprocess.STDOUT, env=env)

    def query(self, key, scope='batman', group=None):
        ret = self._raft.get(self.path(key, scope, group))
        return ret and ret['value']

    def set(self, key, value):
        self._raft.set(self.path(key), value)

    def cleanup_service_tree(self):
        from patroni.dcs.raft import KVStoreTTL

        if self._raft:
            self._raft.destroy()
            self.stop()
            os.makedirs(self._work_directory)
            self.start()

        ready_event = threading.Event()
        self._raft = KVStoreTTL(ready_event.set, None, None,
                                partner_addrs=[self.CONTROLLER_ADDR], password=self.PASSWORD)
        self._raft.startAutoTick()
        ready_event.wait()


class PatroniPoolController(object):

    PYTHON = sys.executable.replace('\\', '/')
    BACKUP_SCRIPT = [PYTHON, 'features/backup_create.py']
    BACKUP_RESTORE_SCRIPT = ' '.join((PYTHON, os.path.abspath('features/backup_restore.py'))).replace('\\', '/')
    ARCHIVE_RESTORE_SCRIPT = ' '.join((PYTHON, os.path.abspath('features/archive-restore.py')))

    def __init__(self, context):
        self._context = context
        self._dcs = None
        self._output_dir = None
        self._patroni_path = None
        self._processes = {}
        self.create_and_set_output_directory('')
        self._check_postgres_ssl()
        self.known_dcs = {subclass.name(): subclass for subclass in AbstractDcsController.get_subclasses()}

    def _check_postgres_ssl(self):
        try:
            subprocess.check_output(['postgres', '-D', os.devnull, '-c', 'ssl=on'], stderr=subprocess.STDOUT)
            raise Exception  # this one should never happen because the previous line will always raise and exception
        except Exception as e:
            self._context.postgres_supports_ssl = isinstance(e, subprocess.CalledProcessError)\
                and 'SSL is not supported by this build' not in e.output.decode()

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

    def start(self, name, max_wait_limit=40, custom_config=None):
        if name not in self._processes:
            self._processes[name] = PatroniController(self._context, name, self.patroni_path,
                                                      self._output_dir, custom_config)
        self._processes[name].start(max_wait_limit)

    def __getattr__(self, func):
        if func not in ['stop', 'query', 'write_label', 'read_label', 'check_role_has_changed_to',
                        'add_tag_to_config', 'get_watchdog', 'patroni_hang', 'backup', 'read_patroni_log']:
            raise AttributeError("PatroniPoolController instance has no attribute '{0}'".format(func))

        def wrapper(name, *args, **kwargs):
            return getattr(self._processes[name], func)(*args, **kwargs)
        return wrapper

    def stop_all(self):
        for ctl in self._processes.values():
            ctl.cancel_background()
            ctl.stop()
        self._processes.clear()

    def create_and_set_output_directory(self, feature_name):
        feature_dir = os.path.join(self.patroni_path, 'features', 'output', feature_name.replace(' ', '_'))
        if os.path.exists(feature_dir):
            shutil.rmtree(feature_dir)
        os.makedirs(feature_dir)
        self._output_dir = feature_dir

    def clone(self, from_name, cluster_name, to_name):
        f = self._processes[from_name]
        custom_config = {
            'scope': cluster_name,
            'bootstrap': {
                'method': 'pg_basebackup',
                'pg_basebackup': {
                    'command': " ".join(self.BACKUP_SCRIPT
                                        + ['--walmethod=stream', '--dbname="{0}"'.format(f.backup_source)])
                },
                'dcs': {
                    'postgresql': {
                        'parameters': {
                            'max_connections': 101
                        }
                    }
                }
            },
            'postgresql': {
                'parameters': {
                    'archive_mode': 'on',
                    'archive_command': (self.ARCHIVE_RESTORE_SCRIPT + ' --mode archive '
                                        + '--dirname {} --filename %f --pathname %p')
                    .format(os.path.join(self.patroni_path, 'data', 'wal_archive_clone').replace('\\', '/'))
                },
                'authentication': {
                    'superuser': {'password': 'zalando1'},
                    'replication': {'password': 'rep-pass1'}
                }
            }
        }
        self.start(to_name, custom_config=custom_config)

    def backup_restore_config(self, params=None):
        return {
            'command': (self.BACKUP_RESTORE_SCRIPT
                        + ' --sourcedir=' + os.path.join(self.patroni_path, 'data', 'basebackup')).replace('\\', '/'),
            'test-argument': 'test-value',  # test config mapping approach on custom bootstrap/replica creation
            **(params or {}),
        }

    def bootstrap_from_backup(self, name, cluster_name):
        custom_config = {
            'scope': cluster_name,
            'bootstrap': {
                'method': 'backup_restore',
                'backup_restore': self.backup_restore_config({
                    'recovery_conf': {
                        'recovery_target_action': 'promote',
                        'recovery_target_timeline': 'latest',
                        'restore_command': (self.ARCHIVE_RESTORE_SCRIPT + ' --mode restore '
                                            + '--dirname {} --filename %f --pathname %p').format(
                            os.path.join(self.patroni_path, 'data', 'wal_archive_clone').replace('\\', '/'))
                    },
                })
            },
            'postgresql': {
                'authentication': {
                    'superuser': {'password': 'zalando2'},
                    'replication': {'password': 'rep-pass2'}
                }
            }
        }
        self.start(name, custom_config=custom_config)

    def bootstrap_from_backup_no_leader(self, name, cluster_name):
        custom_config = {
            'scope': cluster_name,
            'postgresql': {
                'create_replica_methods': ['no_leader_bootstrap'],
                'no_leader_bootstrap': self.backup_restore_config({'no_leader': '1'})
            }
        }
        self.start(name, custom_config=custom_config)

    @property
    def dcs(self):
        if self._dcs is None:
            self._dcs = os.environ.pop('DCS', 'etcd')
            assert self._dcs in self.known_dcs, 'Unsupported dcs: ' + self._dcs
        return self._dcs


class WatchdogMonitor(object):
    """Testing harness for emulating a watchdog device as a named pipe. Because we can't easily emulate ioctl's we
    require a custom driver on Patroni side. The device takes no action, only notes if it was pinged and/or triggered.
    """
    def __init__(self, name, work_directory, output_dir):
        self.fifo_path = os.path.join(work_directory, 'data', 'watchdog.{0}.fifo'.format(name))
        self.fifo_file = None
        self._stop_requested = False  # Relying on bool setting being atomic
        self._thread = None
        self.last_ping = None
        self.was_pinged = False
        self.was_closed = False
        self._was_triggered = False
        self.timeout = 60
        self._log_file = open(os.path.join(output_dir, 'watchdog.{0}.log'.format(name)), 'w')
        self._log("watchdog {0} initialized".format(name))

    def _log(self, msg):
        tstamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")
        self._log_file.write("{0}: {1}\n".format(tstamp, msg))

    def start(self):
        assert self._thread is None
        self._stop_requested = False
        self._log("starting fifo {0}".format(self.fifo_path))
        fifo_dir = os.path.dirname(self.fifo_path)
        if os.path.exists(self.fifo_path):
            os.unlink(self.fifo_path)
        elif not os.path.exists(fifo_dir):
            os.mkdir(fifo_dir)
        os.mkfifo(self.fifo_path)
        self.last_ping = time.time()

        self._thread = threading.Thread(target=self.run)
        self._thread.start()

    def run(self):
        try:
            while not self._stop_requested:
                self._log("opening")
                self.fifo_file = os.open(self.fifo_path, os.O_RDONLY)
                try:
                    self._log("Fifo {0} connected".format(self.fifo_path))
                    self.was_closed = False
                    while not self._stop_requested:
                        c = os.read(self.fifo_file, 1)

                        if c == b'X':
                            self._log("Stop requested")
                            return
                        elif c == b'':
                            self._log("Pipe closed")
                            break
                        elif c == b'C':
                            command = b''
                            c = os.read(self.fifo_file, 1)
                            while c != b'\n' and c != b'':
                                command += c
                                c = os.read(self.fifo_file, 1)
                            command = command.decode('utf8')

                            if command.startswith('timeout='):
                                self.timeout = int(command.split('=')[1])
                                self._log("timeout={0}".format(self.timeout))
                        elif c in [b'V', b'1']:
                            cur_time = time.time()
                            if cur_time - self.last_ping > self.timeout:
                                self._log("Triggered")
                                self._was_triggered = True
                            if c == b'V':
                                self._log("magic close")
                                self.was_closed = True
                            elif c == b'1':
                                self.was_pinged = True
                                self._log("ping after {0} seconds".format(cur_time - (self.last_ping or cur_time)))
                                self.last_ping = cur_time
                        else:
                            self._log('Unknown command {0} received from fifo'.format(c))
                finally:
                    self.was_closed = True
                    self._log("closing")
                    os.close(self.fifo_file)
        except Exception as e:
            self._log("Error {0}".format(e))
        finally:
            self._log("stopping")
            self._log_file.flush()
            if os.path.exists(self.fifo_path):
                os.unlink(self.fifo_path)

    def stop(self):
        self._log("Monitor stop")
        self._stop_requested = True
        try:
            if os.path.exists(self.fifo_path):
                fd = os.open(self.fifo_path, os.O_WRONLY)
                os.write(fd, b'X')
                os.close(fd)
        except Exception as e:
            self._log("err while closing: {0}".format(str(e)))
        if self._thread:
            self._thread.join()
            self._thread = None

    def reset(self):
        self._log("reset")
        self.was_pinged = self.was_closed = self._was_triggered = False

    @property
    def was_triggered(self):
        delta = time.time() - self.last_ping
        triggered = self._was_triggered or not self.was_closed and delta > self.timeout
        self._log("triggered={0}, {1}s left".format(triggered, self.timeout - delta))
        return triggered


# actions to execute on start/stop of the tests and before running individual features
def before_all(context):
    context.ci = os.name == 'nt' or\
        any(a in os.environ for a in ('TRAVIS_BUILD_NUMBER', 'BUILD_NUMBER', 'GITHUB_ACTIONS'))
    context.timeout_multiplier = 5 if context.ci else 1  # MacOS sometimes is VERY slow
    context.pctl = PatroniPoolController(context)

    context.keyfile = os.path.join(context.pctl.output_dir, 'patroni.key')
    context.certfile = os.path.join(context.pctl.output_dir, 'patroni.crt')
    try:
        if sys.platform == 'darwin' and 'GITHUB_ACTIONS' in os.environ:
            raise Exception
        with open(os.devnull, 'w') as null:
            ret = subprocess.call(['openssl', 'req', '-nodes', '-new', '-x509', '-subj', '/CN=batman.patroni',
                                   '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', context.keyfile,
                                   '-out', context.certfile], stdout=null, stderr=null)
            if ret != 0:
                raise Exception
            os.chmod(context.keyfile, stat.S_IWRITE | stat.S_IREAD)
    except Exception:
        context.keyfile = context.certfile = None

    os.environ.update({'PATRONI_RESTAPI_USERNAME': 'username', 'PATRONI_RESTAPI_PASSWORD': 'password'})
    ctl = {'auth': os.environ['PATRONI_RESTAPI_USERNAME'] + ':' + os.environ['PATRONI_RESTAPI_PASSWORD']}
    if context.certfile:
        os.environ.update({'PATRONI_RESTAPI_CAFILE': context.certfile,
                           'PATRONI_RESTAPI_CERTFILE': context.certfile,
                           'PATRONI_RESTAPI_KEYFILE': context.keyfile,
                           'PATRONI_RESTAPI_VERIFY_CLIENT': 'required',
                           'PATRONI_CTL_INSECURE': 'on',
                           'PATRONI_CTL_CERTFILE': context.certfile,
                           'PATRONI_CTL_KEYFILE': context.keyfile})
        ctl.update({'cacert': context.certfile, 'certfile': context.certfile, 'keyfile': context.keyfile})
    context.request_executor = PatroniRequest({'ctl': ctl}, True)
    context.dcs_ctl = context.pctl.known_dcs[context.pctl.dcs](context)
    context.dcs_ctl.start()
    try:
        context.dcs_ctl.cleanup_service_tree()
    except AssertionError:  # after_all handlers won't be executed in before_all
        context.dcs_ctl.stop()
        raise


def after_all(context):
    context.dcs_ctl.stop()
    subprocess.call([sys.executable, '-m', 'coverage', 'combine'])
    subprocess.call([sys.executable, '-m', 'coverage', 'report'])


def before_feature(context, feature):
    """ create per-feature output directory to collect Patroni and PostgreSQL logs """
    if feature.name == 'watchdog' and os.name == 'nt':
        return feature.skip("Watchdog isn't supported on Windows")
    elif feature.name == 'citus':
        lib = subprocess.check_output(['pg_config', '--pkglibdir']).decode('utf-8').strip()
        if not os.path.exists(os.path.join(lib, 'citus.so')):
            return feature.skip("Citus extenstion isn't available")
    context.pctl.create_and_set_output_directory(feature.name)


def after_feature(context, feature):
    """ send SIGCONT to a dcs if neccessary,
    stop all Patronis remove their data directory and cleanup the keys in etcd """
    context.dcs_ctl.stop_outage()
    context.pctl.stop_all()
    data = os.path.join(context.pctl.patroni_path, 'data')
    if os.path.exists(data):
        shutil.rmtree(data)
    context.dcs_ctl.cleanup_service_tree()

    found = False
    logs = glob.glob(context.pctl.output_dir + '/patroni_*.log')
    for log in logs:
        with open(log) as f:
            for line in f:
                if 'please report it as a BUG' in line:
                    print(':'.join([log, line.rstrip()]))
                    found = True

    if feature.status == 'failed' or found:
        shutil.copytree(context.pctl.output_dir, context.pctl.output_dir + '_failed')
    if found:
        raise Exception('Unexpected errors in Patroni log files')


def before_scenario(context, scenario):
    if 'slot-advance' in scenario.effective_tags:
        for p in context.pctl._processes.values():
            if p._conn and p._conn.server_version < 110000:
                scenario.skip('pg_replication_slot_advance() is not supported on {0}'.format(p._conn.server_version))
                break
    if 'dcs-failsafe' in scenario.effective_tags and not context.dcs_ctl._handle:
        scenario.skip('it is not possible to control state of {0} from tests'.format(context.dcs_ctl.name()))
    if 'reject-duplicate-name' in scenario.effective_tags and context.dcs_ctl.name() == 'raft':
        scenario.skip('Flaky test with Raft')
