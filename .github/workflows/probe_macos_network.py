import json
import os
import pwd
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

from pathlib import Path


def main():
    if sys.platform != 'darwin' or os.getuid() == 0:
        raise RuntimeError('Run this diagnostic as the non-root macOS runner user')

    output = Path('macos-network-probe-output').resolve()
    output.mkdir()
    workspace = Path.cwd()
    user = pwd.getpwuid(os.getuid()).pw_name
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/opt/postgresql@16/bin:.' + os.pathsep + env['PATH']
    env['PYTHONUNBUFFERED'] = '1'
    command = [sys.executable, '-m', 'behave', '--no-color', '--show-timings',
               'features/basic_replication.feature', '--name', '^check replication of a single table$']
    results = []

    with tempfile.TemporaryDirectory(prefix='.patroni-network-probe-', dir=Path.home()) as directory:
        directory = Path(directory)
        host_key = directory / 'host_key'
        client_key = directory / 'client_key'
        for key in (host_key, client_key):
            subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
        authorized_keys = directory / 'authorized_keys'
        authorized_keys.write_text(client_key.with_suffix('.pub').read_text())
        authorized_keys.chmod(0o600)
        known_hosts = directory / 'known_hosts'
        known_hosts.write_text('[127.0.0.1]:22222 ' + host_key.with_suffix('.pub').read_text())
        pid_file = directory / 'sshd.pid'
        config = directory / 'sshd_config'
        config.write_text('\n'.join([
            'Port 22222',
            'ListenAddress 127.0.0.1',
            'HostKey "{}"'.format(host_key),
            'PidFile "{}"'.format(pid_file),
            'AuthorizedKeysFile "{}"'.format(authorized_keys),
            'AuthenticationMethods publickey',
            'PubkeyAuthentication yes',
            'PasswordAuthentication no',
            'KbdInteractiveAuthentication no',
            'PermitRootLogin no',
            'AllowUsers ' + user,
            'UsePAM no',
            'PrintMotd no',
            'LogLevel VERBOSE',
            '',
        ]))
        server_log = output / 'sshd.log'
        server_log.touch(mode=0o600)
        subprocess.run(['sudo', '-n', '/usr/sbin/sshd', '-t', '-f', str(config)], check=True)
        subprocess.run(['sudo', '-n', '/usr/sbin/sshd', '-f', str(config), '-E', str(server_log)], check=True)
        try:
            ssh = ['/usr/bin/ssh', '-F', '/dev/null', '-p', '22222', '-i', str(client_key),
                   '-o', 'IdentitiesOnly=yes', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
                   '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(known_hosts),
                   '-l', user, '127.0.0.1']
            for attempt in range(10):
                check = subprocess.run(ssh + ['id -u'], capture_output=True, text=True, timeout=10)
                if check.returncode == 0:
                    break
                print('Waiting for local SSH: ' + check.stderr.strip(), flush=True)
                time.sleep(1)
            else:
                raise RuntimeError('Local SSH did not become available: ' + check.stderr)
            if int(check.stdout.strip()) != os.getuid():
                raise RuntimeError('SSH must execute as the existing non-root runner user')

            names = ('PATH', 'DCS', 'PGVERSION', 'GITHUB_ACTIONS', 'CI', 'TMPDIR',
                     'LANG', 'LC_ALL', 'LC_CTYPE', 'PYTHONUNBUFFERED')
            remote_env = ['{}={}'.format(name, env[name]) for name in names if name in env]
            remote = 'cd {} && {}'.format(shlex.quote(str(workspace)),
                                          shlex.join(['env'] + remote_env + command))

            # Repeat normal execution after SSH to distinguish a context change from a cold-start cost.
            for label in ('normal-1', 'ssh-1', 'ssh-2', 'normal-2'):
                print('Starting ' + label, flush=True)
                log_path = output / (label + '.log')
                start = time.monotonic()
                try:
                    with log_path.open('w') as log:
                        result = subprocess.run(ssh + [remote] if label.startswith('ssh') else command,
                                                env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300)
                finally:
                    if Path('features/output').exists():
                        shutil.copytree('features/output', output / (label + '-services'),
                                        ignore=shutil.ignore_patterns('*.key', '*.crt', '*.yml'))
                text = log_path.read_text()
                match = re.search(r'I start postgres-0[^\n]*?\s(\d+\.\d+)s', text)
                if not match:
                    print(text, flush=True)
                    raise RuntimeError('No first-node startup timing was recorded for ' + label)
                results.append({'mode': label, 'exit_code': result.returncode,
                                'duration_seconds': time.monotonic() - start,
                                'first_node_seconds': float(match.group(1))})
                (output / 'results.json').write_text(json.dumps(results, indent=2))
                print(json.dumps(results[-1]), flush=True)
            return int(any(result['exit_code'] != 0 for result in results))
        finally:
            if not pid_file.exists():
                raise RuntimeError('Cannot find the diagnostic SSH server PID for cleanup')
            subprocess.run(['sudo', '-n', 'kill', str(int(pid_file.read_text().strip()))], check=True)


if __name__ == '__main__':
    sys.exit(main())
