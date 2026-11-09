import json
import os
import shutil
import subprocess
import sys
import time

from pathlib import Path


def main():
    if sys.platform != 'darwin' or os.getuid() == 0:
        raise RuntimeError('Run this diagnostic as the non-root macOS runner user')
    output = Path('macos-network-probe-output').resolve()
    output.mkdir()
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/opt/postgresql@16/bin:.' + os.pathsep + env['PATH']
    env['PYTHONUNBUFFERED'] = '1'
    report_path = output / 'trace.json'
    command = [sys.executable, '-m', 'behave', '--no-color', '--show-timings',
               'features/basic_replication.feature', '--name', '^check replication of a single table$',
               '--format', 'json', '--outfile', str(report_path), '--format', 'plain', '--outfile', '-']
    start = time.monotonic()
    try:
        with (output / 'trace.log').open('w') as log:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=300)
    finally:
        if Path('features/output').exists():
            shutil.copytree('features/output', output / 'trace-services',
                            ignore=shutil.ignore_patterns('*.key', '*.crt', '*.yml'))
    report = json.loads(report_path.read_text())
    starts = [step['result']['duration'] for feature in report for scenario in feature['elements']
              if scenario['name'] == 'check replication of a single table' for step in scenario['steps']
              if step['name'] == 'I start postgres-0']
    if len(starts) != 1:
        raise RuntimeError('Expected exactly one first-node startup timing')
    summary = {'exit_code': result.returncode, 'duration_seconds': time.monotonic() - start,
               'first_node_seconds': starts[0]}
    (output / 'results.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)
    return result.returncode


if __name__ == '__main__':
    sys.exit(main())
