import os

from behave import step


@step('I ensure {name:name} fails to start after a failure')
def spoil_autoconf(context, name):
    with open(os.path.join(context.pctl._processes[name]._data_dir, 'postgresql.auto.conf'), 'w') as f:
        f.write('foo=bar')
