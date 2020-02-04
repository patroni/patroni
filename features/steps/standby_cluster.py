import os
import sys
import time

from behave import step


select_replication_query = """
SELECT * FROM pg_catalog.pg_stat_replication
WHERE application_name = '{0}'
"""

callback = sys.executable + " features/callback2.py "


@step('I start {name:w} with callback configured')
def start_patroni_with_callbacks(context, name):
    return context.pctl.start(name, custom_config={
        "postgresql": {
            "callbacks": {
                "on_role_change": sys.executable + " features/callback.py"
            }
        }
    })


@step('I start {name:w} in a cluster {cluster_name:w}')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "postgresql": {
            "callbacks": {c: callback + name for c in ('on_start', 'on_stop', 'on_restart', 'on_role_change')},
            "backup_restore": {
                "command": (sys.executable + " features/backup_restore.py --sourcedir=" +
                            os.path.join(context.pctl.patroni_path, 'data', 'basebackup'))}
        }
    })


@step('I start {name:w} in a standby cluster {cluster_name:w} as a clone of {name2:w}')
def start_patroni_standby_cluster(context, name, cluster_name, name2):
    # we need to remove patroni.dynamic.json in order to "bootstrap" standby cluster with existing PGDATA
    os.unlink(os.path.join(context.pctl._processes[name]._data_dir, 'patroni.dynamic.json'))
    port = context.pctl._processes[name2]._connkwargs.get('port')
    context.pctl._processes[name].update_config({
        "scope": cluster_name,
        "bootstrap": {
            "dcs": {
                "ttl": 20,
                "loop_wait": 2,
                "retry_timeout": 5,
                "standby_cluster": {
                    "host": "localhost",
                    "port": port,
                    "primary_slot_name": "pm_1",
                    "create_replica_methods": ["backup_restore", "basebackup"]
                }
            }
        },
        "postgresql": {
            "callbacks": {c: callback + name for c in ('on_start', 'on_stop', 'on_restart', 'on_role_change')}
        }
    })
    return context.pctl.start(name)


@step('{pg_name1:w} is replicating from {pg_name2:w} after {timeout:d} seconds')
def check_replication_status(context, pg_name1, pg_name2, timeout):
    bound_time = time.time() + timeout

    while time.time() < bound_time:
        cur = context.pctl.query(
            pg_name2,
            select_replication_query.format(pg_name1),
            fail_ok=True
        )

        if cur and len(cur.fetchall()) != 0:
            break

        time.sleep(1)
    else:
        assert False, "{0} is not replicating from {1} after {2} seconds".format(pg_name1, pg_name2, timeout)
