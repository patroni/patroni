import json
import os
import time

from behave import step
from patroni_api import check_response, do_request


def callbacks(context, name):
    return {c: '{0} features/callback2.py {1}'.format(context.pctl.PYTHON, name)
            for c in ('on_start', 'on_stop', 'on_restart', 'on_role_change')}


@step('I start {name:name} in a cluster {cluster_name:w}')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "postgresql": {
            "callbacks": callbacks(context, name),
            "backup_restore": context.pctl.backup_restore_config()
        }
    })


@step('I start {name:name} in a standby cluster {cluster_name:w} as a clone of {name2:name}')
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
                "synchronous_mode": True,  # should be completely ignored
                "standby_cluster": {
                    "host": "localhost",
                    "port": port,
                    "primary_slot_name": "pm_1",
                    "create_replica_methods": ["backup_restore", "basebackup"]
                },
                "postgresql": {"parameters": {"wal_level": "logical"}}
            }
        },
        "postgresql": {
            "callbacks": callbacks(context, name)
        }
    })
    return context.pctl.start(name)


@step('{pg_name1:name} is replicating from {pg_name2:name} after {timeout:d} seconds')
def check_replication_status(context, pg_name1, pg_name2, timeout):
    bound_time = time.time() + timeout * context.timeout_multiplier

    while time.time() < bound_time:
        cur = context.pctl.query(
            pg_name2,
            "SELECT * FROM pg_catalog.pg_stat_replication WHERE application_name = '{0}'".format(pg_name1),
            fail_ok=True
        )

        if cur and len(cur.fetchall()) != 0:
            break

        time.sleep(1)
    else:
        assert False, "{0} is not replicating from {1} after {2} seconds".format(pg_name1, pg_name2, timeout)


@step('I switch standby cluster {scope:name} to archive recovery')
def standby_cluster_archive(context, scope, demote=False):
    for name, proc in context.pctl._processes.items():
        if proc._scope != scope or not proc._is_running:
            continue

        config = context.pctl.read_config(name)
        url = f'http://{config["restapi"]["connect_address"]}/config'
        data = {
            "standby_cluster": {
                "restore_command": config['bootstrap']['dcs']['postgresql']['parameters']['restore_command']
            }
        }
        if not demote:
            data['standby_cluster']['primary_slot_name'] = data['standby_cluster']['host'] =\
                data['standby_cluster']['port'] = None
        do_request(context, 'PATCH', url, json.dumps(data))
        check_response(context, 'code', 200)
        break


@step('I demote cluster {scope:name}')
def demote_cluster(context, scope):
    standby_cluster_archive(context, scope, True)
