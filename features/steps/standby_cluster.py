import time

from behave import step


select_replication = """
SELECT * FROM pg_catalog.pg_stat_replication
WHERE application_name = '{0}'
"""


@step('I start {name:w} in a cluster {cluster_name:w}')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name
    })


@step('I start {name:w} in a standby cluster {cluster_name:w} as a clone of {name2:w}')
def start_patroni_stanby_cluster(context, name, cluster_name, name2):
    port = context.pctl._processes[name2]._connkwargs.get('port')
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "bootstrap": {
            "dcs": {
                "standby_cluster" :{
                    "host": "localhost",
                    "port": port,
                    "primary_slot_name": "postgresql1",
                }
            }
        }
    })

@step('{pg_name1:w} is replicating from {pg_name2:w} after {timeout:d} seconds')
def check_replication_slot_existense(context, pg_name1, pg_name2, timeout):
    bound_time = time.time() + timeout

    while time.time() < bound_time:
        cur = context.pctl.query(
            pg_name2,
            select_replication.format(pg_name1),
            fail_ok=True
        )

        if cur and len(cur.fetchall()) != 0:
            return True

        time.sleep(1)

    return False
