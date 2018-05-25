import time

from behave import step


select_replication_query = """
SELECT * FROM pg_catalog.pg_stat_replication
WHERE application_name = '{0}'
"""

create_replication_slot_query = """
SELECT pg_create_physical_replication_slot('{0}')
"""

create_role_query = """
CREATE ROLE {} WITH REPLICATION
"""


@step('I start {name:w} without slots sync')
def start_patroni_without_slots_sync(context, name):
    return context.pctl.start(name, custom_config={
        "bootstrap": {
            "dcs" : {
                "postgresql": {
                    "use_slots": False
                }
            }
        }
    })


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
                "standby_cluster": {
                    "host": "localhost",
                    "port": port,
                    "primary_slot_name": "postgres1",
                }
            }
        }
    })

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
            return True

        time.sleep(1)

    return False

@step('I create a replication slot {slot_name:w} on {pg_name:w}')
def create_replication_slot(context, slot_name, pg_name):
    return context.pctl.query(
        pg_name,
        create_replication_slot_query.format(slot_name),
        fail_ok=True
    )

@step('I create a role {role_name:w} on {pg_name:w}')
def create_role(context, role_name, pg_name):
    return context.pctl.query(
        pg_name,
        create_role_query.format(role_name),
        fail_ok=True
    )
