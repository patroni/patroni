import time

from behave import step, then


@step('I start {name:name} in site {site_name:w} with failover priority {failover_priority:d}, '
      'sync priority {sync_priority:d}')
def start_patroni_tags(context, name, site_name, failover_priority, sync_priority):
    config = {
        "site": site_name,
        "tags": {
            "clonefrom": True
        }
    }
    if failover_priority is not None:
        config["tags"]["failover_priority"] = failover_priority
    if sync_priority is not None:
        config["tags"]["sync_priority"] = sync_priority

    return context.pctl.start(name, custom_config=config)


@step('I start {name:name} in site {site_name:w}')
def start_patroni(context, name, site_name):
    start_patroni_tags(context, name, site_name, None, None)


@then('{name:name} is in sync with primary after {timeout:d} seconds')
def replica_not_lagging(context, name, timeout):
    leader = context.dcs_ctl.query("leader")
    bound_time = time.time() + timeout
    function_name = 'pg_current_xlog_location()' if context.pctl.server_version / 10000 < 10 else 'pg_current_wal_lsn()'
    location_name = 'replay_location' if context.pctl.server_version / 10000 < 10 else 'replay_lsn'

    while time.time() < bound_time:
        cur = context.pctl.query(
            leader,
            f"SELECT {function_name} - {location_name} FROM pg_catalog.pg_stat_replication \
                WHERE application_name = '{name}'",
            fail_ok=True
        )
        if cur and cur.fetchall() == [(0,)]:
            break

        time.sleep(1)
    else:
        assert False, "{0} is lagging behind after {1} seconds".format(name, timeout)
