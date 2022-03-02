import json
import patroni.psycopg as pg

from behave import step, then
from time import sleep, time


@step('I start {name:w} as static primary')
def start_patroni_with_static_primary(context, name):
    return context.pctl.start(name, custom_config={'bootstrap': {'dcs': {'static_primary': name}}})


@step('I start {name:w} with a configured static primary will not boot after {time_limit:d} seconds')
def start_patroni_as_replica_with_static_primary(context, name, time_limit):
    return context.pctl.start_with_expected_failure(name, max_wait_limit=time_limit)


@step('"{name}" key not in DCS after waiting {time_limit:d} seconds')
def check_member_not_present(context, name, time_limit):
    sleep(time_limit)
    try:
        json.loads(context.dcs_ctl.query(name))
        assert False, "found value under DCS key {} after {} seconds".format(name, time_limit)
    except Exception:
        return
