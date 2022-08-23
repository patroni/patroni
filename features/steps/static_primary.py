import json

from behave import step
from time import sleep


@step('I start {name:w} as static primary')
def start_patroni_with_static_primary(context, name):
    return context.pctl.start(name, custom_config={'bootstrap': {'dcs': {'static_primary': name}}})


@step('I start {name:w} with a configured static primary will not boot after {time_limit:d} seconds')
def start_patroni_as_replica_with_static_primary(context, name, time_limit):
    return context.pctl.start_with_expected_failure(name, max_wait_limit=time_limit)


@step('"{name}" key not in DCS after waiting {time_limit:d} seconds')
def check_member_not_present(context, name, time_limit):
    sleep(time_limit)
    found_value = False
    try:
        res = json.loads(context.dcs_ctl.query(name))
        if res is not None:
            found_value = True
    except Exception:
        pass

    if found_value:
        print("found value under DCS key {}: {}".format(name, res))
        assert False, "found value under DCS key {} after {} seconds".format(name, time_limit)
    return True


@step('"{name}" is stopped and uninitialized after waiting {time_limit:d} seconds')
def member_is_stopped_and_uninitialized(context, name, time_limit):
    sleep(time_limit)
    value = None
    try:
        value = json.loads(context.dcs_ctl.query(name))
        print("response from dcs_ctl.query({}): {}".format(name, value))
    except Exception:
        pass

    if value is None:
        assert False, "context.dcs_ctl.query({}) unexpectedly returned None".format(name)

    state = value.get("state")
    role = value.get("role")
    if state != "stopped":
        assert False, "{} has state {}, expected 'stopped', after {} seconds".format(name, state, time_limit)
    if role != "uninitialized":
        assert False, "{} has role {}, expected 'uninitialized', after {} seconds".format(name, role, time_limit)
    return True
