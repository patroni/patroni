import json
import time

from behave import step, then


@step('I configure and start {name:w} with a tag {tag_name:w} {tag_value:w}')
def start_patroni_with_a_name_value_tag(context, name, tag_name, tag_value):
    return context.pctl.start(name, custom_config={'tags': {tag_name: tag_value}})


@then('There is a {label} with "{content}" in {name:w} data directory')
def check_label(context, label, content, name):
    label = context.pctl.read_label(name, label)
    label = label.replace('\n', '\\n')
    assert content in label, "{0} doesn't contain {1}".format(label, content)


@step('I create label with "{content:w}" in {name:w} data directory')
def write_label(context, content, name):
    context.pctl.write_label(name, content)


@step('"{name}" key in DCS has {key:w}={value:w} after {time_limit:d} seconds')
def check_member(context, name, key, value, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while time.time() < max_time:
        try:
            response = json.loads(context.dcs_ctl.query(name))
            if response.get(key) == value:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, "{0} does not have {1}={2} in dcs after {3} seconds".format(name, key, value, time_limit)


@step('there is a non empty {key:w} key in DCS after {time_limit:d} seconds')
def check_initialize(context, key, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while time.time() < max_time:
        try:
            if context.dcs_ctl.query(key):
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, "There is no {0} in dcs after {1} seconds".format(key, time_limit)
