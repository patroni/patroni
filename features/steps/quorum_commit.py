import json
import re
import time

from behave import step, then


@step('sync key in DCS has {key:w}={value} after {time_limit:d} seconds')
def check_sync(context, key, value, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    dcs_value = None
    while time.time() < max_time:
        try:
            response = json.loads(context.dcs_ctl.query('sync'))
            dcs_value = response.get(key)
            if key == 'sync_standby' and set((dcs_value or '').split(',')) == set(value.split(',')):
                return
            elif str(dcs_value) == value:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, "sync does not have {0}={1} (found {2}) in dcs after {3} seconds".format(key, value,
                                                                                           dcs_value, time_limit)


@then('synchronous_standby_names on {name:2} is set to "{value}" after {time_limit:d} seconds')
def check_synchronous_standby_names(context, name, value, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)

    if value == '_empty_str_':
        value = ''

    if '(' in value:
        m = re.match(r'.*(\d+) \(([^)]+)\)', value)
        expected_value = set(m.group(2).split())
        expected_num = m.group(1)
    else:
        expected_value = set([value])
        expected_num = '1'

    while time.time() < max_time:
        try:
            ssn = context.pctl.query(name, "SHOW synchronous_standby_names").fetchone()[0]
            if '(' in ssn:
                m = re.match(r'.*(\d+) \(([^)]+)\)', ssn)
                db_value = set(m.group(2).split())
                db_num = m.group(1)
            else:
                db_value = set([ssn])
                db_num = '1'
            if expected_value == db_value and expected_num == db_num:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, "synchronous_standby_names is not set to '{0}' (found '{1}') after {2} seconds".format(value, ssn,
                                                                                                         time_limit)
