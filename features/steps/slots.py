import json
import time

from behave import step, then
import patroni.psycopg as pg


@step('I create a logical replication slot {slot_name} on {pg_name:w} with the {plugin:w} plugin')
def create_logical_replication_slot(context, slot_name, pg_name, plugin):
    try:
        output = context.pctl.query(pg_name, ("SELECT pg_create_logical_replication_slot('{0}', '{1}'),"
                                              " current_database()").format(slot_name, plugin))
        print(output.fetchone())
    except pg.Error as e:
        print(e)
        assert False, "Error creating slot {0} on {1} with plugin {2}".format(slot_name, pg_name, plugin)


@step('{pg_name:w} has a logical replication slot named {slot_name}'
      ' with the {plugin:w} plugin after {time_limit:d} seconds')
@then('{pg_name:w} has a logical replication slot named {slot_name}'
      ' with the {plugin:w} plugin after {time_limit:d} seconds')
def has_logical_replication_slot(context, pg_name, slot_name, plugin, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while time.time() < max_time:
        try:
            row = context.pctl.query(pg_name, ("SELECT slot_type, plugin FROM pg_replication_slots"
                                               f" WHERE slot_name = '{slot_name}'")).fetchone()
            if row:
                assert row[0] == "logical", f"Replication slot {slot_name} isn't a logical but {row[0]}"
                assert row[1] == plugin, f"Replication slot {slot_name} using plugin {row[1]} rather than {plugin}"
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, f"Error looking for slot {slot_name} on {pg_name} with plugin {plugin}"


@step('{pg_name:w} does not have a replication slot named {slot_name:w}')
@then('{pg_name:w} does not have a replication slot named {slot_name:w}')
def does_not_have_replication_slot(context, pg_name, slot_name):
    try:
        row = context.pctl.query(pg_name, ("SELECT 1 FROM pg_replication_slots"
                                           " WHERE slot_name = '{0}'").format(slot_name)).fetchone()
        assert not row, "Found unexpected replication slot named {0}".format(slot_name)
    except pg.Error:
        assert False, "Error looking for slot {0} on {1}".format(slot_name, pg_name)


@step('{slot_type:w} slot {slot_name:w} is in sync between {pg_name1:w} and {pg_name2:w} after {time_limit:d} seconds')
def slots_in_sync(context, slot_type, slot_name, pg_name1, pg_name2, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    column = 'confirmed_flush_lsn' if slot_type.lower() == 'logical' else 'restart_lsn'
    query = f"SELECT {column} FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
    while time.time() < max_time:
        try:
            slot1 = context.pctl.query(pg_name1, query).fetchone()
            slot2 = context.pctl.query(pg_name2, query).fetchone()
            if slot1[0] == slot2[0]:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, \
        f"{slot_type} slot {slot_name} is not in sync between {pg_name1} and {pg_name2} after {time_limit} seconds"


@step('I get all changes from logical slot {slot_name:w} on {pg_name:w}')
def logical_slot_get_changes(context, slot_name, pg_name):
    context.pctl.query(pg_name, "SELECT * FROM pg_logical_slot_get_changes('{0}', NULL, NULL)".format(slot_name))


@step('I get all changes from physical slot {slot_name:w} on {pg_name:w}')
def physical_slot_get_changes(context, slot_name, pg_name):
    context.pctl.query(pg_name, f"SELECT * FROM pg_replication_slot_advance('{slot_name}', pg_current_wal_lsn())")


@step('{pg_name:w} has a physical replication slot named {slot_name} after {time_limit:d} seconds')
def has_physical_replication_slot(context, pg_name, slot_name, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    query = f"SELECT * FROM pg_catalog.pg_replication_slots WHERE slot_type = 'physical' AND slot_name = '{slot_name}'"
    while time.time() < max_time:
        try:
            row = context.pctl.query(pg_name, query).fetchone()
            if row:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, f"Physical slot {slot_name} doesn't exist after {time_limit} seconds"


@step('"{name}" key in DCS has {subkey:w} in {key:w}')
def dcs_key_contains(context, name, subkey, key):
    response = json.loads(context.dcs_ctl.query(name))
    assert key in response and subkey in response[key], f"{name} key in DCS doesn't have {subkey} in {key}"


@step('"{name}" key in DCS does not have {subkey:w} in {key:w}')
def dcs_key_does_not_contain(context, name, subkey, key):
    response = json.loads(context.dcs_ctl.query(name))
    assert key not in response or subkey not in response[key], f"{name} key in DCS has {subkey} in {key}"
