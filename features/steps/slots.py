import json
import time

from behave import step, then

import patroni.psycopg as pg


@step('I create a logical {slot_type} slot {slot_name} on {pg_name:name} with the {plugin:w} plugin')
def create_logical_replication_slot(context, slot_type, slot_name, pg_name, plugin):
    failover = ', failover=>true' if slot_type == 'failover' else ''
    try:
        context.pctl.query(pg_name, f"SELECT pg_create_logical_replication_slot('{slot_name}', '{plugin}'{failover})")
    except pg.Error as e:
        assert False, "Error creating slot {0} on {1} with plugin {2}: {3}".format(slot_name, pg_name, plugin, e)


@step('{pg_name:name} has a {slot_type:w} replication slot named {slot_name}'
      ' with the {plugin:w} plugin after {time_limit:d} seconds')
@then('{pg_name:name} has a {slot_type:w} replication slot named {slot_name}'
      ' with the {plugin:w} plugin after {time_limit:d} seconds')
def has_logical_replication_slot(context, slot_type, pg_name, slot_name, plugin, time_limit):
    synced = ', synced' if slot_type == 'synced' and context.pctl.server_version >= 170000 else ''
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while time.time() < max_time:
        try:
            if synced:
                try:
                    context.pctl.query(pg_name, "SELECT pg_sync_replication_slots()")
                except Exception:
                    pass
            row = context.pctl.query(pg_name, (f"SELECT slot_type, plugin{synced} FROM pg_replication_slots"
                                               f" WHERE slot_name = '{slot_name}'")).fetchone()
            if row:
                assert row[0] == "logical", f"Replication slot {slot_name} isn't a logical but {row[0]}"
                assert row[1] == plugin, f"Replication slot {slot_name} using plugin {row[1]} rather than {plugin}"
                if not synced or row[2]:
                    return
        except Exception:
            pass
        time.sleep(1)
    assert False, f"Error looking for slot {slot_name} on {pg_name} with plugin {plugin}"


@step('{pg_name:name} does not have a replication slot named {slot_name:w}')
@then('{pg_name:name} does not have a replication slot named {slot_name:w}')
def does_not_have_replication_slot(context, pg_name, slot_name):
    try:
        row = context.pctl.query(pg_name, ("SELECT 1 FROM pg_replication_slots"
                                           " WHERE slot_name = '{0}'").format(slot_name)).fetchone()
        assert not row, "Found unexpected replication slot named {0}".format(slot_name)
    except pg.Error:
        assert False, "Error looking for slot {0} on {1}".format(slot_name, pg_name)


@step('{slot_type:w} slot {slot_name:w} is in sync between '
      '{pg_name1:name} and {pg_name2:name} after {time_limit:d} seconds')
def slots_in_sync(context, slot_type, slot_name, pg_name1, pg_name2, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    column = 'confirmed_flush_lsn' if slot_type.lower() == 'logical' else 'restart_lsn'
    query = f"SELECT {column} FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
    server_version = context.pctl.server_version
    while time.time() < max_time:
        if server_version >= 170000 and slot_type.lower() == 'logical':
            try:
                context.pctl.query(pg_name2, "SELECT pg_sync_replication_slots()")
            except Exception:
                pass
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


@step('I get all changes from logical slot {slot_name:w} on {pg_name:name}')
def logical_slot_get_changes(context, slot_name, pg_name):
    context.pctl.query(pg_name, "SELECT * FROM pg_logical_slot_get_changes('{0}', NULL, NULL)".format(slot_name))


@step('I get all changes from physical slot {slot_name:w} on {pg_name:name}')
def physical_slot_get_changes(context, slot_name, pg_name):
    context.pctl.query(pg_name, f"SELECT * FROM pg_replication_slot_advance('{slot_name}', pg_current_wal_lsn())")


@step('{pg_name:name} has a physical replication slot named {slot_name} after {time_limit:d} seconds')
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


@step('physical replication slot named {slot_name} on {pg_name:name} has no xmin value after {time_limit:d} seconds')
def physical_slot_no_xmin(context, pg_name, slot_name, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    query = "SELECT xmin FROM pg_catalog.pg_replication_slots WHERE slot_type = 'physical'" +\
        f" AND slot_name = '{slot_name}'"
    exists = False
    while time.time() < max_time:
        try:
            row = context.pctl.query(pg_name, query).fetchone()
            exists = bool(row)
            if exists and row[0] is None:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, f"Physical slot {slot_name} doesn't exist after {time_limit} seconds" if not exists \
        else f"Physical slot {slot_name} has xmin value after {time_limit} seconds"


@step('"{name}" key in DCS has {subkey} in {key:w}')
def dcs_key_contains(context, name, subkey, key):
    response = json.loads(context.dcs_ctl.query(name))
    assert key in response and subkey in response[key], f"{name} key in DCS doesn't have {subkey} in {key}"


@step('"{name}" key in DCS does not have {subkey} in {key:w}')
def dcs_key_does_not_contain(context, name, subkey, key):
    response = json.loads(context.dcs_ctl.query(name))
    assert key not in response or subkey not in response[key], f"{name} key in DCS has {subkey} in {key}"


@then("synchronized_standby_slots on {name:2} is set to '{value}' after {time_limit:d} seconds")
def check_synchronized_standby_slots(context, name, value, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    expected = set(value.split(',')) if value else set()
    actual = None
    while time.time() < max_time:
        try:
            actual = context.pctl.query(name, "SHOW synchronized_standby_slots").fetchone()[0]
            if set(actual.split(',')) - {''} == expected:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, (f"synchronized_standby_slots on {name} is not set to '{value}' "
                   f"(found '{actual}') after {time_limit} seconds")


@then('synchronized_standby_slots on {name:2} is empty after {time_limit:d} seconds')
def check_synchronized_standby_slots_empty(context, name, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    actual = None
    while time.time() < max_time:
        try:
            actual = context.pctl.query(name, "SHOW synchronized_standby_slots").fetchone()[0]
            if not actual:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, (f"synchronized_standby_slots on {name} is not empty "
                   f"(found '{actual}') after {time_limit} seconds")


@then('synchronized_standby_slots on {name:2} matches existing physical slots after {time_limit:d} seconds')
def check_synchronized_standby_slots_match_physical(context, name, time_limit):
    """Assert every name in synchronized_standby_slots corresponds to a real physical slot.

    This is the killer assertion for the slot-naming regression where
    quoted member names (e.g. ``"postgres-1"``) were fed through
    ``slot_name_from_member_name`` producing garbage like ``u0034postgres_1u0034``."""
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    declared = actual_slots = None
    while time.time() < max_time:
        try:
            declared_raw = context.pctl.query(name, "SHOW synchronized_standby_slots").fetchone()[0]
            declared = set(s for s in declared_raw.split(',') if s)
            if not declared:
                time.sleep(1)
                continue
            actual_slots = {row[0] for row in context.pctl.query(
                name, "SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'physical'"
            ).fetchall()}
            if declared.issubset(actual_slots):
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, (f"synchronized_standby_slots on {name} (={declared}) does not match existing "
                   f"physical slots (={actual_slots}) after {time_limit} seconds")
