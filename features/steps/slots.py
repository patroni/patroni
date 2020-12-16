from behave import step, then
import psycopg2 as pg


@step('I create a logical replication slot {slot_name} on {pg_name:w} with the {plugin:w} plugin')
def create_logical_replication_slot(context, slot_name, pg_name, plugin):
    try:
        output = context.pctl.query(pg_name, ("SELECT pg_create_logical_replication_slot('{0}', '{1}'),"
                                              " current_database()").format(slot_name, plugin))
        print(output.fetchone())
    except pg.Error as e:
        print(e)
        assert False, "Error creating slot {0} on {1} with plugin {2}".format(slot_name, pg_name, plugin)


@then('{pg_name:w} has a logical replication slot named {slot_name} with the {plugin:w} plugin')
def has_logical_replication_slot(context, pg_name, slot_name, plugin):
    try:
        row = context.pctl.query(pg_name, ("SELECT slot_type, plugin FROM pg_replication_slots"
                                           " WHERE slot_name = '{0}'").format(slot_name)).fetchone()
        assert row, "Couldn't find replication slot named {0}".format(slot_name)
        assert row[0] == "logical", "Found replication slot named {0} but wasn't a logical slot".format(slot_name)
        assert row[1] == plugin, ("Found replication slot named {0} but was using plugin "
                                  "{1} rather than {2}").format(slot_name, row[1], plugin)
    except pg.Error:
        assert False, "Error looking for slot {0} on {1} with plugin {2}".format(slot_name, pg_name, plugin)


@then('{pg_name:w} does not have a logical replication slot named {slot_name}')
def does_not_have_logical_replication_slot(context, pg_name, slot_name):
    try:
        row = context.pctl.query(pg_name, ("SELECT 1 FROM pg_replication_slots"
                                           " WHERE slot_name = '{0}'").format(slot_name)).fetchone()
        assert not row, "Found unexpected replication slot named {0}".format(slot_name)
    except pg.Error:
        assert False, "Error looking for slot {0} on {1}".format(slot_name, pg_name)
