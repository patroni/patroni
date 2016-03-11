import psycopg2 as pg

from behave import step, then
from time import sleep, time


@step('I start {name}')
def start_patroni(context, name):
    return context.pctl.start(name)


@step('I shut down {name}')
def stop_patroni(context, name):
    return context.pctl.stop(name)


@step('I kill {name}')
def kill_patroni(context, name):
    return context.pctl.stop(name, kill=True)


@step('I add the table {table_name} to {pg_name}')
def add_table(context, table_name, pg_name):
    # parse the configuration file and get the port
    try:
        context.pctl.query(pg_name, "CREATE TABLE {0}()".format(table_name))
    except pg.Error as e:
        assert False, "Error creating table {0} on {1}: {2}".format(table_name, pg_name, e)


@then('Table {table_name} is present on {pg_name} after {max_replication_delay} seconds')
def table_is_present_on(context, table_name, pg_name, max_replication_delay):
    for _ in range(int(max_replication_delay)):
        if context.pctl.query(pg_name, "SELECT 1 FROM {0}".format(table_name), fail_ok=True) is not None:
            break
        sleep(1)
    else:
        assert False,\
            "Table {0} is not present on {1} after {2} seconds".format(table_name, pg_name, max_replication_delay)


@then('{pg_name} role is the {pg_role} after {max_promotion_timeout} seconds')
def check_role(context, pg_name, pg_role, max_promotion_timeout):
    if not context.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout)):
        assert False,\
            "{0} role didn't change to {1} after {2} seconds".format(pg_name, pg_role, max_promotion_timeout)


@step('replication works from {master} to {replica} after {time_limit} seconds')
@then('replication works from {master} to {replica} after {time_limit} seconds')
def replication_works(context, master, replica, time_limit):
    context.execute_steps(u"""
        When I add the table test_{0} to {1}
        Then table test_{0} is present on {2} after {3} seconds
    """.format(int(time()), master, replica, time_limit))
