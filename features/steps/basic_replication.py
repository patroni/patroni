import patroni.psycopg as pg

from behave import step, then
from time import sleep, time


@step('I start {name:w}')
def start_patroni(context, name):
    return context.pctl.start(name)


@step('I shut down {name:w}')
def stop_patroni(context, name):
    return context.pctl.stop(name, timeout=60)


@step('I kill {name:w}')
def kill_patroni(context, name):
    return context.pctl.stop(name, kill=True)


@step('I kill postmaster on {name:w}')
def stop_postgres(context, name):
    return context.pctl.stop(name, postgres=True)


@step('I add the table {table_name:w} to {pg_name:w}')
def add_table(context, table_name, pg_name):
    # parse the configuration file and get the port
    try:
        context.pctl.query(pg_name, "CREATE TABLE {0}()".format(table_name))
    except pg.Error as e:
        assert False, "Error creating table {0} on {1}: {2}".format(table_name, pg_name, e)


@step('I {action:w} wal replay on {pg_name:w}')
def toggle_wal_replay(context, action, pg_name):
    # pause or resume the wal replay process
    try:
       version = context.pctl.query(pg_name, "select pg_catalog.pg_read_file('PG_VERSION', 0, 2)").fetchone()
       wal = version and version[0] and int(version[0].split('.')[0]) < 10 and "xlog" or "wal"
       context.pctl.query(pg_name, "SELECT pg_{0}_replay_{1}()".format(wal, action))
    except pg.Error as e:
        assert False, "Error during {0} wal recovery on {1}: {2}".format(action, pg_name, e)


@step('I {action:w} table on {pg_name:w}')
def crdr_mytest(context, action, pg_name):
    try:
       if (action == "create"):
           context.pctl.query(pg_name, "create table if not exists mytest(id Numeric)")
       else:
           context.pctl.query(pg_name, "drop table if exists mytest")
    except pg.Error as e:
        assert False, "Error {0} table mytest on {1}: {2}".format(action, pg_name, e)


@step('I load data on {pg_name:w}')
def initiate_load(context, pg_name):
    # perform dummy load
    try:
       context.pctl.query(pg_name, "begin; insert into mytest select r::numeric from generate_series(1, 350000) r; commit;")
    except pg.Error as e:
        assert False, "Error loading test data on {0}: {1}".format(pg_name, e)


@then('Table {table_name:w} is present on {pg_name:w} after {max_replication_delay:d} seconds')
def table_is_present_on(context, table_name, pg_name, max_replication_delay):
    max_replication_delay *= context.timeout_multiplier
    for _ in range(int(max_replication_delay)):
        if context.pctl.query(pg_name, "SELECT 1 FROM {0}".format(table_name), fail_ok=True) is not None:
            break
        sleep(1)
    else:
        assert False,\
            "Table {0} is not present on {1} after {2} seconds".format(table_name, pg_name, max_replication_delay)


@then('{pg_name:w} role is the {pg_role:w} after {max_promotion_timeout:d} seconds')
def check_role(context, pg_name, pg_role, max_promotion_timeout):
    max_promotion_timeout *= context.timeout_multiplier
    assert context.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout)),\
        "{0} role didn't change to {1} after {2} seconds".format(pg_name, pg_role, max_promotion_timeout)


@step('replication works from {master:w} to {replica:w} after {time_limit:d} seconds')
@then('replication works from {master:w} to {replica:w} after {time_limit:d} seconds')
def replication_works(context, master, replica, time_limit):
    context.execute_steps(u"""
        When I add the table test_{0} to {1}
        Then table test_{0} is present on {2} after {3} seconds
    """.format(int(time()), master, replica, time_limit))
