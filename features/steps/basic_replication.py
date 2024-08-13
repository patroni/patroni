import json
import patroni.psycopg as pg

from behave import step, then
from time import sleep, time


@step('I start {name:w}')
def start_patroni(context, name):
    return context.pctl.start(name)


@step('I start duplicate {name:w} on port {port:d}')
def start_duplicate_patroni(context, name, port):
    config = {
        "name": name,
        "restapi": {
            "listen": "127.0.0.1:{0}".format(port)
        }
    }
    try:
        context.pctl.start('dup-' + name, custom_config=config)
        assert False, "Process was expected to fail"
    except AssertionError as e:
        assert 'is not running after being started' in str(e), \
            "No error was raised by duplicate start of {0} ".format(name)


@step('I shut down {name:w}')
def stop_patroni(context, name):
    return context.pctl.stop(name, timeout=60)


@step('I kill {name:w}')
def kill_patroni(context, name):
    return context.pctl.stop(name, kill=True)


@step('I shut down postmaster on {name:w}')
def stop_postgres(context, name):
    return context.pctl.stop(name, postgres=True)


@step('I kill postmaster on {name:w}')
def kill_postgres(context, name):
    return context.pctl.stop(name, kill=True, postgres=True)


def get_wal_name(context, pg_name):
    version = context.pctl.query(pg_name, "SHOW server_version_num").fetchone()[0]
    return 'xlog' if int(version) / 10000 < 10 else 'wal'


@step('I add the table {table_name:w} to {pg_name:w}')
def add_table(context, table_name, pg_name):
    # parse the configuration file and get the port
    try:
        context.pctl.query(pg_name, "CREATE TABLE public.{0}()".format(table_name))
        context.pctl.query(pg_name, "SELECT pg_switch_{0}()".format(get_wal_name(context, pg_name)))
    except pg.Error as e:
        assert False, "Error creating table {0} on {1}: {2}".format(table_name, pg_name, e)


@step('I {action:w} wal replay on {pg_name:w}')
def toggle_wal_replay(context, action, pg_name):
    # pause or resume the wal replay process
    try:
        context.pctl.query(pg_name, "SELECT pg_{0}_replay_{1}()".format(get_wal_name(context, pg_name), action))
    except pg.Error as e:
        assert False, "Error during {0} wal recovery on {1}: {2}".format(action, pg_name, e)


@step('I {action:w} table on {pg_name:w}')
def crdr_mytest(context, action, pg_name):
    try:
        if (action == "create"):
            context.pctl.query(pg_name, "create table if not exists public.mytest(id numeric)")
        else:
            context.pctl.query(pg_name, "drop table if exists public.mytest")
    except pg.Error as e:
        assert False, "Error {0} table mytest on {1}: {2}".format(action, pg_name, e)


@step('I load data on {pg_name:w}')
def initiate_load(context, pg_name):
    # perform dummy load
    try:
        context.pctl.query(pg_name, "insert into public.mytest select r::numeric from generate_series(1, 350000) r")
    except pg.Error as e:
        assert False, "Error loading test data on {0}: {1}".format(pg_name, e)


@then('Table {table_name:w} is present on {pg_name:w} after {max_replication_delay:d} seconds')
def table_is_present_on(context, table_name, pg_name, max_replication_delay):
    max_replication_delay *= context.timeout_multiplier
    for _ in range(int(max_replication_delay)):
        if context.pctl.query(pg_name, "SELECT 1 FROM public.{0}".format(table_name), fail_ok=True) is not None:
            break
        sleep(1)
    else:
        assert False, \
            "Table {0} is not present on {1} after {2} seconds".format(table_name, pg_name, max_replication_delay)


@then('{pg_name:w} role is the {pg_role:w} after {max_promotion_timeout:d} seconds')
def check_role(context, pg_name, pg_role, max_promotion_timeout):
    max_promotion_timeout *= context.timeout_multiplier
    assert context.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout)), \
        "{0} role didn't change to {1} after {2} seconds".format(pg_name, pg_role, max_promotion_timeout)


@step('replication works from {primary:w} to {replica:w} after {time_limit:d} seconds')
@then('replication works from {primary:w} to {replica:w} after {time_limit:d} seconds')
def replication_works(context, primary, replica, time_limit):
    context.execute_steps(u"""
        When I add the table test_{0} to {1}
        Then table test_{0} is present on {2} after {3} seconds
    """.format(str(time()).replace('.', '_').replace(',', '_'), primary, replica, time_limit))


@step('there is one of {message_list} {level:w} in the {node} patroni log after {timeout:d} seconds')
def check_patroni_log(context, message_list, level, node, timeout):
    timeout *= context.timeout_multiplier
    message_list = json.loads(message_list)

    for _ in range(int(timeout)):
        messsages_of_level = context.pctl.read_patroni_log(node, level)
        if any(any(message in line for line in messsages_of_level) for message in message_list):
            break
        sleep(1)
    else:
        assert False, f"There were none of {message_list} {level} in the {node} patroni log after {timeout} seconds"
