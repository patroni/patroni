import json
import time

from behave import step, then
from dateutil import tz
from datetime import datetime
from functools import partial
from threading import Thread, Event

tzutc = tz.tzutc()


@step('{name:w} is a leader in a group {group:d} after {time_limit:d} seconds')
@then('{name:w} is a leader in a group {group:d} after {time_limit:d} seconds')
def is_a_group_leader(context, name, group, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while (context.dcs_ctl.query("leader", group=group) != name):
        time.sleep(1)
        assert time.time() < max_time, "{0} is not a leader in dcs after {1} seconds".format(name, time_limit)


@step('"{name}" key in a group {group:d} in DCS has {key:w}={value} after {time_limit:d} seconds')
def check_group_member(context, name, group, key, value, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    dcs_value = None
    response = None
    while time.time() < max_time:
        try:
            response = json.loads(context.dcs_ctl.query(name, group=group))
            dcs_value = response.get(key)
            if dcs_value == value:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, ("{0} in a group {1} does not have {2}={3} (found {4}) in dcs"
                   " after {5} seconds").format(name, group, key, value, response, time_limit)


@step('I start {name:w} in citus group {group:d}')
def start_citus(context, name, group):
    return context.pctl.start(name, custom_config={"citus": {"database": "postgres", "group": int(group)}})


@step('{name1:w} is registered in the {name2:w} as the {role:w} in group {group:d} after {time_limit:d} seconds')
def check_registration(context, name1, name2, role, group, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)

    worker_port = int(context.pctl.query(name1, "SHOW port").fetchone()[0])

    while time.time() < max_time:
        try:
            cur = context.pctl.query(name2, "SELECT nodeport, noderole"
                                            " FROM pg_catalog.pg_dist_node WHERE groupid = {0}".format(group))
            mapping = {r[0]: r[1] for r in cur}
            if mapping.get(worker_port) == role:
                return
        except Exception:
            pass
        time.sleep(1)
    assert False, "Node {0} is not registered in pg_dist_node on the node {1}".format(name1, name2)


@step('I create a distributed table on {name:w}')
def create_distributed_table(context, name):
    context.pctl.query(name, 'CREATE TABLE public.d(id int not null)')
    context.pctl.query(name, "SELECT create_distributed_table('public.d', 'id')")


@step('I cleanup a distributed table on {name:w}')
def cleanup_distributed_table(context, name):
    context.pctl.query(name, 'TRUNCATE public.d')


def insert_thread(query_func, context):
    while True:
        if context.thread_stop_event.is_set():
            break

        context.insert_counter += 1
        query_func('INSERT INTO public.d VALUES({0})'.format(context.insert_counter))

        context.thread_stop_event.wait(0.01)


@step('I start a thread inserting data on {name:w}')
def start_insert_thread(context, name):
    context.thread_stop_event = Event()
    context.insert_counter = 0
    query_func = partial(context.pctl.query, name)
    thread_func = partial(insert_thread, query_func, context)
    context.thread = Thread(target=thread_func)
    context.thread.daemon = True
    context.thread.start()


@then('a thread is still alive')
def thread_is_alive(context):
    assert context.thread.is_alive(), "Thread is not alive"


@step("I stop a thread")
def stop_insert_thread(context):
    context.thread_stop_event.set()
    context.thread.join(1 * context.timeout_multiplier)
    assert not context.thread.is_alive(), "Thread is still alive"


@step("a distributed table on {name:w} has expected rows")
def count_rows(context, name):
    rows = context.pctl.query(name, "SELECT COUNT(*) FROM public.d").fetchone()[0]
    assert rows == context.insert_counter, "Distributed table doesn't have expected amount of rows"


@step("there is a transaction in progress on {name:w} changing pg_dist_node after {time_limit:d} seconds")
def check_transaction(context, name, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while time.time() < max_time:
        cur = context.pctl.query(name, "SELECT xact_start FROM pg_stat_activity WHERE pid <> pg_backend_pid()"
                                       " AND state = 'idle in transaction' AND query ~ 'citus_update_node'")
        if cur.rowcount == 1:
            context.xact_start = cur.fetchone()[0]
            return
        time.sleep(1)
    assert False, f"There is no idle in transaction on {name} updating pg_dist_node after {time_limit} seconds"


@step("a transaction finishes in {timeout:d} seconds")
def check_transaction_timeout(context, timeout):
    assert (datetime.now(tzutc) - context.xact_start).seconds >= timeout, \
        "a transaction finished earlier than in {0} seconds".format(timeout)
