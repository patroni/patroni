import time

from behave import step, then


def polling_loop(timeout, interval=1):
    """Returns an iterator that returns values until timeout has passed. Timeout is measured from start of iteration."""
    start_time = time.time()
    iteration = 0
    end_time = start_time + timeout
    while time.time() < end_time:
        yield iteration
        iteration += 1
        time.sleep(interval)


@step('I start {name:name} with watchdog')
def start_patroni_with_watchdog(context, name):
    return context.pctl.start(name, custom_config={'watchdog': True, 'bootstrap': {'dcs': {'ttl': 21}}})


@step('{name:name} watchdog has been pinged after {timeout:d} seconds')
def watchdog_was_pinged(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_pinged:
            return True
    return False


@then('{name:name} watchdog has been closed after {timeout:d} seconds')
def watchdog_was_closed(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_closed:
            return
    assert False


@step('{name:name} watchdog has a {timeout:d} second timeout after {timeout2:d} seconds')
def watchdog_has_timeout(context, name, timeout, timeout2):
    for _ in polling_loop(timeout2):
        if context.pctl.get_watchdog(name).timeout == timeout:
            return
    assert False


@step('I reset {name:name} watchdog state')
def watchdog_reset_pinged(context, name):
    context.pctl.get_watchdog(name).reset()


@then('{name:name} watchdog is triggered after {timeout:d} seconds')
def watchdog_was_triggered(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_triggered:
            return True
    assert False


@step('{name:name} hangs for {timeout:d} seconds')
def patroni_hang(context, name, timeout):
    return context.pctl.patroni_hang(name, timeout)


@step('primary race backoff is triggered on {replica:name} in {timeout:d} seconds because {primary:name} is active')
def check_primary_race_backoff(context, replica, timeout, primary):
    timeout *= context.timeout_multiplier

    for _ in polling_loop(timeout):
        messages_of_level = context.pctl.read_patroni_log(replica, 'INFO')
        if any('wal position moved since last heart beat loop' in line for line in messages_of_level):
            break
        context.pctl.query(primary, "SELECT pg_catalog.txid_current()")
    else:
        assert False, f"primary race backoff is not triggered on {replica} in {timeout} seconds"


@step('{replica:name} is promoted to primary in {timeout:d} seconds despite {primary:name} being active')
def check_primary_race_backoff_finish(context, replica, timeout, primary):
    timeout *= context.timeout_multiplier

    for _ in polling_loop(timeout):
        cur = context.pctl.query(replica, "SELECT pg_catalog.pg_is_in_recovery()")
        row = cur.fetchone()
        if row and row[0] is False:
            break
        context.pctl.query(primary, "SELECT pg_catalog.txid_current()")
    else:
        assert False, f"{replica} is not promoted in {timeout} seconds"
