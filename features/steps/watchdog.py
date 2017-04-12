from behave import step, then
import time

def polling_loop(timeout, interval=1):
    """Returns an iterator that returns values until timeout has passed. Timeout is measured from start of iteration."""
    start_time = time.time()
    iteration = 0
    end_time = start_time + timeout
    while time.time() < end_time:
        yield iteration
        iteration += 1
        time.sleep(interval)



@step('I start {name:w} with watchdog')
def start_patroni_with_watchdog(context, name):
    return context.pctl.start(name, with_watchdog=True)


@step('{name:w} watchdog has been pinged after {timeout:d} seconds')
def watchdog_was_pinged(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_pinged:
            return True
    return False


@then('{name:w} watchdog has been closed')
def watchdog_was_closed(context, name):
    assert context.pctl.get_watchdog(name).was_closed


@step('I wait for next {name:w} watchdog ping')
def watchdog_reset_pinged(context, name):
    context.pctl.get_watchdog(name).reset()


@then('{name:w} watchdog is triggered after {timeout:d} seconds')
def watchdog_was_triggered(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_triggered:
            return True
    assert False


@then('{name:w} watchdog was not triggered')
def watchdog_was_not_triggered(context, name):
    assert not context.pctl.get_watchdog(name).was_triggered


@step('{name:w} checkpoint takes {timeout:d} seconds')
def checkpoint_hang(context, name, timeout):
    assert context.pctl.checkpoint_hang(name, timeout)


@step('{name:w} hangs for {timeout:d} seconds')
def postmaster_hang(context, name, timeout):
    return context.pctl.postmaster_hang(name, timeout)


@step('I terminate {name:w} user processes')
def terminate_backends(context, name):
    return context.pctl.terminate_backends(name)


@step('Sleep for {timeout:d} seconds')
def dcs_connection_lost(context, timeout):
    time.sleep(timeout)


@then('{name:w} database is running')
def database_is_running(context, name):
    assert context.pctl.database_is_running(name)
