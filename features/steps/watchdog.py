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
    return context.pctl.start(name, custom_config={'watchdog': True, 'bootstrap': {'dcs': {'ttl': 20}}})


@step('{name:w} watchdog has been pinged after {timeout:d} seconds')
def watchdog_was_pinged(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_pinged:
            return True
    return False


@then('{name:w} watchdog has been closed')
def watchdog_was_closed(context, name):
    assert context.pctl.get_watchdog(name).was_closed


@step('{name:w} watchdog has a {timeout:d} second timeout')
def watchdog_has_timeout(context, name, timeout):
    assert context.pctl.get_watchdog(name).timeout == timeout


@step('I reset {name:w} watchdog state')
def watchdog_reset_pinged(context, name):
    context.pctl.get_watchdog(name).reset()


@then('{name:w} watchdog is triggered after {timeout:d} seconds')
def watchdog_was_triggered(context, name, timeout):
    for _ in polling_loop(timeout):
        if context.pctl.get_watchdog(name).was_triggered:
            return True
    assert False


@step('{name:w} hangs for {timeout:d} seconds')
def patroni_hang(context, name, timeout):
    return context.pctl.patroni_hang(name, timeout)
