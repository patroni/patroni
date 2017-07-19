import time

from behave import step, then


@step('I start {name:w} in a cluster {cluster_name:w} as a clone of {name2:w}')
def start_cluster_clone(context, name, cluster_name, name2):
    context.pctl.clone(name2, cluster_name, name)


@step('I start {name:w} in a cluster {cluster_name:w} from backup')
def start_cluster_from_backup(context, name, cluster_name):
    context.pctl.bootstrap_from_backup(name, cluster_name)


@then('{name:w} is a leader of {cluster_name:w} after {time_limit:d} seconds')
def is_a_leader(context, name, cluster_name, time_limit):
    time_limit *= context.timeout_multiplier
    max_time = time.time() + int(time_limit)
    while (context.dcs_ctl.query("leader", scope=cluster_name) != name):
        time.sleep(1)
        assert time.time() < max_time, "{0} is not a leader in dcs after {1} seconds".format(name, time_limit)


@step('I do a backup of {name:w}')
def do_backup(context, name):
    context.pctl.backup(name)
