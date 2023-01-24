from behave import step


@step('DCS is down')
def start_dcs_outage(context):
    context.dcs_ctl.start_outage()


@step('DCS is up')
def stop_dcs_outage(context):
    context.dcs_ctl.stop_outage()


@step('I start {name:w} in a cluster {cluster_name:w} from backup with no_leader')
def start_cluster_from_backup_no_leader(context, name, cluster_name):
    context.pctl.bootstrap_from_backup_no_leader(name, cluster_name)
