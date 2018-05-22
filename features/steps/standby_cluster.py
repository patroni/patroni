from behave import step


@step('I start {name:w} in a cluster {cluster_name:w}')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name
    })


@step('I start {name:w} in a standby cluster {cluster_name:w} as a clone of {name2:w}')
def start_patroni_stanby_cluster(context, name, cluster_name, name2):
    port = context.pctl._processes[name2]._connkwargs.get('port')
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "bootstrap": {
            "dcs": {
                "standby_cluster" :{
                    "host": "localhost",
                    "port": port,
                    "primary_slot_name": "postgresql1",
                }
            }
        }
    })
