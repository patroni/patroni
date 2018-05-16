from behave import step


@step('I start {name:w} in a cluster {cluster_name:w}')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name
    })


@step('I start {name:w} in a standby cluster {cluster_name:w} as a clone of {name2:w}')
def start_patroni_stanby_cluster(context, name, cluster_name, name2):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "bootstrap": {
            "dcs": {
                "standby_cluster" :{
                    "conn_url": "postgresql://127.0.0.1:5441/postgres",
                    "replication_slot": "postgresql1"
                }
            }
        }
    })
