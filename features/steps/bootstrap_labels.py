from behave import step, then


@step('I start {name:name} in a cluster {cluster_name:w} as a long-running clone of {name2:name}')
def start_cluster_clone(context, name, cluster_name, name2):
    context.pctl.clone(name2, cluster_name, name, True)


@step('I start {name:name} in cluster {cluster_name:w} using long-running backup_restore')
def start_patroni(context, name, cluster_name):
    return context.pctl.start(name, custom_config={
        "scope": cluster_name,
        "postgresql": {
            'create_replica_methods': ['backup_restore'],
            "backup_restore": context.pctl.backup_restore_config(long_running=True),
            'authentication': {
                'superuser': {'password': 'patroni1'},
                'replication': {'password': 'rep-pass1'}
            }
        }
    }, max_wait_limit=-1)


@then('{name:name} is labeled with "{label:w}"')
def pod_labeled(context, name, label):
    assert label in context.dcs_ctl.pod_labels(name), f'pod {name} is not labeled with {label}'


@then('{name:name} is not labeled with "{label:w}"')
def pod_not_labeled(context, name, label):
    assert label not in context.dcs_ctl.pod_labels(name), f'pod {name} is still labeled with {label}'
