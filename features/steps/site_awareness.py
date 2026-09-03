from behave import step


@step('I start {name:name} in site {site_name:w} with failover priority {failover_priority:d}, '
      'sync priority {sync_priority:d}')
def start_patroni_tags(context, name, site_name, failover_priority, sync_priority):
    config = {
        "site": site_name,
        "tags": {
            "clonefrom": True
        }
    }
    if failover_priority is not None:
        config["tags"]["failover_priority"] = failover_priority
    if sync_priority is not None:
        config["tags"]["sync_priority"] = sync_priority

    return context.pctl.start(name, custom_config=config)


@step('I start {name:name} in site {site_name:w}')
def start_patroni(context, name, site_name):
    start_patroni_tags(context, name, site_name, None, None)
