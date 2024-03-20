from behave import step


@step('I configure and start {name:w} as a nostream node')
def start_patroni(context, name):
    return context.pctl.start(name, custom_config={
        'tags': {'nostream': 'true'},
        'postgresql': {'recovery_conf': context.pctl.recovery_conf_config()}
    })
