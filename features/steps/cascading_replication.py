from behave import step, then


@step('I configure and start {name:w} with a tag {tag_name:w} {tag_value:w}')
def start_patroni_with_a_name_value_tag(context, name, tag_name, tag_value):
    return context.pctl.start(name, tags={tag_name: tag_value})


@then('There is a label with "{content:w}" in {name:w} data directory')
def check_label(context, content, name):
    label = context.pctl.read_label(name)
    assert label == content, "{0} is not equal to {1}".format(label, content)


@step('I create label with "{content:w}" in {name:w} data directory')
def write_label(context, content, name):
    context.pctl.write_label(name, content)
