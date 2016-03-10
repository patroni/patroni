from lettuce import world, steps


@steps
class CascadingReplicationSteps(object):

    def __init__(self, environ):
        self.env = environ

    @staticmethod
    def start_patroni_with_a_name_value_tag(step, name, tag_name, tag_value):
        '''I configure and start (\w+) with a tag (\w+) (\w+)'''
        return world.pctl.start(name, tags={tag_name: tag_value})

    @staticmethod
    def check_label(step, content, name):
        '''There is a label with "(\w+)" in (\w+) data directory'''
        label = world.pctl.read_label(name)
        assert label == content, "{0} is not equal to {1}".format(label, content)

    @staticmethod
    def write_label(step, content, name):
        '''I create label with "(\w+)" in (\w+) data directory'''
        world.pctl.write_label(name, content)

CascadingReplicationSteps(world)
