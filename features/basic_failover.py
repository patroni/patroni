from lettuce import world, steps

PATRONI_CONFIG = '{}.yml'


@steps
class BasicFailoverSteps(object):

    def __init__(self, environ):
        self.env = environ

    def basic_replication(self, step):
        '''Basic replication'''
        step.behave_as("""
            Given I start postgres0
            And I start postgres1
            When I add the table foo to postgres0
            Then table foo is present on postgres1 after 10 seconds
            """)

    def start_patroni(self, step, pg_name):
        '''I start (\w+)'''
        return world.pctl.start_patroni(pg_name)

    def stop_patroni(self, step, pg_name):
        '''I shut down (\w+)'''
        return world.pctl.stop_patroni(pg_name)

    def check_role(self, step, pg_name, pg_role, max_promotion_timeout):
        '''(\w+) role is the (\w+) after (\d+) seconds'''
        return world.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout))

BasicFailoverSteps(world)
