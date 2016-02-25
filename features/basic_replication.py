import psycopg2 as pg
from time import sleep

from lettuce import world, steps


@steps
class BasicReplicationSteps(object):

    def __init__(self, environ):
        self.env = environ

    @staticmethod
    def start_patroni(step, name):
        '''I start (\w+)'''
        return world.pctl.start(name)

    @staticmethod
    def stop_patroni(step, name):
        '''I shut down (\w+)'''
        return world.pctl.stop(name)

    @staticmethod
    def kill_patroni(step, name):
        '''I kill (\w+)'''
        return world.pctl.stop(name, kill=True)

    @staticmethod
    def add_table(step, table_name, pg_name):
        '''I add the table (\w+) to (\w+)'''
        # parse the configuration file and get the port
        try:
            world.pctl.query(pg_name, "CREATE TABLE {0}()".format(table_name))
        except pg.Error as e:
            assert False, "Error creating table {0} on {1}: {2}".format(table_name, pg_name, e)

    @staticmethod
    def table_is_present_on(step, table_name, pg_name, max_replication_delay):
        '''Table (\w+) is present on (\w+) after (\d+) seconds'''
        for _ in range(int(max_replication_delay)):
            if world.pctl.query(pg_name, "SELECT 1 FROM {0}".format(table_name), fail_ok=True) is not None:
                break
            sleep(1)
        else:
            assert False,\
                "Table {0} is not present on {1} after {2} seconds".format(table_name, pg_name, max_replication_delay)

    @staticmethod
    def check_role(step, pg_name, pg_role, max_promotion_timeout):
        '''(\w+) role is the (\w+) after (\d+) seconds'''
        if not world.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout)):
            assert False, "{0} role didn't change to {1} after {2} seconds".format(pg_name, pg_role, max_promotion_timeout)


BasicReplicationSteps(world)
