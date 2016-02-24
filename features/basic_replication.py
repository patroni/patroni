import psycopg2 as pg
from time import sleep

from lettuce import world, steps

PATRONI_CONFIG = '{}.yml'


@steps
class BasicReplicationSteps(object):

    def __init__(self, environ):
        self.env = environ

    def start_patroni(self, step, pg_name):
        '''I start (\w+)'''
        return world.pctl.start_patroni(pg_name)

    def stop_patroni(self, step, pg_name):
        '''I shut down (\w+)'''
        return world.pctl.stop_patroni(pg_name)

    def kill_patroni(self, step, pg_name):
        '''I kill (\w+)'''
        return world.pctl.stop_patroni(pg_name, kill=True)

    def do_sleep(self, step, sleep_seconds):
        '''I sleep (\w+)'''
        sleep(int(sleep_seconds))

    def add_table(self, step, table_name, pg_name):
        '''I add the table (\w+) to (\w+)'''
        # parse the configuration file and get the port
        try:
            world.pctl.query(pg_name, "CREATE TABLE {0}()".format(table_name))
        except pg.Error as e:
            assert False, "Error creating table {0} on {1}: {2}".format(table_name, pg_name, e)

    def table_is_present_on(self, step, table_name, pg_name, max_replication_delay):
        '''Table (\w+) is present on (\w+) after (\d+) seconds'''
        for i in range(int(max_replication_delay)):
            if world.pctl.query(pg_name, "SELECT 1 FROM {0}".format(table_name), fail_ok=True) is not None:
                break
            sleep(1)
        else:
            assert False,\
                "Table {0} is not present on {1} after {2} seconds".format(table_name, pg_name, max_replication_delay)

    def check_role(self, step, pg_name, pg_role, max_promotion_timeout):
        '''(\w+) role is the (\w+) after (\d+) seconds'''
        if not world.pctl.check_role_has_changed_to(pg_name, pg_role, timeout=int(max_promotion_timeout)):
            assert False, "pg_name role didn't change to {0} after {1} seconds".format(pg_role, max_promotion_timeout)


BasicReplicationSteps(world)
