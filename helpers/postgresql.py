import os
import psycopg2
import re
import time
import logging

from urlparse import urlparse


logger = logging.getLogger(__name__)


class Postgresql:

    def __init__(self, config):
        self.name = config["name"]
        self.host, self.port = config["listen"].split(":")
        self.data_dir = config["data_dir"]
        self.replication = config["replication"]

        self.config = config

        self.cursor_holder = None
        self.connection_string = "postgres://%s:%s@%s:%s/postgres" % (
            self.replication["username"], self.replication["password"], self.host, self.port)

        self.conn = None

    def cursor(self):
        if not self.cursor_holder:
            self.conn = psycopg2.connect("postgres://%s:%s/postgres" % (self.host, self.port))
            self.conn.autocommit = True
            self.cursor_holder = self.conn.cursor()

        return self.cursor_holder

    def disconnect(self):
        try:
            self.conn.close()
        except Exception as e:
            logger.error("Error disconnecting: %s" % e)

    def query(self, sql):
        max_attempts = 0
        while True:
            try:
                self.cursor().execute(sql)
                break
            except psycopg2.OperationalError as e:
                if self.conn:
                    self.disconnect()
                self.cursor_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return self.cursor()

    def data_directory_empty(self):
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def initialize(self):
        if os.system("initdb -D %s" % self.data_dir) == 0:
            self.write_pg_hba()

            return True

        return False

    def sync_from_leader(self, leader):
        pgpass = "pgpass"
        leader = urlparse(leader["address"])

        f = open(pgpass, "w")
        f.write("%(hostname)s:%(port)s:*:%(username)s:%(password)s\n" %
                {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password})
        f.close()

        os.chmod(pgpass, 0600)

        return os.system("PGPASSFILE={pgpass} pg_basebackup -R -D {data_dir} --host={hostname} --port={port} -U {username}".format(
            pgpass=pgpass, data_dir=self.data_dir, hostname=leader.hostname, port=leader.port, username=leader.username)) == 0

    def is_leader(self):
        return not self.query("SELECT pg_is_in_recovery();").fetchone()[0]

    def is_running(self):
        return os.system("pg_ctl status -D %s > /dev/null" % self.data_dir) == 0

    def start(self):
        if self.is_running():
            logger.error("Cannot start PostgreSQL because one is already running.")
            return False

        pid_path = "%s/postmaster.pid" % self.data_dir
        if os.path.exists(pid_path):
            os.remove(pid_path)
            logger.info("Removed %s" % pid_path)

        command_code = os.system("postgres -D %s %s &" % (self.data_dir, self.server_options()))
        time.sleep(5)
        return command_code != 0

    def stop(self):
        return os.system("pg_ctl stop -w -D %s -m fast -w" % self.data_dir) != 0

    def reload(self):
        return os.system("pg_ctl reload -w -D %s" % self.data_dir) == 0

    def restart(self):
        return os.system("pg_ctl restart -w -D %s -m fast" % self.data_dir) == 0

    def server_options(self):
        options = "-c listen_addresses=%s -c port=%s" % (self.host, self.port)
        for setting, value in self.config["parameters"].iteritems():
            options += " -c \"%s=%s\"" % (setting, value)
        return options

    def is_healthy(self):
        if not self.is_running():
            logger.warning("Postgresql is not running.")
            return False

        return True

    def is_healthiest_node(self, members):
        for member in members:
            if member["hostname"] == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member["address"])
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute(
                    "SELECT '%s'::pg_lsn - pg_last_xlog_replay_location() AS bytes;" % self.xlog_position())
                xlog_diff = member_cursor.fetchone()[0]
                logger.info([self.name, member["hostname"], xlog_diff])
                if xlog_diff < 0:
                    member_cursor.close()
                    return False
                member_cursor.close()
            except psycopg2.OperationalError:
                continue
        return True

    def replication_slot_name(self):
        member = os.environ.get("MEMBER")
        (member, _) = re.subn(r'[^a-z0-9]+', r'_', member)
        return member

    def write_pg_hba(self):
        f = open("%s/pg_hba.conf" % self.data_dir, "a")
        f.write("host replication %(username)s %(network)s md5" %
                {"username": self.replication["username"], "network": self.replication["network"]})
        f.close()

    def write_recovery_conf(self, leader_hash):
        leader = urlparse(leader_hash["address"])

        f = open("%s/recovery.conf" % self.data_dir, "w")
        f.write("""
standby_mode = 'on'
primary_slot_name = '%(recovery_slot)s'
primary_conninfo = 'user=%(user)s password=%(password)s host=%(hostname)s port=%(port)s sslmode=prefer sslcompression=1'
recovery_target_timeline = 'latest'
""" % {"recovery_slot": self.name, "user": leader.username, "password": leader.password, "hostname": leader.hostname, "port": leader.port})
        if "recovery_conf" in self.config:
            for name, value in self.config["recovery_conf"].iteritems():
                f.write("%s = '%s'\n" % (name, value))
        f.close()

    def follow_the_leader(self, leader_hash):
        leader = urlparse(leader_hash["address"])
        pattern = 'host={hostname} port={port}'.format(hostname=leader.hostname, port=leader.port)
        with open(os.path.join(self.data_dir, 'recovery.conf', 'r')) as f:
            for line in f:
                if pattern in line:
                    return
        self.write_recovery_conf(leader_hash)
        self.restart()

    def promote(self):
        return os.system("pg_ctl promote -w -D %s" % self.data_dir) == 0

    def demote(self, leader):
        self.write_recovery_conf(leader)
        self.restart()

    def create_replication_user(self):
        self.query("CREATE USER \"%s\" WITH REPLICATION ENCRYPTED PASSWORD '%s';" %
                   (self.replication["username"], self.replication["password"]))

    def xlog_position(self):
        return self.query("SELECT pg_last_xlog_replay_location();").fetchone()[0]
