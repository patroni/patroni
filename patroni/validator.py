#!/usr/bin/env python3
import logging
import os
import socket

from patroni.utils import split_host_port
from patroni.ctl import find_executable

logger = logging.getLogger(__name__)


def data_directory_empty(data_dir):
    if os.path.isfile(os.path.join(data_dir, "global", "pg_control")):
        return False
    if not os.path.exists(data_dir):
        return True
    return all(
        os.name != "nt" and (n.startswith(".") or n == "lost+found")
        for n in os.listdir(data_dir)
    )

def config_validator(config):
    bin_dir=None
    data_dir=""
    fatal=None
    if "name" not in config:
        logger.error("name is not defined")
        fatal=True
    elif not isinstance(config["name"], str):
        logger.error("name is not a string")
        fatal=True
    if "scope" not in config:
        logger.error("scope is not defined")
        fatal=True
    elif not isinstance(config["scope"], str):
        logger.error("scope is not a string")
        fatal=True
    if "postgresql" not in config:
        logger.error("postgresql section is missing")
        fatal=True
    else:
        if not isinstance(config["postgresql"], dict):
            logger.warning("postgresql is not a dictionary")
        else:
            if "authentication" not in config["postgresql"]:
                logger.warning("postgresql.authentication is not defined")
            elif not isinstance(config["postgresql"]["authentication"], dict):
                logger.warning("postgresql.authentication is not a dictionary")
            else:
                if "replication" not in config["postgresql"]["authentication"]:
                    logger.warning("postgresql.authentication.replication is not defined")
                elif not isinstance(config["postgresql"]["authentication"]["replication"], dict):
                    logger.warning("postgresql.authentication.replication is not a dictionary")
                if "superuser" not in config["postgresql"]["authentication"]:
                    logger.warning("postgresql.authentication.superuser is not defined")
                elif not isinstance(config["postgresql"]["authentication"]["superuser"], dict):
                    logger.warning("postgresql.authentication.superuser is not a dictionary")

            if "bin_dir" in config["postgresql"]:
                if not isinstance(config["postgresql"]["bin_dir"], str):
                    logger.warning("postgresql.bin_dir is not a string")
                else:
                    bin_dir=config["postgresql"]["bin_dir"]
                    if not os.path.exists(bin_dir):
                        logger.warning("Directory '%s' does not exist. Configured in postgresql.bin_dir", bin_dir)
                    elif not os.path.isdir(bin_dir):
                        logger.warning("'%s' is not a directory. Configured in postgresql.bin_dir", bin_dir)
            else:
                logger.info("postgresql.bin_dir is not defined")
            if "data_dir" not in config["postgresql"]:
                logger.info("postgresql.data_dir is not defined")
            elif not isinstance(config["postgresql"]["data_dir"], str):
                    logger.warning("postgresql.data_dir is not a string")
            else:
                data_dir = config["postgresql"]["data_dir"]
                if not data_directory_empty(data_dir):
                    if not os.path.exists(
                        os.path.join(data_dir, "PG_VERSION")
                    ):
                        logger.warning(
                            "%s doesn't look like a valid data directory"
                            " make sure you provide a valid path in configuration. Configured in postgresql.data_dir",
                            data_dir
                        )
                    elif not os.path.isdir(
                        os.path.join(data_dir, "pg_wal")
                    ) and not os.path.isdir(
                        os.path.join(data_dir, "pg_xlog")
                    ):
                        logger.warning(
                            'data dir for the cluster is not empty, but doesn\'t contain "pg_wal" nor "pg_xlog" directory'
                        )
            if "listen" not in config["postgresql"]:
                logger.warning("postgresql.listen is not defined")
            elif not isinstance(config["postgresql"]["listen"], str):
                logger.warning("postgresql.listen is not a string")
            else:
                try:
                    hosts, port = split_host_port(config["postgresql"]["listen"], 5432)
                except ValueError:
                    logger.error("postgresql.listen contains a wrong value: '%s'", config["postgresql"]["listen"])
                else:
                    for host in hosts.split(","):
                        host = host.strip()
                        if host == '*':
                            host = '0.0.0.0'
                        elif not host:
                            logger.error("postgresql.listen contains a wrong value: '%s'", config["postgresql"]["listen"])
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            try:
                                if s.connect_ex((host, port)) == 0:
                                    logger.warning("Port %s is already in use.", port)
                            except socket.gaierror as e:
                                logger.error("postgresql.listen might have a wrong value: '%s' -> %s", config["postgresql"]["listen"], e)
                connect_address = config["postgresql"].get("connect_address", config["postgresql"]["listen"].split(",")[0])
                if not isinstance(connect_address, str):
                    logger.warning("postgresql.connect_address is not a string")
                else:
                    connect_host, _ =  split_host_port(connect_address, 5432)
                    if connect_host in ["localhost", "127.0.0.1", "0.0.0.0", "::1", "*"]:
                        logger.warning("postgresql.connect_address has wrong host part(%s)", connect_host)

    if "restapi" not in config:
        logger.warning("restapi section is missing")
    elif not isinstance(config["restapi"], dict):
        logger.warning("restapi is not a dictionary")
    elif "listen" not in config["restapi"]:
        logger.warning("restapi.listen is not defined")
    elif not isinstance(config["restapi"]["listen"], str):
        logger.warning("restapi.listen is not a string")
    else:
        try:
            host, port = split_host_port(config["restapi"]["listen"], None)
        except (ValueError, TypeError):
            logger.error("restapi.listen contains a wrong value: '%s'", config["restapi"]["listen"])
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    if s.connect_ex((host, port)) == 0:
                        logger.warning("Port %s is already in use.", port)
                except socket.gaierror as e:
                    logger.error("restapi.listen might have a wrong value: '%s' -> %s", config["restapi"]["listen"], e)
    for program in ["pg_ctl", "initdb", "pg_controldata", "pg_basebackup", "postgres"]:
        if not find_executable(program, bin_dir):
            logger.warning("Program '%s' not found.", program)
    if "bootstrap" not in config:
        logger.warning("bootstrap section is missing")
    elif not isinstance(config["bootstrap"], dict):
        logger.warning("bootstrap is not a dictionary")
    else:
        if "dcs" not in config["bootstrap"]:
            logger.warning("bootstrap.dcs is not defined")
        elif not isinstance(config["bootstrap"]["dcs"], dict):
            logger.warning("bootstrap.dcs is not a dictionary")
        if "initdb" in config["bootstrap"]:
            if not isinstance(config["bootstrap"]["initdb"], list):
                logger.warning("bootstrap.initdb is not a list")
            initdb = {
                'xlogdir': os.path.join(data_dir, "pg_xlog"),
                'waldir': os.path.join(data_dir, "pg_wal"),
                'log_directory': os.path.join(data_dir, "pg_log")}
            for item in config["bootstrap"]["initdb"]:
                if isinstance(item, dict):
                    initdb.update(item)

            if initdb["waldir"] == initdb["log_directory"]:
                logger.warning(
                    "waldir(%s) and log_directory (%s) are pointing to the same path",
                    initdb["waldir"],
                    initdb["log_directory"]
                )
            if initdb["xlogdir"] == initdb["log_directory"]:
                logger.warning(
                    "xlogdir(%s) and log_directory (%s) are pointing to the same path",
                    initdb["xlogdir"],
                    initdb["log_directory"]
                )
    if not any([dcs in config for dcs in ["consul", "etcd", "exhibitor", "kubernetes", "zookeeper"]]):
        logger.error("Neither of distributed configuration store is configured, please configure one of these: consul, etcd, exhibitor, kubernetes, zookeeper")
        fatal=1

    if fatal:
        return "Configuration is not valid."
