#!/usr/bin/env python3
import logging
import os
import socket

from patroni.utils import split_host_port
from patroni.ctl import find_executable
from patroni.dcs import dcs_modules

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
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s')
    bin_dir = None
    data_dir = ""
    fatal = None
    if "name" not in config:
        logger.error("name is not defined")
        fatal = True
    elif not isinstance(config["name"], str):
        logger.error("name is not a string")
        fatal = True
    if "scope" not in config:
        logger.error("scope is not defined")
        fatal = True
    elif not isinstance(config["scope"], str):
        logger.error("scope is not a string")
        fatal = True
    if "postgresql" not in config:
        logger.error("postgresql section is missing")
        fatal = True
    else:
        if not isinstance(config["postgresql"], dict):
            logger.warning("postgresql is not a dictionary")
        else:
            if "authentication" not in config["postgresql"]:
                logger.warning("postgresql.authentication is not defined")
            elif not isinstance(config["postgresql"]["authentication"], dict):
                logger.warning("postgresql.authentication is not a dictionary")
            else:
                for user in ["replication", "superuser", "rewind"]:
                    if user not in config["postgresql"]["authentication"]:
                        logger.warning("postgresql.authentication.%s is not defined", user)
                    elif not isinstance(config["postgresql"]["authentication"][user], dict):
                        logger.warning("postgresql.authentication.%s is not a dictionary", user)
                    else:
                        if "username" not in config["postgresql"]["authentication"][user]:
                            logger.warning("postgresql.authentication.%s.username is not defined", user)
                        elif not isinstance(config["postgresql"]["authentication"][user]["username"], str):
                            logger.warning("postgresql.authentication.%s.username is not a string", user)
                        if "password" not in config["postgresql"]["authentication"][user]:
                            logger.info("postgresql.authentication.%s.password is not defined", user)
                        elif not isinstance(config["postgresql"]["authentication"][user]["password"], str):
                            logger.warning("postgresql.authentication.%s.password is not a string", user)

            if "bin_dir" in config["postgresql"]:
                if not isinstance(config["postgresql"]["bin_dir"], str):
                    logger.warning("postgresql.bin_dir is not a string")
                else:
                    bin_dir = config["postgresql"]["bin_dir"]
                    if not os.path.exists(bin_dir):
                        logger.warning("Directory '%s' does not exist. Configured in postgresql.bin_dir", bin_dir)
                    elif not os.path.isdir(bin_dir):
                        logger.warning("'%s' is not a directory. Configured in postgresql.bin_dir", bin_dir)
            else:
                logger.info("postgresql.bin_dir is not defined")

            if "data_dir" not in config["postgresql"]:
                logger.warning("postgresql.data_dir is not defined")
            elif not isinstance(config["postgresql"]["data_dir"], str):
                logger.warning("postgresql.data_dir is not a string")
            elif not config["postgresql"]["data_dir"]:
                logger.warning("postgresql.data_dir is an empty string")
            else:
                data_dir = config["postgresql"]["data_dir"]
                if os.path.exists(data_dir) and not os.path.isdir(data_dir):
                    logger.error("postgresql.data_dir (%s) is not a directory", data_dir)
                else:
                    if not data_directory_empty(data_dir):
                        if not os.path.exists(
                            os.path.join(data_dir, "PG_VERSION")
                        ):
                            logger.warning("%s doesn't look like a valid data directory make sure you provide "
                                           "a valid path in configuration. Configured in postgresql.data_dir",
                                           data_dir)
                        elif not os.path.isdir(
                            os.path.join(data_dir, "pg_wal")
                        ) and not os.path.isdir(
                            os.path.join(data_dir, "pg_xlog")
                        ):
                            logger.warning(
                                "data dir for the cluster is not empty,"
                                "but doesn't contain \"pg_wal\" nor \"pg_xlog\" directory"
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
                            logger.error("postgresql.listen contains a wrong value: '%s'",
                                         config["postgresql"]["listen"])
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            try:
                                if s.connect_ex((host, port)) == 0:
                                    logger.warning("Port %s is already in use.", port)
                            except socket.gaierror as e:
                                logger.error("postgresql.listen might have a wrong value: '%s' -> %s",
                                             config["postgresql"]["listen"], e)
                connect_address = config["postgresql"].get("connect_address",
                                                           config["postgresql"]["listen"].split(",")[0])
                if not isinstance(connect_address, str):
                    logger.warning("postgresql.connect_address is not a string")
                else:
                    connect_host, _ = split_host_port(connect_address, 5432)
                    if connect_host in ["localhost", "127.0.0.1", "0.0.0.0", "::1", "*"]:
                        logger.warning("postgresql.connect_address has wrong host part(%s)", connect_host)

            if "parameters" not in config["postgresql"]:
                logger.warning("postgresql.parameters is not defined")
            elif not isinstance(config["postgresql"]["parameters"], dict):
                logger.warning("postgresql.parameters is not a dictionary")
            else:
                if "unix_socket_directories" not in config["postgresql"]["parameters"]:
                    logger.warning("postgresql.parameters.unix_socket.directories is not defined")
                elif not isinstance(config["postgresql"]["parameters"]["unix_socket_directories"], str):
                    logger.warning("postgresql.parameters.unix_socket_directories is not a string")

    if "restapi" not in config:
        logger.error("restapi section is missing")
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
            logger.info("bootstrap.dcs is not defined")
        elif not isinstance(config["bootstrap"]["dcs"], dict):
            logger.warning("bootstrap.dcs is not a dictionary")

    available_dcs = [m.split(".")[-1] for m in dcs_modules()]
    if not any([dcs in config for dcs in available_dcs]):
        logger.error("Neither of distributed configuration store is configured, "
                     "please configure one of these: %s", ", ".join(available_dcs))
        fatal = 1
    elif "etcd" in config:
        if not isinstance(config["etcd"], dict):
            logger.warning("etcd is not a dictionary")
        elif not any([i in config["etcd"] for i in ["srv", "hosts", "host", "url", "proxy"]]):
            logger.warning("Neither srv, hosts, host, url nor proxy are defined in etcd section of config")
        elif "host" in config["etcd"]:
            if not isinstance(config["etcd"]["host"], str):
                logger.warning("etcd.host is not a string")

    if fatal:
        return "Configuration is not valid."
