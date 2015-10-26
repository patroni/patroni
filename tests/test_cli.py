#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest

from click.testing import CliRunner
from patroni.cli import cli, members, store_config, load_config, output_members
from test_ha import get_cluster_initialized_with_leader

CONFIG_FILE_PATH = './test-cli.yaml'


def test_output_members():
    cluster = get_cluster_initialized_with_leader()
    output_members(cluster, name='abc', format='pretty')
    output_members(cluster, name='abc', format='json')


def test_rw_config():
    runner = CliRunner()
    config = 'a:b'
    with runner.isolated_filesystem():
        os.mkdir(CONFIG_FILE_PATH)
        with pytest.raises(Exception):
            result = load_config(CONFIG_FILE_PATH, None)
            assert 'Could not load configuration file' in result.output

        with pytest.raises(Exception):
            store_config(config, CONFIG_FILE_PATH)
        os.rmdir(CONFIG_FILE_PATH)

    store_config(config, 'abc/CONFIG_FILE_PATH')
    load_config(CONFIG_FILE_PATH, None)


def test_cli():
    runner = CliRunner()

    runner.invoke(cli, ['list'])

    result = runner.invoke(cli, ['--help'])
    assert 'Usage:' in result.output


def test_members():
    runner = CliRunner()

    runner.invoke(members)


