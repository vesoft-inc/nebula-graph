# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
import os
import time

from pathlib import Path
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

from tests.common.configs import all_configs
from tests.common.nebula_service import NebulaService
from tests.common.csv_import import CSVImporter


DOCKER_GRAPHD_DIGESTS = os.getenv('NEBULA_GRAPHD_DIGESTS')
if DOCKER_GRAPHD_DIGESTS is None:
    DOCKER_GRAPHD_DIGESTS = '0'
DOCKER_METAD_DIGESTS = os.getenv('NEBULA_METAD_DIGESTS')
if DOCKER_METAD_DIGESTS is None:
    DOCKER_METAD_DIGESTS = '0'
DOCKER_STORAGED_DIGESTS = os.getenv('NEBULA_STORAGED_DIGESTS')
if DOCKER_STORAGED_DIGESTS is None:
    DOCKER_STORAGED_DIGESTS = '0'

tests_collected = set()
tests_executed = set()
data_dir = os.getenv('NEBULA_DATA_DIR')


# pytest hook to handle test collection when xdist is used (parallel tests)
# https://github.com/pytest-dev/pytest-xdist/pull/35/commits (No official documentation available)
def pytest_xdist_node_collection_finished(node, ids):
    tests_collected.update(set(ids))


# link to pytest_collection_modifyitems
# https://docs.pytest.org/en/5.3.2/writing_plugins.html#hook-function-validation-and-execution
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    for item in items:
        tests_collected.add(item.nodeid)


# link to pytest_runtest_logreport
# https://docs.pytest.org/en/5.3.2/reference.html#_pytest.hookspec.pytest_runtest_logreport
def pytest_runtest_logreport(report):
    if report.passed:
        tests_executed.add(report.nodeid)


def pytest_addoption(parser):
    for config in all_configs:
        parser.addoption(config,
                         dest=all_configs[config][0],
                         default=all_configs[config][1],
                         help=all_configs[config][2])

    parser.addoption("--build_dir",
                     dest="build_dir",
                     default="",
                     help="Nebula Graph CMake build directory")

    parser.addoption("--project_dir",
                     dest="project_dir",
                     default="",
                     help="Nebula Graph CMake project directory")


def pytest_configure(config):
    pytest.cmdline.address = config.getoption("address")
    pytest.cmdline.user = config.getoption("user")
    pytest.cmdline.password = config.getoption("password")
    pytest.cmdline.replica_factor = config.getoption("replica_factor")
    pytest.cmdline.partition_num = config.getoption("partition_num")
    if data_dir is None:
        pytest.cmdline.data_dir = config.getoption("data_dir")
    else:
        pytest.cmdline.data_dir = data_dir
    pytest.cmdline.stop_nebula = config.getoption("stop_nebula")
    pytest.cmdline.rm_dir = config.getoption("rm_dir")
    pytest.cmdline.debug_log = config.getoption("debug_log")
    pytest.cmdline.build_dir = config.getoption("build_dir")
    pytest.cmdline.project_dir = config.getoption("project_dir")
    config._metadata['graphd digest'] = DOCKER_GRAPHD_DIGESTS
    config._metadata['metad digest'] = DOCKER_METAD_DIGESTS
    config._metadata['storaged digest'] = DOCKER_STORAGED_DIGESTS


def init_conn_pool(host: str, port: int):
    config = Config()
    config.max_connection_pool_size = 20
    config.timeout = 60000
    # init connection pool
    pool = ConnectionPool()
    if not pool.init([(host, port)], config):
        raise Exception("Fail to init connection pool.")
    return pool


@pytest.fixture(scope="global")
def conn_pool():
    build_dir = pytest.cmdline.build_dir
    project_dir = pytest.cmdline.project_dir
    nb = NebulaService(build_dir, project_dir)
    nb.install()
    port = nb.start()
    try:
        time.sleep(5)
        client = init_conn_pool("127.0.0.1", port)
        yield client
        client.close()
    except Exception as e:
        print('fail to init conn pool: ', e)
    finally:
        nb.stop(cleanup=True)


@pytest.fixture(scope="session")
def session(conn_pool):
    user = pytest.cmdline.user
    password = pytest.cmdline.password
    sess = conn_pool.get_session(user, password)
    yield sess
    sess.release()


@pytest.fixture(scope="global")
def load_nba_data(conn_pool):
    ngqls = """
DROP SPACE IF EXISTS nba;
CREATE SPACE nba(partition_num=7, replica_factor=1, vid_type=FIXED_STRING(30));
USE nba;
CREATE TAG IF NOT EXISTS player(name string, age int);
CREATE TAG IF NOT EXISTS team(name string);
CREATE TAG IF NOT EXISTS bachelor(name string, speciality string);
CREATE EDGE IF NOT EXISTS like(likeness int);
CREATE EDGE IF NOT EXISTS serve(start_year int, end_year int);
CREATE EDGE IF NOT EXISTS teammate(start_year int, end_year int);
CREATE TAG INDEX IF NOT EXISTS player_name_index ON player(name(64));
CREATE TAG INDEX IF NOT EXISTS player_age_index ON player(age);
CREATE TAG INDEX IF NOT EXISTS team_name_index ON team(name(64));
    """
    user = pytest.cmdline.user
    password = pytest.cmdline.password
    sess = conn_pool.get_session(user, password)
    rs = sess.execute(ngqls)
    assert rs.is_succeeded()

    time.sleep(5)

    data_dir = pytest.cmdline.data_dir
    for path in Path(data_dir).rglob('*.csv'):
        for stmt in CSVImporter(path):
            rs = sess.execute(stmt)
            assert rs.is_succeeded()

    sess.release()
