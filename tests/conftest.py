# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest
import os
import time
import logging
import json

from filelock import FileLock
from pathlib import Path
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

from tests.common.configs import all_configs
from tests.common.nebula_service import NebulaService
from tests.common.csv_import import CSVImporter

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


def get_conn_pool(host: str, port: int):
    config = Config()
    config.max_connection_pool_size = 20
    config.timeout = 60000
    # init connection pool
    pool = ConnectionPool()
    if not pool.init([(host, port)], config):
        raise Exception("Fail to init connection pool.")
    return pool


@pytest.fixture(scope="session")
def conn_pool(pytestconfig, worker_id, tmp_path_factory):
    addr = pytestconfig.getoption("address")
    if addr:
        addrsplit = addr.split(":")
        assert len(addrsplit) == 2
        pool = get_conn_pool(addrsplit[0], addrsplit[1])
        yield pool
        pool.close()
        return

    build_dir = pytestconfig.getoption("build_dir")
    project_dir = pytestconfig.getoption("project_dir")

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "nebula-test"
    nb = None
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            data = json.loads(fn.read_text())
            port = data["port"]
            logging.info(f"session-{worker_id} read the port: {port}")
            pool = get_conn_pool("localhost", port)
            data["num_workers"] += 1
            fn.write_text(json.dumps(data))
        else:
            nb = NebulaService(build_dir, project_dir)
            nb.install()
            port = nb.start()
            pool = get_conn_pool("localhost", port)
            data = dict(port=port, num_workers=1, finished=0)
            fn.write_text(json.dumps(data))
            logging.info(f"session-{worker_id} write the port: {port}")

    yield pool
    pool.close()

    if nb is None:
        with FileLock(str(fn) + ".lock"):
            data = json.loads(fn.read_text())
            data["finished"] += 1
            fn.write_text(json.dumps(data))
    else:
        while True:
            data = json.loads(fn.read_text())
            if data["finished"] + 1 == data["num_workers"]:
                nb.stop()
                break
            time.sleep(1)


@pytest.fixture(scope="session")
def session(conn_pool, pytestconfig):
    user = pytestconfig.getoption("user")
    password = pytestconfig.getoption("password")
    sess = conn_pool.get_session(user, password)
    yield sess
    sess.release()


def load_csv_data(conn_pool, folder: str):
    curr_path = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(curr_path, 'data', folder)
    schema_path = os.path.join(data_dir, 'schema.ngql')

    user = pytest.cmdline.user
    password = pytest.cmdline.password
    sess = conn_pool.get_session(user, password)

    with open(schema_path, 'r') as f:
        stmts = []
        for line in f.readlines():
            ln = line.strip()
            if ln.startswith('--'):
                continue
            stmts.append(ln)
        rs = sess.execute(' '.join(stmts))
        assert rs.is_succeeded()

    time.sleep(3)

    for path in Path(data_dir).rglob('*.csv'):
        for stmt in CSVImporter(path):
            rs = sess.execute(stmt)
            assert rs.is_succeeded()

    sess.release()


# TODO(yee): optimize data load fixtures
@pytest.fixture(scope="session")
def load_nba_data(conn_pool, tmp_path_factory, worker_id):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "csv-data-nba"
    with FileLock(str(fn) + ".lock"):
        if not fn.is_file():
            load_csv_data(conn_pool, "nba")
            fn.write_text("nba")
            logging.info(f"session-{worker_id} load nba csv data")
        else:
            logging.info(f"session-{worker_id} need not to load nba csv data")


@pytest.fixture(scope="session")
def load_student_data(conn_pool, tmp_path_factory, worker_id):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    fn = root_tmp_dir / "csv-data-student"
    with FileLock(str(fn) + ".lock"):
        if not fn.is_file():
            load_csv_data(conn_pool, "student")
            fn.write_text("nba")
            logging.info(f"session-{worker_id} load nba csv data")
        else:
            logging.info(f"session-{worker_id} need not to load nba csv data")
