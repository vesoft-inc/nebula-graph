#!/usr/bin/env python3
# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import itertools
import os
import sys
from pathlib import Path
from time import localtime, strftime

CURR_PATH = os.path.dirname(os.path.abspath(__file__))
NEBULA_HOME = os.getenv('NEBULA_SOURCE_DIR', os.path.join(CURR_PATH, '..'))
TEST_DIR = os.path.join(NEBULA_HOME, 'tests')
sys.path.insert(0, NEBULA_HOME)
sys.path.insert(0, TEST_DIR)

import pytest
from tests.common.configs import init_configs
from tests.common.global_data_loader import GlobalDataLoader
from tests.common.nebula_service import NebulaService


TEST_LOGS_DIR = os.getenv('NEBULA_TEST_LOGS_DIR')
if TEST_LOGS_DIR is None or TEST_LOGS_DIR == "":
    TEST_LOGS_DIR = os.environ['HOME']

NEBULA_BUILD_DIR = os.getenv('NEBULA_BUILD_DIR')
if NEBULA_BUILD_DIR is None:
    NEBULA_BUILD_DIR = str(Path(TEST_DIR).parent)
NEBULA_SOURCE_DIR = os.getenv('NEBULA_SOURCE_DIR')
if NEBULA_SOURCE_DIR is None:
    NEBULA_SOURCE_DIR = str(Path(TEST_DIR).parent)

RESULT_DIR = os.path.join(TEST_LOGS_DIR, 'results')
LOGGING_ARGS = {'--html': 'TEST-nebula-{0}.html'}


<<<<<<< HEAD
class TestExecutor(object):
    def __init__(self, exit_on_error=True):
        self._exit_on_error = exit_on_error
        self.tests_failed = False
        self.total_executed = 0

    def run_tests(self, args):
        error_code = 0
        try:
            error_code = pytest.main(args)
        except Exception:
            sys.stderr.write(
                "Unexpected exception with pytest {0}".format(args))
            error_code = 1
        return error_code
=======
def init_parser():
    from optparse import OptionParser
    opt_parser = OptionParser()
    opt_parser.add_option('--build_dir',
                          dest='build_dir',
                          default=os.path.join(NEBULA_HOME, 'build'),
                          help='Build directory of nebula graph')
    opt_parser.add_option('--rm_dir',
                          dest='rm_dir',
                          default='true',
                          help='Whether to remove the test folder')
    opt_parser.add_option('--user',
                          dest='user',
                          default='root',
                          help='nebula graph user')
    opt_parser.add_option('--password',
                          dest='password',
                          default='nebula',
                          help='nebula graph password')
    opt_parser.add_option('--cmd',
                          dest='cmd',
                          default='',
                          help='start or stop command')
    return opt_parser


def start_nebula(nb, configs):
    nb.install()
    port = nb.start()

    # Load csv data
    pool = get_conn_pool("localhost", port)
    sess = pool.get_session(configs.user, configs.password)

    if not os.path.exists(TMP_DIR):
        os.mkdir(TMP_DIR)

    with open(SPACE_TMP_PATH, "w") as f:
        spaces = []
        for space in ("nba", "nba_int_vid", "student"):
            data_dir = os.path.join(CURR_PATH, "data", space)
            space_desc = load_csv_data(sess, data_dir, space)
            spaces.append(space_desc.__dict__)
        f.write(json.dumps(spaces))

    with open(NB_TMP_PATH, "w") as f:
        data = {
            "ip": "localhost",
            "port": port,
            "work_dir": nb.work_dir
        }
        f.write(json.dumps(data))
    print('Start nebula successfully')


def stop_nebula(nb):
    with open(NB_TMP_PATH, "r") as f:
        data = json.loads(f.readline())
        nb.set_work_dir(data["work_dir"])
    nb.stop()
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print('nebula services have been stopped.')


def opt_is(val, expect):
    return type(val) == str and val.lower() == expect
>>>>>>> d965a363... Fix unstable test cases related to index (#904)


if __name__ == "__main__":
    # If the user is just asking for --help, just print the help test and then exit.
    executor = TestExecutor()
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    nebula_svc = NebulaService(NEBULA_BUILD_DIR, NEBULA_SOURCE_DIR)
    stop_nebula = True
    error_code = 1
    configs = None
    try:
        # Init args
        parser = init_configs()
        (configs, opts) = parser.parse_args()

        # Create the test result directory if it doesn't already exist.
        if not os.path.exists(RESULT_DIR):
            os.makedirs(RESULT_DIR)

        commandline_args = itertools.chain(
            *[arg.split('=') for arg in sys.argv[1:]])
        current_time = strftime("%Y-%m-%d-%H:%M:%S", localtime())
        args = []
        for arg, log in LOGGING_ARGS.items():
            args.extend(
                [arg, os.path.join(RESULT_DIR, log.format(current_time))])

        args.extend(list(commandline_args))

        nebula_ip = ''
        nebula_port = 0
        if len(configs.address) == 0:
            nebula_svc.install()
            graph_port = nebula_svc.start(configs.debug_log)
            args.extend(['--address', '127.0.0.1:' + str(graph_port)])
            nebula_ip = '127.0.0.1'
            nebula_port = graph_port
        else:
            stop_nebula = False
            addr = configs.address.split(':')
            nebula_ip = addr[0]
            nebula_port = int(addr[1])

        if len(configs.data_dir) == 0:
            args.extend(['--data_dir', TEST_DIR])
        print("Running TestExecutor with args: {} ".format(args))

        # load nba data
        with GlobalDataLoader(TEST_DIR, nebula_ip, nebula_port, configs.user, configs.password) as data_loader:
            data_loader.load_all_test_data()

            # Switch to your $src_dir/tests
            os.chdir(TEST_DIR)
            error_code = executor.run_tests(args)
    except Exception as x:
        print('\033[31m' + str(x) + '\033[0m')
        import traceback
        print(traceback.format_exc())

    finally:
        if stop_nebula and configs.stop_nebula.lower() == 'true':
            nebula_svc.stop(configs.rm_dir.lower() == 'true')

    sys.exit(error_code)
