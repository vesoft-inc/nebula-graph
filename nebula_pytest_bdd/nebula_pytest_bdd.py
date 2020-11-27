import pytest
import itertools
import os
import sys
import json
from pathlib import Path
from time import localtime, strftime

CURR_PATH = os.path.dirname(os.path.abspath(__file__))
NEBULA_HOME = os.getenv('NEBULA_SOURCE_DIR', os.path.join(CURR_PATH, '..'))


TEST_DIR = os.path.join(NEBULA_HOME, 'nebula_pytest_bdd')
sys.path.insert(0, NEBULA_HOME)
sys.path.insert(0, TEST_DIR)
sys.path.insert(0,'./common/')



from configs import init_configs
#from global_data_loader import GlobalDataLoader
from nebula_service import NebulaService


TEST_LOGS_DIR = os.getenv('NEBULA_TEST_LOGS_DIR')
if TEST_LOGS_DIR is None or TEST_LOGS_DIR == "":
    TEST_LOGS_DIR = os.environ['HOME']

NEBULA_BUILD_DIR = os.getenv('NEBULA_BUILD_DIR')
if NEBULA_BUILD_DIR is None:
    NEBULA_BUILD_DIR = os.path.join(CURR_PATH, '../build/')
NEBULA_SOURCE_DIR = os.getenv('NEBULA_SOURCE_DIR')
if NEBULA_SOURCE_DIR is None:
    NEBULA_SOURCE_DIR = str(Path(TEST_DIR).parent)




if __name__ == '__main__':
    if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
        executor.run_tests(sys.argv[1:])
        sys.exit(0)
    nebula_svc = NebulaService(NEBULA_BUILD_DIR, NEBULA_SOURCE_DIR)
    stop_nebula = True
    error_code = 1
    configs = None
  
    try:
        print("start service")
        parser = init_configs()
        (configs, opts) = parser.parse_args()
        current_time = strftime("%Y-%m-%d-%H:%M:%S", localtime())
        args = []
        print("args")
#        args.extend(list(commandline_args))
        nebula_ip = ''
        nebula_port = 0
        if len(configs.address) == 0:
            nebula_svc.install()
            storage_port, graph_port = nebula_svc.start(configs.debug_log)
            args.extend(['--address', '127.0.0.1:' + str(graph_port)])
            args.extend(['--storage', '127.0.0.1:' + str(storage_port)])
            nebula_ip = '127.0.0.1'
            nebula_port = graph_port
        else:
            stop_nebula = False
            addr = configs.address.split(':')
            nebula_ip = addr[0]
            nebula_port = int(addr[1])
        jsonfile = TEST_DIR + '/nebula.json'

        with open(jsonfile,'r') as fr:
            data = json.load(fr)
            data['ip'] = nebula_ip
            data['port'] = nebula_port
        fr.close()
        with open(jsonfile,'w') as fw: 
            json.dump(data,fw)
        fw.close()
        path_deploy=os.getcwd()
        os.chdir(TEST_DIR)
        pytest.main()
        os.chdir(path_deploy)
    except Exception as x:
        print('\033[31m' + str(x) + '\033[0m')
        import traceback
        print(traceback.format_exc())

    finally:
        if stop_nebula and configs.stop_nebula.lower() == 'true':
            nebula_svc.stop(configs.rm_dir.lower() == 'true')


