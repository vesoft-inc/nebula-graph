import time
import sys
import pytest_bdd
import pytest
import json
import os
from nebula2.common import ttypes
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

CURR_PATH = os.path.dirname(os.path.abspath(__file__))
NEBULA_HOME = os.getenv('NEBULA_SOURCE_DIR', os.path.join(CURR_PATH, '..'))
TEST_DIR = os.path.join(NEBULA_HOME, 'nebula_tck_test')

UTILS_DIR = os.path.join(TEST_DIR,'utils')
sys.path.insert(0,UTILS_DIR)


@pytest.fixture(scope='session')
def init_conn_pool():
    conf = Config()
    jsonfile = "nebula.json"
    data = json.load(open(jsonfile))
    conf.timeout = data['timeout']
    conf.idle_time = data['idle_time']
    conf.max_connection_pool_size = data['max_connection_pool_size']
    conf.max_retry_time = data['max_retry_time']
    addresses = list()
    addresses.append((data['ip'],data['port']))
    connection_pool = ConnectionPool()
    connection_pool.init(addresses,conf)
    client_list = list()
    client_list.append(connection_pool)
    client_list.append(data['user'])
    client_list.append(data['pwd'])
    return client_list

@pytest.fixture(scope='session')
def get_Delay_Time(init_conn_pool):
    connection_pool = init_conn_pool[0];
    client = connection_pool.get_session(init_conn_pool[1], init_conn_pool[2])
    assert client != None
    resp = client.execute_query(
        'get configs GRAPH:heartbeat_interval_secs')
    assert resp.is_succeeded()
    assert len(resp.data.rows) == 1, "invalid row size: {}".format(resp.data.rows)
    Graph_Delay = resp.data.rows[0].values[4].get_iVal() + 1

    resp = client.execute_query(
        'get configs STORAGE:heartbeat_interval_secs')
    assert resp.is_succeeded()
    assert len(resp.data.rows) == 1, "invalid row size: {}".format(resp.data.rows)
    Storage_Delay = resp.data.rows[0].values[4].get_iVal() + 1
    Delay = max(Graph_Delay, Storage_Delay) * 3
    return Delay


@pytest.fixture(scope='session')
def load_nba(init_conn_pool):
    connection_pool = init_conn_pool[0];
    client = connection_pool.get_session(init_conn_pool[1],init_conn_pool[2])
    assert client != None
    

    nba_file = os.getcwd() + '/data/nba.ngql'
    print("open: ", nba_file)
    with open(nba_file, 'r') as data_file:
        resp = client.execute(
                'DROP SPACE IF EXISTS nba; CREATE SPACE IF NOT EXISTS nba(partition_num=10, replica_factor=1, vid_type = fixed_string(30));USE nba;')
        assert resp.is_succeeded()

        lines = data_file.readlines()
        ddl = False
        ngql_statement = ""
        for line in lines:
            strip_line = line.strip()
            if len(strip_line) == 0:
                continue
            elif strip_line.startswith('--'):
                comment = strip_line[2:] 
                if comment == 'DDL':
                    ddl = True
                elif comment == 'END':
                    if ddl:
                        time.sleep(3)
                        ddl = False
            else:
                line = line.rstrip()
                ngql_statement += " " + line
                if line.endswith(';'):
                    resp = client.execute(ngql_statement)
                    assert resp.is_succeeded()
                    ngql_statement = ""    
    yield "load_nba setup"

@pytest.fixture(scope='session')
def load_students(init_conn_pool):
    connection_pool = init_conn_pool[0];
    client = connection_pool.get_session(init_conn_pool[1],init_conn_pool[2])
    resp = client.execute(
            'DROP SPACE IF EXISTS student_space;CREATE SPACE IF NOT EXISTS student_space(partition_num=10, replica_factor=1, vid_type = fixed_string(8)); USE student_space;')
    assert resp.is_succeeded()

    resp = client.execute('CREATE TAG IF NOT EXISTS person(name string, age int, gender string);')
    assert resp.is_succeeded()

    resp = client.execute('CREATE TAG IF NOT EXISTS teacher(grade int, subject string);') 
    assert resp.is_succeeded()

    resp = client.execute('CREATE TAG IF NOT EXISTS student(grade int, hobby string DEFAULT "");')
    assert resp.is_succeeded()

    resp = client.execute('CREATE EDGE IF NOT EXISTS is_schoolmate(start_year int, end_year int);')
    assert resp.is_succeeded()

    resp = client.execute('CREATE EDGE IF NOT EXISTS is_teacher(start_year int, end_year int);')
    assert resp.is_succeeded()

    resp = client.execute('CREATE EDGE IF NOT EXISTS is_friend(start_year int, intimacy double);')
    assert resp.is_succeeded()

    resp = client.execute('CREATE EDGE IF NOT EXISTS is_colleagues(start_year int, end_year int);')
    assert resp.is_succeeded()
        # TODO: update the time when config can use
    time.sleep(5)

    resp = client.execute('INSERT VERTEX person(name, age, gender), teacher(grade, subject) VALUES \
                                "2001":("Mary", 25, "female", 5, "Math"), \
                                "2002":("Ann", 23, "female", 3, "English"), \
                                "2003":("Julie", 33, "female", 6, "Math"), \
                                "2004":("Kim", 30,"male",  5, "English"), \
                                "2005":("Ellen", 27, "male", 4, "Art"), \
                                "2006":("ZhangKai", 27, "male", 3, "Chinese"), \
                                "2007":("Emma", 26, "female", 2, "Science"), \
                                "2008":("Ben", 24, "male", 4, "Music"), \
                                "2009":("Helen", 24, "male", 2, "Sports") ,\
                                "2010":("Lilan", 32, "male", 5, "Chinese");')
    assert resp.is_succeeded()

    resp = client.execute('INSERT VERTEX person(name, age, gender), student(grade) VALUES \
                                "1001":("Anne", 7, "female", 2), \
                                "1002":("Cynthia", 7, "female", 2), \
                                "1003":("Jane", 6, "male", 2), \
                                "1004":("Lisa", 8, "female", 3), \
                                "1005":("Peggy", 8, "male", 3), \
                                "1006":("Kevin", 9, "male", 3), \
                                "1007":("WangLe", 8, "male", 3), \
                                "1008":("WuXiao", 9, "male", 4), \
                                "1009":("Sandy", 9, "female", 4), \
                                "1010":("Harry", 9, "female", 4), \
                                "1011":("Ada", 8, "female", 4), \
                                "1012":("Lynn", 9, "female", 5), \
                                "1013":("Bonnie", 10, "female", 5), \
                                "1014":("Peter", 10, "male", 5), \
                                "1015":("Carl", 10, "female", 5), \
                                "1016":("Sonya", 11, "male", 6), \
                                "1017":("HeNa", 11, "female", 6), \
                                "1018":("Tom", 12, "male", 6), \
                                "1019":("XiaMei", 11, "female", 6), \
                                "1020":("Lily", 10, "female", 6);')
    assert resp.is_succeeded()

    resp = client.execute('INSERT EDGE is_schoolmate(start_year, end_year) VALUES \
                                "1001" -> "1002":(2018, 2019), \
                                "1001" -> "1003":(2017, 2019), \
                                "1002" -> "1003":(2017, 2018), \
                                "1002" -> "1001":(2018, 2019), \
                                "1004" -> "1005":(2016, 2019), \
                                "1004" -> "1006":(2017, 2019), \
                                "1004" -> "1007":(2016, 2018), \
                                "1005" -> "1004":(2017, 2018), \
                                "1005" -> "1007":(2017, 2018), \
                                "1006" -> "1004":(2017, 2018), \
                                "1006" -> "1007":(2018, 2019), \
                                "1008" -> "1009":(2015, 2019), \
                                "1008" -> "1010":(2017, 2019), \
                                "1008" -> "1011":(2018, 2019), \
                                "1010" -> "1008":(2017, 2018), \
                                "1011" -> "1008":(2018, 2019), \
                                "1012" -> "1013":(2015, 2019), \
                                "1012" -> "1014":(2017, 2019), \
                                "1012" -> "1015":(2018, 2019), \
                                "1013" -> "1012":(2017, 2018), \
                                "1014" -> "1015":(2018, 2019), \
                                "1016" -> "1017":(2015, 2019), \
                                "1016" -> "1018":(2014, 2019), \
                                "1018" -> "1019":(2018, 2019), \
                                "1017" -> "1020":(2013, 2018), \
                                "1017" -> "1016":(2018, 2019);')
    assert resp.is_succeeded()

    resp = client.execute('INSERT EDGE is_friend(start_year, intimacy) VALUES \
                                "1003" -> "1004":(2017, 80.0), \
                                "1013" -> "1007":(2018, 80.0), \
                                "1016" -> "1008":(2015, 80.0), \
                                "1016" -> "1018":(2014, 85.0), \
                                "1017" -> "1020":(2018, 78.0), \
                                "1018" -> "1016":(2013, 83.0), \
                                "1018" -> "1020":(2018, 88.0);')
    assert resp.is_succeeded()

    resp = client.execute('INSERT EDGE is_colleagues(start_year, end_year) VALUES \
                                "2001" -> "2002":(2015, 0), \
                                "2001" -> "2007":(2014, 0), \
                                "2001" -> "2003":(2018, 0), \
                                "2003" -> "2004":(2013, 2017), \
                                "2002" -> "2001":(2016, 2017), \
                                "2007" -> "2001":(2013, 2018), \
                                "2010" -> "2008":(2018, 0);')
    assert resp.is_succeeded()

    resp = client.execute('INSERT EDGE is_teacher(start_year, end_year) VALUES \
                                "2002" -> "1004":(2018, 2019), \
                                "2002" -> "1005":(2018, 2019), \
                                "2002" -> "1006":(2018, 2019), \
                                "2002" -> "1007":(2018, 2019), \
                                "2002" -> "1009":(2017, 2018), \
                                "2002" -> "1012":(2015, 2016), \
                                "2002" -> "1013":(2015, 2016), \
                                "2002" -> "1014":(2015, 2016), \
                                "2002" -> "1019":(2014, 2015), \
                                "2010" -> "1016":(2018,2019), \
                                "2006" -> "1008":(2017, 2018);')
    assert resp.is_succeeded() 
    yield "load_students steup"
