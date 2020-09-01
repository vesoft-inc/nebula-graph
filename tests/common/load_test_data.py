# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


import time
from nebula2.Client import AuthException, ExecutionException, GraphClient
from nebula2.Common import *
from nebula2.ConnectionPool import ConnectionPool
from nebula2.graph import ttypes
from tests.common.configs import get_delay_time
from nebula2.common import ttypes as CommonTtypes
import re

VERTEXS = dict()
EDGES = dict()

class LoadGlobalData(object):
    def __init__(self, data_dir, ip, port, user, password):
        self.data_dir = data_dir
        self.ip = ip
        self.port = port
        self.client_pool = ConnectionPool(ip = self.ip, port = self.port, network_timeout = 0)
        self.client = GraphClient(self.client_pool)
        self.user = user
        self.password = password
        self.client.authenticate(self.user, self.password)

    def load_all_test_data(self):
        if self.client is None:
            assert False, 'Connect to {}:{}'.format(self.ip, self.port)
        self.load_nba()
        self.load_student()

    # The whole test will load once, for the only read tests
    def load_nba(self):
        nba_file = self.data_dir + '/data/nba.ngql'
        print("will open ", nba_file)
        with open(nba_file, 'r') as data_file:
            resp = self.client.execute(
                'CREATE SPACE IF NOT EXISTS nba(partition_num=10, replica_factor=1, vid_size=30);USE nba;')
            assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

            lines = data_file.readlines()
            ddl = False
            dataType = ['none']
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
                            time.sleep(get_delay_time(self.client))
                            ddl = False
                else:
                    line = line.rstrip()
                    if not ddl:
                        self.parse_line(line.strip(), dataType)
                    ngql_statement += " " + line
                    if line.endswith(';'):
                        resp = self.client.execute(ngql_statement)
                        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
                        ngql_statement = ""
                        dataType[0] = 'none'

    # The whole test will load once, for the only read tests
    def load_student(self):
        resp = self.client.execute(
            'CREATE SPACE IF NOT EXISTS student_space(partition_num=10, replica_factor=1); USE student_space;')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE TAG IF NOT EXISTS person(name string, age int, gender string);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE TAG IF NOT EXISTS teacher(grade int, subject string);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE TAG IF NOT EXISTS student(grade int, hobby string DEFAULT "");')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_schoolmate(start_year int, end_year int);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_teacher(start_year int, end_year int);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_friend(start_year int, intimacy double);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('CREATE EDGE IF NOT EXISTS is_colleagues(start_year int, end_year int);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
        # TODO: update the time when config can use
        time.sleep(get_delay_time(self.client))

        resp = self.client.execute('INSERT VERTEX person(name, age, gender), teacher(grade, subject) VALUES \
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
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('INSERT VERTEX person(name, age, gender), student(grade) VALUES \
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
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('INSERT EDGE is_schoolmate(start_year, end_year) VALUES \
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
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('INSERT EDGE is_friend(start_year, intimacy) VALUES \
                                "1003" -> "1004":(2017, 80.0), \
                                "1013" -> "1007":(2018, 80.0), \
                                "1016" -> "1008":(2015, 80.0), \
                                "1016" -> "1018":(2014, 85.0), \
                                "1017" -> "1020":(2018, 78.0), \
                                "1018" -> "1016":(2013, 83.0), \
                                "1018" -> "1020":(2018, 88.0);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('INSERT EDGE is_colleagues(start_year, end_year) VALUES \
                                "2001" -> "2002":(2015, 0), \
                                "2001" -> "2007":(2014, 0), \
                                "2001" -> "2003":(2018, 0), \
                                "2003" -> "2004":(2013, 2017), \
                                "2002" -> "2001":(2016, 2017), \
                                "2007" -> "2001":(2013, 2018), \
                                "2010" -> "2008":(2018, 0);')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

        resp = self.client.execute('INSERT EDGE is_teacher(start_year, end_year) VALUES \
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
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg

    def drop_data(self):
        resp = self.client.execute('DROP SPACE nba; DROP SPACE student_space;')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED, resp.error_msg
        self.client.sign_out()
        self.client_pool.close()

    def parse_line(self, line, dataType):
        if line.startswith('INSERT') or line.startswith('VALUES'):
            return 'error'

        if line.startswith('VERTEX player'):
            dataType[0] = 'player'
        elif line.startswith('VERTEX team'):
            dataType[0] = 'team'
        elif line.startswith('VERTEX bachelor'):
            dataType[0] = 'bachelor'
        elif line.startswith('EDGE serve'):
            dataType[0] = 'serve'
        elif line.startswith('EDGE like'):
            dataType[0] = 'like'
        elif line.startswith('EDGE teammate'):
            dataType[0] = 'teammate'
        else:
            line = re.split(':|,|->', line.strip(',; \t'))
            line = list(map(lambda i: i.strip(' ()"'), line))
            value = CommonTtypes.Value()
            if dataType[0] == 'none':
                return 'error'
            elif dataType[0] == 'player':
                vertex = self.create_vertex_player(line)
                key = str(vertex.vid, encoding='utf-8')
                if key in VERTEXS:
                    temp = VERTEXS[key].get_vVal()
                    temp.tags.append(vertex.tags[0])
                    temp.tags.sort(key=lambda x : x.name)
                    value.set_vVal(temp)
                    VERTEXS[key] = value
                else:
                    value.set_vVal(vertex)
                    VERTEXS[key] = value
            elif dataType[0] == 'team':
                vertex = self.create_vertex_team(line)
                value.set_vVal(vertex)
                key = str(vertex.vid, encoding = 'utf-8')
                VERTEXS[key] = value
            elif dataType[0] == 'bachelor':
                vertex = self.create_vertex_bachelor(line)
                key = str(vertex.vid, encoding = 'utf-8')
                if key in VERTEXS:
                    temp = VERTEXS[key].get_vVal()
                    temp.tags.append(vertex.tags[0])
                    temp.tags.sort(key=lambda x : x.name)
                    value.set_vVal(temp)
                    VERTEXS[key] = value
                else:
                    value.set_vVal(vertex)
                    VERTEXS[key] = value
            elif dataType[0] == 'serve':
                edge = self.create_edge_serve(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                EDGES[key] = value
            elif dataType[0] == 'like':
                edge = self.create_edge_like(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                EDGES[key] = value
            elif dataType[0] == 'teammate':
                edge = self.create_edge_teammate(line)
                value.set_eVal(edge)
                key = str(edge.src, encoding = 'utf-8') + str(edge.dst, encoding = 'utf-8') + str(edge.name, encoding = 'utf-8') + str(edge.ranking)
                EDGES[key] = value
            else:
                assert False

    def create_vertex_player(self, line):
        if len(line) != 3:
            assert False

        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('player', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        age = CommonTtypes.Value()
        age.set_iVal(int(line[2]))
        props[bytes('age', encoding = 'utf-8')] = age
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    def create_vertex_team(self, line):
        if len(line) != 2:
            assert False
        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('team', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    def create_vertex_bachelor(self, line):
        if len(line) != 3:
            assert False

        vertex = CommonTtypes.Vertex()
        vertex.vid = bytes(line[0], encoding = 'utf-8')
        tags = []
        tag = CommonTtypes.Tag()
        tag.name = bytes('bachelor', encoding = 'utf-8')

        props = dict()
        name = CommonTtypes.Value()
        name.set_sVal(bytes(line[1], encoding = 'utf-8'))
        props[bytes('name', encoding = 'utf-8')] = name
        speciality = CommonTtypes.Value()
        speciality.set_sVal(bytes(line[2], encoding = 'utf-8'))
        props[bytes('speciality', encoding = 'utf-8')] = speciality
        tag.props = props
        tags.append(tag)
        vertex.tags = tags
        return vertex

    def create_edge_serve(self, line):
        if len(line) != 4:
            assert False
        edge = CommonTtypes.Edge()
        edge.src = bytes(line[0], encoding = 'utf-8')
        if '@' in line[1]:
            temp = list(map(lambda i: i.strip('"'), re.split('@', line[1])))
            edge.dst = bytes(temp[0], encoding = 'utf-8')
            edge.ranking = int(temp[1])
        else:
            edge.dst = bytes(line[1], encoding = 'utf-8')
            edge.ranking = 0
        edge.type = 0
        edge.name = bytes('serve', encoding = 'utf-8')
        props = dict()
        start_year = CommonTtypes.Value()
        start_year.set_iVal(int(line[2]))
        end_year = CommonTtypes.Value()
        end_year.set_iVal(int(line[3]))
        props[bytes('start_year', encoding = 'utf-8')] = start_year
        props[bytes('end_year', encoding = 'utf-8')] = end_year
        edge.props = props
        return edge

    def create_edge_like(self, line):
        if len(line) != 3:
            assert False
        edge = CommonTtypes.Edge()

        edge.src = bytes(line[0], encoding = 'utf-8')
        edge.dst = bytes(line[1], encoding = 'utf-8')
        edge.type = 0
        edge.ranking = 0
        edge.name = bytes('like', encoding = 'utf-8')
        props = dict()
        likeness = CommonTtypes.Value()
        likeness.set_iVal(int(line[2]))
        props[bytes('likeness', encoding = 'utf-8')] = likeness
        edge.props = props
        return edge

    def create_edge_teammate(self, line):
        if len(line) != 4:
            assert False
        edge = CommonTtypes.Edge()
        edge.src = bytes(line[0], encoding = 'utf-8')
        edge.dst = bytes(line[1], encoding = 'utf-8')
        edge.type = 0
        edge.ranking = 0
        edge.name = bytes('teammate', encoding = 'utf-8')
        props = dict()
        start_year = CommonTtypes.Value()
        start_year.set_iVal(int(line[2]))
        end_year = CommonTtypes.Value()
        end_year.set_iVal(int(line[3]))
        props[bytes('start_year', encoding = 'utf-8')] = start_year
        props[bytes('end_year', encoding = 'utf-8')] = end_year
        edge.props = props
        return edge
