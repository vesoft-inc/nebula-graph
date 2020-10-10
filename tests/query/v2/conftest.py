# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import pytest

from nebula2.common import ttypes as CommonTtypes


def edgekey(edge):
    return utf8s(edge.src) + utf8s(edge.dst) + utf8s(edge.name) \
        + str(edge.ranking)


def utf8b(s: str):
    return bytes(s, encoding='utf-8')


def utf8s(b):
    return str(b, encoding='utf-8')


def create_vertex_team(line):
    assert len(line) == 2
    vertex = CommonTtypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = CommonTtypes.Tag()
    tag.name = utf8b('team')

    props = dict()
    name = CommonTtypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_vertex_player(line):
    assert len(line) == 3
    vertex = CommonTtypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = CommonTtypes.Tag()
    tag.name = utf8b('player')

    props = dict()
    name = CommonTtypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    age = CommonTtypes.Value()
    age.set_iVal(int(line[2]))
    props[utf8b('age')] = age
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_vertex_bachelor(line):
    assert len(line) == 3
    vertex = CommonTtypes.Vertex()
    vertex.vid = utf8b(line[0])
    tags = []
    tag = CommonTtypes.Tag()
    tag.name = utf8b('bachelor')

    props = dict()
    name = CommonTtypes.Value()
    name.set_sVal(utf8b(line[1]))
    props[utf8b('name')] = name
    speciality = CommonTtypes.Value()
    speciality.set_sVal(utf8b(line[2]))
    props[utf8b('speciality')] = speciality
    tag.props = props
    tags.append(tag)
    vertex.tags = tags
    return vertex


def create_edge_serve(line):
    assert len(line) == 4
    edge = CommonTtypes.Edge()
    edge.src = utf8b(line[0])
    if '@' in line[1]:
        temp = list(map(lambda i: i.strip('"'), re.split('@', line[1])))
        edge.dst = utf8b(temp[0])
        edge.ranking = int(temp[1])
    else:
        edge.dst = utf8b(line[1])
        edge.ranking = 0
    edge.type = 1
    edge.name = utf8b('serve')
    props = dict()
    start_year = CommonTtypes.Value()
    start_year.set_iVal(int(line[2]))
    end_year = CommonTtypes.Value()
    end_year.set_iVal(int(line[3]))
    props[utf8b('start_year')] = start_year
    props[utf8b('end_year')] = end_year
    edge.props = props
    return edge


def create_edge_like(line):
    assert len(line) == 3
    edge = CommonTtypes.Edge()

    edge.src = utf8b(line[0])
    edge.dst = utf8b(line[1])
    edge.type = 1
    edge.ranking = 0
    edge.name = utf8b('like')
    props = dict()
    likeness = CommonTtypes.Value()
    likeness.set_iVal(int(line[2]))
    props[utf8b('likeness')] = likeness
    edge.props = props
    return edge


def create_edge_teammate(line):
    assert len(line) == 4
    edge = CommonTtypes.Edge()
    edge.src = utf8b(line[0])
    edge.dst = utf8b(line[1])
    edge.type = 1
    edge.ranking = 0
    edge.name = utf8b('teammate')
    props = dict()
    start_year = CommonTtypes.Value()
    start_year.set_iVal(int(line[2]))
    end_year = CommonTtypes.Value()
    end_year.set_iVal(int(line[3]))
    props[utf8b('start_year')] = start_year
    props[utf8b('end_year')] = end_year
    edge.props = props
    return edge


def get_datatype(line):
    if line.startswith('VERTEX player'):
        return True, 'player'
    elif line.startswith('VERTEX team'):
        return True, 'team'
    elif line.startswith('VERTEX bachelor'):
        return True, 'bachelor'
    elif line.startswith('EDGE serve'):
        return True, 'serve'
    elif line.startswith('EDGE like'):
        return True, 'like'
    elif line.startswith('EDGE teammate'):
        return True, 'teammate'
    return False, None


def parse_line(line, dataType, VERTEXS, EDGES):
    if line.startswith('INSERT') or line.startswith('VALUES'):
        return ''

    ok, dt = get_datatype(line)
    if ok:
        dataType[0] = dt
    else:
        line = re.split(':|,|->', line.strip(',; \t'))
        line = list(map(lambda i: i.strip(' ()"'), line))
        value = CommonTtypes.Value()
        if dataType[0] == 'none':
            assert False
        elif dataType[0] == 'player':
            vertex = create_vertex_player(line)
            key = utf8s(vertex.vid)
            if key in VERTEXS:
                temp = VERTEXS[key].get_vVal()
                temp.tags.append(vertex.tags[0])
                temp.tags.sort(key=lambda x: x.name)
                value.set_vVal(temp)
                VERTEXS[key] = value
            else:
                value.set_vVal(vertex)
                VERTEXS[key] = value
        elif dataType[0] == 'team':
            vertex = create_vertex_team(line)
            value.set_vVal(vertex)
            key = utf8s(vertex.vid)
            VERTEXS[key] = value
        elif dataType[0] == 'bachelor':
            vertex = create_vertex_bachelor(line)
            key = utf8s(vertex.vid)
            if key in VERTEXS:
                temp = VERTEXS[key].get_vVal()
                temp.tags.append(vertex.tags[0])
                temp.tags.sort(key=lambda x: x.name)
                value.set_vVal(temp)
                VERTEXS[key] = value
            else:
                value.set_vVal(vertex)
                VERTEXS[key] = value
        elif dataType[0] == 'serve':
            edge = create_edge_serve(line)
            value.set_eVal(edge)
            key = edgekey(edge)
            EDGES[key] = value
        elif dataType[0] == 'like':
            edge = create_edge_like(line)
            value.set_eVal(edge)
            key = edgekey(edge)
            EDGES[key] = value
        elif dataType[0] == 'teammate':
            edge = create_edge_teammate(line)
            value.set_eVal(edge)
            key = edgekey(edge)
            EDGES[key] = value
        else:
            assert False


@pytest.fixture(scope="class")
def set_vertices_and_edges(request):
    VERTEXS = {}
    EDGES = {}
    nba_file = request.cls.data_dir + '/data/nba.ngql'
    print("load will open ", nba_file)
    with open(nba_file, 'r') as data_file:
        lines = data_file.readlines()
        ddl = False
        dataType = ['none']
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
                        ddl = False
            else:
                if not ddl:
                    parse_line(line.strip(), dataType, VERTEXS, EDGES)
                if line.endswith(';'):
                    dataType[0] = 'none'

    request.cls.VERTEXS = VERTEXS
    request.cls.EDGES = EDGES
