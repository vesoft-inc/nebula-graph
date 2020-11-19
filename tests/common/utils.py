# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import pdb
from typing import Pattern

from nebula2.common import ttypes as CommonTtypes


def _compare_values_by_pattern(real, expect):
    if real.getType() == CommonTtypes.Value.BVAL:
        return expect.match(str(real.get_bVal()))
    if real.getType() == CommonTtypes.Value.IVAL:
        return expect.match(str(real.get_iVal()))
    if real.getType() == CommonTtypes.Value.SVAL:
        return expect.match(real.get_sVal().decode('utf-8'))
    if real.getType() == CommonTtypes.Value.FVAL:
        return expect.match(str(real.get_fVal()))
    return False


def _compare_list(rvalues, evalues):
    if len(rvalues) != len(evalues):
        pdb.set_trace()
        return False

    for rval in rvalues:
        found = False
        for ev in evalues:
            if compare_value(rval, ev):
                found = True
                break
        if not found:
            pdb.set_trace()
            return False
    return True


def _compare_map(rvalues, evalues):
    for key in rvalues:
        ev = evalues.get(key)
        if ev is None:
            pdb.set_trace()
            return False
        rv = rvalues.get(key)
        if not compare_value(rv, ev):
            pdb.set_trace()
            return False
    return True


def compare_value(real, expect):
    if isinstance(expect, Pattern):
        return _compare_values_by_pattern(real, expect)

    if real.getType() == CommonTtypes.Value.LVAL:
        rvalues = real.get_lVal().values
        if expect.getType() == CommonTtypes.Value.LVAL:
            evalues = expect.get_lVal().values
            return _compare_list(rvalues, evalues)
        if type(expect) is list:
            return _compare_list(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.UVAL:
        rvalues = real.get_uVal().values
        if expect.getType() == CommonTtypes.Value.UVAL:
            evalues = expect.get_uVal().values
            return _compare_list(rvalues, evalues)
        if type(expect) is set:
            return _compare_list(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.MVAL:
        rvalues = real.get_mVal().kvs
        if expect.getType() == CommonTtypes.Value.MVAL:
            evalues = expect.get_mVal().kvs
            return _compare_map(rvalues, evalues)
        if type(expect) is dict:
            return _compare_map(rvalues, expect)
        return False

    if real.getType() == CommonTtypes.Value.EVAL:
        if expect.getType() != CommonTtypes.Value.EVAL:
            return False
        redge = real.get_eVal()
        eedge = expect.get_eVal()
        rsrc, rdst = redge.src, redge.dst
        if redge.type < 0:
            rsrc, rdst = rdst, rsrc
        esrc, edst = eedge.src, eedge.dst
        if eedge.type < 0:
            esrc, edst = edst, esrc
        # ignore props comparation
        return rsrc == esrc and rdst == edst \
            and redge.ranking == eedge.ranking \
            and redge.name == eedge.name

    return real == expect


def value_to_string(value):
    if value.getType() == CommonTtypes.Value.__EMPTY__:
        return '__EMPTY__'
    elif value.getType() == CommonTtypes.Value.NVAL:
        return '__NULL__'
    elif value.getType() == CommonTtypes.Value.BVAL:
        return str(value.get_bVal())
    elif value.getType() == CommonTtypes.Value.IVAL:
        return str(value.get_iVal())
    elif value.getType() == CommonTtypes.Value.FVAL:
        return str(value.get_fVal())
    elif value.getType() == CommonTtypes.Value.SVAL:
        return value.get_sVal().decode('utf-8')
    elif value.getType() == CommonTtypes.Value.DVAL:
        return date_to_string(value.get_dVal())
    elif value.getType() == CommonTtypes.Value.TVAL:
        return time_to_string(value.get_tVal())
    elif value.getType() == CommonTtypes.Value.DTVAL:
        return date_time_to_string(value.get_dtVal())
    elif value.getType() == CommonTtypes.Value.EVAL:
        return edge_to_string(value.get_eVal())
    elif value.getType() == CommonTtypes.Value.LVAL:
        return list_to_string(value.get_lVal())
    elif value.getType() == CommonTtypes.Value.VVAL:
        return f'({value.get_vVal().vid})'
    elif value.getType() == CommonTtypes.Value.UVAL:
        return list_to_string(value.get_uVal())
    elif value.getType() == CommonTtypes.Value.MVAL:
        return map_to_string(value.get_mVal())
    elif value.getType() == CommonTtypes.Value.PVAL:
        return path_to_string(value.get_pVal())
    elif value.getType() == CommonTtypes.Value.GVAL:
        return dataset_to_string(value.get_gVal())
    elif type(value) is CommonTtypes.Tag:
        return tag_to_string(value)
    elif type(value) is CommonTtypes.Row:
        return list_to_string(value.values)
    elif type(value) is CommonTtypes.Step:
        return step_to_string(value)
    else:
        return value.__repr__()


def row_to_string(row):
    value_list = map(value_to_string, row.values)
    return '[' + ','.join(value_list) + ']'


def date_to_string(date):
    return f'{date.year}/{date.month}/{date.day}'


def time_to_string(time):
    return f'{time.hour}:{time.minute}:{time.sec}.{time.microsec}'


def date_time_to_string(date_time):
    return f'{date_to_string(date_time)} {time_to_string(date_time)}'


def _kv_to_string(kv):
    dec = kv[0].decode('utf-8')
    return f'"{dec}": {value_to_string(kv[1])}'


def map_to_string(map_val):
    values = list(map(_kv_to_string, map_val.kvs.items()))
    sorted_keys = sorted(values)
    return '{' + ", ".join(sorted_keys) + '}'


def list_to_string(lst):
    values = map(value_to_string, lst.values)
    sorted_values = sorted(values)
    return '[' + ', '.join(sorted_values) + ']'


def set_to_string(set_val):
    values = map(value_to_string, set_val.values)
    sorted_values = sorted(values)
    return '{' + ', '.join(sorted_values) + '}'


def edge_to_string(edge):
    name = edge.name.decode("utf-8")
    src, dst = (edge.src, edge.dst) if edge.type > 0 else (edge.dst, edge.src)
    return f'({src})-[:{name}@{edge.ranking} {map_to_string(edge.props)}]->({dst})'


def vertex_to_string(vertex):
    vid = vertex.vid.decode('utf-8')
    tags = list_to_string(vertex.tags)
    return f'({vid} {tags})'


def tag_to_string(tag):
    name = tag.name.decode('utf-8')
    props = map_to_string(tag.props)
    return f':{name} {props}'


def step_to_string(step):
    dst = vertex_to_string(step.dst)
    name = step.name.decode('utf-8')
    ranking = step.ranking
    props = map_to_string(step.props)
    if step.type > 0:
        return f'-[:{name}@{ranking} {props}]->{dst}'
    else:
        return f'<-[:{name}@{ranking} {props}]-{dst}'


def path_to_string(path):
    return vertex_to_string(path.src) \
        + ''.join(map(step_to_string, path.steps))


def dataset_to_string(dataset):
    column_names = ','.join(map(lambda x: x.decode('utf-8'), dataset.column_names))
    rows = '\n'.join(map(row_to_string, dataset.rows))
    return '\n'.join([column_names, rows])
