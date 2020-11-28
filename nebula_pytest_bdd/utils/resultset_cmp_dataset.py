import sys
sys.path.insert(0,'../nebula-clients/python/')
from nebula2.gclient.data.DataObject import DataSetWrapper
from nebula2.common import ttypes
from nebula2.common.ttypes import Value,Vertex,Edge,Path,Step,NullType,DataSet,Row,List
from nebula2.gclient.data.ResultSet import ResultSet


def rst_cmp_dst_in_order(resultset,dataset):
    print(resultset)
    print(dataset)
    assert resultset.is_succeeded()
    assert dataset is not None
    assert resultset.keys() == dataset.column_names, \
        'column_name is not equal, expect: {} != return: {}'.format(dataset.column_names, resultset.keys())
    if len(resultset.rows()) == len(dataset.rows) == 0:
        return
    assert len(resultset.rows()) == len(dataset.rows), 'rows len is not equal, expect: {} != return: {}'\
        .format(dataset.rows, resultset.rows)
    for i, recode in enumerate(resultset):
        assert len(dataset.rows[i].values) == recode.size(), \
            'row len is not equal, expect: {} != return: {}'.format(len(dataset.rows[i].values), recode.size())
        for j, value in enumerate(recode):
            expect_value = dataset.rows[i].values[j]
            if value.is_empty():
                assert expect_value.getType() == ttypes.Value.__EMPTY__
                assert expect_value == Value()
            elif value.is_null():
                assert expect_value.getType() == ttypes.Value.NVAL
            elif value.is_bool():
                assert expect_value.getType() == ttypes.Value.BVAL
                assert value.as_bool() == expect_value.get_bVal()
            elif value.is_int():
                assert expect_value.getType() == ttypes.Value.IVAL
                assert value.as_int() == expect_value.get_iVal()
            elif value.is_double():
                assert expect_value.getType() == ttypes.Value.FVAL
                assert value.as_double() == expect_value.get_fVal()
            elif value.is_string():
                assert expect_value.getType() == ttypes.Value.SVAL
                assert value.as_string() == expect_value.get_sVal()
            elif value.is_list():
                assert expect_value.getType() == ttypes.Value.LVAL
                assert value.as_list() == expect_value.get_lVal()
            elif value.is_set():
                assert expect_value.getType() == ttypes.Value.UVAL
                assert value.as_set() == expect_value.get_uVal()
            elif value.is_map():
                assert expect_value.getType() == ttypes.Value.MVAL
                assert value.as_map() == expect_value.get_mVal()
            else:
                assert 'Unsupported type, expect: {}, return: {}'.format(expect_value, value)
    return True
    return True
   
    
