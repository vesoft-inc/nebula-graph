# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from nebula2.common.ttypes import DataSet, Row
from nebula2.data.DataObject import DataSetWrapper
from tests.tck.utils.nbv import parse


def _table(text):
    lines = text.splitlines()
    assert len(lines) >= 1

    def parse_line(line):
        return list(map(lambda x: x.strip(),
                        filter(lambda x: x, line.split('|'))))

    table = []
    column_names = list(map(lambda x: bytes(x, 'utf-8'), parse_line(lines[0])))
    for line in lines[1:]:
        row = {}
        cells = parse_line(line)
        for i, cell in enumerate(cells):
            row[column_names[i]] = cell
        table.append(row)

    return {
        "column_names": column_names,
        "rows": table,
    }


@pytest.fixture
def table():
    return _table


def _dataset(string_table):
    ds = DataSet()
    ds.column_names = string_table['column_names']
    ds.rows = []
    for row in string_table['rows']:
        nrow = Row()
        nrow.values = []
        for column in ds.column_names:
            value = parse(row[column])
            assert value is not None
            nrow.values.append(value)
        ds.rows.append(nrow)
    return ds


@pytest.fixture
def dataset(table):
    return _dataset


def _datasetwrapper(ds: DataSet):
    return DataSetWrapper(ds)


@pytest.fixture
def datasetwrapper(dataset):
    def _dswrapper(string_table):
        return _datasetwrapper(dataset(string_table))
    return _dswrapper
