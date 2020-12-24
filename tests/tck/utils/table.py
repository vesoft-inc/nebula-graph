# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import csv
import io
import re

from tests.tck.utils.nbv import parse
from nebula2.common.ttypes import DataSet, Row, Value

pattern = re.compile(r"^<\[(\w+)\]>$")
col_names_pattern = re.compile(r"(.+)_\d+$")


def _parse_value(cell: str, variables: dict) -> Value:
    m = pattern.match(cell)
    if m:
        var = m.group(1)
        assert var in variables, f"Invalid expect variable usages: {cell}"
        cell = variables.get(var, None)
        assert cell is not None

    value = parse(cell)
    assert value is not None, f"parse error: column is {cell}"
    return value


def table(text):
    lines = list(csv.reader(io.StringIO(text), delimiter="|"))
    header = lines[0][1:-1]
    column_names = [f"{column.strip()}_{i}" for i, column in enumerate(header)]
    table = [{
        column_name: cell.strip()
        for (column_name, cell) in zip(column_names, line[1:-1])
    } for line in lines[1:]]

    return {
        "column_names": column_names,
        "rows": table,
    }


def dataset(string_table, variables: dict):
    ds = DataSet()
    column_names = string_table['column_names']
    ds.column_names = [
        col_names_pattern.match(column).group(1) for column in column_names
    ]
    ds.rows = [
        Row(values=[
            _parse_value(row[column], variables) for column in column_names
        ]) for row in string_table['rows']
    ]
    return ds
