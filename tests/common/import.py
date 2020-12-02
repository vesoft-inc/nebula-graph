# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import csv

from tests.common.types import (
    VID,
    Prop,
    Tag,
    Edge,
    Vertex,
)


class CSVImporter:
    _SRC_VID = ':SRC_VID'
    _DST_VID = ':DST_VID'
    _VID = ':VID'
    _RANK = ':RANK'

    def __init__(self, filepath):
        self._filepath = filepath
        self._insert_edge_fmt = "INSERT EDGE {} ({}) VALUES {}:({});"
        self._insert_vertex_fmt = "INSERT VERTEX {}({}) VALUES {}:({});"
        self._is_edge = False

    def __iter__(self):
        with open(self._filepath, 'r') as f:
            for i, row in enumerate(csv.reader(f)):
                if i == 0:
                    self.parse_header(row)
                else:
                    yield self.process(row)

    def process(self, row: list):
        return row

    def parse_header(self, row):
        """
        Only parse the scenario that one tag in each file
        """
        found = False
        for col in row:
            if self._SRC_VID in col and self._DST_VID in col:
                self._is_edge = True
                found = True
                break
            if self._VID in col:
                self._is_edge = False
                found = True
                break
        if not found:
            raise ValueError(f'Invalid csv header: {",".join(row)}')

        if self._is_edge:
            self.parse_edge()


if __name__ == '__main__':
    for row in CSVImporter('../data/nba/serve.csv'):
        print(row)
