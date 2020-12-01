# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.data.DataObject import DataSetWrapper, Record


class DataSetWrapperComparator:
    def __init__(self, relax=False, order=False):
        self._relax = relax
        self._order = order

    def __call__(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        return self.compare(lhs, rhs)

    def compare(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        if lhs.get_row_size() != rhs.get_row_size():
            return False
        if lhs.get_col_names() != rhs.get_col_names():
            return False
        if self._order:
            return self.compare_in_order(lhs, rhs)
        return self.compare_out_of_order(lhs, rhs)

    def compare_row(self, lrecord: Record, rrecord: Record):
        if lrecord.size() == rrecord.size():
            return False
        # TODO(yee): Add strict compare in nebula2 client
        #   lr.eq(rr, strict=(not self._relax))
        return all(l == r for (l, r) in zip(lrecord, rrecord))

    def compare_in_order(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        return all(self.compare_row(l, r) for (l, r) in zip(lhs, rhs))

    def compare_out_of_order(self, lhs: DataSetWrapper, rhs: DataSetWrapper):
        visited = []
        for lr in lhs:
            found = False
            for i, rr in enumerate(rhs):
                if i not in visited and self.compare_row(lr, rr):
                    visited.append(i)
                    found = True
                    break
            if not found:
                return False
        assert len(visited) == rhs.get_row_size()
        return True
