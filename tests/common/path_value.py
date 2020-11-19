# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.common import ttypes as CommonTtypes


class PathVal:
    def __init__(self, items):
        self.items = items

    def to_value(self, col):
        path = CommonTtypes.Path()
        path.steps = []
        for col, j in zip(col.items, range(len(col.items))):
            if j == 0:
                path.src = col.get_vVal()
            elif (j % 2) == 1:
                edge = col[0].get_eVal()
                step = CommonTtypes.Step()
                step.name = edge.name
                step.ranking = edge.ranking
                step.type = col[1]
                step.props = edge.props
                path.steps.append(step)
            else:
                print("step: %d", len(path.steps))
                path.steps[-1].dst = col.get_vVal()
        return path
