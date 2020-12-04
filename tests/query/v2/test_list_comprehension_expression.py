# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY
import pytest


class TestCaseExpression(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_list_comprehension_expression(self):
        stmt = 'YIELD [n IN range(1, 5) where n > 2 | n + 10]'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[13, 14, 15]]]
        self.check_result(resp, expected_data)

        stmt = 'YIELD [n IN [1, 2, 3, 4, 5] where n > 2]'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[3, 4, 5]]]
        self.check_result(resp, expected_data)
        
        stmt = 'YIELD tail([n IN range(1, 5) | 2 * n - 10])'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[-6, -4, -2, 0]]]
        self.check_result(resp, expected_data)

        stmt = 'YIELD [n IN range(1, 3) WHERE true | n] as r'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[1, 2, 3]]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tony Parker" OVER like WHERE like.likeness IN \
            [x IN [95,  100] | x] YIELD like._dst, like.likeness'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [["Manu Ginobili", 95], ["Tim Duncan", 95]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) return [n IN nodes(p) \
            WHERE n.name NOT STARTS WITH "LeBron" | n.age + 100] as r'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[134]], [[133]], [[131]], [[129]], [[137]], [[126]]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) \
            return [n IN nodes(p) | n.age + 100]'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[[134, 143]]]
        self.check_out_of_order_result(resp, expected_data)
