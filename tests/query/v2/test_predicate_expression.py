# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY
import pytest


class TestPredicateExpression(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_predicate_expression(self):
        stmt = 'YIELD all(n IN range(1, 5) WHERE n > 2)'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[False]]
        self.check_result(resp, expected_data)

        stmt = 'YIELD any(n IN [1, 2, 3, 4, 5] WHERE n > 2)'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        stmt = 'YIELD single(n IN range(1, 5) WHERE n == 3)'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        stmt = 'YIELD none(n IN range(1, 3) WHERE n == 0) as r'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_result(resp, expected_data)

        stmt = '''GO FROM "Tony Parker" OVER like WHERE \
            any(x IN [5,  10] WHERE like.likeness + $$.player.age + x > 100) YIELD like._dst, like.likeness'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [["LaMarcus Aldridge", 90], ["Manu Ginobili", 95], ["Tim Duncan", 95]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) \
            RETURN nodes(p)[0].name, nodes(p)[1].name, all(n IN nodes(p) \
            WHERE n.name NOT STARTS WITH "D") as r'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [["LeBron James", "Carmelo Anthony", True],
                         ["LeBron James", "Chris Paul", True],
                         ["LeBron James",  "Danny Green", False],
                         ["LeBron James", "Dejounte Murray", False],
                         ["LeBron James", "Dwyane Wade", False],
                         ["LeBron James", "Kyrie Irving", True]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) \
            RETURN single(n IN nodes(p) WHERE n.age > 40)'''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[True]]
        self.check_out_of_order_result(resp, expected_data)
