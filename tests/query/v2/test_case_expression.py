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

    def cleanup():
        pass

    def test_generic_case_expression(self):
        stmt = 'YIELD CASE 2 + 3 WHEN 4 THEN 0 WHEN 5 THEN 1 ELSE 2 END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE true WHEN false THEN 0 END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[T_NULL]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Jonathon Simmons" OVER serve YIELD $$.team.name as name, \
            CASE serve.end_year > 2017 WHEN true THEN "ok" ELSE "no" END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'no'], ['Magic', 'ok'], ['76ers', 'ok']]
        self.check_out_of_order_result(resp, expected_data)

        stmt = '''GO FROM "Boris Diaw" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, \
            CASE serve.start_year > 2006 WHEN true THEN "new" ELSE "old" END, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [
            ["Boris Diaw", 2003, 2005, "old", "Hawks"],
            ["Boris Diaw", 2005, 2008, "old", "Suns"],
            ["Boris Diaw", 2008, 2012, "new", "Hornets"],
            ["Boris Diaw", 2012, 2016, "new", "Spurs"],
            ["Boris Diaw", 2016, 2017, "new", "Jazz"]
        ]
        self.check_out_of_order_result(resp, expected_data)

        # # we are not able to deduce the return type of case expression in where_clause
        # stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
        #     CASE serve.start_year WHEN 2016 THEN true ELSE false END YIELD \
        #     $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        # resp = self.execute_query(stmt)
        # self.check_resp_succeeded(resp)
        # expected_data = [
        #     ["Rajon Rondo", 2016, 2017, "Bulls"],
        # ]
        # self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE WHEN 4 > 5 THEN 0 WHEN 3+4==7 THEN 1 ELSE 2 END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD CASE WHEN false THEN 0 ELSE 1 END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Tim Duncan" OVER serve YIELD $$.team.name as name, \
            CASE WHEN serve.start_year < 1998 THEN "old" ELSE "young" END'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'old']]
        self.check_out_of_order_result(resp, expected_data)

        # # we are not able to deduce the return type of case expression in where_clause
        # stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
        #     CASE WHEN serve.start_year > 2016 THEN true ELSE false END YIELD \
        #     $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        # resp = self.execute_query(stmt)
        # self.check_resp_succeeded(resp)
        # expected_data = [
        #     ["Rajon Rondo", 2018, 2019, "Lakers"],
        #     ["Rajon Rondo", 2017, 2018, "Pelicans"]
        # ]
        # self.check_out_of_order_result(resp, expected_data)

    def test_conditional_case_expression(self):
        stmt = 'YIELD 3 > 5 ? 0 : 1'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [[1]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'YIELD true ? "yes" : "no"'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [["yes"]]
        self.check_out_of_order_result(resp, expected_data)

        stmt = 'GO FROM "Tim Duncan" OVER serve YIELD $$.team.name as name, \
            serve.start_year < 1998 ? "old" : "young"'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = [['Spurs', 'old']]
        self.check_out_of_order_result(resp, expected_data)

        # # we are not able to deduce the return type of case expression in where_clause
        # stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
        #     serve.start_year > 2016 ? true : false YIELD \
        #     $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        # resp = self.execute_query(stmt)
        # self.check_resp_succeeded(resp)
        # expected_data = [
        #     ["Rajon Rondo", 2018, 2019, "Lakers"],
        #     ["Rajon Rondo", 2017, 2018, "Pelicans"]
        # ]
        # self.check_out_of_order_result(resp, expected_data)
