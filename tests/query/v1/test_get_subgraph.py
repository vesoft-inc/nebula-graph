# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL
import pytest

class TestGroupBy(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    def cleanup():
        pass

    def test_invalid_input(self):
        stmt = 'GET SUBGRAPH 0 STEPS FROM "Tim Duncan"'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = 'GET SUBGRAPH FROM $-.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = 'GET SUBGRAPH FROM $a.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = 'GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS id | GET SUBGRAPH FROM $-.id'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD $$.player.age AS ID; GET SUBGRAPH FROM $a.ID'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $b.src'
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # stmt = 'GO FROM "Tim Duncan" OVER like YIELD $$._dst AS id, $^._src AS id | GET SUBGRAPH FROM $-.id'
        # resp = self.execute_query(stmt)
        # self.check_resp_failed(resp)

        # stmt = '$a = GO FROM "Tim Duncan" OVER like YIELD $$._dst AS id, $^._src AS id; GET SUBGRAPH FROM $a.id'
        # resp = self.execute_query(stmt)
        # self.check_resp_failed(resp)


    def test_subgraph(self):
        stmt = "GET SUBGRAPH FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like, serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan', 'James Harden' IN like OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' OUT serve BOTH like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])


        stmt = "GET SUBGRAPH FROM 'Tim Duncan' BOTH like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                ["Tony Parker"]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
