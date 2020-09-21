# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_NULL, T_EMPTY
import pytest

class TestINandNotIn(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def cleanup():
        pass

    def test_in_list(self):
        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Danny Green'] YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                []
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Danny Green']"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like.likeness IN [95,56,21]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 1 IN [1, 2, 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 0 IN [1, 2, 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [False]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'hello' IN ['hello', 'world', 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : [],
            "rows" : [
                [True]
            ]
        }
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_in_list(self):
        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst NOT IN ['Danny Green'] YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN ['Danny Green']"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like.likeness IN [95,56,21]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 1 NOT IN [1, 2, 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 0 NOT IN [1, 2, 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'hello' NOT IN ['hello', 'world', 3]"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_in_set(self):
        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN {'Danny Green'} YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN {'Danny Green'}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like.likeness IN {95,56,21,95,90}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 1 IN {1, 2, 3}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 0 IN {1, 2, 3, 1, 2}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'hello' IN {'hello', 'world', 3}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

    def test_not_in_set(self):
        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst NOT IN {'Danny Green'} YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like._dst IN {'Danny Green'}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GO FROM 'Tony Parker' OVER like WHERE like.likeness IN {95,56,21}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 1 NOT IN {1, 2, 3}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 0 NOT IN {1, 2, 3}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "YIELD 'hello' NOT IN {'hello', 'world', 3}"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        expected_data = {
            "column_names" : ['$$.player.name'],
            "rows" : [
                ['vertices']
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])
