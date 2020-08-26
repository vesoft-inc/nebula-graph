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

        vertex1 = {"type" : "vertex", "value" : [{"vid" : "Tim Duncan", "tags":[{"name" : "bachelor", "props" : {"name" : "Tim Duncan", "speciality" : "psychology"}},
                                                                                {"name" : "player", "props" : {"age" : 42, "name" : "Tim Duncan"}},]},]}
        edge1 = {"type" : "edge", "value" : [{"src" : "Tim Duncan", "dst" : "Manu Ginobili", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tim Duncan", "dst" : "Tony Parker", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tim Duncan", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 1997}},
                                             {"src" : "Tim Duncan", "dst" : "Danny Green", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2010}},
                                             {"src" : "Tim Duncan", "dst" : "LaMarcus Aldridge", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2015}},
                                             {"src" : "Tim Duncan", "dst" : "Manu Ginobili", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2002}},
                                             {"src" : "Tim Duncan", "dst" : "Tony Parker", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2001}},]}

        vertex2 = {"type" : "vertex", "value" : [{"vid" : "Tony Parker", "tags":[{"name" : "player", "props" : {"age" : 36, "name" : "Tony Parker"}}]},
                                                 {"vid" : "Manu Ginobili", "tags":[{"name" : "player", "props" : {"age" : 41, "name" : "Manu Ginobili"}}]},
                                                 {"vid" : "Danny Green", "tags":[{"name" : "player", "props" : {"age" : 31, "name" : "Danny Green"}}]},
                                                 {"vid" : "LaMarcus Aldridge", "tags":[{"name" : "player", "props" : {"age" : 33, "name" : "LaMarcus Aldridge"}}]},]}

        edge2 = {"type" : "edge", "value" : [{"src" : "Tony Parker", "dst" : "LaMarcus Aldridge", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 90}},
                                             {"src" : "Tony Parker", "dst" : "Manu Ginobili", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tony Parker", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tony Parker", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 1999}},
                                             {"src" : "Manu Ginobili", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 90}},
                                             {"src" : "Manu Ginobili", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2002}},
                                             {"src" : "Manu Ginobili", "dst" : "Tony Parker", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2002}},
                                             {"src" : "Danny Green", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 70}},
                                             {"src" : "Danny Green", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2010}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 75}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Tony Parker", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 75}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2019, "start_year" : 2015}},]}


        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = {"type" : "vertex", "value" : [{"vid" : "Tim Duncan", "tags":[{"name" : "bachelor", "props" : {"name" : "Tim Duncan", "speciality" : "psychology"}},
                                                                                {"name" : "player", "props" : {"age" : 42, "name" : "Tim Duncan"}},]},]}
        edge1 = {"type" : "edge", "value" : [{"src" : "Tim Duncan", "dst" : "Manu Ginobili", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tim Duncan", "dst" : "Tony Parker", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tim Duncan", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 1997}},
                                             {"src" : "Tim Duncan", "dst" : "Danny Green", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2010}},
                                             {"src" : "Tim Duncan", "dst" : "LaMarcus Aldridge", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2015}}]}

        vertex2 = {"type" : "vertex", "value" : [{"vid" : "Tony Parker", "tags":[{"name" : "player", "props" : {"age" : 36, "name" : "Tony Parker"}}]},
                                                 {"vid" : "Manu Ginobili", "tags":[{"name" : "player", "props" : {"age" : 41, "name" : "Manu Ginobili"}}]},
                                                 {"vid" : "Danny Green", "tags":[{"name" : "player", "props" : {"age" : 31, "name" : "Danny Green"}}]},
                                                 {"vid" : "LaMarcus Aldridge", "tags":[{"name" : "player", "props" : {"age" : 33, "name" : "LaMarcus Aldridge"}}]},]}

        edge2 = {"type" : "edge", "value" : [{"src" : "Tony Parker", "dst" : "LaMarcus Aldridge", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 90}},
                                             {"src" : "Tony Parker", "dst" : "Manu Ginobili", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tony Parker", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 95}},
                                             {"src" : "Tony Parker", "dst" : "Hornets", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2019, "start_year" : 2018}},
                                             {"src" : "Tony Parker", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 1999}},
                                             {"src" : "Tony Parker", "dst" : "Kyle Anderson", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2014}},
                                             {"src" : "Manu Ginobili", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 90}},
                                             {"src" : "Manu Ginobili", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2002}},
                                             {"src" : "Manu Ginobili", "dst" : "Tony Parker", "type" : 0, "name" : "teammate", "ranking" : 0, "props" : {"end_year" : 2016, "start_year" : 2002}},
                                             {"src" : "Danny Green", "dst" : "LeBron James", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 80}},
                                             {"src" : "Danny Green", "dst" : "Marco Belinelli", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 83}},
                                             {"src" : "Danny Green", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 70}},
                                             {"src" : "Danny Green", "dst" : "Cavaliers", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2010, "start_year" : 2009}},
                                             {"src" : "Danny Green", "dst" : "Raptors", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2019, "start_year" : 2018}},
                                             {"src" : "Danny Green", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2010}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 75}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Tony Parker", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 75}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2019, "start_year" : 2015}},
                                             {"src" : "LaMarcus Aldridge", "dst" : "Trail Blazers", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2015, "start_year" : 2006}},]}

        vertex3 = {"type" : "vertex", "value" : [{"vid" : "LeBron James", "tags":[{"name" : "player", "props" : {"age" : 34, "name" : "LeBron James"}}]},
                                                 {"vid" : "Kyle Anderson", "tags":[{"name" : "player", "props" : {"age" : 25, "name" : "Kyle Anderson"}}]},
                                                 {"vid" : "Marco Belinelli", "tags":[{"name" : "player", "props" : {"age" : 32, "name" : "Marco Belinelli"}}]},]}

        edge3 = {"type" : "edge", "value" : [{"src" : "LeBron James", "dst" : "Cavaliers", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2014}},
                                             {"src" : "Kyle Anderson", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2018, "start_year" : 2014}},
                                             {"src" : "Marco Belinelli", "dst" : "Danny Green", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 60}},
                                             {"src" : "Marco Belinelli", "dst" : "Tim Duncan", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 55}},
                                             {"src" : "Marco Belinelli", "dst" : "Tony Parker", "type" : 0, "name" : "like", "ranking" : 0, "props" : {"likeness" : 50}},
                                             {"src" : "Marco Belinelli", "dst" : "Hornets", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2017, "start_year" : 2016}},
                                             {"src" : "Marco Belinelli", "dst" : "Raptors", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2010, "start_year" : 2009}},
                                             {"src" : "Marco Belinelli", "dst" : "Spurs", "type" : 0, "name" : "serve", "ranking" : 0, "props" : {"end_year" : 2019, "start_year" : 2018}},]}

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
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
