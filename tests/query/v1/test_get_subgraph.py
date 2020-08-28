# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL
from tests.common.load_test_data import VERTEXS, EDGES
import pytest

class TestSubGraph(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

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

        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                 EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                 EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']]

        vertex2 = [VERTEXS['Tony Parker'],
                   VERTEXS['Manu Ginobili'],
                   VERTEXS['Danny Green'],
                   VERTEXS['LaMarcus Aldridge']]

        edge2 = [EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                 EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                 EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                 EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],
                 EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                 EDGES['Danny Green'+'Spurs'+'serve'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0']]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        #self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan'"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                 EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                 EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']]

        vertex2 = [VERTEXS['Tony Parker'],
                   VERTEXS['Manu Ginobili'],
                   VERTEXS['Danny Green'],
                   VERTEXS['LaMarcus Aldridge']]

        edge2 = [EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                 EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                 EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                 EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                 EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],

                 EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                 EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],

                 EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                 EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                 EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                 EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                 EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                 EDGES['Danny Green'+'Spurs'+'serve'+'0'],

                 EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                 EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0']]

        vertex3 = [VERTEXS['LeBron James'],
                   VERTEXS['Kyle Anderson'],
                   VERTEXS['Marco Belinelli']]

        edge3 = [EDGES['LeBron James'+'Cavaliers'+'serve'+'0'],
                 EDGES['LeBron James'+'Cavaliers'+'serve'+'1'],
                 EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],

                 EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        #self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like, serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                 EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                 EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']]

        vertex2 = [VERTEXS['Tony Parker'],
                   VERTEXS['Manu Ginobili'],
                   VERTEXS['Danny Green'],
                   VERTEXS['LaMarcus Aldridge']]

        edge2 = [EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                 EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                 EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                 EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                 EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],

                 EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                 EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],

                 EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                 EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                 EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                 EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                 EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                 EDGES['Danny Green'+'Spurs'+'serve'+'0'],

                 EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                 EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0']]

        vertex3 = [VERTEXS['LeBron James'],
                   VERTEXS['Kyle Anderson'],
                   VERTEXS['Marco Belinelli']]

        edge3 = [EDGES['LeBron James'+'Cavaliers'+'serve'+'0'],
                 EDGES['LeBron James'+'Cavaliers'+'serve'+'1'],
                 EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],

                 EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        #self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan' IN like OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tim Duncan"]]

        edge1 = [EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                 EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                 EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0']]

        vertex2 = [VERTEXS['Tony Parker'],
                   VERTEXS['Manu Ginobili'],
                   VERTEXS['Danny Green'],
                   VERTEXS['LaMarcus Aldridge']]

        edge2 = [EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                 EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                 EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                 EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                 EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],

                 EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                 EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],

                 EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                 EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                 EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                 EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                 EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                 EDGES['Danny Green'+'Spurs'+'serve'+'0'],

                 EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                 EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0']]

        vertex3 = [VERTEXS['LeBron James'],
                   VERTEXS['Kyle Anderson'],
                   VERTEXS['Marco Belinelli']]

        edge3 = [EDGES['LeBron James'+'Cavaliers'+'serve'+'0'],
                 EDGES['LeBron James'+'Cavaliers'+'serve'+'1'],
                 EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],

                 EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'1']]

        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        self.check_column_names(resp, expected_data["column_names"])
        #self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 2 STEPS FROM 'Tim Duncan', 'James Harden' IN teammate OUT serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        vertex1 = [VERTEXS["Tim Duncan"],
                   VERTEXS["James Harden"]]

        edge1 = [EDGES['Tim Duncan'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'like'+'0'],
                 EDGES['Tim Duncan'+'Spurs'+'serve'+'0'],
                 EDGES['Tim Duncan'+'Danny Green'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tim Duncan'+'Tony Parker'+'teammate'+'0'],

                 EDGES['James Harden'+'Russell Westbrook'+'like'+'0'],
                 EDGES['James Harden'+'Rockets'+'serve'+'0'],
                 EDGES['James Harden'+'Thunders'+'serve'+'0']]

        vertex2 = [VERTEXS['Tony Parker'],
                   VERTEXS['Manu Ginobili'],
                   VERTEXS['Danny Green'],
                   VERTEXS['LaMarcus Aldridge'],
                   VERTEXS['Russell Westbrook']]

        edge2 = [EDGES['Tony Parker'+'LaMarcus Aldridge'+'like'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'like'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'like'+'0'],
                 EDGES['Tony Parker'+'Hornets'+'serve'+'0'],
                 EDGES['Tony Parker'+'Spurs'+'serve'+'0'],
                 EDGES['Tony Parker'+'Kyle Anderson'+'teammate'+'0'],
                 EDGES['Tony Parker'+'LaMarcus Aldridge'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Manu Ginobili'+'teammate'+'0'],
                 EDGES['Tony Parker'+'Tim Duncan'+'teammate'+'0'],

                 EDGES['Manu Ginobili'+'Tim Duncan'+'like'+'0'],
                 EDGES['Manu Ginobili'+'Spurs'+'serve'+'0'],
                 EDGES['Manu Ginobili'+'Tim Duncan'+'teammate'+'0'],
                 EDGES['Manu Ginobili'+'Tony Parker'+'teammate'+'0'],

                 EDGES['Danny Green'+'LeBron James'+'like'+'0'],
                 EDGES['Danny Green'+'Marco Belinelli'+'like'+'0'],
                 EDGES['Danny Green'+'Tim Duncan'+'like'+'0'],
                 EDGES['Danny Green'+'Cavaliers'+'serve'+'0'],
                 EDGES['Danny Green'+'Raptors'+'serve'+'0'],
                 EDGES['Danny Green'+'Spurs'+'serve'+'0'],

                 EDGES['LaMarcus Aldridge'+'Tim Duncan'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Tony Parker'+'like'+'0'],
                 EDGES['LaMarcus Aldridge'+'Spurs'+'serve'+'0'],
                 EDGES['LaMarcus Aldridge'+'Trail Blazers'+'serve'+'0'],

                 EDGES['Russell Westbrook'+'James Harden'+'like'+'0'],
                 EDGES['Russell Westbrook'+'Paul George'+'like'+'0'],
                 EDGES['Russell Westbrook'+'Thunders'+'serve'+'0']]

        vertex3 = [VERTEXS['LeBron James'],
                   VERTEXS['Kyle Anderson'],
                   VERTEXS['Marco Belinelli'],
                   VERTEXS['Paul George']]

        edge3 = [EDGES['LeBron James'+'Cavaliers'+'serve'+'0'],
                 EDGES['LeBron James'+'Cavaliers'+'serve'+'1'],
                 EDGES['Kyle Anderson'+'Spurs'+'serve'+'0'],

                 EDGES['Marco Belinelli'+'Danny Green'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tim Duncan'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Tony Parker'+'like'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Raptors'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'0'],
                 EDGES['Marco Belinelli'+'Hornets'+'serve'+'1'],
                 EDGES['Marco Belinelli'+'Spurs'+'serve'+'1'],

                 EDGES['Paul George'+'Russell Westbrook'+'like'+'0'],
                 EDGES['Paul George'+'Thunders'+'serve'+'0']]


        expected_data = {
            "column_names" : ["_vertices", "_edges"],
            "rows" : [
                [vertex1, edge1],
                [vertex2, edge2],
                [vertex3, edge3]
            ]
        }
        #import pdb; pdb.set_trace()
        self.check_column_names(resp, expected_data["column_names"])
        #self.check_out_of_order_result(resp, expected_data["rows"])

        stmt = "GET SUBGRAPH 3 STEPS FROM 'Paul George' OUT serve BOTH like"
        resp = self.execute_query(stmt)
        import pdb; pdb.set_trace()
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
