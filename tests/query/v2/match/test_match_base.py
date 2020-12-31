# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import functools
import pytest

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.path_value import PathVal


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestMatchBase(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.use_nba()

    def test_single_node(self):
        VERTICES = self.VERTEXS
        stmt = 'MATCH (v:player {name: "Yao Ming"}) RETURN v'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['v'],
            "rows": [
                [VERTICES['Yao Ming']]
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player) WHERE v.name == "Yao Ming" RETURN v.age AS Age'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Age'],
            "rows": [
                [38]
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player {age: 29}) return v.name AS Name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name'],
            "rows": [
                ['James Harden'],
                ['Jonathon Simmons'],
                ['Klay Thompson'],
                ['Dejounte Murray']
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player {age: 29}) WHERE v.name STARTS WITH "J" return v.name AS Name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name'],
            "rows": [
                ['James Harden'],
                ['Jonathon Simmons'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v:player) WHERE v.age >= 38 AND v.age < 45 return v.name AS Name, v.age AS Age'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            "column_names": ['Name', 'Age'],
            "rows": [
                ['Paul Gasol', 38],
                ['Kobe Bryant', 40],
                ['Vince Carter', 42],
                ['Tim Duncan', 42],
                ['Yao Ming', 38],
                ['Dirk Nowitzki', 40],
                ['Manu Ginobili', 41],
                ['Ray Allen', 43],
                ['David West', 38],
                ['Tracy McGrady', 39],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_one_step(self):
        stmt = 'MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2) RETURN type(r) AS Type, v2.name AS Name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['like', 'Ray Allen'],
                ['serve', 'Cavaliers'],
                ['serve', 'Heat'],
                ['serve', 'Cavaliers'],
                ['serve', 'Lakers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = 'MATCH (v1:player{name: "LeBron James"}) -[r:serve|:like]-> (v2) RETURN type(r) AS Type, v2.name AS Name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['like', 'Ray Allen'],
                ['serve', 'Cavaliers'],
                ['serve', 'Heat'],
                ['serve', 'Cavaliers'],
                ['serve', 'Lakers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2)
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['serve', 'Cavaliers'],
                ['serve', 'Heat'],
                ['serve', 'Cavaliers'],
                ['serve', 'Lakers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
                  RETURN type(r) AS Type, v2.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Type', 'Name'],
            'rows': [
                ['serve', 'Cavaliers'],
                ['serve', 'Cavaliers'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "LeBron James"}) -[r:serve]-> (v2 {name: "Cavaliers"})
                  WHERE r.start_year <= 2005 AND r.end_year >= 2005
                  RETURN r.start_year AS Start_Year, r.end_year AS Start_Year
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Start_Year', 'Start_Year'],
            'rows': [
                [2003, 2010],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "Danny Green"}) -[:like]-> (v2)
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }

        stmt = '''
                  MATCH (v1:player{name: "Danny Green"}) <-[:like]- (v2)
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
            ]
        }

        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "Danny Green"}) <-[:like]-> (v2)
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: "Danny Green"}) -[:like]- (v2)
                  RETURN v1.name AS Name, v2.name AS Friend
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Friend'],
            'rows': [
                ['Danny Green', 'Dejounte Murray'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'LeBron James'],
                ['Danny Green', 'Marco Belinelli'],
                ['Danny Green', 'Tim Duncan'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_two_steps(self):
        stmt = '''
                  MATCH (v1:player{age: 28}) -[:like]-> (v2) -[:like]-> (v3)
                  RETURN v1.name AS Player, v2.name AS Friend, v3.name AS FoF
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Player', 'Friend', 'FoF'],
            'rows': [
                ['Paul George', 'Russell Westbrook', 'James Harden'],
                ['Paul George', 'Russell Westbrook', 'Paul George'],
                ['Damian Lillard', 'LaMarcus Aldridge', 'Tim Duncan'],
                ['Damian Lillard', 'LaMarcus Aldridge', 'Tony Parker']
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (v1:player{name: 'Tony Parker'}) -[r1:serve]-> (v2) <-[r2:serve]- (v3)
                  WHERE r1.start_year <= r2.end_year AND
                        r1.end_year >= r2.start_year AND
                        v1.name <> v3.name AND
                        v3.name STARTS WITH 'D'
                  RETURN v1.name AS Player, v2.name AS Team, v3.name AS Teammate
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Player', 'Team', 'Teammate'],
            'rows': [
                ['Tony Parker', 'Hornets', 'Dwight Howard'],
                ['Tony Parker', 'Spurs', 'Danny Green'],
                ['Tony Parker', 'Spurs', 'Dejounte Murray'],
                ['Tony Parker', 'Spurs', 'David West'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_distinct(self):
        stmt = '''
                  MATCH (:player{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3)
                  RETURN v3.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name'],
            'rows': [
                ['Carmelo Anthony'],
                ['Dwyane Wade'],
                ['Dwyane Wade'],
                ['LeBron James'],
                ['LeBron James'],
                ['Chris Paul'],
                ['Ray Allen'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        stmt = '''
                  MATCH (:player{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3)
                  RETURN DISTINCT v3.name AS Name
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name'],
            'rows': [
                ['Carmelo Anthony'],
                ['Dwyane Wade'],
                ['LeBron James'],
                ['Chris Paul'],
                ['Ray Allen'],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

    def test_order_skip_limit(self):
        # ORDER BY
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['Tim Duncan', 42],
                ['Manu Ginobili', 41],
                ['Tony Parker', 36],
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
                ['Danny Green', 31],
                ['Kevin Durant', 30],
                ['Russell Westbrook', 30],
                ['James Harden', 29],
                ['Kyle Anderson', 25],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY LIMIT
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['Tim Duncan', 42],
                ['Manu Ginobili', 41],
                ['Tony Parker', 36],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY SKIP
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
                ['Danny Green', 31],
                ['Kevin Durant', 30],
                ['Russell Westbrook', 30],
                ['James Harden', 29],
                ['Kyle Anderson', 25],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY SKIP LIMIT
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 3
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': [
                ['LeBron James', 34],
                ['Chris Paul', 33],
                ['Marco Belinelli', 32],
            ]
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])
        # SKIP all rows
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  SKIP 11
                  LIMIT 3
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': []
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # LIMIT 0
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY Age DESC, Name ASC
                  LIMIT 0
               '''
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        expected = {
            'column_names': ['Name', 'Age'],
            'rows': []
        }
        self.check_column_names(resp, expected['column_names'])
        self.check_out_of_order_result(resp, expected['rows'])

        # ORDER BY expr
        stmt = '''
                  MATCH (:player{name:'Dejounte Murray'}) -[:like]-> (v)
                  RETURN v.name AS Name, v.age AS Age
                  ORDER BY v.age DESC, v.name ASC
               '''
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

    def test_return_path(self):
        VERTICES, EDGES = self.VERTEXS, self.EDGES

        stmt = 'MATCH p = (n:player{name:"Tony Parker"}) return p,n'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp);
        columns_name = ['p', 'n']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["Tony Parker"]]), VERTICES["Tony Parker"]]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH p = (n:player{name:"LeBron James"})-[:like]->(m) return p, n.name, m.name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"]]),
                "LeBron James", "Ray Allen"]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m) return p, n.name, m.name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dejounte Murray"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dejounte Murray"]]),
                "LeBron James", "Dejounte Murray"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Carmelo Anthony"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Carmelo Anthony"]]),
                "LeBron James", "Carmelo Anthony"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Kyrie Irving"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Kyrie Irving"]]),
                "LeBron James", "Kyrie Irving"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dwyane Wade"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dwyane Wade"]]),
                "LeBron James", "Dwyane Wade"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Danny Green"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Danny Green"]]),
                "LeBron James", "Danny Green"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Chris Paul"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Chris Paul"]]),
                "LeBron James", "Chris Paul"],
            ]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH p = (n:player{name:"LeBron James"})-[:like]-(m) return p, n.name, m.name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"]]),
                "LeBron James", "Ray Allen"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dejounte Murray"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dejounte Murray"]]),
                "LeBron James", "Dejounte Murray"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Carmelo Anthony"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Carmelo Anthony"]]),
                "LeBron James", "Carmelo Anthony"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Kyrie Irving"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Kyrie Irving"]]),
                "LeBron James", "Kyrie Irving"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Dwyane Wade"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Dwyane Wade"]]),
                "LeBron James", "Dwyane Wade"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Danny Green"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Danny Green"]]),
                "LeBron James", "Danny Green"],
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["Chris Paul"+"LeBron James"+"like"+"0"], -1),
                    VERTICES["Chris Paul"]]),
                "LeBron James", "Chris Paul"],
            ]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)-[:like]->(k) return p, n.name, m.name, k.name'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p', 'n.name', 'm.name', 'k.name']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"],
                    (EDGES["Ray Allen"+"Rajon Rondo"+"like"+"0"], 1),
                    VERTICES["Rajon Rondo"]]),
                "LeBron James", "Ray Allen", "Rajon Rondo"]
            ]
        self.check_out_of_order_result(resp, result)

        stmt = 'MATCH p=(:player{name:"LeBron James"})-[:like]->()-[:like]->() RETURN *'
        resp = self.execute(stmt)
        self.check_resp_succeeded(resp)
        columns_name = ['p']
        self.check_column_names(resp, columns_name)
        result = [
            [PathVal([VERTICES["LeBron James"],
                    (EDGES["LeBron James"+"Ray Allen"+"like"+"0"], 1),
                    VERTICES["Ray Allen"],
                    (EDGES["Ray Allen"+"Rajon Rondo"+"like"+"0"], 1),
                    VERTICES["Rajon Rondo"]])]
            ]
        self.check_out_of_order_result(resp, result)

    def test_failures(self):
        # No RETURN
        stmt = 'MATCH (v:player{name:"abc")'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

    def test_unimplemented_features(self):
        # No label
        stmt = 'MATCH (v) return v'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

        # Scan by label
        # stmt = 'MATCH (v:player) return v'
        # resp = self.execute(stmt)
        # self.check_resp_failed(resp)

        # Scan by label
        stmt = 'MATCH (v:player:person) return v'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

        # start from edge
        stmt = 'MATCH () -[r:serve]-> () return *'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

        # start from tail
        stmt = 'MATCH () -[]-> (v) return *'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

        # start from middle
        stmt = 'MATCH () --> (v) --> () return *'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

        # multiple steps
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*2]-> () return *'
        resp = self.execute(stmt)
        self.check_resp_failed(resp)

