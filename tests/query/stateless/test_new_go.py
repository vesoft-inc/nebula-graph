# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from query.stateless.prepare_data import PrepareData


class TestGoQuery(PrepareData):
    def test_one_step(self):
        stmt = 'GO FROM "Tim Duncan" OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Boris Diaw" OVER serve YIELD \
            $^.player.name , serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Rajon Rondo" OVER serve WHERE \
            serve.start_year >= 2013 && serve.end_year <= 2018 YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Thunders' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_assignment_simple(self):
        stmt = '''$var = GO FROM "Tracy McGrady" OVER like YIELD like._dst as id; \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst"],
            rows : [
                []
            ]
        }

    def test_assignment_pipe(self):
        stmt = '''$var = (GO FROM "Tracy McGrady" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id); \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst"],
            rows : [
                []
            ]
        }

    def test_assignment_empty_result(self):
        stmt = '''$var = GO FROM "-1" OVER like YIELD like._dst as id; \
            GO FROM $var.id OVER like'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_variable_undefined(self):
        stmt = "GO FROM $var OVER like"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_distinct(self):
        stmt = '''GO FROM "Nobody" OVER serve \
            YIELD DISTINCT $^.player.name as name, $$.team.name as name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)

        stmt = '''GO FROM "Boris Diaw" OVER like YIELD like._dst as id \
            | GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve \
            YIELD DISTINCT serve._dst, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "$$.team.name"],
            rows : [
                []
            ]
        }

        stmt = 'GO 2 STEPS FROM "Tony Parker" OVER like YIELD DISTINCT like._dst'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_vertex_noexist(self):
        stmt = 'GO FROM "NON EXIST VERTEX ID" OVER serve'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD \
            $^.player.name, serve.start_year, serve.end_year, $$team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "NON EXIST VERTEX ID" OVER serve YIELD DISTINCT \
            $^.player.name, serve.start_year, serve.end_year, $$team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_multi_edges(self):
        stmt = "GO FROM 'Russell Westbrook' OVER serve, like"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Russell Westbrook" OVER serve, like REVERSELY \
            YIELD serve._dst, like._dst, serve._type, like._type'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Russell Westbrook" OVER serve, like REVERSELY YIELD serve._src, like._src'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Russell Westbrook" OVER serve, like REVERSELY'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Russell Westbrook" OVER * REVERSELY YIELD serve._src, like._src'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Russell Westbrook" OVER * REVERSELY'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Manu Ginobili" OVER like, teammate REVERSELY YIELD like.likeness, \
            teammate.start_year, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Russell Westbrook" OVER serve, like \
            YIELD serve.start_year, like.likeness, serve._type, like._type'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Shaquile O\'Neal" OVER serve, like'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Dirk Nowitzki" OVER * YIELD serve._dst, like._dst'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "Paul Gasol" OVER *'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = 'GO FROM "LaMarcus Aldridge" OVER * YIELD $$.team.name, $$.player.name'
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Boris Diaw" OVER like, serve YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM "Boris Diaw" OVER * YIELD like._dst as id \
            | ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id OVER serve )'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_reference_pipein_yieldandwhere(self):
        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            YIELD $-.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$-.name", "$^.player.name", "$$.player.name"],
            rows : [
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }

        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            WHERE $-.name != $$.player.name \
            YIELD $-.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$-.name", "$^.player.name", "$$.player.name"],
            rows : [
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }

        stmt = '''GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id \
            | GO FROM $-.id OVER like \
            YIELD $-.*, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$-.name", "$^.player.name", "$$.player.name"],
            rows : [
                []
            ]
        }

    def test_reference_variable_in_yieldandwhere(self):
        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            YIELD $var.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$var.name", "$^.player.name", "$$.player.name"],
            rows : [
                ["Tim Duncan", "Manu Ginobili", "Tim Duncan"],
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Tim Duncan", "Tony Parker", "Tim Duncan"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "Chris Paul"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "Chris Paul"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }

        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            WHERE $var.name != $$.player.name \
            YIELD $var.name, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$var.name", "$^.player.name", "$$.player.name"],
            rows : [
                ["Tim Duncan", "Tony Parker", "LaMarcus Aldridge"],
                ["Tim Duncan", "Tony Parker", "Manu Ginobili"],
                ["Chris Paul", "LeBron James", "Ray Allen"],
                ["Chris Paul", "Carmelo Anthony", "LeBron James"],
                ["Chris Paul", "Carmelo Anthony", "Dwyane Wade"],
                ["Chris Paul", "Dwyane Wade", "LeBron James"],
                ["Chris Paul", "Dwyane Wade", "Carmelo Anthony"]
            ]
        }

        stmt = '''$var = GO FROM 'Tim Duncan', 'Chris Paul' OVER like \
            YIELD $^.player.name AS name, like._dst AS id; \
            GO FROM $var.id OVER like \
            YIELD $var.*, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$var.name", "$^.player.name", "$$.player.name"],
            rows : [
                []
            ]
        }

    def test_no_existent_prop(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD $^.player.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = "GO FROM 'Tim Duncan' OVER serve yield $^.player.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD serve.test"
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_is_incall(self):
        stmt = '''GO FROM 'Boris Diaw' OVER serve \
            WHERE udf_is_in($$.team.name, 'Hawks', 'Suns') \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.name"],
            rows : [
                []
            ]
        }