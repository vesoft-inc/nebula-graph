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

        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id \
            | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123)'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id \
            | GO FROM  $-.id OVER serve WHERE udf_is_in($-.id, 'Tony Parker', 123) && 1 == 1'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

    def test_return_test(self):
        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $rA IF $rA IS NOT NULL; \
            GO FROM $A.dst OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $rA IF $rA IS NOT NULL; \
            GO FROM $A.dst OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$A.dst"],
            rows : [
                []
            ]
        }

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dstA; \
            $rA = YIELD $A.* WHERE $A.dstA == 123; \
            RETURN $rA IF $rA IS NOT NULL; \
            $B = GO FROM $A.dstA OVER like YIELD like._dst AS dstB; \
            $rB = YIELD $B.* WHERE $B.dstB == 456; \
            RETURN $rB IF $rB IS NOT NULL; \
            GO FROM $B.dstB OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $rA IF $rA IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE 1 == 1; \
            RETURN $rA IF $rA IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$A.dst"],
            rows : [
                []
            ]
        }

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $B IF $B IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        stmt = '''$A = GO FROM 'Tim Duncan' OVER like YIELD like._dst AS dst; \
            $rA = YIELD $A.* WHERE $A.dst == 123; \
            RETURN $B IF $A IS NOT NULL'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

        # will return error
        stmt = '''RETURN $rA IF $rA IS NOT NULL;'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)

    def test_reversely_one_step(self):
        stmt = "GO FROM 'Tim Duncan' OVER like REVERSELY YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER * REVERSELY YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER like REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Tim Duncan' OVER like REVERSELY \
            WHERE $$.player.age < 35 \
            YIELD $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_only_id_two_steps(self):
        stmt = "GO 2 STEPS FROM 'Tony Parker' OVER like YIELD like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_reversely_two_steps(self):
        stmt = "GO 2 STEPS FROM 'Kobe Bryant' OVER like REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 2 STEPS FROM 'Kobe Bryant' OVER * REVERSELY YIELD $$.player.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_reversely_with_pipe(self):
        stmt = '''GO FROM 'LeBron James' OVER serve YIELD serve._dst AS id \
            | GO FROM $-.id OVER serve REVERSELY YIELD $^.team.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.team.name", "$$.player.name"],
            rows : [
                ["Cavaliers", "Kyrie Irving"],
                ["Cavaliers", "Dwyane Wade"],
                ["Cavaliers", "Shaquile O'Neal"],
                ["Cavaliers", "Danny Green"],
                ["Cavaliers", "LeBron James"],
                ["Heat", "Dwyane Wade"],
                ["Heat", "LeBron James"],
                ["Heat", "Ray Allen"],
                ["Heat", "Shaquile O'Neal"],
                ["Heat", "Amar'e Stoudemire"],
                ["Lakers", "Kobe Bryant"],
                ["Lakers", "LeBron James"],
                ["Lakers", "Rajon Rondo"],
                ["Lakers", "Steve Nash"],
                ["Lakers", "Paul Gasol"],
                ["Lakers", "Shaquile O'Neal"],
                ["Lakers", "JaVale McGee"],
                ["Lakers", "Dwight Howard"]
            ]
        }

        stmt = '''GO FROM 'LeBron James' OVER serve YIELD serve._dst AS id \
            | GO FROM $-.id OVER serve REVERSELY WHERE $$.player.name != 'LeBron James' \
            YIELD $^.team.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.team.name", "$$.player.name"],
            rows : [
                ["Cavaliers", "Kyrie Irving"],
                ["Cavaliers", "Dwyane Wade"],
                ["Cavaliers", "Shaquile O'Neal"],
                ["Cavaliers", "Danny Green"],
                ["Heat", "Dwyane Wade"],
                ["Heat", "Ray Allen"],
                ["Heat", "Shaquile O'Neal"],
                ["Heat", "Amar'e Stoudemire"],
                ["Lakers", "Kobe Bryant"],
                ["Lakers", "Rajon Rondo"],
                ["Lakers", "Steve Nash"],
                ["Lakers", "Paul Gasol"],
                ["Lakers", "Shaquile O'Neal"],
                ["Lakers", "JaVale McGee"],
                ["Lakers", "Dwight Howard"]
            ]
        }

        stmt = '''GO FROM 'Manu Ginobili' OVER like REVERSELY YIELD like._dst AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Manu Ginobili' OVER * REVERSELY YIELD like._dst AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_bidirect(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER like bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER serve, like bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER * bidirect"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst", "teammate._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER serve bidirect YIELD $$.team.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$$.team.name"],
            rows : [
                ["Spurs"]
            ]
        }

        stmt = "GO FROM 'Tim Duncan' OVER like bidirect YIELD $$.team.name"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$$.player.name"],
            rows : [
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["Tony Parker"],
                ["Manu Ginobili"],
                ["LaMarcus Aldridge"],
                ["Marco Belinelli"],
                ["Danny Green"],
                ["Aron Baynes"],
                ["Boris Diaw"],
                ["Tiago Splitter"],
                ["Dejounte Murray"],
                ["Shaquile O'Neal"]
            ]
        }

        stmt = '''GO FROM 'Tim Duncan' OVER like bidirect WHERE like.likeness > 90 \
            YIELD $^.player.name, like._dst, $$.player.name, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "like._dst", "$$.player.name", "like.likeness"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Tim Duncan' OVER * bidirect \
            YIELD $^.player.name, serve._dst, $$.team.name, like._dst, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve._dst", "$$.team.name", "like._dst", "$$.player.name"],
            rows : [
                []
            ]
        }

    def test_filter_push_down(self):
        # 1550-2249
        pass

    def test_duplicate_column_name(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD serve._dst, serve._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "serve._dst"],
            rows : [
                []
            ]
        }
    
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._dst AS id, like.likeness AS id \
            | GO FROM $-.id OVER serve'''
        resp = self.execute_query(stmt)
        self.check_resp_failed(resp)


    def test_contains(self):
        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE $$.team.name CONTAINS Haw\
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.nam"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE (string)serve.start_year CONTAINS "05" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.nam"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE $^.player.name CONTAINS "Boris" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.nam"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE !($^.player.name CONTAINS "Boris") \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.nam"],
            rows : [
                []
            ]
        }

        stmt = '''GO FROM 'Boris Diaw' OVER serve WHERE "Leo" CONTAINS "Boris" \
            YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$^.player.name", "serve.start_year", "serve.end_year", "$$.team.nam"],
            rows : [
                []
            ]
        }


    def test_with_intermediate_data(self):
        # zero to zero
        stmt = "GO 0 TO 0 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        # simple
        stmt = "GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst"],
            rows : [
                []
            ]
        }

        stmt = '''GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like \
            YIELD DISTINCT like._dst, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst", "like.likeness", "$$.player.name"],
            rows : [
                []
            ]
        }

        stmt = '''GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like \
            YIELD DISTINCT like._dst, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["like._dst", "like.likeness", "$$.player.name"],
            rows : [
                []
            ]
        }

        stmt = "GO 1 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 2 TO 3 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        #reversely
        stmt = "GO 1 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 2 TO 2 STEPS FROM 'Tony Parker' OVER like REVERSELY YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        # empty starts before last step
        stmt = "GO 1 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 3 STEPS FROM 'Spurs' OVER serve REVERSELY"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        # bidirectionally
        stmt = "GO 1 TO 2 STEPS FROM 'Spurs' OVER like BIDIRECT YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 2 STEPS FROM 'Tony Parker' OVER like BIDIRECT YIELD DISTINCT like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

        # over
        stmt = "GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD DISTINCT serve._dst, like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

        stmt = "GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * YIELD DISTINCT serve._dst, like._dst"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

        # with properties
        stmt = '''GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst", "serve.start_year", "like.likeness", "$$.player.name"],
            rows : [
                []
            ]
        }

        stmt = '''GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            YIELD serve._dst, like._dst, serve.start_year, like.likeness, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst", "serve.start_year", "like.likeness", "$$.player.name"],
            rows : [
                []
            ]
        }

        stmt = '''GO 1 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            REVERSELY YIELD serve._dst, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

        stmt = '''GO 0 TO 2 STEPS FROM 'Russell Westbrook' OVER * \
            REVERSELY YIELD serve._dst, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

    def test_error_massage(self):
        stmt = "GO FROM 'Tim Duncan' OVER serve YIELD $$.player.name as name";
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : [],
            rows : [
                []
            ]
        }

    def test_zero_step(self):
        stmt = "GO 0 STEPS FROM 'Tim Duncan' OVER serve BIDIRECT"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

        stmt = "GO 0 STEPS FROM 'Tim Duncan' OVER serve"
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        self.check_empty_result(resp)

    def test_go_cover_input(self):
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO FROM $-.src OVER like \
            YIELD $-.src as src, like._dst as dst, $^.player.name, $$.player.name'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["serve._dst", "like._dst"],
            rows : [
                []
            ]
        }

        stmt = '''$a = GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst; \
            GO FROM $a.src OVER like YIELD $a.src as src, like._dst as dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["src", "dst"],
            rows : [
                []
            ]
        }

        # with intermidate data pipe
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, like._dst as dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["src", "dst"],
            rows : [
                []
            ]
        }

        # var with properties
        stmt = '''GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as dst \
            | GO 1 TO 2 STEPS FROM $-.src OVER like \
            YIELD $-.src as src, $-.dst, like._dst as dst, like.likeness'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["src", "$-.dst", "dst", "like.likeness"],
            rows : [
                []
            ]
        }

        # partial neighbors input
        stmt = '''GO FROM 'Danny Green' OVER like YIELD like._src AS src, like._dst AS dst \
            | GO FROM $-.dst OVER teammate YIELD $-.src AS src, $-.dst, teammate._dst AS dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["src", "$-.dst", "dst"],
            rows : [
                []
            ]
        }

        # var
        stmt = '''$a = GO FROM 'Danny Green' OVER like YIELD like._src AS src, like._dst AS dst; \
            GO FROM $a.dst OVER teammate YIELD $a.src AS src, $a.dst, teammate._dst AS dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["src", "$a.dst", "dst"],
            rows : [
                []
            ]
        }

    def test_backtrack_overlap(self):
        stmt = '''GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst \
            | GO 2 STEPS FROM $-.src OVER like YIELD $-.src, $-.dst, like._src, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$-.src", "$-.dst", "like._src", "like._dst"],
            rows : [
                []
            ]
        }

        stmt = '''$a = GO FROM 'Tony Parker' OVER like YIELD like._src as src, like._dst as dst; \
            GO 2 STEPS FROM $a.src OVER like YIELD $a.src, $a.dst, like._src, like._dst'''
        resp = self.execute_query(stmt)
        self.check_resp_succeeded(resp)
        expected_data = {
            column_names : ["$a.src", "$a.dst", "like._src", "like._dst"],
            rows : [
                []
            ]
        }
