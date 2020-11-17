# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.usefixtures('set_vertices_and_edges')
class TestVariableLengthRelationshipMatch(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        cls.use_nba()

    @pytest.mark.skip
    def test_to_be_deleted(self):
        # variable steps
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..3]-> () return *'
        self.fail_query(stmt)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*..3]-> () return *'
        self.fail_query(stmt)
        stmt = 'MATCH (v:player:{name: "abc"}) -[r*1..]-> () return *'
        self.fail_query(stmt)

    @pytest.mark.skip
    def test_hops_0_to_1(self):
        VERTICES, EDGES = self.VERTEXS, self.EDGS

        def _row(src: str, etype: str, dst: str, rank: int = 0):
            return [[EDGES[src+dst+etype+str(rank)]], VERTICES[dst]]

        def like_row(dst: str):
            return _row('Tracy McGrady', 'like', dst, 0)

        def serve_row(dst):
            return _row('Tracy McGrady', 'serve', dst, 0)

        # single both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic")
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # single both direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:like*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),  # like each other
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{start_year: 2000}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                serve_row("Magic"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                like_row("Vince Carter"),
                like_row("Yao Ming"),
                like_row("Grant Hill"),
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

        # multiple single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tracy McGrady"})-[e:serve|like*0..1]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                [[], VERTICES["Tracy McGrady"]],
                like_row("Kobe Bryant"),
                like_row("Grant Hill"),
                like_row("Rudy Gay"),
                serve_row("Raptors"),
                serve_row("Magic"),
                serve_row("Spurs"),
                serve_row("Rockets"),
            ]
        }
        self.check_rows_with_header(stmt, expected)

    def test_hops_m_to_n(self):
        VERTICES, EDGES = self.VERTEXS, self.EDGES

        def like(src: str, dst: str):
            return EDGES[src+dst+'like'+str(0)]

        def serve(src: str, dst: str):
            return EDGES[src+dst+'serve'+str(0)]

        def like_row_2hop(dst1: str, dst2: str):
            return [[like('Tim Duncan', dst1), like(dst1, dst2)], VERTICES[dst2]]

        def like_row_3hop(dst1: str, dst2: str, dst3: str):
            return [[like('Tim Duncan', dst1), like(dst1, dst2), like(dst2, dst3)],
                    VERTICES[dst3]]

        def serve_row_2hop(dst1, dst2):
            return [[serve('Tim Duncan', dst1), serve(dst1, dst2)], VERTICES[dst2]]

        def serve_row_3hop(dst1, dst2, dst3):
            return [
                [serve('Tim Duncan', dst1), serve(dst1, dst2), serve(dst2, dst3)],
                VERTICES[dst3]
            ]

        # single both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [like_row_2hop("Manu Ginobili", "Tiago Splitter")],
        }
        self.check_rows_with_header(stmt, expected)

        # single direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3{start_year: 2000}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})<-[e:like*2..3{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [like_row_2hop("Manu Ginobili", "Tiago Splitter")],
        }
        self.check_rows_with_header(stmt, expected)

        # single both direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                serve_row_2hop("Spurs", "Dejounte Murray"),
                serve_row_2hop("Spurs", "Marco Belinelli"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Bulls"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hawks"),
                serve_row_3hop("Spurs", "Marco Belinelli", "76ers"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Spurs"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Raptors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Warriors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Kings"),
                serve_row_2hop("Spurs", "Danny Green"),
                serve_row_3hop("Spurs", "Danny Green", "Cavaliers"),
                serve_row_3hop("Spurs", "Danny Green", "Raptors"),
                serve_row_2hop("Spurs", "Aron Baynes"),
                serve_row_3hop("Spurs", "Aron Baynes", "Pistons"),
                serve_row_3hop("Spurs", "Aron Baynes", "Celtics"),
                serve_row_2hop("Spurs", "Jonathon Simmons"),
                serve_row_3hop("Spurs", "Jonathon Simmons", "76ers"),
                serve_row_3hop("Spurs", "Jonathon Simmons", "Magic"),
                serve_row_2hop("Spurs", "Rudy Gay"),
                serve_row_3hop("Spurs", "Rudy Gay", "Raptors"),
                serve_row_3hop("Spurs", "Rudy Gay", "Kings"),
                serve_row_3hop("Spurs", "Rudy Gay", "Grizzlies"),
                serve_row_2hop("Spurs", "Tony Parker"),
                serve_row_3hop("Spurs", "Tony Parker", "Hornets"),
                serve_row_2hop("Spurs", "Manu Ginobili"),
                serve_row_2hop("Spurs", "David West"),
                serve_row_3hop("Spurs", "David West", "Pacers"),
                serve_row_3hop("Spurs", "David West", "Warriors"),
                serve_row_3hop("Spurs", "David West", "Hornets"),
                serve_row_2hop("Spurs", "Tracy McGrady"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Raptors"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Magic"),
                serve_row_3hop("Spurs", "Tracy McGrady", "Rockets"),
                serve_row_2hop("Spurs", "Marco Belinelli"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Bulls"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Spurs"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hawks"),
                serve_row_3hop("Spurs", "Marco Belinelli", "76ers"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Hornets"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Raptors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Warriors"),
                serve_row_3hop("Spurs", "Marco Belinelli", "Kings"),
                serve_row_2hop("Spurs", "Paul Gasol"),
                serve_row_3hop("Spurs", "Paul Gasol", "Lakers"),
                serve_row_3hop("Spurs", "Paul Gasol", "Bulls"),
                serve_row_3hop("Spurs", "Paul Gasol", "Grizzlies"),
                serve_row_3hop("Spurs", "Paul Gasol", "Bucks"),
                serve_row_2hop("Spurs", "LaMarcus Aldridge"),
                serve_row_3hop("Spurs", "LaMarcus Aldridge", "Trail Blazers"),
                serve_row_2hop("Spurs", "Tiago Splitter"),
                serve_row_3hop("Spurs", "Tiago Splitter", "Hawks"),
                serve_row_3hop("Spurs", "Tiago Splitter", "76ers"),
                serve_row_2hop("Spurs", "Cory Joseph"),
                serve_row_3hop("Spurs", "Cory Joseph", "Pacers"),
                serve_row_3hop("Spurs", "Cory Joseph", "Raptors"),
                serve_row_2hop("Spurs", "Kyle Anderson"),
                serve_row_3hop("Spurs", "Kyle Anderson", "Grizzlies"),
                serve_row_2hop("Spurs", "Boris Diaw"),
                serve_row_3hop("Spurs", "Boris Diaw", "Suns"),
                serve_row_3hop("Spurs", "Boris Diaw", "Jazz"),
                serve_row_3hop("Spurs", "Boris Diaw", "Hawks"),
                serve_row_3hop("Spurs", "Boris Diaw", "Hornets"),
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # stmt = '''
        # MATCH (:player{name:"Tim Duncan"})-[e:like*2..3]-(v)
        # RETURN count(*)
        # '''
        # expected = {
        #     "column_names": ['count(*)'],
        #     "rows": [292],
        # }
        # self.check_rows_with_header(stmt, expected)

        # single direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve*2..3]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row_2hop("Tony Parker", "Tim Duncan"),
                like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili"),
                like_row_2hop("Tony Parker", "Manu Ginobili"),
                like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan"),
                like_row_2hop("Tony Parker", "LaMarcus Aldridge"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan"),
                like_row_2hop("Manu Ginobili", "Tim Duncan"),
                like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker"),
            ],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [like_row_2hop("Manu Ginobili", "Tiago Splitter")],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{start_year: 2000}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple direction edge with properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3{likeness: 90}]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [],
        }
        self.check_rows_with_header(stmt, expected)

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})<-[e:serve|like*2..3{likeness: 90}]-(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [like_row_2hop("Manu Ginobili", "Tiago Splitter")],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple both direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3]-(v)
        RETURN count(*)
        '''
        expected = {
            "column_names": ['COUNT(*)'],
            "rows": [927],
        }
        self.check_rows_with_header(stmt, expected)

        # multiple direction edge without properties
        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:serve|like*2..3]->(v)
        RETURN e, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": [
                like_row_2hop("Tony Parker", "Tim Duncan"),
                like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "Tim Duncan"),
                        serve("Tim Duncan", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Tony Parker", "Manu Ginobili"),
                like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "Manu Ginobili"),
                        serve("Manu Ginobili", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Tony Parker", "LaMarcus Aldridge"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan"),
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "LaMarcus Aldridge"),
                        serve("LaMarcus Aldridge", "Trail Blazers"),
                    ],
                    VERTICES['Trail Blazers'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        like("Tony Parker", "LaMarcus Aldridge"),
                        serve("LaMarcus Aldridge", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        serve("Tony Parker", "Hornets"),
                    ],
                    VERTICES['Hornets'],
                ],
                [
                    [
                        like("Tim Duncan", "Tony Parker"),
                        serve("Tony Parker", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                like_row_2hop("Manu Ginobili", "Tim Duncan"),
                like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker"),
                [
                    [
                        like("Tim Duncan", "Manu Ginobili"),
                        like("Manu Ginobili", "Tim Duncan"),
                        serve("Tim Duncan", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
                [
                    [
                        like("Tim Duncan", "Manu Ginobili"),
                        serve("Manu Ginobili", "Spurs"),
                    ],
                    VERTICES['Spurs'],
                ],
            ],
        }
        self.check_rows_with_header(stmt, expected)

    @pytest.mark.skip
    def test_mix_hops(self):
        stmt = '''
        MATCH (:player{name: "Tim Duncan"})-[e1:like]->()-[e2:serve*0..3]->()<-[e3:serve]-(v)
        RETURN e1, e2, e3, v
        '''
        expected = {
            "column_names": ['e', 'v'],
            "rows": []
        }
        self.check_rows_with_header(stmt, expected)

    def test_return_all(self):
        EDGES = self.EDGES

        def like(src: str, dst: str):
            return EDGES[src+dst+'like'+str(0)]

        def like_row_2hop(dst1: str, dst2: str):
            return [like('Tim Duncan', dst1), like(dst1, dst2)]

        def like_row_3hop(dst1: str, dst2: str, dst3: str):
            return [like('Tim Duncan', dst1), like(dst1, dst2), like(dst2, dst3)]

        stmt = '''
        MATCH (:player{name:"Tim Duncan"})-[e:like*2..3]->()
        RETURN *
        '''
        expected = {
            "column_names": ['e'],
            "rows": [
                like_row_2hop("Tony Parker", "Tim Duncan"),
                like_row_3hop("Tony Parker", "Tim Duncan", "Manu Ginobili"),
                like_row_2hop("Tony Parker", "Manu Ginobili"),
                like_row_3hop("Tony Parker", "Manu Ginobili", "Tim Duncan"),
                like_row_2hop("Tony Parker", "LaMarcus Aldridge"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tony Parker"),
                like_row_3hop("Tony Parker", "LaMarcus Aldridge", "Tim Duncan"),
                like_row_2hop("Manu Ginobili", "Tim Duncan"),
                like_row_3hop("Manu Ginobili", "Tim Duncan", "Tony Parker"),
            ],
        }
        self.check_rows_with_header(stmt, expected)

    def test_more_cases(self):
        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*0]-()
        RETURN e
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*1]-()
        RETURN e
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()
        RETURN e
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*1..1]-()
        RETURN e
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*]-()
        RETURN e
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*0..0]-()-[e2:like*0..0]-()
        RETURN e, e2
        '''

        stmt = '''
        MATCH (v:player{name: 'Tim Duncan'})-[e:like*2..3]-()
        WHERE e[0].likeness > 80
        RETURN e
        '''
