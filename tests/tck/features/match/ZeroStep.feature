# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@test
Feature: Variable length Pattern match (0 step)

  Background:
    Given a graph with space named "nba"

    Examples:
      | left_node                        | edge_dir_left | edge     | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | -             | [like*0] | -              | ()                               |
      | ()                               | -             | [like*0] | ->             | (v :player {name: "Tim Duncan"}) |

  Scenario Outline: raise invalid label semantic errors
    Given a graph with space named "<space_name>"
    When executing query:
      """
      FETCH PROP ON player <vid> YIELD name
      """
    Then a SemanticError should be raised at runtime: Invalid label identifiers: name

# match p= ()<-[*0]-(v :player {name: "Tim Duncan"}) return p
  # Scenario: single node right expand bi-direction edge without properties 0 step
  # Scenario: single node bi-directional edge with properties 0 step
  # Scenario: single node one-directional edge without properties 0 step
  # Scenario: single node one-directional edge with properties 0 step
  Scenario: return path of single node both direction edge with properties 0 step
    When executing query:
      """
      MATCH p = (v :player {name: "Tim Duncan"})-[like*0]->()
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |

  Scenario: return path of single node both direction edge with properties 0 step
    When executing query:
      """
      MATCH p = <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |

  Scenario: return path of single node both direction edge with properties 0 to N step
    When executing query:
      """
      MATCH p = (v :player {name: "Tim Duncan"})-[*0..1]->()
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                                                                                                                                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})>                                                                                                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2010}]->("Danny Green" :player{age: 31, name: "Danny Green"})>             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2015}]->("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})> |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2002}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>         |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:teammate@0 {end_year: 2016, start_year: 2001}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                 |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:like@0 {likeness: 95}]->("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                     |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})-[:serve@0 {end_year: 2016, start_year: 1997}]->("Spurs" :team{name: "Spurs"})>                                       |
