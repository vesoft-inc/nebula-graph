# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@test
Feature: Variable length Pattern match (0 step)

  Background:
    Given a graph with space named "nba"

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

  # match p= ()-[*0]-(v :player {name: "Tim Duncan"}) return p
  Scenario Outline: single node with all bi-direction edges without properties 0 step, return path
    When executing query:
      """
      MATCH p= ()-[*0]-(v :player) 
      RETURN p
      """
    Then the result should be, in any order:
      | p                                                                                                             |
      | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
      | <("Ricky Rubio" :player{age: 28, name: "Ricky Rubio"})>                                                       |
      | <("Giannis Antetokounmpo" :player{age: 24, name: "Giannis Antetokounmpo"})>                                   |
      | <("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})>                                             |
      | <("Damian Lillard" :player{age: 28, name: "Damian Lillard"})>                                                 |
      | <("Paul Gasol" :player{age: 38, name: "Paul Gasol"})>                                                         |
      | <("Rudy Gay" :player{age: 32, name: "Rudy Gay"})>                                                             |
      | <("Cory Joseph" :player{age: 27, name: "Cory Joseph"})>                                                       |
      | <("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})>                                               |
      | <("Yao Ming" :player{age: 38, name: "Yao Ming"})>                                                             |
      | <("Klay Thompson" :player{age: 29, name: "Klay Thompson"})>                                                   |
      | <("Chris Paul" :player{age: 33, name: "Chris Paul"})>                                                         |
      | <("Kristaps Porzingis" :player{age: 23, name: "Kristaps Porzingis"})>                                         |
      | <("Aron Baynes" :player{age: 32, name: "Aron Baynes"})>                                                       |
      | <("Steve Nash" :player{age: 45, name: "Steve Nash"})>                                                         |
      | <("Carmelo Anthony" :player{age: 34, name: "Carmelo Anthony"})>                                               |
      | <("Ray Allen" :player{age: 43, name: "Ray Allen"})>                                                           |
      | <("Russell Westbrook" :player{age: 30, name: "Russell Westbrook"})>                                           |
      | <("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})>                                                 |
      | <("Kevin Durant" :player{age: 30, name: "Kevin Durant"})>                                                     |
      | <("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})>                                                   |
      | <("Stephen Curry" :player{age: 31, name: "Stephen Curry"})>                                                   |
      | <("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})>                                                   |
      | <("Grant Hill" :player{age: 46, name: "Grant Hill"})>                                                         |
      | <("Marc Gasol" :player{age: 34, name: "Marc Gasol"})>                                                         |
      | <("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})>                                               |
      | <("Jason Kidd" :player{age: 45, name: "Jason Kidd"})>                                                         |
      | <("Vince Carter" :player{age: 42, name: "Vince Carter"})>                                                     |
      | <("Amar'e Stoudemire" :player{age: 36, name: "Amar'e Stoudemire"})>                                           |
      | <("LeBron James" :player{age: 34, name: "LeBron James"})>                                                     |
      | <("Boris Diaw" :player{age: 36, name: "Boris Diaw"})>                                                         |
      | <("DeAndre Jordan" :player{age: 30, name: "DeAndre Jordan"})>                                                 |
      | <("Nobody" :player{age: 0, name: "Nobody"})>                                                                  |
      | <("James Harden" :player{age: 29, name: "James Harden"})>                                                     |
      | <("David West" :player{age: 38, name: "David West"})>                                                         |
      | <("Paul George" :player{age: 28, name: "Paul George"})>                                                       |
      | <("Joel Embiid" :player{age: 25, name: "Joel Embiid"})>                                                       |
      | <("Tony Parker" :player{age: 36, name: "Tony Parker"})>                                                       |
      | <("Danny Green" :player{age: 31, name: "Danny Green"})>                                                       |
      | <("Blake Griffin" :player{age: 30, name: "Blake Griffin"})>                                                   |
      | <("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})>                                                   |
      | <("Ben Simmons" :player{age: 22, name: "Ben Simmons"})>                                                       |
      | <("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})>                                           |
      | <("JaVale McGee" :player{age: 31, name: "JaVale McGee"})>                                                     |
      | <("Dwight Howard" :player{age: 33, name: "Dwight Howard"})>                                                   |
      | <("Kyrie Irving" :player{age: 26, name: "Kyrie Irving"})>                                                     |
      | <("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})>                                               |
      | <("Luka Doncic" :player{age: 20, name: "Luka Doncic"})>                                                       |
      | <("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})>                                                   |
      | <("Rajon Rondo" :player{age: 33, name: "Rajon Rondo"})>                                                       |
      | <("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})>                                                       |
      | <("Dwyane Wade" :player{age: 37, name: "Dwyane Wade"})>                                                       |



# Scenario Outline: single node bi-directional edge with properties 0 step
# Scenario Outline: single node one-directional edge without properties 0 step
# Scenario Outline: single node one-directional edge with properties 0 step
# Scenario Outline: return path of single node both direction edge with properties 0 step
# When executing query:
# """
# MATCH p = (v :player {name: "Tim Duncan"})-[like*0]->()
# RETURN p
# """
# Then the result should be, in any order:
# | p                                                                                                             |
# | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
  # Scenario Outline: return path of single node both direction edge with properties 0 step
  #   When executing query:
  #     """
  #     MATCH p = <left_node><edge_dir_left><edge><edge_dir_right><right_node>
  #     RETURN p
  #     """
  #   Then the result should be, in any order:
  #     | p                                                                                                             |
  #     | <("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})> |
  #   Examples:
  #     | left_node                        | edge_dir_left | edge | edge_dir_right | right_node                       |
  #     | (v :player {name: "Tim Duncan"}) | -             | [*0] | -              | ()                               |
  #     | ()                               | -             | [*0] | -              | (v :player {name: "Tim Duncan"}) |
