# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Variable length Pattern match (0 step)

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario Outline: Use step all-direction edge with properties 0 step, return node
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                          |
      | ("Tim Duncan":bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

    Examples:
      | left_node                        | edge_dir_left | edge | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | -             | [*0] | -              | ()                               |
      | (v :player {name: "Tim Duncan"}) | <-            | [*0] | -              | ()                               |
      | (v :player {name: "Tim Duncan"}) | -             | [*0] | ->             | ()                               |
      | ()                               | -             | [*0] | -              | (v :player {name: "Tim Duncan"}) |
      | ()                               | <-            | [*0] | -              | (v :player {name: "Tim Duncan"}) |
      | ()                               | -             | [*0] | ->             | (v :player {name: "Tim Duncan"}) |

  # match (v :player {name: "Tim Duncan"})-[e :like*0..1]-() return e
  Scenario Outline: Use step all-direction edge without properties 0 to 1 step, return edge
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN e
      """
    Then the result should be, in any order:
      | e                                                             |
      | [[:like "Aron Baynes"->"Tim Duncan" @0 {likeness: 80}]]       |
      | [[:like "Boris Diaw"->"Tim Duncan" @0 {likeness: 80}]]        |
      | [[:like "Danny Green"->"Tim Duncan" @0 {likeness: 70}]]       |
      | [[:like "Dejounte Murray"->"Tim Duncan" @0 {likeness: 99}]]   |
      | [[:like "LaMarcus Aldridge"->"Tim Duncan" @0 {likeness: 75}]] |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]     |
      | [[:like "Marco Belinelli"->"Tim Duncan" @0 {likeness: 55}]]   |
      | [[:like "Shaquile O'Neal"->"Tim Duncan" @0 {likeness: 80}]]   |
      | [[:like "Tiago Splitter"->"Tim Duncan" @0 {likeness: 80}]]    |
      | [[:like "Tony Parker"->"Tim Duncan" @0 {likeness: 95}]]       |
      | [[:like "Tim Duncan"->"Manu Ginobili" @0 {likeness: 95}]]     |
      | [[:like "Tim Duncan"->"Tony Parker" @0 {likeness: 95}]]       |
      | []                                                            |
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..1]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e                                                                      | v                                                                                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]] | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                                     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

    Examples:
      | left_node                        | edge_dir_left | edge           | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | -             | [e :like*0..1] | -              | ()                               |
      | ()                               | -             | [e :like*0..1] | -              | (v :player {name: "Tim Duncan"}) |

  # match (v :player {name: "Tim Duncan"})-[e :like*0..1{likeness: 90}]->() return e
  Scenario Outline: Use step one-direction edge with properties 0 to 1 step, return edge
    When executing query:
      """
      MATCH <left_node><edge_dir_left><edge><edge_dir_right><right_node>
      RETURN e
      """
    Then the result should be, in any order:
      | e                                                         |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] |
      | []                                                        |

    Examples:
      | left_node                        | edge_dir_left | edge                         | edge_dir_right | right_node                       |
      | (v :player {name: "Tim Duncan"}) | <-            | [e :like*0..1{likeness: 90}] | -              | ()                               |
      | ()                               | -             | [e :like*0..1{likeness: 90}] | ->             | (v :player {name: "Tim Duncan"}) |

  Scenario: Edge without properties 0 to n step, return edge and dst
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..2]-(v)
      RETURN e, v
      """
    Then the result should be, in any order:
      | e                                                                                                                                                   | v                                                                                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Aron Baynes"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]       | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Boris Diaw"->"Spurs" @0 {end_year: 2016, start_year: 2012}]]        | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Cory Joseph"->"Spurs" @0 {end_year: 2015, start_year: 2011}]]       | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Danny Green"->"Spurs" @0 {end_year: 2018, start_year: 2010}]]       | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "David West"->"Spurs" @0 {end_year: 2016, start_year: 2015}]]        | ("David West" :player{age: 38, name: "David West"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Dejounte Murray"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]   | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Jonathon Simmons"->"Spurs" @0 {end_year: 2017, start_year: 2015}]]  | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Kyle Anderson"->"Spurs" @0 {end_year: 2018, start_year: 2014}]]     | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "LaMarcus Aldridge"->"Spurs" @0 {end_year: 2019, start_year: 2015}]] | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Manu Ginobili"->"Spurs" @0 {end_year: 2018, start_year: 2002}]]     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @0 {end_year: 2015, start_year: 2013}]]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Paul Gasol"->"Spurs" @0 {end_year: 2019, start_year: 2016}]]        | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Rudy Gay"->"Spurs" @0 {end_year: 2019, start_year: 2017}]]          | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tiago Splitter"->"Spurs" @0 {end_year: 2015, start_year: 2010}]]    | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Marco Belinelli"->"Spurs" @1 {end_year: 2019, start_year: 2018}]]   | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tony Parker"->"Spurs" @0 {end_year: 2018, start_year: 1999}]]       | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}], [:serve "Tracy McGrady"->"Spurs" @0 {end_year: 2013, start_year: 2013}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:serve "Tim Duncan"->"Spurs" @0 {end_year: 2016, start_year: 1997}]]                                                                              | ("Spurs" :team{name: "Spurs"})                                                                              |
      | []                                                                                                                                                  | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |

  Scenario: Multiple variable length with edge filter
    When executing query:
      """
      MATCH (:player{name:"Tim Duncan"})-[e:serve*0..2]-(v)-[e2 :like*0..2{likeness:90}]-(v2)
      RETURN e2, v2
      """
    Then the result should be, in any order:
      | e2                                                                                                                     | v2                                                                                                          |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]       | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}]]     | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}], [:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]] | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}], [:like "Yao Ming"->"Shaquile O'Neal" @0 {likeness: 90}]]       | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
      | [[:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}], [:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]]         | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}], [:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]] | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]       | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}], [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}], [:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}], [:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}], [:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]]      | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | [[:like "Paul Gasol"->"Kobe Bryant" @0 {likeness: 90}]]                                                                | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                                                         | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]]                                                          | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tony Parker"->"LaMarcus Aldridge" @0 {likeness: 90}]]                                                         | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                              | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | [[:like "Tiago Splitter"->"Manu Ginobili" @0 {likeness: 90}]]                                                          | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}]]                                                                | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | [[:like "Manu Ginobili"->"Tim Duncan" @0 {likeness: 90}]]                                                              | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | [[:like "Tracy McGrady"->"Rudy Gay" @0 {likeness: 90}]]                                                                | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | [[:like "Tracy McGrady"->"Kobe Bryant" @0 {likeness: 90}]]                                                             | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | [[:like "Tracy McGrady"->"Grant Hill" @0 {likeness: 90}]]                                                              | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | [[:like "Yao Ming"->"Tracy McGrady" @0 {likeness: 90}]]                                                                | ("Yao Ming" :player{age: 38, name: "Yao Ming"})                                                             |
      | [[:like "Vince Carter"->"Tracy McGrady" @0 {likeness: 90}]]                                                            | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | [[:like "Grant Hill"->"Tracy McGrady" @0 {likeness: 90}]]                                                              | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | []                                                                                                                     | ("Paul Gasol" :player{age: 38, name: "Paul Gasol"})                                                         |
      | []                                                                                                                     | ("Cory Joseph" :player{age: 27, name: "Cory Joseph"})                                                       |
      | []                                                                                                                     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | []                                                                                                                     | ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})                                               |
      | []                                                                                                                     | ("Tony Parker" :player{age: 36, name: "Tony Parker"})                                                       |
      | []                                                                                                                     | ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})                                               |
      | []                                                                                                                     | ("David West" :player{age: 38, name: "David West"})                                                         |
      | []                                                                                                                     | ("Tiago Splitter" :player{age: 34, name: "Tiago Splitter"})                                                 |
      | []                                                                                                                     | ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})                                                         |
      | []                                                                                                                     | ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})                                           |
      | []                                                                                                                     | ("Aron Baynes" :player{age: 32, name: "Aron Baynes"})                                                       |
      | []                                                                                                                     | ("Kyle Anderson" :player{age: 25, name: "Kyle Anderson"})                                                   |
      | []                                                                                                                     | ("Danny Green" :player{age: 31, name: "Danny Green"})                                                       |
      | []                                                                                                                     | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | []                                                                                                                     | ("Rudy Gay" :player{age: 32, name: "Rudy Gay"})                                                             |
      | []                                                                                                                     | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | []                                                                                                                     | ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})                                                   |
      | []                                                                                                                     | ("Jonathon Simmons" :player{age: 29, name: "Jonathon Simmons"})                                             |
      | []                                                                                                                     | ("Spurs" :team{name: "Spurs"})                                                                              |
