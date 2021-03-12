# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@skip
# add support to tck frame to compare operator info
Feature: Unary Expression

  Background:
    Given a graph with space named "nba"

  Scenario: Unary deduce
    When profiling query:
      """
      MATCH (v:player) WHERE !!(v.age>=40)
      RETURN v
      """
    Then the result should be, in any order:
      | v                                                                                                           |
      | ("Kobe Bryant" :player{age: 40, name: "Kobe Bryant"})                                                       |
      | ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})                                                   |
      | ("Ray Allen" :player{age: 43, name: "Ray Allen"})                                                           |
      | ("Dirk Nowitzki" :player{age: 40, name: "Dirk Nowitzki"})                                                   |
      | ("Jason Kidd" :player{age: 45, name: "Jason Kidd"})                                                         |
      | ("Vince Carter" :player{age: 42, name: "Vince Carter"})                                                     |
      | ("Steve Nash" :player{age: 45, name: "Steve Nash"})                                                         |
      | ("Grant Hill" :player{age: 46, name: "Grant Hill"})                                                         |
      | ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"}) |
      | ("Shaquile O'Neal" :player{age: 47, name: "Shaquile O'Neal"})                                               |
    And the execution plan should be:
      | name        | dependencies | operator info           |
      | Project     | 9            |                         |
      | Filter      | 8            |                         |
      | Filter      | 7            |                         |
      | Project     | 6            |                         |
      | Project     | 5            |                         |
      | Filter      | 4            |                         |
      | GetVertices | 3            |                         |
      | Dedup       | 2            |                         |
      | Project     | 11           |                         |
      | IndexScan   | 0            | indexCtx["columnHints"] |
      | Start       |              |                         |
