# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Basic match

  Scenario: one step
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age >= 38 AND v.age < 45
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name            | Age |
      | 'Paul Gasol'    | 38  |
      | 'Kobe Bryant'   | 40  |
      | 'Vince Carter'  | 42  |
      | 'Tim Duncan'    | 42  |
      | 'Yao Ming'      | 38  |
      | 'Dirk Nowitzki' | 40  |
      | 'Manu Ginobili' | 41  |
      | 'Ray Allen'     | 43  |
      | 'David West'    | 38  |
      | 'Tracy McGrady' | 39  |
    When executing query:
      """
      MATCH (v:player {age: 29})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: Match a node while given the tag without property
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->(v2:team)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |
