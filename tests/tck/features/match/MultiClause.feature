# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@jie
Feature: Multi Clause

  Background:
    Given a graph with space named "nba"

  Scenario: with match
    When executing query:
      """
      WITH "Yao Ming" AS a MATCH (v:player) WHERE v.name == a RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v            |
      | ("Yao Ming") |
    When executing query:
      """
      WITH 38 as a MATCH (v:player) WHERE v.age == a RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | "David West" | 38  |
      | "Paul Gasol" | 38  |
      | "Yao Ming"   | 38  |
    When executing query:
      """
      WITH "Tony Parker" AS a, 38 AS b
      MATCH (v:player) WHERE v.name == a OR v.age == b
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name          | Age |
      | "David West"  | 38  |
      | "Paul Gasol"  | 38  |
      | "Yao Ming"    | 38  |
      | "Tony Parker" | 36  |

  Scenario: unwind match
    When executing query:
      """
      UNWIND ["Tony Parker", "Yao Ming"] AS a
      MATCH (v:player) WHERE v.name == a
      return v
      """
    Then the result should be, in any order, with relax comparison:
      | v               |
      | ("Yao Ming")    |
      | ("Tony Parker") |
    When executing query:
      """
      UNWIND [36, 37] AS a
      MATCH (v:player) WHERE v.age == a
      return v
      """
    Then the result should be, in any order, with relax comparison:
      | v                      |
      | ("Amar'e Stoudemire" ) |
      | ("Boris Diaw")         |
      | ("Tony Parker")        |
      | ("Dwyane Wade")        |


  Scenario: multi match
    When executing query:
      """
      MATCH (v:player) WHERE v.name == "Yao Ming"
      MATCH (v2:player) WHERE v2.age > v.age + 5
      return v, v2
      """
    Then the result should be, in any order, with relax comparison:
      | v            | v2                  |
      | ("Yao Ming") | ("Steve Nash")      |
      | ("Yao Ming") | ("Grant Hill")      |
      | ("Yao Ming") | ("Shaquile O'Neal") |
      | ("Yao Ming") | ("Jason Kidd")      |
