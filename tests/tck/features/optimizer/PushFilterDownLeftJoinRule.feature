# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@push_down_join
Feature: Push Filter down LeftJoin rule

  Background:
    Given a graph with space named "nba"

  Scenario: push filter down LeftJoin
    When profiling query:
      """
      LOOKUP ON player WHERE player.name=='Tim Duncan'
      | YIELD $-.VertexID AS vid
      |  GO FROM $-.vid OVER like BIDIRECT
      WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name, like._dst AS vid
      | GO FROM $-.vid OVER like BIDIRECT WHERE any(x in split($$.player.name, ' ') WHERE x contains 'Ti')
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Tim Duncan"   |
    And the execution plan should be:
      | id | name               | dependencies | operator info |
      | 26 | Project            | 25           |               |
      | 25 | Filter             | 24           |               |
      | 24 | InnerJoin          | 22,23        |               |
      | 22 | LeftJoin           | 19,21        |               |
      | 19 | Project            | 32           |               |
      | 32 | GetNeighbors       | 15           |               |
      | 15 | Project            | 14           |               |
      | 14 | Filter             | 13           |               |
      | 13 | InnerJoin          | 11,12        |               |
      | 11 | LeftJoin           | 8,10         |               |
      | 8  | Project            | 31           |               |
      | 31 | GetNeighbors       | 4            |               |
      | 4  | Project            | 3            |               |
      | 3  | Project            | 27           |               |
      | 27 | TagIndexPrefixScan | 0            |               |
      | 0  | Start              |              |               |
      | 10 | Project            | 9            |               |
      | 9  | GetVertices        | 8            |               |
      | 12 | Start              |              |               |
      | 21 | Project            | 20           |               |
      | 20 | GetVertices        | 19           |               |
      | 23 | Start              |              |               |
    When profiling query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE $$.player.age >= 32 AND like.likeness > 85
      YIELD like._dst AS id, like.likeness AS likeness, $$.player.age AS age
      """
    Then the result should be, in any order:
      | id                  | likeness | age |
      | "LaMarcus Aldridge" | 90       | 33  |
      | "Manu Ginobili"     | 95       | 41  |
      | "Tim Duncan"        | 95       | 42  |
    And the execution plan should be:
      | id | name         | dependencies | operator info |
      | 7  | Project      | 10           |               |
      | 10 | Filter       | 9            |               |
      | 9  | LeftJoin     | 12,4         |               |
      | 12 | Project      | 13           |               |
      | 13  | GetNeighbors | 0            |               |
      | 0  | Start        |              |               |
      | 4  | Project      | 3            |               |
      | 3  | GetVertices  | 2            |               |
      | 2  | Project      | 13            |               |
