# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: merge get neighbors, dedup and project rule

  Background:
    Given a graph with space named "nba"

  Scenario: apply get nbrs, dedup and project merge opt rule
    When profiling query:
      """
      MATCH (v:player{name: 'Tim Duncan'})-[:like*0..1]->(v2)
      RETURN v2.name AS Name
      """
    Then the result should be, in any order:
      | Name            |
      | "Manu Ginobili" |
      | "Tony Parker"   |
      | "Tim Duncan"    |
    And the execution plan should be:
      | id | name               | dependencies | operator info      |
      | 25 | Project            | 24           |                    |
      | 24 | Filter             | 23           |                    |
      | 23 | Project            | 22           |                    |
      | 22 | InnerJoin          | 17,21        |                    |
      | 17 | Filter             | 16           |                    |
      | 16 | UnionAllVersionVar | 15           |                    |
      | 15 | Loop               | 6            | {"loopBody": "14"} |
      | 14 | Filter             | 13           |                    |
      | 13 | Project            | 12           |                    |
      | 12 | InnerJoin          | 7,11         |                    |
      | 7  | Start              |              |                    |
      | 11 | Project            | 32           |                    |
      | 32 | GetNeighbors       | 7            | {"dedup": "true"}  |
      | 6  | Project            | 5            |                    |
      | 5  | Filter             | 29           |                    |
      | 29 | GetVertices        | 26           | {"dedup": "true"}  |
      | 26 | IndexScan          | 0            |                    |
      | 0  | Start              |              |                    |
      | 21 | Project            | 30           |                    |
      | 30 | GetVertices        | 17           | {"dedup": "true"}  |
