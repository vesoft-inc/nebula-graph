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
      | id | name               | dependencies | operator info                                       |
      | 0  | Project            | 1            |                                                     |
      | 1  | Filter             | 2            |                                                     |
      | 2  | Project            | 3            |                                                     |
      | 3  | InnerJoin          | 4            |                                                     |
      | 4  | Project            | 5            |                                                     |
      | 5  | GetVertices        | 6            | {"dedup": "true"}                                   |
      | 6  | DataCollect        | 7            |                                                     |
      | 7  | Filter             | 8            |                                                     |
      | 8  | UnionAllVersionVar | 9            |                                                     |
      | 9  | Loop               | 16           | {"loopBody": "9"}                                   |
      | 10 | Filter             | 11           |                                                     |
      | 11 | Project            | 12           |                                                     |
      | 12 | InnerJoin          | 13           | {"inputVar": {"rightVar":{"__Project_11":"0"}}}     |
      | 13 | Project            | 14           |                                                     |
      | 14 | GetNeighbors       | 15           | {"dedup": "true"}                                   |
      | 15 | Start              |              |                                                     |
      | 16 | Project            | 17           |                                                     |
      | 17 | Filter             | 18           |                                                     |
      | 18 | GetVertices        | 19           | {"dedup": "true"}                                   |
      | 19 | IndexScan          | 20           | {"indexCtx": {"columnHints":{"scanType":"PREFIX"}}} |
      | 20 | Start              |              |                                                     |
