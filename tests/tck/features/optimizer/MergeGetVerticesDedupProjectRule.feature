# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: merge get vertices, dedup and project rule

  Background:
    Given a graph with space named "nba"

  Scenario: apply get vert, dedup and project merge opt rule
    When profiling query:
      """
      MATCH (v)
      WHERE id(v) == 'Tim Duncan'
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name         |
      | "Tim Duncan" |
    And the execution plan should be:
      | name        | dependencies | operator info      |
      | Project     | 1            |                    |
      | Filter      | 2            |                    |
      | Filter      | 3            |                    |
      | Project     | 4            |                    |
      | Project     | 5            |                    |
      | GetVertices | 6            | {"dedup" : "true"} |
      | PassThrough | 7            |                    |
      | Start       |              |                    |
