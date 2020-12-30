# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
@wyc
Feature: Fix match losing undefined vertex tag info

  Background:
    Given a graph with space named "nba"
    And having executed:
      """
      CREATE TAG IF NOT EXISTS empty_tag();
      """
    And wait 2 seconds
    And having executed:
      """
      INSERT VERTEX empty_tag() values "Tim Duncan":()
      """
    And wait 2 seconds

  Scenario: single vertex
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                            |
      | ["empty_tag","bachelor","player"] |

  Scenario: one step
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->()
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                            |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
      | ["empty_tag","bachelor","player"] |
