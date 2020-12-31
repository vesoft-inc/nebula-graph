# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Fix match losing undefined vertex tag info

  Background: Prepare Space
    Given an empty graph
    And load "nba" csv data to a new space
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
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |

  Scenario: one step with direction
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->()
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |
      | ["player", "empty_tag", "bachelor"] |

  Scenario: one step without direction
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})--()
      RETURN labels(v) AS Labels
      """
    Then the result should be, in any order:
      | Labels                              |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
      | ["empty_tag", "bachelor", "player"] |
