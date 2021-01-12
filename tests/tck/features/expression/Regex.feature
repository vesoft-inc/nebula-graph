# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Regex match expression

  Background:
    Given a graph with space named "nba"

  Scenario: regex match with yield
    When executing query:
      """
      YIELD "abcd\xA3g1234efgh\x49ijkl" =~ "\\w{4}\xA3g12\\d*e\\w+\x49\\w+"
      """
    Then the result should be, in any order:
      | (abcdxA3g1234efghx49ijkl=~\w{4}xA3g12\d*e\w+x49\w+) |
      | true                                                |
    When executing query:
      """
      YIELD "Tony Parker" =~ "T\\w+\\s\\w+"
      """
    Then the result should be, in any order:
      | (Tony Parker=~T\w+\s\w+) |
      | true                     |
    When executing query:
      """
      YIELD "010-12345" =~ "\\d{3}\\-\\d{3,8}"
      """
    Then the result should be, in any order:
      | (010-12345=~\d{3}\-\d{3,8}) |
      | true                        |
    When executing query:
      """
      YIELD "test_space_128" =~ "[a-zA-Z_][0-9a-zA-Z_]{0,19}"
      """
    Then the result should be, in any order:
      | (test_space_128=~[a-zA-Z_][0-9a-zA-Z_]{0,19}) |
      | true                                          |
    When executing query:
      """
      YIELD "2001-09-01 08:00:00" =~ "\\d+\\-0\\d?\\-\\d+\\s\\d+:00:\\d+"
      """
    Then the result should be, in any order:
      | (2001-09-01 08:00:00=~\d+\-0\d?\-\d+\s\d+:00:\d+) |
      | true                                              |
    When executing query:
      """
      YIELD "2019" =~ "\\d+"
      """
    Then the result should be, in any order:
      | (2019=~\d+) |
      | true        |
    When executing query:
      """
      YIELD "jack138tom发abc数据库烫烫烫" =~ "j\\w*\\d+\\w+\u53d1[a-c]+\u6570\u636e\u5e93[\x70EB]+"
      """
    Then the result should be, in any order:
      | (jack138tom 发 abc 数据库烫烫烫=~j\w*\d+\w+发[a-c]+数据库[烫]+) |
      | true                                                            |
    When executing query:
      """
      YIELD "a good person" =~ "a\\s\\w+"
      """
    Then the result should be, in any order:
      | (a good person=~a\s\w+) |
      | false                   |
    When executing query:
      """
      YIELD "Trail Blazers" =~ "\\w+"
      """
    Then the result should be, in any order:
      | (Trail Blazers=~\w+) |
      | false                |
    When executing query:
      """
      YIELD "Tony No.1" =~ "\\w+No\\.\\d+"
      """
    Then the result should be, in any order:
      | (Tony No.1=~\w+No\.\d+) |
      | false                   |

  Scenario: regex match with go
    When executing query:
      """
      GO FROM "Tracy McGrady" OVER like
      WHERE $$.player.name =~ "\\w+\\s?.*"
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | "Kobe Bryant"  |
      | "Grant Hill"   |
      | "Rudy Gay"     |
    When executing query:
      """
      GO FROM "Marco Belinelli" OVER serve
      WHERE $$.team.name =~ "\\d+\\w+"
      YIELD $$.team.name
      """
    Then the result should be, in any order:
      | $$.team.name |
      | "76ers"      |
    When executing query:
      """
      GO FROM "Yao Ming" OVER like
      WHERE $$.player.name =~ "\\w+\\s?\\w+'\\w+"
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.team.name      |
      | "Shaquile O'Neal" |
