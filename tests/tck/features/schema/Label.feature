# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Multi language label

  Scenario: Schema Label
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    # empty prop
    When executing query:
      """
      CREATE TAG `中文`();
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG `中文`;
      """
    Then the result should be, in any order:
      | Tag    | Create Tag                                              |
      | "中文" | 'CREATE TAG `中文` (\n) ttl_duration = 0, ttl_col = ""' |
    When executing query:
      """
      CREATE TAG ` 中文 `();
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG ` 中文 `;
      """
    Then the result should be, in any order:
      | Tag      | Create Tag                                                |
      | " 中文 " | 'CREATE TAG ` 中文 ` (\n) ttl_duration = 0, ttl_col = ""' |
