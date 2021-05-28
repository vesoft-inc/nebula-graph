# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Multi language label

  Scenario: Escaped Schema Label
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG `个人信息`(`姓名` string, `年龄` int);
      """
    Then the execution should be successful
    When executing query:
      """
      SHOW CREATE TAG `个人信息`;
      """
    Then the result should be, in any order:
      | Tag        | Create Tag                                                                                            |
      | "个人信息" | 'CREATE TAG `个人信息` (\n `姓名` string NULL,\n `年龄` int64 NULL\n) ttl_duration = 0, ttl_col = ""' |
    # insert data
    When try to execute query:
      """
      INSERT VERTEX `个人信息`(`姓名`, `年龄`) VALUES "张三":("张三", 22);
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON `个人信息` "张三" YIELD `个人信息`.`姓名` AS `姓名`, `个人信息`.`年龄`
      """
    Then the result should be, in any order:
      | VertexID | 姓名   | 个人信息.年龄 |
      | "张三"   | "张三" | 22            |
    When executing query:
      """
      FETCH PROP ON `个人信息` "张三" YIELD `个人信息`.`姓名` AS `姓名`
      | FETCH PROP ON `个人信息` $-.`姓名` YIELD `个人信息`.`姓名` AS `姓名`, `个人信息`.`年龄`
      """
    Then the result should be, in any order:
      | VertexID | 姓名   | 个人信息.年龄 |
      | "张三"   | "张三" | 22            |
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

  Scenario: Invalid escaped label
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    When executing query:
      """
      CREATE TAG `_中文`();
      """
    Then a SyntaxError should be raised at runtime: Label can't start with `_' near ``_中文`'
    When executing query:
      """
      CREATE TAG `*中文`();
      """
    Then a SyntaxError should be raised at runtime: Label can't start with `*' near ``*中文`'
    When executing query:
      """
      CREATE TAG `*`();
      """
    Then a SyntaxError should be raised at runtime: Label can't start with `*' near ``*`'
