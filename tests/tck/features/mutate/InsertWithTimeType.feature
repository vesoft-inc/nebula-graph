# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Insert with time-dependent types

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS TAG_TIMESTAMP(a timestamp);
      CREATE TAG IF NOT EXISTS TAG_TIME(a time);
      CREATE TAG IF NOT EXISTS TAG_DATE(a date);
      CREATE TAG IF NOT EXISTS TAG_DATETIME(a datetime);
      """
    And wait 3 seconds

  Scenario: insert wrong format timestamp
    When executing query:
      """
      INSERT VERTEX TAG_TIMESTAMP(a) VALUES "TEST_VERTEX":("2000.0.0 10:0:0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX TAG_TIMESTAMP(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":("10:0:0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful
    And drop the used space
