Feature: Insert with time-dependent types

  Background: Prepare space
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
    Then a ExecutionError should be raised at runtime:Storage Error: Invalid field value: may be the filed is not NULL or without default value or wrong schema.

  Scenario: insert null timestamp
    When executing query:
      """
      INSERT VERTEX TAG_TIMESTAMP(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful

  Scenario: insert wrong type of time
    When executing query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":("10:0:0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.

  Scenario: insert null time
    When executing query:
      """
      INSERT VERTEX TAG_TIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful

  Scenario: insert wrong type of date
    When executing query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.

  Scenario: insert null date
    When executing query:
      """
      INSERT VERTEX TAG_DATE(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful

  Scenario: insert wrong type of dateTIME
    When executing query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":("2000.0.0")
      """
    Then a ExecutionError should be raised at runtime:Storage Error: The data type does not meet the requirements. Use the correct type of data.

  Scenario: insert null dateTIME
    When executing query:
      """
      INSERT VERTEX TAG_DATETIME(a) VALUES "TEST_VERTEX":(NULL)
      """
    Then the execution should be successful

