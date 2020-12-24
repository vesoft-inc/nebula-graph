Feature: Datetime default value

  Scenario: DateTime Default Value
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    When executing query:
      """
      create tag ddl_tag6(col1 date default date("2017-03-04"),
                          col2 datetime default datetime("2017-03-04T00:00:01"),
                          col3 time default time("11:11:11"));
      """
    Then the execution should be successful
