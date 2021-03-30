# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Insert string vid of vertex and edge

  Scenario: insert vertex and edge test
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(20) |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS like(likeness int);
      """
    # insert vertex succeeded
    When try to execute query:
      """
      INSERT VERTEX person(name, age) VALUES "Tom":("Tom", 22)
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX person(age, name) VALUES "Conan":(10, "Conan")
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 10  |
    # insert vertex if not exists
    When try to execute query:
      """
      INSERT VERTEX IF NOT EXISTS person(name, age) VALUES "Conan":("Conan", 20)
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person "Conan" YIELD person.age as age
      """
    Then the result should be, in any order, with relax comparison:
      | VertexID | age |
      | "Conan"  | 10  |
    # insert edge
    When executing query:
      """
      INSERT EDGE like(likeness) VALUES "Tom"->"Conan":(87)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 87            |
    # insert edge
    When executing query:
      """
      INSERT EDGE IF NOT EXISTS like(likeness) VALUES "Tom"->"Conan":(10)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON like "Tom"->"Conan" YIELD like.likeness
      """
    Then the result should be, in any order:
      | like._src | like._dst | like._rank | like.likeness |
      | 'Tom'     | 'Conan'   | 0          | 87            |
