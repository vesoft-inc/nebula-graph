Feature: Test lookup on tag index

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9          |
      | replica_factor | 1          |
      | vid_type       | <vid_type> |

  Scenario Outline: LookupTest SimpleVertex
    Given having executed:
      """
      CREATE TAG lookup_tag_1(col1 int, col2 int, col3 int);
      CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3);
      CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX
        lookup_tag_1(col1, col2, col3)
      VALUES
        <vid_200>:(200, 200, 200),
        <vid_201>:(201, 201, 201),
        <vid_202>:(202, 202, 202);
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE col1 == 200;
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 300
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200
      """
    Then the result should be, in any order:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200
      YIELD lookup_tag_1.col1, lookup_tag_1.col2, lookup_tag_1.col3
      """
    Then the result should be, in any order:
      | VertexID | lookup_tag_1.col1 | lookup_tag_1.col2 | lookup_tag_1.col3 |
      | <id_200> | 200               | 200               | 200               |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 == 200 AND
        lookup_tag_1.col3 == 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 == 200 AND
        lookup_tag_1.col3 == 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col2 AS col2
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 >= 200 AND
        lookup_tag_1.col3 == 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 >= 200 AND
        lookup_tag_1.col3 == 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col2 AS col2
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 >= 200 AND
        lookup_tag_1.col3 != 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 >= 200 AND
        lookup_tag_1.col3 != 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col2 AS col2
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 == 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 == 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 != 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 == 200 AND
        lookup_tag_1.col2 != 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 >= 200 AND
        lookup_tag_1.col2 == 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 >= 200 AND
        lookup_tag_1.col2 == 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 >= 200 AND
        lookup_tag_1.col2 != 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 >= 200 AND
        lookup_tag_1.col2 != 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 != 200
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    When executing query:
      """
      LOOKUP ON
        lookup_tag_1
      WHERE
        lookup_tag_1.col1 != 200
      YIELD
        lookup_tag_1.col1 AS col1
        lookup_tag_1.col3
      """
    Then the result should be:
      | VertexID |
      | <id_200> |
    Then drop the used space

    Examples:
      | vid_type         | id_200 | id_201 | id_202 |
      | int64            | 200    | 201    | 202    |
      | FIXED_STRING(16) | "200"  | "201"  | "202"  |
