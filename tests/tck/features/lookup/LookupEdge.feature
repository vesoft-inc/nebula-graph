Feature: Test lookup on edge index

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9          |
      | replica_factor | 1          |
      | vid_type       | <vid_type> |

  Scenario Outline: LookupTest IntVid SimpleEdge
    Given having executed:
      """
      CREATE EDGE lookup_edge_1(col1 int, col2 int, col3 int);
      CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3);
      CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3);
      """
    And wait 6 seconds
    When try to execute query:
      """
      INSERT EDGE
        lookup_edge_1(col1, col2, col3)
      VALUES
        <id_200> -> <id_201>@0:(201, 201, 201),
        <id_200> -> <id_202>@0:(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE col1 == 201
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 300
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201
      """
    Then the result should be, in any order:
      | SrcVID   | DstVID   | Ranking |
      | <id_200> | <id_201> | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 YIELD lookup_edge_1.col1, lookup_edge_1.col2, lookup_edge_1.col3
      """
    Then the result should be, in any order:
      | SrcVID   | DstVID   | Ranking | lookup_edge_1.col1 | lookup_edge_1.col2 | lookup_edge_1.col3 |
      | <id_200> | <id_201> | 0       | 201                | 201                | 201                |
    Then drop the used space

    Examples:
      | vid_type         | id_200 | id_201 | id_202 |
      | int64            | 200    | 201    | 202    |
      | FIXED_STRING(16) | "200"  | "201"  | "202"  |
