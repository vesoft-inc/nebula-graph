Feature: LookUpTest_Vid_String

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(32) |

  Scenario: LookupTest SimpleVertex
    Given having executed:
      """
      CREATE TAG lookup_tag_1(col1 int, col2 int, col3 int);
      CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3);
      CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX lookup_tag_1(col1, col2, col3) VALUES "200":(200, 200, 200),"201":(201, 201, 201), "202":(202, 202, 202);
      """
    And wait 6 seconds
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE col1 == 200;
      """
    Then a ExecutionError should be raised at runtime:
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
      | "200"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200
      YIELD lookup_tag_1.col1, lookup_tag_1.col2, lookup_tag_1.col3
      """
    Then the result should be, in any order:
      | VertexID | lookup_tag_1.col1 | lookup_tag_1.col2 | lookup_tag_1.col3 |
      | "200"    | 200               | 200               | 200               |
    Then drop the used space

  Scenario: LookupTest SimpleEdge
    Given having executed:
      """
      CREATE EDGE lookup_edge_1(col1 int, col2 int, col3 int);
      CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3);
      CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES
                       "200" -> "201"@0:(201, 201, 201),
                       "200" -> "202"@0:(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE col1 == 201
      """
    Then a ExecutionError should be raised at runtime:
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
      | SrcVID | DstVID | Ranking |
      | "200"  | "201"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 YIELD lookup_edge_1.col1, lookup_edge_1.col2, lookup_edge_1.col3
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking | lookup_edge_1.col1 | lookup_edge_1.col2 | lookup_edge_1.col3 |
      | "200"  | "201"  | 0       | 201                | 201                | 201                |
    Then drop the used space

  Scenario: LookupTest VertexIndexHint
    Given having executed:
      """
      CREATE TAG lookup_tag_1(col1 int, col2 int, col3 int);
      CREATE TAG lookup_tag_2(col1 bool, col2 int, col3 double, col4 bool);
      CREATE TAG INDEX t_index_1 ON lookup_tag_1(col1, col2, col3);
      CREATE TAG INDEX t_index_2 ON lookup_tag_2(col2, col3, col4);
      CREATE TAG INDEX t_index_3 ON lookup_tag_1(col2, col3);
      CREATE TAG INDEX t_index_4 ON lookup_tag_2(col3, col4);
      CREATE TAG INDEX t_index_5 ON lookup_tag_2(col4);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX lookup_tag_1(col1, col2, col3) VALUES
                     "200":(200, 200, 200),
                     "201":(201, 201, 201),
                     "202":(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == 200
      """
    Then the result should be, in any order:
      | VertexID |
      | "200"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col1 == true
      """
    Then a ExecutionError should be raised at runtime:
    Then drop the used space

  Scenario: LookupTest EdgeIndexHint
    Given having executed:
      """
      CREATE EDGE lookup_edge_1(col1 int, col2 int, col3 int);
      CREATE EDGE lookup_edge_2(col1 bool,col2 int, col3 double, col4 bool);
      CREATE EDGE INDEX e_index_1 ON lookup_edge_1(col1, col2, col3);
      CREATE EDGE INDEX e_index_2 ON lookup_edge_2(col2, col3, col4);
      CREATE EDGE INDEX e_index_3 ON lookup_edge_1(col2, col3);
      CREATE EDGE INDEX e_index_4 ON lookup_edge_2(col3, col4);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES
                       "200" -> "201"@0:(201, 201, 201),
                       "200" -> "202"@0:(202, 202, 202)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col2 == 201
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "200"  | "201"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col1 == 200
      """
    Then a SemanticError should be raised at runtime:
    Then drop the used space

  @skip
  Scenario: LookupTest VertexConditionScan
    Given having executed:
      """
      CREATE TAG lookup_tag_2(col1 bool, col2 int, col3 double, col4 bool);
      CREATE TAG INDEX t_index_2 ON lookup_tag_2(col2, col3, col4);
      CREATE TAG INDEX t_index_4 ON lookup_tag_2(col3, col4);
      CREATE TAG INDEX t_index_5 ON lookup_tag_2(col4);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES
                   "220":(true, 100, 100.5, true),
                   "221":(true, 200, 200.5, true),
                   "222":(true, 300, 300.5, true),
                   "223":(true, 400, 400.5, true),
                   "224":(true, 500, 500.5, true),
                   "225":(true, 600, 600.5, true)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 == 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 == 100 OR lookup_tag_2.col2 == 200
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 != 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >=100
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >=100 AND lookup_tag_2.col4 == true
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >=100 AND lookup_tag_2.col4 != true
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >= 100 AND lookup_tag_2.col2 <= 400
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5 AND lookup_tag_2.col2 == 100
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5 AND lookup_tag_2.col2 == 200
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 > 100.5
      """
    Then the result should be, in any order:
      | VertexID |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.1
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 >= 100.5 AND lookup_tag_2.col3 <= 300.5
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
    Then drop the used space

  @skip
  Scenario: LookupTest EdgeConditionScan
    Given having executed:
      """
      CREATE EDGE lookup_edge_2(col1 bool,col2 int, col3 double, col4 bool);
      CREATE EDGE INDEX e_index_2 ON lookup_edge_2(col2, col3, col4);
      CREATE EDGE INDEX e_index_4 ON lookup_edge_2(col3, col4);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT EDGE lookup_edge_2(col1, col2, col3, col4) VALUES
                       "220" -> "221"@0:(true, 100, 100.5, true),
                       "220" -> "222"@0:(true, 200, 200.5, true),
                       "220" -> "223"@0:(true, 300, 300.5, true),
                       "220" -> "224"@0:(true, 400, 400.5, true),
                       "220" -> "225"@0:(true, 500, 500.5, true)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 == 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 == 100 OR lookup_edge_2.col2 == 200
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
      | "220"  | "222"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 > 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
      | "220"  | "225"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 != 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
      | "220"  | "225"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
      | "220"  | "225"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 AND lookup_edge_2.col4 == true
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
      | "220"  | "225"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 AND lookup_edge_2.col4 != true
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 AND lookup_edge_2.col2 <= 400
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5 AND lookup_edge_2.col2 == 100
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5 AND lookup_edge_2.col2 == 200
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 > 100.5
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
      | "220"  | "224"  | 0       |
      | "220"  | "225"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.1
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
    When executing query:
      """
      LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 >= 100.5
      AND lookup_edge_2.col3 <= 300.5
      """
    Then the result should be, in any order:
      | SrcVID | DstVID | Ranking |
      | "220"  | "221"  | 0       |
      | "220"  | "222"  | 0       |
      | "220"  | "223"  | 0       |
    Then drop the used space

  @skip
  Scenario: LookupTest FunctionExprTest
    Given having executed:
      """
      CREATE TAG lookup_tag_2(col1 bool, col2 int, col3 double, col4 bool);
      CREATE TAG INDEX t_index_2 ON lookup_tag_2(col2, col3, col4);
      CREATE TAG INDEX t_index_4 ON lookup_tag_2(col3, col4);
      CREATE TAG INDEX t_index_5 ON lookup_tag_2(col4);
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES
                       "220":(true, 100, 100.5, true),
                       "221":(true, 200, 200.5, true),
                       "222":(true, 300, 300.5, true),
                       "223":(true, 400, 400.5, true),
                       "224":(true, 500, 500.5, true),
                       "225":(true, 600, 600.5, true)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE 1 == 1
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE 1 != 1
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE udf_is_in(lookup_tag_2.col2, 100, 200)
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > abs(-5)
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 < abs(-5)
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > (1 + 2)
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 < (1 + 2)
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 != (true AND true)
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == (true AND true)
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == (true or false)
      """
    Then the result should be, in any order:
      | VertexID |
      | "220"    |
      | "221"    |
      | "222"    |
      | "223"    |
      | "224"    |
      | "225"    |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == strcasecmp("HelLo", "HelLo")
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == strcasecmp("HelLo", "hello")
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 != lookup_tag_2.col3
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > (lookup_tag_2.col3 - 100)
      """
    Then the result should be, in any order:
      | VertexID |
    Then drop the used space

  Scenario: LookupTest YieldClauseTest
    Given having executed:
      """
      CREATE TAG student(number int, age int)
      """
    And having executed:
      """
      CREATE TAG INDEX student_index ON student(number, age)
      """
    And having executed:
      """
      CREATE TAG teacher(number int, age int)
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX student(number, age), teacher(number, age)  VALUES
                     "220":(1, 20, 1, 30),
                     "221":(2, 22, 2, 32)
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON student WHERE student.number == 1 YIELD teacher.age
      """
    Then a SemanticError should be raised at runtime.
    When executing query:
      """
      LOOKUP ON student WHERE student.number == 1 YIELD teacher.age AS student_name
      """
    Then a SemanticError should be raised at runtime.
    When executing query:
      """
      LOOKUP ON student WHERE teacher.number == 1 YIELD student.age
      """
    Then a SemanticError should be raised at runtime.
    When executing query:
      """
      LOOKUP ON student WHERE student.number == 1 YIELD student.age
      """
    Then the result should be, in any order:
      | VertexID | student.age |
      | "220"    | 20          |
    Then drop the used space

  Scenario: LookupTest OptimizerTest
    Given having executed:
      """
      CREATE TAG t1(c1 int, c2 int, c3 int, c4 int, c5 int)
      """
    And having executed:
      """
      CREATE TAG INDEX i1 ON t1(c1, c2)
      """
    And having executed:
      """
      CREATE TAG INDEX i2 ON t1(c2, c1)
      """
    And having executed:
      """
      CREATE TAG INDEX i3 ON t1(c3)
      """
    And having executed:
      """
      CREATE TAG INDEX i4 ON t1(c1, c2, c3, c4)
      """
    And having executed:
      """
      CREATE TAG INDEX i5 ON t1(c1, c2, c3, c5)
      """
    And wait 6 seconds
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c1 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c1 == 1 AND t1.c2 > 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c1 > 1 AND t1.c2 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c1 > 1 AND t1.c2 == 1 AND  t1.c3 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c3 > 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c3 > 1 AND t1.c1 >1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c4 > 1
      """
    Then a ExecutionError should be raised at runtime: IndexNotFound: No valid index found
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c2 > 1 AND t1.c3 > 1
      """
    Then a ExecutionError should be raised at runtime: IndexNotFound: No valid index found
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c2 > 1 AND t1.c1 != 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1 WHERE t1.c2 != 1
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: LookupTest OptimizerWithStringFieldTest
    Given having executed:
      """
      CREATE TAG t1_str(c1 int, c2 int, c3 string, c4 string)
      """
    And having executed:
      """
      CREATE TAG INDEX i1_str ON t1_str(c1, c2)
      """
    And having executed:
      """
      CREATE TAG INDEX i2_str ON t1_str(c4(30))
      """
    And having executed:
      """
      CREATE TAG INDEX i3_str ON t1_str(c3(30), c1)
      """
    And having executed:
      """
      CREATE TAG INDEX i4_str ON t1_str(c3(30), c4(30))
      """
    And having executed:
      """
      CREATE TAG INDEX i5_str ON t1_str(c1, c2, c3(30), c4(30))
      """
    And wait 6 seconds
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c1 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c1 == 1 AND t1_str.c2 >1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c3 == "a"
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c4 == "a"
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c3 == "a"  AND t1_str.c4 == "a"
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c3 == "a"  AND t1_str.c1 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c3 == "a" AND t1_str.c2 == 1  AND t1_str.c1 == 1
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON t1_str WHERE t1_str.c4 == "a" AND t1_str.c3 == "a" AND t1_str.c2 == 1  AND t1_str.c1 == 1
      """
    Then the execution should be successful
    Then drop the used space

  Scenario: LookupTest StringFieldTest
    Given having executed:
      """
      CREATE TAG tag_with_str(c1 int, c2 string, c3 string)
      """
    And having executed:
      """
      CREATE TAG INDEX i1_with_str ON tag_with_str(c1, c2(30))
      """
    And having executed:
      """
      CREATE TAG INDEX i2_with_str ON tag_with_str(c2(30), c3(30))
      """
    And having executed:
      """
      CREATE TAG INDEX i3_with_str ON tag_with_str(c1, c2(30), c3(30))
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX tag_with_str(c1, c2, c3) VALUES
                     "1":(1, "c1_row1", "c2_row1"),
                     "2":(2, "c1_row2", "c2_row2"),
                     "3":(3, "abc", "abc"),
                     "4":(4, "abc", "abc"),
                     "5":(5, "ab", "cabc"),
                     "6":(5, "abca", "bc")
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 1
      """
    Then the result should be, in any order:
      | VertexID |
      | "1"      |
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 1 AND tag_with_str.c2 == "ccc"
      """
    Then the result should be, in any order:
      | VertexID |
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 1 AND tag_with_str.c2 == "c1_row1"
      """
    Then the result should be, in any order:
      | VertexID |
      | "1"      |
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 5 AND tag_with_str.c2 == "ab"
      """
    Then the result should be, in any order:
      | VertexID |
      | "5"      |
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c2 == "abc" AND tag_with_str.c3 == "abc"
      """
    Then the result should be, in any order:
      | VertexID |
      | "3"      |
      | "4"      |
    When executing query:
      """
      LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 5 AND tag_with_str.c2 == "abca" AND tag_with_str.c3 == "bc"
      """
    Then the result should be, in any order:
      | VertexID |
      | "6"      |
    Then drop the used space

  Scenario: LookupTest ConditionTest
    Given having executed:
      """
      CREATE TAG identity (BIRTHDAY int, NATION string, BIRTHPLACE_CITY string)
      """
    And having executed:
      """
      CREATE TAG INDEX idx_identity_cname_birth_gender_nation_city
                    ON identity(BIRTHDAY, NATION(30), BIRTHPLACE_CITY(30))
      """
    And wait 6 seconds
    When executing query:
      """
      INSERT VERTEX identity (BIRTHDAY, NATION, BIRTHPLACE_CITY)
                    VALUES "1" : (19860413, "汉族", "aaa")
      """
    Then the execution should be successful
    When executing query:
      """
      LOOKUP ON identity
      WHERE identity.NATION == "汉族" AND identity.BIRTHDAY > 19620101 AND identity.BIRTHDAY < 20021231 AND identity.BIRTHPLACE_CITY == "bbb";
      """
    Then the result should be, in any order:
      | VertexID |
    Then drop the used space
