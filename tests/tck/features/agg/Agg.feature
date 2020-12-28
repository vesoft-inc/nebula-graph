Feature: Basic Agg and GroupBy

  Background:
    Given a graph with space named "nba"

  Scenario: Basic Agg
    When executing query:
      """
      YIELD COUNT(*), 1+1
      """
    Then the result should be, in any order, with relax comparison:
      | COUNT(*) | (1+1) |
      | 1        | 2     |
    When executing query:
      """
      YIELD count(*)+1 ,1+2 ,(INT)abs(count(2))
      """
    Then the result should be, in any order, with relax comparison:
      | (COUNT(*)+1) | (1+2) | (INT)abs(COUNT(2)) |
      | 2            | 3     | 1                  |

  Scenario: Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age; YIELD count($var.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 36.0  |
      | "Manu Ginobili" | 41.0  |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 36.0  |
      | "Manu Ginobili" | 41.0  |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 36.0  |
      | "Manu Ginobili" | 41.0  |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age; YIELD $var.dst AS dst, avg(distinct $var.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 36.0  |
      | "Manu Ginobili" | 41.0  |

  Scenario: Implicit GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 37.0  |
      | "Manu Ginobili" | 42.0  |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;YIELD $var.dst AS dst, (INT)abs(1+avg(distinct $var.age)) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 37  |
      | "Manu Ginobili" | 42  |

  Scenario: Error Check
    When executing query:
      """
      YIELD avg(*)+1 ,1+2 ,(INT)abs(min(2))
      """
    Then a SemanticError should be raised at runtime: Could not apply aggregation function `AVG(*)` on `*`
    # When executing query:
    # """
    # GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | YIELD $-.dst, $$.player.age AS dst, avg(distinct $-.age) AS age
    # """
    # Then a SemanticError should be raised at runtime:  Only support input and variable in GroupBy sentence.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  GroupBy list must in Yield list
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst,$-.age YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: GroupBy item `$-.dst` must be in Yield list
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst,$-.x YIELD avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  `$-.x', not exist prop `x'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.age+1 YIELD $-.age+1,age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not supported expression `age' for props deduction.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.age+1 YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Yield non-agg expression `$-.age` must be functionally dependent on items in GROUP BY clause
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | YIELD $var.dst AS dst, avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not support both input and variable in GroupBy sentence.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD count(*)
      """
    Then a SemanticError should be raised at runtime: `COUNT(*)`, not support aggregate function in go sentence.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like where count(*) > 2
      """
    Then a SemanticError should be raised at runtime: `(COUNT(*)>2)`, not support aggregate function in where sentence.
