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
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD DISTINCT count(*) AS count where $-.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1     |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD count($var.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 2     |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD DISTINCT like._dst AS dst, $$.player.age AS age;
      YIELD count($var.dst) AS count where $var.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | count |
      | 1     |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD DISTINCT $var.dst AS dst, avg(distinct $var.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 36.0 |
      | "Manu Ginobili" | 41.0 |

  Scenario: Implicit GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Tony Parker"   | 37.0 |
      | "Manu Ginobili" | 42.0 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age where $-.age > 40
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age  |
      | "Manu Ginobili" | 42.0 |
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      YIELD $var.dst AS dst, (INT)abs(1+avg(distinct $var.age)) AS age
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | age |
      | "Tony Parker"   | 37  |
      | "Manu Ginobili" | 42  |
    When executing query:
      """
      $var1=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      $var2=GO FROM "Tim Duncan" OVER serve YIELD serve._dst AS dst;
      YIELD $var1.dst AS dst, count($var1.dst) AS count
      """
    Then the result should be, in any order, with relax comparison:
      | dst             | count |
      | "Tony Parker"   | 1     |
      | "Manu Ginobili" | 1     |

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
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst YIELD avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  GroupBy list must in Yield list
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst,$-.age YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: GroupBy item `$-.dst` must be in Yield list
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.dst,$-.x YIELD avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  `$-.x', not exist prop `x'
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not supported expression `age' for props deduction.
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Yield non-agg expression `$-.age` must be functionally dependent on items in GROUP BY clause
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,abs(avg(distinct count($-.age))) AS age
      """
    Then a SemanticError should be raised at runtime: Agg function nesting is not allowed: `abs(AVG(distinct COUNT($-.age)))`
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY $-.age+1 YIELD $-.age+1,avg(distinct count($-.age+1)) AS age
      """
    Then a SemanticError should be raised at runtime: Agg function nesting is not allowed: `AVG(distinct COUNT(($-.age+1)))`
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | GROUP BY avg($-.age+1)+1 YIELD $-.age,avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime:  Group `(AVG(($-.age+1))+1)` invalid
    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age
      | YIELD $var.dst AS dst, avg(distinct $-.age) AS age
      """
    Then a SemanticError should be raised at runtime: Not support both input and variable in GroupBy sentence.
    When executing query:
      """
      $var1=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst;
      $var2=GO FROM "Tim Duncan" OVER serve YIELD serve._dst AS dst;
      YIELD count($var1.dst),$var2.dst AS count
      """
    Then a SemanticError should be raised at runtime: Only one variable allowed to use.
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
