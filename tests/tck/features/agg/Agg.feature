Feature: Basic Agg and GroupBy

  Background:
    Given a graph with space named "nba"

  Scenario: Basic Agg
    When executing query:
      """
      YIELD count(*)+1 ,1+2 ,(INT)abs(count(2))
      """
    Then the result should be, in any order:
      | (COUNT(*)+1) | (1+2) | (INT)abs(count(2)) |
      |      2       |   3   |          1         |

  Scenario: Basic GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age 
      | YIELD count(*) AS count

      """
    Then the result should be, in any order, with relax comparision:
      | count |
      |   2   |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD $-.dst AS dst, avg(distinct $-.age) AS age

      """
    Then the result should be, in any order, with relax comparision:
      |       dst       | age |
      | "Tony Parker"   |  41 | 
      | "Manu Ginobili" |  2  |

    When executing query:
      """
       $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age; YIELD $var.dst AS dst, avg(distinct $var.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
      |       dst       | age |
      | "Tony Parker"   |  41 | 
      | "Manu Ginobili" |  2  |

  Scenario: Implicit GroupBy
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age 
      | YIELD $-.dst AS dst, 1+avg(distinct $-.age) AS age

      """
    Then the result should be, in any order, with relax comparision:
      |       dst       | age |
      | "Tony Parker"   |  41 | 
      | "Manu Ginobili" |  2  |

  Scenario: Error Check
    When executing query:
      """
      YIELD avg(*)+1 ,1+2 ,(INT)abs(min(2))
      """
    Then the result should be, in any order:
      SemanticError: `AVG(*)` invaild, * valid in count.

    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | YIELD $-.dst, $$.player.age AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
     SemanticError: Only support input and variable in GroupBy sentence.

    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst YIELD avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
     SemanticError: GROUP BY item `$-.dst` must be in the yield list
     
    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.dst,$-.x YIELD avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
     SemanticError: `$-.x', not exist prop `x'

    When executing query:
      """
      GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age | GROUP BY $-.age+1 YIELD $-.age+1,age,avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
     SemanticError: ---

    When executing query:
      """
      $var=GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age;GO FROM "Tim Duncan" OVER like YIELD like._dst AS dst, $$.player.age AS age YIELD $var.dst AS dst, avg(distinct $-.age) AS age
      """
    Then the result should be, in any order, with relax comparision:
     SemanticError: ---
     