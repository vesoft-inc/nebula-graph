# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Reduce

  Scenario: yield a reduce
    Given a graph with space named "nba"
    When executing query:
      """
      YIELD reduce(totalNum = 10, n IN range(1, 3) | totalNum + n) AS r
      """
    Then the result should be, in any order:
      | r  |
      | 16 |
    When executing query:
      """
      YIELD reduce(totalNum = -4 * 5, n IN [1, 2] | totalNum + n * 2) AS r
      """
    Then the result should be, in any order:
      | r   |
      | -14 |

  Scenario: use a reduce in GO
    Given a graph with space named "nba"
    When executing query:
      """
      GO FROM "Tony Parker" OVER like
      WHERE like.likeness != reduce(totalNum = 5, n IN range(1, 3) | $$.player.age + totalNum + n)
      YIELD like._dst AS id, $$.player.age AS age, like.likeness AS likeness
      """
    Then the result should be, in any order:
      | id                  | age | likeness |
      | "Manu Ginobili"     | 41  | 95       |
      | "Tim Duncan"        | 42  | 95       |
      | "LaMarcus Aldridge" | 33  | 90       |

  Scenario: use a reduce in MATCH
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})<-[:like]-(m)
      RETURN
        nodes(p)[0].age AS age1,
        nodes(p)[1].age AS age2,
        reduce(totalAge = 100, n IN nodes(p) | totalAge + n.age) as r
      """
    Then the result should be, in any order:
      | age1 | age2 | r   |
      | 34   | 34   | 168 |
      | 34   | 33   | 167 |
      | 34   | 31   | 165 |
      | 34   | 29   | 163 |
      | 34   | 37   | 171 |
      | 34   | 26   | 160 |
    When executing query:
      """
      MATCH p = (n:player{name:"LeBron James"})-[:like]->(m)
      RETURN nodes(p)[0].age AS age1,
             nodes(p)[1].age AS age2,
             reduce(x = 10, n IN nodes(p) | n.age - x) as x
      """
    Then the result should be, in any order:
      | age1 | age2 | x  |
      | 34   | 43   | 19 |
