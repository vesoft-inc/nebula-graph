Feature: With clause and Unwind clause

  Background:
    Given a graph with space named "nba"

  Scenario: with return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order:
      | a       | b       |
      | [1,2,3] | "hello" |
    When executing query:
      """
      WITH [1, 2, 3] AS a
      WITH a AS a, "hello" AS b
      RETURN a, b
      """
    Then the result should be, in any order, with relax comparison:
      | a       | b       |
      | [1,2,3] | "hello" |

  Scenario: unwind return
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a |
      | 1 |
      | 2 |
      | 3 |
    When executing query:
      """
      UNWIND [1, NULL, 3] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | 1    |
      | NULL |
      | 3    |
    When executing query:
      """
      UNWIND [1, [2, 3, NULL, 4], 5] AS a
      RETURN a
      """
    Then the result should be, in any order:
      | a               |
      | 1               |
      | [2, 3, NULL, 4] |
      | 5               |
    When executing query:
      """
      UNWIND [1, [2, 3, NULL, 4], 5] AS a
      UNWIND a AS b
      RETURN b
      """
    Then the result should be, in any order:
      | b    |
      | 1    |
      | 2    |
      | 3    |
      | NULL |
      | 4    |
      | 5    |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      UNWIND [4, 5] AS b
      RETURN b, a
      """
    Then the result should be, in any order:
      | b | a |
      | 4 | 1 |
      | 5 | 1 |
      | 4 | 2 |
      | 5 | 2 |
      | 4 | 3 |
      | 5 | 3 |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      UNWIND [5, 4] AS b
      UNWIND [8, 9] AS c
      RETURN c, b, a
      """
    Then the result should be, in any order, with relax comparison:
      | c | b | a |
      | 8 | 5 | 1 |
      | 9 | 5 | 1 |
      | 8 | 4 | 1 |
      | 9 | 4 | 1 |
      | 8 | 5 | 2 |
      | 9 | 5 | 2 |
      | 8 | 4 | 2 |
      | 9 | 4 | 2 |
      | 8 | 5 | 3 |
      | 9 | 5 | 3 |
      | 8 | 4 | 3 |
      | 9 | 4 | 3 |

  Scenario: with unwind return
    When executing query:
      """
      WITH [1, 2, 3] AS a, "hello" AS b
      UNWIND a as c
      RETURN c
      """
    Then the result should be, in any order, with relax comparison:
      | c |
      | 1 |
      | 2 |
      | 3 |
    When executing query:
      """
      UNWIND [1, 2, 3] AS a
      WITH "hello" AS b, a AS c
      UNWIND c as d
      RETURN b, c, d
      """
    Then the result should be, in any order, with relax comparison:
      | b       | c | d |
      | "hello" | 1 | 1 |
      | "hello" | 2 | 2 |
      | "hello" | 3 | 3 |

  Scenario: with match return
    When executing query:
      """
      WITH "Yao Ming" AS a
      MATCH (v:player) WHERE v.name == a
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v            |
      | ("Yao Ming") |
    When executing query:
      """
      WITH 38 as a
      MATCH (v:player) WHERE v.age == a
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name         | Age |
      | "David West" | 38  |
      | "Paul Gasol" | 38  |
      | "Yao Ming"   | 38  |
    When executing query:
      """
      WITH "Tony Parker" AS a, 38 AS b
      MATCH (v:player) WHERE v.name == a OR v.age == b
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name          | Age |
      | "David West"  | 38  |
      | "Paul Gasol"  | 38  |
      | "Yao Ming"    | 38  |
      | "Tony Parker" | 36  |

  @skip
  Scenario: with match return
    When executing query:
      """
      WITH "Tony Parker" AS a
      MATCH (v:player{name: a})
      RETURN v.age AS age
      """
    Then the result should be, in any order, with relax comparison:
      | age |
      | 23  |

  Scenario: unwind match return
    When executing query:
      """
      UNWIND ["Tony Parker", "Yao Ming"] AS a
      MATCH (v:player) WHERE v.name == a
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v               |
      | ("Yao Ming")    |
      | ("Tony Parker") |
    When executing query:
      """
      UNWIND [36, 37] AS a
      MATCH (v:player) WHERE v.age == a
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                      |
      | ("Amar'e Stoudemire" ) |
      | ("Boris Diaw")         |
      | ("Tony Parker")        |
      | ("Dwyane Wade")        |
    When executing query:
      """
      UNWIND [36, 37] AS a
      UNWIND [38] AS b
      MATCH (v:player) WHERE v.age == a
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v                      |
      | ("Amar'e Stoudemire" ) |
      | ("Boris Diaw")         |
      | ("Tony Parker")        |
      | ("Dwyane Wade")        |

  @skip
  Scenario: unwind match return
    When executing query:
      """
      UNWIND ["Tony Parker", "Yao Ming"] AS a
      MATCH (v:player{name:a})
      RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v               |
      | ("Yao Ming")    |
      | ("Tony Parker") |

  Scenario: with unwind match return
    When executing query:
      """
      UNWIND [36, 37] AS a
      WITH "hello" AS b, a AS a
      MATCH (v:player) WHERE v.age == a
      RETURN v, b, a
      """
    Then the result should be, in any order, with relax comparison:
      | v                      | b       | a  |
      | ("Amar'e Stoudemire" ) | "hello" | 36 |
      | ("Boris Diaw")         | "hello" | 36 |
      | ("Tony Parker")        | "hello" | 36 |
      | ("Dwyane Wade")        | "hello" | 37 |
    When executing query:
      """
      UNWIND [36, 37] AS a
      WITH "hello" AS b, a AS a
      UNWIND ["Tony Parker", "Yao Ming"] AS c
      MATCH (v:player) WHERE v.age == a
      RETURN v, b, a
      """
    Then the result should be, in any order, with relax comparison:
      | v                     | b       | a  |
      | ("Tony Parker")       | "hello" | 36 |
      | ("Boris Diaw")        | "hello" | 36 |
      | ("Amar'e Stoudemire") | "hello" | 36 |
      | ("Tony Parker")       | "hello" | 36 |
      | ("Boris Diaw")        | "hello" | 36 |
      | ("Amar'e Stoudemire") | "hello" | 36 |
      | ("Dwyane Wade")       | "hello" | 37 |
      | ("Dwyane Wade")       | "hello" | 37 |
