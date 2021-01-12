# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: In and not in expressions

  Background:
    Given a graph with space named "nba"

  Scenario: in list
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN ['Tim Duncan', 'Danny Green']
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | 'Tim Duncan'   |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN ['Danny Green']
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness IN [95,56,21]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Tim Duncan'    |
      | 'Manu Ginobili' |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID |
      GO FROM $-.ID OVER like WHERE like.likeness IN [95,56,21]
      """
    Then the result should be, in any order:
      | like._dst       |
      | 'Tony Parker'   |
      | 'Manu Ginobili' |

  Scenario: basic yield in expression
    When executing query:
      """
      YIELD 1 IN [1, 2, 3]
      """
    Then the result should be, in any order:
      | (1 IN [1, 2, 3]) |
      | true             |
    When executing query:
      """
      YIELD 0 IN [1, 2, 3]
      """
    Then the result should be, in any order:
      | (0 IN [1, 2, 3]) |
      | false            |
    When executing query:
      """
      YIELD 'hello' IN ['hello', 'world', 3]
      """
    Then the result should be, in any order:
      | ('hello' IN ['hello', 'world', 3]) |
      | true                               |

  Scenario: not in list
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN ['Danny Green']
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN [95,56,21]
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' |            90 |
    When executing query:
      """
      $A = GO FROM 'Tony Parker' OVER like YIELD like._dst AS ID;
      GO FROM $A.ID OVER like WHERE like.likeness NOT IN [95,56,21]
      YIELD $^.player.name, $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $^.player.name      | $$.player.name | like.likeness |
      | 'Manu Ginobili'     | 'Tim Duncan'   |            90 |
      | 'LaMarcus Aldridge' | 'Tim Duncan'   |            75 |
      | 'LaMarcus Aldridge' | 'Tony Parker'  |            75 |

  Scenario: basic yield not in expression
    When executing query:
      """
      YIELD 1 NOT IN [1, 2, 3]
      """
    Then the result should be, in any order:
      | (1 NOT IN [1, 2, 3]) |
      | false                |
    When executing query:
      """
      YIELD 0 NOT IN [1, 2, 3]
      """
    Then the result should be, in any order:
      | (0 NOT IN [1, 2, 3]) |
      | true                 |
    When executing query:
      """
      YIELD 'hello' NOT IN ['hello', 'world', 3]
      """
    Then the result should be, in any order:
      | ('hello' NOT IN ['hello', 'world', 3]) |
      | false                                  |

  Scenario: in set
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN {'Tim Duncan', 'Danny Green'}
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name |
      | 'Tim Duncan'   |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst IN {'Danny Green'}
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness IN {95,56,21,95,90}
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' |            90 |
      | 'Manu Ginobili'     |            95 |
      | 'Tim Duncan'        |            95 |

  Scenario: basic yield in set
    When executing query:
      """
      YIELD 1 IN {1, 2, 3}
      """
    Then the result should be, in any order:
      | (1 IN {1, 2, 3}) |
      | true             |
    When executing query:
      """
      YIELD 0 IN {1, 2, 3, 1, 2}
      """
    Then the result should be, in any order:
      | (0 IN {1, 2, 3, 1, 2}) |
      | false                  |
    When executing query:
      """
      YIELD 'hello' IN {'hello', 'world', 3}
      """
    Then the result should be, in any order:
      | ('hello' IN {'hello', 'world', 3}) |
      | true                               |

  Scenario: not in set
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      YIELD $$.player.name
      """
    Then the result should be, in any order:
      | $$.player.name      |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like._dst NOT IN {'Danny Green'}
      """
    Then the result should be, in any order:
      | like._dst           |
      | 'LaMarcus Aldridge' |
      | 'Manu Ginobili'     |
      | 'Tim Duncan'        |
    When executing query:
      """
      GO FROM 'Tony Parker' OVER like
      WHERE like.likeness NOT IN {95,56,21}
      YIELD $$.player.name, like.likeness
      """
    Then the result should be, in any order:
      | $$.player.name      | like.likeness |
      | 'LaMarcus Aldridge' |            90 |

  Scenario: basic yield not in set
    When executing query:
      """
      YIELD 1 NOT IN {1, 2, 3}
      """
    Then the result should be, in any order:
      | (1 NOT IN {1, 2, 3}) |
      | false                |
    When executing query:
      """
      YIELD 0 NOT IN {1, 2, 3, 1, 2}
      """
    Then the result should be, in any order:
      | (0 NOT IN {1, 2, 3, 1, 2}) |
      | true                       |
    When executing query:
      """
      YIELD 'hello' NOT IN {'hello', 'world', 3}
      """
    Then the result should be, in any order:
      | ('hello' NOT IN {'hello', 'world', 3}) |
      | false                                  |
