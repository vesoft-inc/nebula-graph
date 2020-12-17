Feature: Fetch Vertices

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: [1] Fetch Vertices 
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [2] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name | player.age | (player.age>30) |
      | -7391649757245641883 | Boris Diaw  | 36         | true            |

  Scenario: [3] Fetch Vertices
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name | player.age |
      | -7579316172763586624 | Tony Parker | 36         |
      | 5662213458193308137  | Tim Duncan  | 42         |

  Scenario: [4] Fetch Vertices
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name   | player.age | $-.id                |
      | -7579316172763586624 | "Tony Parker" | 36         | -7579316172763586624 |
      | 5662213458193308137  | "Tim Duncan"  | 42         | 5662213458193308137  |

  Scenario: [5] Fetch Vertices
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name   | player.age |
      | -7579316172763586624 | "Tony Parker" | 36         |
      | 5662213458193308137  | "Tim Duncan"  | 42         |

  Scenario: [6] Fetch Vertices
    When executing query:
      """
      $var = GO FROM hash('Boris Diaw') over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name as name, player.age | ORDER BY name
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | name          | player.age |
      | 5662213458193308137  | "Tim Duncan"  | 42         |
      | -7579316172763586624 | "Tony Parker" | 36         |

  Scenario: [7] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [8] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw') YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757168799460 | "Boris Diaw" | 36         |

  Scenario: [9] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [10] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [11] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player uuid('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757168799460 | "Boris Diaw" | 36         |

  Scenario: [12] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [13] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw'), hash('Boris Diaw') YIELD DISTINCT player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.age |
      | -7391649757245641883 | 36         |

  Scenario: [14] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: Unsupported src/dst property expression in yield.

  Scenario: [15] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime: Unsupported src/dst property expression in yield.

  Scenario: [16] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD abc.name, player.age
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [17] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON abc hash('Boris Diaw')
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [18] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('NON EXIST VERTEX ID')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID |

  Scenario: [19] Fetch Vertices
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') OVER serve | FETCH PROP ON team $-
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID |

  Scenario: [20] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('NON EXIST VERTEX ID')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID |

  Scenario: [21] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparision:
   |VertexID| player.name| player.age|
|-7391649757245641883|"Boris Diaw"| 36|

  Scenario: [22] Fetch Vertices
    When executing query:
      """
      YIELD hash('Boris Diaw') as id | FETCH PROP ON * $-.id
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [23] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('Boris Diaw'), hash('Boris Diaw')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.age |
      | -7391649757245641883 | "Boris Diaw" | 36         |
      | -7391649757245641883 | "Boris Diaw" | 36         |

  Scenario: [24] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON bachelor hash('Tim Duncan')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID            | bachelor.name | bachelor.speciality |
      | 5662213458193308137 | "Tim Duncan"  | "psychology"        |

  Scenario: [25] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID            | player.name  | player.age | bachelor.name | bachelor.speciality |
      | 5662213458193308137 | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: [26] Fetch Vertices
    When executing query:
      """
      YIELD hash('Tim Duncan') as id | FETCH PROP ON * $-.id
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID            | player.name  | player.age | bachelor.name | bachelor.speciality |
      | 5662213458193308137 | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: [27] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan'), hash('Tim Duncan')
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID            | player.name  | player.age | bachelor.name | bachelor.speciality |
      | 5662213458193308137 | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
      | 5662213458193308137 | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |

  Scenario: [28] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON * hash('Tim Duncan'), hash('Tim Duncan') YIELD player.name, player.age
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID            | player.name  | player.age |
      | 5662213458193308137 | "Tim Duncan" | 42         |
      | 5662213458193308137 | "Tim Duncan" | 42         |

  Scenario: [29] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name, player.name
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID             | player.name  | player.name  |
      | -7391649757245641883 | "Boris Diaw" | "Boris Diaw" |

  Scenario: [30] Fetch Vertices
    When executing query:
      """
      GO FROM hash('Boris Diaw') over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [31] Fetch Vertices
    When executing query:
      """
      FETCH PROP ON player hash('Boris Diaw') YIELD player.name1
      """
    Then a ExecutionError should be raised at runtime:

  Scenario: [32] Fetch Vertices
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over like YIELD like._dst as id | FETCH PROP ON player $-.id
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID |

  Scenario: [33] Fetch Vertices
    When executing query:
      """
      GO FROM hash('NON EXIST VERTEX ID') over serve YIELD serve._dst as id, serve.start_year as start | YIELD $-.id as id WHERE $-.start > 20000 | FETCH PROP ON player $-.id
      """
    Then the result should be, in any order, with relax comparision:
      | VertexID |
