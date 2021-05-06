Feature: Fetch String Vertices
  Examples:
    | space_name  | hash_begin  | hash_end |
    | nba         |             |          |
    | nba_int_vid | hash(       |    )     |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: Fetch prop on one tag, one vertex
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         |
    # Fetch Vertices yield duplicate columns
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD player.name, player.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.name  |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | "Boris Diaw" |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD player.name, player.age, player.age > 30
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | (player.age>30) |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         | True            |
    # Fetch Vertices without YIELD
    When executing query:
      """
      FETCH PROP ON bachelor <hash_begin>'Tim Duncan'<hash_end>
      """
    Then the result should be, in any order:
      | vertices_                                                                                |
      | (<hash_begin>"Tim Duncan"<hash_end>:bachelor{name:"Tim Duncan",speciality:"psychology"}) |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end>
      """
    Then the result should be, in any order:
      | vertices_                                                              |
      | (<hash_begin>"Boris Diaw"<hash_end>:player{name:"Boris Diaw", age:36}) |

  Scenario: Fetch Vertices works with ORDER BY
    When executing query:
      """
      $var = GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id;
      FETCH PROP ON player $var.id YIELD player.name as name, player.age |
      ORDER BY name
      """
    Then the result should be, in order:
      | VertexID                            | name          | player.age |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         |

  Scenario: Fetch Vertices works with DISTINCT
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end>, <hash_begin>'Boris Diaw'<hash_end> YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>"Boris Diaw"<hash_end>, <hash_begin>"Tony Parker"<hash_end> YIELD DISTINCT player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age |
      | <hash_begin>"Boris Diaw"<hash_end>  | "Boris Diaw"  | 36         |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end>, <hash_begin>'Boris Diaw'<hash_end> YIELD DISTINCT player.age
      """
    Then the result should be, in any order:
      | VertexID                           | player.age |
      | <hash_begin>"Boris Diaw"<hash_end> | 36         |

  Scenario: Fetch prop on multiple tags, multiple vertices
    When executing query:
      """
      FETCH PROP ON bachelor, team, player <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Boris Diaw"<hash_end> YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      FETCH PROP ON player, team <hash_begin>"Tim Duncan"<hash_end>,<hash_begin>"Boris Diaw"<hash_end> YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | team.name |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | EMPTY     |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON player, team <hash_begin>"Boris Diaw"<hash_end>,<hash_begin>"Boris Diaw"<hash_end> YIELD player.name, team.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | team.name |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | EMPTY     |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | EMPTY     |
    When executing query:
      """
      FETCH PROP ON team, player, bachelor <hash_begin>"Boris Diaw"<hash_end> YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON player, team, bachelor <hash_begin>"Tim Duncan"<hash_end> YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | team.name | bachelor.name |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | EMPTY     | "Tim Duncan"  |

  Scenario: Fetch prop on not existing vertex
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'NON EXIST VERTEX ID'<hash_end> yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'NON EXIST VERTEX ID'<hash_end>, <hash_begin>"Boris Diaw"<hash_end> yield player.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" |

  Scenario: Fetch prop on *
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'Boris Diaw'<hash_end> yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'Boris Diaw'<hash_end> yield player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      FETCH PROP ON * <hash_begin>"Tim Duncan"<hash_end> YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
    # Fetch prop on * with not existing vertex
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'NON EXIST VERTEX ID'<hash_end> yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'NON EXIST VERTEX ID'<hash_end>, <hash_begin>'Boris Diaw'<hash_end> yield player.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" |
    # Fetch prop on * with multiple vertices
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'Boris Diaw'<hash_end>, <hash_begin>'Boris Diaw'<hash_end> yield player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         |
    When executing query:
      """
      FETCH PROP ON * <hash_begin>'Tim Duncan'<hash_end>, <hash_begin>'Boris Diaw'<hash_end> YIELD player.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | bachelor.name |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | "Tim Duncan"  |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | EMPTY         |
    When executing query:
      """
      FETCH PROP ON * <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Boris Diaw"<hash_end> YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | <hash_begin>"Boris Diaw"<hash_end> | "Boris Diaw" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch from pipe
    # Fetch prop on one tag of vertices from pipe
    When executing query:
      """
      GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         |
    When executing query:
      """
      GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id
      | FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch prop on multi tags of vertices from pipe
    When executing query:
      """
      GO FROM <hash_begin>"Boris Diaw"<hash_end> over like YIELD like._dst as id
      | FETCH PROP ON player, team, bachelor $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
    When executing query:
      """
      GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id
      | FETCH PROP ON player, bachelor $-.id YIELD player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         | EMPTY         | EMPTY               |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         | "Tim Duncan"  | "psychology"        |
    # Fetch vertices with empty input
    When executing query:
      """
      GO FROM <hash_begin>'NON EXIST VERTEX ID'<hash_end> over like YIELD like._dst as id | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    When executing query:
      """
      GO FROM <hash_begin>'NON EXIST VERTEX ID'<hash_end> over serve YIELD serve._dst as id, serve.start_year as start
      | YIELD $-.id as id WHERE $-.start > 20000 | FETCH PROP ON player $-.id yield player.name
      """
    Then the result should be, in any order:
      | VertexID | player.name |
    # Fetch * from input
    When executing query:
      """
      YIELD <hash_begin>'Tim Duncan'<hash_end> as id | FETCH PROP ON * $-.id yield player.name, player.age, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                           | player.name  | player.age | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tim Duncan"<hash_end> | "Tim Duncan" | 42         | "Tim Duncan"  | "psychology"        |
    When executing query:
      """
      GO FROM <hash_begin>"Boris Diaw"<hash_end> over like YIELD like._dst as id
      | FETCH PROP ON * $-.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |

  Scenario: fetch from varibles
    When executing query:
      """
      $var = GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id; FETCH PROP ON player $var.id YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         |
    When executing query:
      """
      $var = GO FROM <hash_begin>"Boris Diaw"<hash_end> over like YIELD like._dst as id; FETCH PROP ON * $var.id YIELD player.name, team.name, bachelor.name
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | team.name | bachelor.name |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | EMPTY     | EMPTY         |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | EMPTY     | "Tim Duncan"  |
    # Fetch prop on multi tags of vertices from variable
    When executing query:
      """
      $var = GO FROM <hash_begin>"Boris Diaw"<hash_end> over like YIELD like._dst as id;
      FETCH PROP ON player, team, bachelor $var.id YIELD player.name, player.age, team.name, bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID                            | player.name   | player.age | team.name | bachelor.name | bachelor.speciality |
      | <hash_begin>"Tim Duncan"<hash_end>  | "Tim Duncan"  | 42         | EMPTY     | "Tim Duncan"  | "psychology"        |
      | <hash_begin>"Tony Parker"<hash_end> | "Tony Parker" | 36         | EMPTY     | EMPTY         | EMPTY               |

  Scenario: Fetch and Yield id(v)
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end>, <hash_begin>'Tony Parker'<hash_end> | YIELD id($-.vertices_) as id
      """
    Then the result should be, in any order:
      | id                                  |
      | <hash_begin>"Boris Diaw"<hash_end>  |
      | <hash_begin>"Tony Parker"<hash_end> |

  @skip
  Scenario: Output fetch result to graph traverse
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'NON EXIST VERTEX ID'<hash_end> | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>"Tim Duncan"<hash_end> | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst                             |
      | <hash_begin>"Manu Ginobili"<hash_end> |
      | <hash_begin>"Tony Parker"<hash_end>   |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Yao Ming"<hash_end> | go from id($-.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst                               |
      | <hash_begin>"Shaquile O'Neal"<hash_end> |
      | <hash_begin>"Tracy McGrady"<hash_end>   |
      | <hash_begin>"Manu Ginobili"<hash_end>   |
      | <hash_begin>"Tony Parker"<hash_end>     |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>"Tim Duncan"<hash_end> yield player.name as id | go from $-.id over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst                             |
      | <hash_begin>"Manu Ginobili"<hash_end> |
      | <hash_begin>"Tony Parker"<hash_end>   |
    When executing query:
      """
      $var = FETCH PROP ON player <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Yao Ming"<hash_end>; go from id($var.vertices_) over like yield like._dst
      """
    Then the result should be, in any order:
      | like._dst                               |
      | <hash_begin>"Shaquile O'Neal"<hash_end> |
      | <hash_begin>"Tracy McGrady"<hash_end>   |
      | <hash_begin>"Manu Ginobili"<hash_end>   |
      | <hash_begin>"Tony Parker"<hash_end>     |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Tony Parker'<hash_end> YIELD player.name as Name
      | GO FROM $-.Name OVER like
      """
    Then the result should be, in any order:
      | like._dst                                 |
      | <hash_begin>"LaMarcus Aldridge"<hash_end> |
      | <hash_begin>"Manu Ginobili"<hash_end>     |
      | <hash_begin>"Tim Duncan"<hash_end>        |
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Tony Parker'<hash_end> YIELD player.name as Name
      | GO FROM $-.VertexID OVER like
      """
    Then the result should be, in any order:
      | like._dst                                 |
      | <hash_begin>"LaMarcus Aldridge"<hash_end> |
      | <hash_begin>"Manu Ginobili"<hash_end>     |
      | <hash_begin>"Tim Duncan"<hash_end>        |

  Scenario: Typical errors
    # Fetch Vertices not support get src property
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD $^.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    # Fetch Vertices not support get dst property
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD $$.player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    # Fetch vertex yields not existing tag
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD not_exist_tag.name, player.age
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON * <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Boris Diaw"<hash_end> YIELD not_exist_tag.name
      """
    Then a ExecutionError should be raised at runtime:
    # Fetch prop no not existing tag
    When executing query:
      """
      FETCH PROP ON not_exist_tag <hash_begin>'Boris Diaw'<hash_end>
      """
    Then a ExecutionError should be raised at runtime:
    When executing query:
      """
      GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id, like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      GO FROM <hash_begin>"11"<hash_end> over like YIELD like._dst as id | FETCH PROP ON player "11" YIELD $-.id
      """
    Then a SemanticError should be raised at runtime:
    # Fetch on existing vertex, and yield not existing property
    When executing query:
      """
      FETCH PROP ON player <hash_begin>'Boris Diaw'<hash_end> YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      FETCH PROP ON * <hash_begin>"Tim Duncan"<hash_end>, <hash_begin>"Boris Diaw"<hash_end> YIELD player.not_exist_prop
      """
    Then a SemanticError should be raised at runtime:

  Scenario: Different from v1.x
    When executing query:
      """
      GO FROM <hash_begin>'Boris Diaw'<hash_end> over like YIELD like._dst as id | FETCH PROP ON player $-.id YIELD player.name, player.age, $-.*
      """
    Then a SemanticError should be raised at runtime:
    # Different from 1.x $- is not supported
    When executing query:
      """
      GO FROM <hash_begin>'NON EXIST VERTEX ID'<hash_end> OVER serve | FETCH PROP ON team $-
      """
    Then a SyntaxError should be raised at runtime:
