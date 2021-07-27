# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: MapProjection

  Scenario: Property selector
    Given a graph with space named "nba"
    When executing query:
      """
      WITH {a:1, b:"hello", c:True} AS a
      RETURN a{.a, .b}
      """
    Then the result should be, in any order:
      | a                    |
      | {"a":1, "b":"hello"} |

  Scenario: Literal entry
    Given a graph with space named "nba"
    When executing query:
      """
      WITH {a:1, b:"hello", c:True} AS a
      RETURN a{d:18.5, e:False} AS a
      """
    Then the result should be, in any order:
      | a                     |
      | {"d":18.5, "e":false} |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age > 40
      RETURN v {name:v.name, age:v.age} AS b
      """
    Then the result should be, in any order:
      | b                                  |
      | {age: 47, name: "Shaquile O'Neal"} |
      | {age: 42, name: "Tim Duncan"}      |
      | {age: 41, name: "Manu Ginobili"}   |
      | {age: 43, name: "Ray Allen"}       |
      | {age: 45, name: "Jason Kidd"}      |
      | {age: 42, name: "Vince Carter"}    |
      | {age: 46, name: "Grant Hill"}      |
      | {age: 45, name: "Steve Nash"}      |
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age > 45
      RETURN v {age:abs(v.age+100), key:"best"} AS b
      """
    Then the result should be, in any order:
      | b                       |
      | {age: 146, key: "best"} |
      | {age: 147, key: "best"} |

  Scenario: Variable selector
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)-[:like]-(v2)
      WHERE v.age > 45
      RETURN v {v2}
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                                 |
      | {v2: ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})}                                                   |
      | {v2: ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})}                                                   |
      | {v2: ("Yao Ming" :player{age: 38, name: "Yao Ming"})}                                                             |
      | {v2: ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})}                                                     |
      | {v2: ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})} |

  Scenario: All-properties selector
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)-[:like]-(v2)
      WHERE v.age > 45
      RETURN v {.*}
      """
    Then the result should be, in any order, with relax comparison:
      | v                                  |
      | {age: 46, name: "Grant Hill"}      |
      | {age: 46, name: "Grant Hill"}      |
      | {age: 47, name: "Shaquile O'Neal"} |
      | {age: 47, name: "Shaquile O'Neal"} |
      | {age: 47, name: "Shaquile O'Neal"} |

  Scenario: Use a combination of Property selector, Literal entry, Variable selector, All-properties selector
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)-[:like]-(v2)
      WHERE v.age > 45
      RETURN v {.name, .*, middleaged: true, v2, playersHeLike: v2 {.*}}
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                                                                                                                                                             |
      | {age: 46, middleaged: true, name: "Grant Hill", playersHeLike: {age: 39, name: "Tracy McGrady"}, v2: ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})}                                                                               |
      | {age: 46, middleaged: true, name: "Grant Hill", playersHeLike: {age: 39, name: "Tracy McGrady"}, v2: ("Tracy McGrady" :player{age: 39, name: "Tracy McGrady"})}                                                                               |
      | {age: 47, middleaged: true, name: "Shaquile O'Neal", playersHeLike: {age: 38, name: "Yao Ming"}, v2: ("Yao Ming" :player{age: 38, name: "Yao Ming"})}                                                                                         |
      | {age: 47, middleaged: true, name: "Shaquile O'Neal", playersHeLike: {age: 31, name: "JaVale McGee"}, v2: ("JaVale McGee" :player{age: 31, name: "JaVale McGee"})}                                                                             |
      | {age: 47, middleaged: true, name: "Shaquile O'Neal", playersHeLike: {age: 42, name: "Tim Duncan", speciality: "psychology"}, v2: ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})} |
    # [bug]agg executed failed here now
    When executing query:
      """
      MATCH (v:player{name: "Tony Parker"})-[:like]->(v2:player)
      RETURN v {.name, .age, playersHeLike: collect(v2 {.name})}
      """
    Then the result should be, in any order, with relax comparison:
      | v                                                                                                                            |
      | {age: BAD_TYPE, name: BAD_TYPE, playersHeLike: [{name: "LaMarcus Aldridge"}, {name: "Manu Ginobili"}, {name: "Tim Duncan"}]} |
    When executing query:
      """
      WITH {a: 1, b: "hello", c: false} AS kvs
      RETURN kvs{.a, b: "world"} AS c
      """
    Then the result should be, in any order:
      | c                  |
      | {a: 1, b: "world"} |
    When executing query:
      """
      WITH {a: 1, b: "hello", c: false} AS kvs
      RETURN kvs{.a, b: "world"}["b"]
      """
    Then the result should be, in any order:
      | kvs["b"] |
      | "world"  |
    When executing query:
      """
      WITH {a: 1, b: "hello", c: false} AS kvs
      RETURN kvs{.a, b: "world"}["b"]
      """
    Then the result should be, in any order:
      | kvs["b"] |
      | "world"  |
    When executing query:
      """
      MATCH (v:player)-[e:like]-()
      WHERE id(v) == "Tony Parker"
      RETURN e { .likeness }
      """
    Then the result should be, in any order, with relax comparison:
      | e              |
      | {likeness: 80} |
      | {likeness: 99} |
      | {likeness: 75} |
      | {likeness: 50} |
      | {likeness: 95} |
      | {likeness: 90} |
      | {likeness: 95} |
      | {likeness: 95} |
    When executing query:
      """
      MATCH (v:player)-[e:like]-(v2)
      WHERE id(v) == "Tony Parker"
      RETURN e { .likeness, v2}
      """
    Then the result should be, in any order, with relax comparison:
      | e                                                                                                                               |
      | {likeness: 80, v2: ("Boris Diaw" :player{age: 36, name: "Boris Diaw"})}                                                         |
      | {likeness: 99, v2: ("Dejounte Murray" :player{age: 29, name: "Dejounte Murray"})}                                               |
      | {likeness: 75, v2: ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})}                                           |
      | {likeness: 50, v2: ("Marco Belinelli" :player{age: 32, name: "Marco Belinelli"})}                                               |
      | {likeness: 95, v2: ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})} |
      | {likeness: 90, v2: ("LaMarcus Aldridge" :player{age: 33, name: "LaMarcus Aldridge"})}                                           |
      | {likeness: 95, v2: ("Manu Ginobili" :player{age: 41, name: "Manu Ginobili"})}                                                   |
      | {likeness: 95, v2: ("Tim Duncan" :bachelor{name: "Tim Duncan", speciality: "psychology"} :player{age: 42, name: "Tim Duncan"})} |

  Scenario: map_variable is not a Vertex/Edge/Map
    Given a graph with space named "nba"
    When executing query:
      """
      WITH 1 AS v
      RETURN v {.*}
      """
    Then the result should be, in any order, with relax comparison:
      | v        |
      | BAD_TYPE |
