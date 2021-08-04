# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Delete string vid of tag

  Scenario: delete string vid one vertex one tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"   | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG "Tim Duncan" player;
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"   | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |

  Scenario: delete string vid one vertex multiple tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"   | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG "Tim Duncan" player, bachelor;
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |

  Scenario: delete string vid one vertex all tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
      | "Tim Duncan" | "Tim Duncan"   | "psychology"        |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |
      | "Tim Duncan" |
    # delete one tag
    When executing query:
      """
      DELETE TAG "Tim Duncan" *;
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
    When executing query:
      """
      FETCH PROP ON bachelor "Tim Duncan" YIELD bachelor.name, bachelor.speciality
      """
    Then the result should be, in any order:
      | VertexID     | bachelor.name  | bachelor.speciality |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |

  Scenario: delete string vid multiple vertex one tag
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
      | "Tim Duncan" | "Tim Duncan" | 42         |
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID      | player.name   | player.age |
      | "Tony Parker" | "Tony Parker" | 36         |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |
      | "Tim Duncan" |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker"
      """
    Then the result should be, in any order:
      | VertexID     |
      | "Tony Parker" |
    # delete one tag
    When executing query:
      """
      DELETE TAG "Tim Duncan", "Tony Parker" player;
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON player "Tim Duncan" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
    When executing query:
      """
      FETCH PROP ON player "Tony Parker" YIELD player.name, player.age
      """
    Then the result should be, in any order:
      | VertexID     | player.name  | player.age |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tim Duncan"
      """
    Then the result should be, in any order:
      | VertexID     |
    When executing query:
      """
      LOOKUP ON player WHERE player.name == "Tony Parker"
      """
    Then the result should be, in any order:
      | VertexID     |

  Scenario: delete string vid from pipe
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order:
      | id         |
      | "Spurs"    |
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name  |
      | "Spurs"  | "Spurs"    |
    # delete one tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id | DELETE TAG $-.id team
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name  |

  Scenario: delete string vid from var
    Given an empty graph
    And load "nba" csv data to a new space
    # before delete tag
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id
      """
    Then the result should be, in any order:
      | id         |
      | "Spurs"    |
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name  |
      | "Spurs"  | "Spurs"    |
    # delete one tag
    When executing query:
      """
      $var = GO FROM "Tim Duncan" OVER serve YIELD serve._dst as id; DELETE TAG $var.id team
      """
    Then the execution should be successful
    # after delete tag
    When executing query:
      """
      FETCH PROP ON team "Spurs" YIELD team.name
      """
    Then the result should be, in any order:
      | VertexID | team.name  |