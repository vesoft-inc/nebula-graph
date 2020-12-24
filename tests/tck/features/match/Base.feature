# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Basic match

  Scenario: one step
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player)
      WHERE v.age >= 38 AND v.age < 45
      RETURN v.name AS Name, v.age AS Age
      """
    Then the result should be, in any order:
      | Name            | Age |
      | 'Paul Gasol'    | 38  |
      | 'Kobe Bryant'   | 40  |
      | 'Vince Carter'  | 42  |
      | 'Tim Duncan'    | 42  |
      | 'Yao Ming'      | 38  |
      | 'Dirk Nowitzki' | 40  |
      | 'Manu Ginobili' | 41  |
      | 'Ray Allen'     | 43  |
      | 'David West'    | 38  |
      | 'Tracy McGrady' | 39  |
    When executing query:
      """
      MATCH (v:player {age: 29})
      RETURN v.name AS Name
      """
    Then the result should be, in any order:
      | Name               |
      | 'James Harden'     |
      | 'Jonathon Simmons' |
      | 'Klay Thompson'    |
      | 'Dejounte Murray'  |

  Scenario: [1] one step given tag without property
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player{name:"Tim Duncan"})-->(v2:team)
      RETURN v2 AS Team
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |

  @skip
  Scenario: [2] one step given tag without property
    When executing query:
      """
      MATCH (v:team{name:"Spurs"})--(v2)
      RETURN v2
      """
    Then the result should be, in any order:
      | "v2"                                                               |
      | {"name":"Boris Diaw","age":36,"player":"Boris Diaw"}               |
      | {"name":"Kyle Anderson","age":25,"player":"Kyle Anderson"}         |
      | {"name":"Cory Joseph","age":27,"player":"Cory Joseph"}             |
      | {"name":"Tiago Splitter","age":34,"player":"Tiago Splitter"}       |
      | {"name":"LaMarcus Aldridge","age":33,"player":"LaMarcus Aldridge"} |
      | {"name":"Paul Gasol","age":38,"player":"Paul Gasol"}               |
      | {"name":"Marco Belinelli","age":32,"player":"Marco Belinelli"}     |
      | {"name":"Tracy McGrady","age":39,"player":"Tracy McGrady"}         |
      | {"name":"David West","age":38,"player":"David West"}               |
      | {"name":"Manu Ginobili","age":41,"player":"Manu Ginobili"}         |
      | {"name":"Tony Parker","age":36,"player":"Tony Parker"}             |
      | {"name":"Rudy Gay","age":32,"player":"Rudy Gay"}                   |
      | {"name":"Jonathon Simmons","age":29,"player":"Jonathon Simmons"}   |
      | {"name":"Aron Baynes","age":32,"player":"Aron Baynes"}             |
      | {"name":"Danny Green","age":31,"player":"Danny Green"}             |
      | {"name":"Tim Duncan","age":42,"player":"Tim Duncan"}               |
      | {"name":"Marco Belinelli","age":32,"player":"Marco Belinelli"}     |
      | {"name":"Dejounte Murray","age":29,"player":"Dejounte Murray"}     |

  @skip
  Scenario: multi steps given tag without property
    Given a graph with space named "nba"
    When executing query:
      """
      MATCH (v:player{name: "Tim Duncan"})-->(v2:team)<--(v3)
      RETURN v2
      """
    Then the result should be, in any order:
      | Team                           |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
      | ("Spurs" :team{name: "Spurs"}) |
