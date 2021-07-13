# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Map4 - Field existence check

  Background:
    Given any graph

  Scenario Outline: [1] `exists()` with literal maps
    Given any graph
    When executing query:
      """
      WITH <map> AS map
      RETURN exists(map.<key>) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |

    Examples:
      | map                              | key   | result |
      | {name: 'Mats', name2: 'Pontus'}  | name  | true   |
      | {name: 'Mats', name2: 'Pontus'}  | name2 | true   |
      | {name: null}                     | name  | true   |
      | {name: null, name2: 'Pontus'}    | name  | true   |
      | {name: null, name2: null}        | name  | true   |
      | {name: null, name2: null}        | name2 | true   |
      | {name: 'Pontus', name2: null}    | name2 | true   |
      | {name: 'Pontus', notName2: null} | name  | true   |
      | {notName: null, notName2: null}  | name  | false  |
      | {notName: 0, notName2: null}     | name  | false  |
      | {notName: 0}                     | name  | false  |
      | {}                               | name  | false  |

  Scenario: [2] Using `exists()` on null map
    When executing query:
      """
      WITH null AS m, { prop: 3 } AS n
      RETURN exists(m.prop), exists((null).prop)
      """
    Then the result should be, in any order:
      | exists(m.prop) | exists((null).prop) |
      | null           | null                |
