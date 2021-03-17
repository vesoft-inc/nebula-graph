# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Map3 - Key function

  Scenario: [1] Using `keys()` on a literal map
    When executing query:
      """
      RETURN keys({name: 'Alice', age: 38, address: {city: 'London', residential: true}}) AS k
      """
    Then the result should be, in any order:
      | k                          |
      | ['name', 'age', 'address'] |

  Scenario Outline: [2] Using `keys()` on a parameter map
    When executing query:
      """
      RETURN keys(<param>) AS k
      """
    Then the result should be (ignoring element order for lists):
      | k                          |
      | ['address', 'name', 'age'] |

    Examples:
      | param                                                                  |
      | {name: 'Alice', age: 38, address: {city: 'London', residential: true}} |

  Scenario: [3] Using `keys()` on null map
    When executing query:
      """
      WITH null AS m
      RETURN keys(m), keys(null)
      """
    Then the result should be, in any order:
      | keys(m) | keys(null) |
      | null    | null       |

  Scenario Outline: [4] Using `keys()` on map with null values
    When executing query:
      """
      RETURN keys(<map>) AS keys
      """
    Then the result should be (ignoring element order for lists):
      | keys     |
      | <result> |

    Examples:
      | map                | result     |
      | {}                 | []         |
      | {k: 1}             | ['k']      |
      | {k: null}          | ['k']      |
      | {k: null, l: 1}    | ['k', 'l'] |
      | {k: 1, l: null}    | ['k', 'l'] |
      | {k: null, l: null} | ['k', 'l'] |
