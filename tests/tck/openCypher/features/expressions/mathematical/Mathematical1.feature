# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Mathematical

  Background:
    Given an empty graph

  Scenario: [1] `sqrt()` returning float values
    When executing query:
      """
      RETURN sqrt(12.96)
      """
    Then the result should be, in any order:
      | sqrt(12.96) |
      | 3.6         |

  Scenario: [2] Arithmetic precedence test
    When executing query:
      """
      RETURN 12 / 4 * 3 - 2 * 4
      """
    Then the result should be, in any order:
      | 12/4*3-2*4 |
      | 1          |

  Scenario: [3] Arithmetic precedence with parenthesis test
    When executing query:
      """
      RETURN 12 / 4 * (3 - 2 * 4)
      """
    Then the result should be, in any order:
      | 12/4*(3-2*4) |
      | -15          |
