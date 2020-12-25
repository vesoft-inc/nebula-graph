Feature: Explain and Profile

  Background:
    Given a graph with space named "nba"

  Scenario Outline: Different format
    When executing query:
      """
      <explain> FORMAT="<format>" YIELD 1
      """
    Then the execution should be successful
    When executing query:
      """
      <explain> FORMAT="<format>" {$var=YIELD 1 AS a; YIELD $var.*;}
      """
    Then the execution should be successful
    When executing query:
      """
      <explain> FORMAT="<format>" {YIELD 1 AS a;}
      """
    Then the execution should be successful

    Examples:
      | explain | format     |
      | EXPLAIN | row        |
      | EXPLAIN | dot        |
      | EXPLAIN | dot:struct |
      | PROFILE | row        |
      | PROFILE | dot        |
      | PROFILE | dot:struct |
