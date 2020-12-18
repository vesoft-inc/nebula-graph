@match_start_from_any_node
Feature: Start From Any Node

  Background:
    Given a graph with space named "nba"

  Scenario: start from middle node with totally 2 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)
      RETURN n,m,l
      """
    Then the result should be, in any order, with relax comparision:
      | n                   | m                 | l                   |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Spurs")           | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Spurs")           | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Spurs")           |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Spurs")           |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Spurs")           |

  Scenario: start from middle node with totally 2 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player)-[]-(l)
      WHERE m.name=="Kyle Anderson"
      RETURN n,m,l
      """
    Then the result should be, in any order, with relax comparision:
      | n                   | m                 | l                   |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Spurs")           | ("Kyle Anderson") | ("Tony Parker")     |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Spurs")           | ("Kyle Anderson") | ("Dejounte Murray") |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies")       |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Spurs")           |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Spurs")           |
      | ("Grizzlies")       | ("Kyle Anderson") | ("Spurs")           |
