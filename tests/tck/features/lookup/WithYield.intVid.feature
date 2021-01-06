Feature: Lookup with yield in integer vid

  Background:
    Given a graph with space named "nba_int_vid"

  Scenario: [1] tag with yield
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name
      """
    Then the result should be, in any order:
      | VertexID              | player.name     |
      | hash('Kobe Bryant')   | 'Kobe Bryant'   |
      | hash('Dirk Nowitzki') | 'Dirk Nowitzki' |

  Scenario: [1] tag with yield rename
    When executing query:
      """
      LOOKUP ON player WHERE player.age == 40 YIELD player.name AS name
      """
    Then the result should be, in any order:
      | VertexID              | name            |
      | hash('Kobe Bryant')   | 'Kobe Bryant'   |
      | hash('Dirk Nowitzki') | 'Dirk Nowitzki' |

  Scenario: [2] edge with yield
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year
      """
    Then the result should be, in any order:
      | SrcVID                    | DstVID            | Ranking | serve.start_year |
      | hash('Russell Westbrook') | hash('Thunders')  | 0       | 2008             |
      | hash('Marc Gasol')        | hash('Grizzlies') | 0       | 2008             |

  Scenario: [2] edge with yield rename
    When executing query:
      """
      LOOKUP ON serve WHERE serve.start_year == 2008 and serve.end_year == 2019
      YIELD serve.start_year AS startYear
      """
    Then the result should be, in any order:
      | SrcVID                    | DstVID            | Ranking | startYear |
      | hash('Russell Westbrook') | hash('Thunders')  | 0       | 2008      |
      | hash('Marc Gasol')        | hash('Grizzlies') | 0       | 2008      |
