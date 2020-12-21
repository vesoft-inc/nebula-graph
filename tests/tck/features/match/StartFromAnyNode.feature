@match_start_from_any_node
Feature: Start From Any Node

  Background:
    Given a graph with space named "nba"

  Scenario: start from middle node, with prop index, with totally 2 steps
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
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)
      RETURN p
      """
    Then the result should be, in any order, with relax comparision:
      | p                                                                                 |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")<-[:teammate@0]-("Tony Parker")> |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")<-[:teammate@0]-("Tony Parker")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")<-[:teammate@0]-("Tony Parker")>          |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")<-[:like@0]-("Dejounte Murray")> |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")<-[:like@0]-("Dejounte Murray")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")<-[:like@0]-("Dejounte Murray")>          |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")>      |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")-[:serve@0]->("Grizzlies")>      |
      | <("Spurs")<-[:serve@0]-("Kyle Anderson")-[:serve@0]->("Grizzlies")>               |
      | <("Tony Parker")-[:teammate@0]->("Kyle Anderson")-[:serve@0]->("Spurs")>          |
      | <("Dejounte Murray")-[:like@0]->("Kyle Anderson")-[:serve@0]->("Spurs")>          |
      | <("Grizzlies")<-[:serve@0]-("Kyle Anderson")-[:serve@0]->("Spurs")>               |

  Scenario: start from middle node, with prop index, with totally 3 steps
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparision:
      | count |
      | 141   |
    When executing query:
      """
      MATCH p = (n)-[]-(m:player{name:"Kyle Anderson"})-[]-(l)-[]-(k)
      WHERE k.name == "Marc Gasol"
      RETURN n, m, l, k
      """
    Then the result should be, in any order, with relax comparision:
      | n                   | m                 | l             | k              |
      | ("Tony Parker")     | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
      | ("Dejounte Murray") | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
      | ("Spurs")           | ("Kyle Anderson") | ("Grizzlies") | ("Marc Gasol") |
    When executing query:
      """
      MATCH p = (k)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparision:
      | count |
      | 46    |
    When executing query:
      """
      MATCH p = (k)-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)
      WHERE l.name == "Lakers"
      RETURN k, n, m, l
      """
    Then the result should be, in any order, with relax comparision:
      | k                | n                 | m               | l          |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Vince Carter") | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Yao Ming")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Grant Hill")   | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Rudy Gay")     | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Magic")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Raptors")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Rockets")      | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Spurs")        | ("Tracy McGrady") | ("Kobe Bryant") | ("Lakers") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Marc Gasol")   | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Bucks")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Bulls")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Grizzlies")    | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Lakers")       | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |
      | ("Spurs")        | ("Paul Gasol")    | ("Kobe Bryant") | ("Lakers") |

  Scenario: start from middle node, with prop index, with totally 4 steps
    When executing query:
      """
      MATCH p = ()-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)-[]-(k)
      RETURN p
      | YIELD count(*) AS count
      """
    Then the result should be, in any order, with relax comparision:
      | count |
      | 348   |
    When executing query:
      """
      MATCH p = ()-[]-(n)-[]-(m:player{name:"Kobe Bryant"})-[]-(l)-[]-(k)
      WHERE k.name == "Paul Gasol"
      RETURN p
      """
    Then the result should be, in any order, with relax comparision:
      | p                                                                                                                            |
      | <("Grant Hill")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>   |
      | <("Vince Carter")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")> |
      | <("Yao Ming")-[:like@0]->("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Grant Hill")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>   |
      | <("Rudy Gay")<-[:like@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Magic")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>       |
      | <("Raptors")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Rockets")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>     |
      | <("Spurs")<-[:serve@0]-("Tracy McGrady")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>       |
      | <("Marc Gasol")-[:like@0]->("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>      |
      | <("Marc Gasol")<-[:like@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>      |
      | <("Bucks")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |
      | <("Bulls")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |
      | <("Grizzlies")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol" )>     |
      | <("Spurs")<-[:serve@0]-("Paul Gasol")-[:like@0]->("Kobe Bryant")-[:serve@0]->("Lakers")<-[:serve@0]-("Paul Gasol")>          |

  Scenario: start from end node, with prop index, with totally 1 steps
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})
      RETURN n, m
      """
    Then the result should be, in any order, with relax comparision:
      | n                   | m                 |
      | ("Tony Parker")     | ("Kyle Anderson") |
      | ("Dejounte Murray") | ("Kyle Anderson") |
      | ("Grizzlies")       | ("Kyle Anderson") |
      | ("Spurs")           | ("Kyle Anderson") |
    When executing query:
      """
      MATCH (n)-[]-(m:player{name:"Kyle Anderson"})
      RETURN *
      """
    Then the result should be, in any order, with relax comparision:
      | n                   | m                 |
      | ("Tony Parker")     | ("Kyle Anderson") |
      | ("Dejounte Murray") | ("Kyle Anderson") |
      | ("Grizzlies")       | ("Kyle Anderson") |
      | ("Spurs")           | ("Kyle Anderson") |

  Scenario: start from end node, with prop index, with totally 2 steps
    When executing query:
      """
      MATCH (l)-[]-(n)-[]-(m:player{name:"Stephen Curry"})
      RETURN l, n, m
      """
    Then the result should be, in any order, with relax comparision:
      | l                     | n                 | m                 |
      | ("Warriors")          | ("Klay Thompson") | ("Stephen Curry") |
      | ("Amar'e Stoudemire") | ("Steve Nash")    | ("Stephen Curry") |
      | ("Dirk Nowitzki")     | ("Steve Nash")    | ("Stephen Curry") |
      | ("Jason Kidd")        | ("Steve Nash")    | ("Stephen Curry") |
      | ("Amar'e Stoudemire") | ("Steve Nash")    | ("Stephen Curry") |
      | ("Dirk Nowitzki")     | ("Steve Nash")    | ("Stephen Curry") |
      | ("Jason Kidd")        | ("Steve Nash")    | ("Stephen Curry") |
      | ("Lakers")            | ("Steve Nash")    | ("Stephen Curry") |
      | ("Mavericks")         | ("Steve Nash")    | ("Stephen Curry") |
      | ("Suns")              | ("Steve Nash")    | ("Stephen Curry") |
      | ("Suns")              | ("Steve Nash")    | ("Stephen Curry") |
      | ("David West")        | ("Warriors")      | ("Stephen Curry") |
      | ("JaVale McGee")      | ("Warriors")      | ("Stephen Curry") |
      | ("Kevin Durant")      | ("Warriors")      | ("Stephen Curry") |
      | ("Klay Thompson")     | ("Warriors")      | ("Stephen Curry") |
      | ("Marco Belinelli")   | ("Warriors")      | ("Stephen Curry") |
    When executing query:
      """
      MATCH (l)-[e1]-(n)-[e2]-(m:player{name:"Ricky Rubio"})
      RETURN *
      """
    Then the result should be, in any order, with relax comparision:
      | l              | e1                               | n        | e2                                | m               |
      | ("Boris Diaw") | [:serve "Jazz"<-"Boris Diaw" @0] | ("Jazz") | [:serve "Ricky Rubio"->"Jazz" @0] | ("Ricky Rubio") |

  @skip
  Scenario: start from middle node, with vertex id, with totally 2 steps
    When executing query:
      """
      MATCH (n)-[]-(m)-[]-(l)
      WHERE id(m)=="Kyle Anderson"
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
