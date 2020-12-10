Feature: Shortest Path

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: SinglePair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker" OVER like
      """
    Then the result should be, in any order:
      | path                                      |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "LaMarcus Aldridge" OVER like
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                                |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge") |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER like, teammate
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                           |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge") |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tiago Splitter" TO "LaMarcus Aldridge" OVER *
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                           |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:teammate]->("LaMarcus Aldridge") |

  Scenario: MultiPair Shortest Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                      |
      | <("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>      |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                          |
      | <("Tim Duncan")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")-[:teammate]->("Tony Parker")> |
      | <("Tim Duncan")-[:serve]->("Spurs")>          |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                                                      |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>                            |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")>                        |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                                             |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                                                          |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")>     |
      | <("Tony Parker")-[:teammate]->("Tim Duncan")-[:teammate]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                                              |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                                          |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                                                     |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tony Parker", "Yao Ming" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                               |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                      |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Tony Parker")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tony Parker")-[:teammate]->("Manu Ginobili")>                                                   |
      | <("Tony Parker")-[:serve]->("Spurs")>                                                              |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli", "Yao Ming" TO "Spurs", "Lakers" OVER * UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                          |
      | <("Yao Ming")-[:like]->("Tracy McGrady")-[:serve]->("Spurs")>                                 |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:serve]->("Lakers")>                              |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan", "Tiago Splitter" TO "Tony Parker","Spurs" OVER like,serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                  |
      | <("Tiago Splitter")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Tiago Splitter")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                             |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                  |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Yao Ming"  TO "Tony Parker","Tracy McGrady" OVER like,serve UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                         |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
      | <("Yao Ming")-[:like]->("Tracy McGrady")>                                                    |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>            |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                                   |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO "Manu Ginobili", "Spurs", "Lakers" OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Marco Belinelli" TO "Spurs", "Lakers" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                          |
      | <("Marco Belinelli")-[:serve]->("Spurs")>                                                     |
      | <("Marco Belinelli")-[:serve@1]->("Spurs")>                                                   |
      | <("Marco Belinelli")-[:like]->("Danny Green")-[:like]->("LeBron James")-[:serve]->("Lakers")> |

  Scenario: MultiPair Shortest Path Empty Path
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path |

  Scenario: MultiPair Shortest Path Run Time input
    When executing query:
      """
      YIELD "Yao Ming" AS src, "Tony Parker" AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                         |
      | <("Yao Ming")-[:like]->("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |
    When executing query:
      """
      YIELD "Shaquile O\'Neal" AS src
      | FIND SHORTEST PATH FROM $-.src TO "Manu Ginobili" OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
    When executing query:
      """
      YIELD "Manu Ginobili" AS dst
      | FIND SHORTEST PATH FROM "Shaquile O\'Neal" TO $-.dst OVER * UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                         |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>     |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:teammate]->("Manu Ginobili")> |
    When executing query:
      """
      GO FROM "Yao Ming" over like YIELD like._dst AS src
      | FIND SHORTEST PATH FROM $-.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                            |
    When executing query:
      """
      $a = GO FROM "Yao Ming" over like YIELD like._dst AS src;
      FIND SHORTEST PATH FROM $a.src TO "Tony Parker" OVER like, serve UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Tracy McGrady")-[:like]->("Rudy Gay")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>                            |
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND SHORTEST PATH FROM $a.src TO $a.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over like YIELD like._src AS src;
      GO FROM "Tony Parker" OVER like YIELD like._src AS src, like._dst AS dst
      | FIND SHORTEST PATH FROM $a.src TO $-.dst OVER like UPTO 5 STEPS
      """
    Then the result should be, in any order:
      | path                                                                     |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")                               |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")> |

  Scenario: Shortest Path With Limit
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like,serve UPTO 3 STEPS | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order:
      | path |
    When executing query:
      """
      FIND SHORTEST PATH FROM "Shaquile O\'Neal", "Nobody" TO "Manu Ginobili", "Spurs", "Lakers" OVER * UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 2
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                     |
      | <("Shaquile O'Neal")-[:serve]->("Lakers")>                               |
      | <("Shaquile O'Neal")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")> |
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 1
      """
    Then the result should be, in any order, with relax comparision:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND SHORTEST PATH FROM $-.src TO $-.dst OVER like UPTO 5 STEPS
      | ORDER BY $-.path | LIMIT 10
      """
    Then the result should be, in any order, with relax comparision:
      | path                                        |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")>   |
