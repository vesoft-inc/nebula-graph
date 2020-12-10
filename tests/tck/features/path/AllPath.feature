Feature: All Path

  Background: Prepare space
    Given a graph with space named "nba"

  Scenario: [1] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: [2] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker", "Manu Ginobili" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>      |

  Scenario: [3] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")>                          |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: [4] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:serve]->("Spurs")>                                                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                                   |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:serve]->("Spurs")>                                     |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>             |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>           |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>          |

  Scenario: [1] ALL Path Run Time Input
    When executing query:
      """
      GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst
      | FIND ALL PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: [2] ALL Path Run Time Input
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: [1] ALL Path With Limit
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","Spurs" OVER like,serve UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order:
      | path                                                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: [2] ALL Path With Limit
    When executing query:
      """
      $a = GO FROM "Tim Duncan" over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 5
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |

  Scenario: [1] ALL PATH REVERSELY
    When executing query:
      """
      FIND SHORTEST PATH FROM "Tim Duncan" TO "Nobody","Spur" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path |

  Scenario: [2] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                              |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                         |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                          |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |

  Scenario: [3] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker","LaMarcus Aldridge" OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparision:
      | path                                                                                                    |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                               |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>                                                         |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>                                |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                    |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>     |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>            |
      | < ("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>     |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>        |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>       |

  Scenario: [2] ALL PATH BIDIRECT
    When executing query:
      """
      FIND ALL PATH FROM "Tim Duncan" TO "Tony Parker" OVER like BIDIRECT UPTO 3 STEPS
      """
    Then the result should be, in any order:
      | path                                                                                                       |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                                  |
      | <("Tim Duncan")-[:like@0]->("Tony Parker")>                                                                |
      | <("Tim Duncan")<-[:like@0]-("Marco Belinelli")-[:like@0]->("Tony Parker")>                                 |
      | <("Tim Duncan")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Tony Parker")>                                 |
      | <("Tim Duncan")<-[:like@0]-("LaMarcus Aldridge")<-[:like@0]-("Tony Parker")>                               |
      | <("Tim Duncan")<-[:like@0]-("LaMarcus Aldridge")-[:like@0]->("Tony Parker")>                               |
      | <("Tim Duncan")<-[:like@0]-("Manu Ginobili")<-[:like@0]-("Tony Parker")>                                   |
      | <("Tim Duncan")-[:like@0]->("Manu Ginobili")<-[:like@0]-("Tony Parker")>                                   |
      | <("Tim Duncan")<-[:like@0]-("Boris Diaw")-[:like@0]->("Tony Parker")>                                      |
      | <("Tim Duncan")<-[:like@0]-("Danny Green")<-[:like@0]-("Marco Belinelli")-[:like@0]->("Tony Parker")>      |
      | <("Tim Duncan")<-[:like@0]-("Danny Green")-[:like@0]->("Marco Belinelli")-[:like@0]->("Tony Parker")>      |
      | <("Tim Duncan")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Marco Belinelli")-[:like@0]->("Tony Parker")>  |
      | < ("Tim Duncan")<-[:like@0]-("Manu Ginobili")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like@0]->("Manu Ginobili")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Tony Parker")>    |
      | <("Tim Duncan")<-[:like@0]-("Danny Green")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Tony Parker")>      |
      | < ("Tim Duncan")<-[:like@0]-("Marco Belinelli")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Tony Parker")> |
      | <("Tim Duncan")<-[:like@0]-("Tony Parker")<-[:like@0]-("LaMarcus Aldridge")<-[:like@0]-("Tony Parker")>    |
      | <("Tim Duncan")-[:like@0]->("Tony Parker")<-[:like@0]-("LaMarcus Aldridge")<-[:like@0]-("Tony Parker")>    |
      | <("Tim Duncan")<-[:like@0]-("Tony Parker")-[:like@0]->("LaMarcus Aldridge")-[:like@0]->("Tony Parker")>    |
      | <("Tim Duncan")-[:like@0]->("Tony Parker")-[:like@0]->("LaMarcus Aldridge")-[:like@0]->("Tony Parker")>    |
      | < ("Tim Duncan")<-[:like@0]-("Manu Ginobili")<-[:like@0]-("Tim Duncan")<-[:like@0]-("Tony Parker")>        |
      | <("Tim Duncan")-[:like@0]->("Manu Ginobili")-[:like@0]->("Tim Duncan")<-[:like@0]-("Tony Parker")>         |
      | < ("Tim Duncan")<-[:like@0]-("Manu Ginobili")<-[:like@0]-("Tim Duncan")-[:like@0]->("Tony Parker")>        |
      | <("Tim Duncan")-[:like@0]->("Manu Ginobili")-[:like@0]->("Tim Duncan")-[:like@0]->("Tony Parker")>         |
      | <("Tim Duncan")<-[:like@0]-("Dejounte Murray")-[:like@0]->("Manu Ginobili")<-[:like@0]-("Tony Parker")>    |
      | <("Tim Duncan")<-[:like@0]-("Tiago Splitter")-[:like@0]->("Manu Ginobili")<-[:like@0]-("Tony Parker")>     |
