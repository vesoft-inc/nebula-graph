@skip
Feature: Integer Vid All Path

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: Integer Vid [1] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: Integer Vid [2] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Manu Ginobili") OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")>                                                       |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Manu Ginobili")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")>      |

  Scenario: Integer Vid [3] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                         |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")>                          |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>      |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")> |

  Scenario: Integer Vid [4] ALL Path
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Spurs") OVER like,serve UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
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

  Scenario: Integer Vid [1] ALL Path Run Time Input
    When executing query:
      """
      GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst
      | FIND ALL PATH FROM $-.src TO $-.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: Integer Vid [2] ALL Path Run Time Input
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Tony Parker")-[:like]->("Tim Duncan")>                                                         |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>      |

  Scenario: Integer Vid [1] ALL Path With Limit
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("Spurs") OVER like,serve UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 3
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                         |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:serve]->("Spurs")>                              |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:serve]->("Spurs")>      |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")> |

  Scenario: Integer Vid [2] ALL Path With Limit
    When executing query:
      """
      $a = GO FROM hash("Tim Duncan") over * YIELD like._dst AS src, serve._src AS dst;
      FIND ALL PATH FROM $a.src TO $a.dst OVER like UPTO 3 STEPS
      | ORDER BY $-.path | LIMIT 5
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")>                                                       |
      | <("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")>      |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tim Duncan")>                          |
      | <("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")-[:like]->("Tim Duncan")> |
      | <("Tony Parker")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")>                              |

  Scenario: Integer Vid [1] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Nobody"), hash("Spur") OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path |

  Scenario: Integer Vid [2] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                              |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                         |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                          |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")> |

  Scenario: Integer Vid [3] ALL PATH REVERSELY
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker"), hash("LaMarcus Aldridge") OVER like REVERSELY UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                    |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                               |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>                                                         |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>                                |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                    |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")>     |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>      |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("Tim Duncan")<-[:like]-("LaMarcus Aldridge")>        |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>       |

  Scenario: Integer Vid [2] ALL PATH BIDIRECT
    When executing query:
      """
      FIND ALL PATH FROM hash("Tim Duncan") TO hash("Tony Parker") OVER like BIDIRECT UPTO 3 STEPS
      """
    Then the result should be, in any order, with relax comparison:
      | path                                                                                                |
      | <("Tim Duncan")<-[:like]-("Tony Parker")>                                                           |
      | <("Tim Duncan")-[:like]->("Tony Parker")>                                                           |
      | <("Tim Duncan")<-[:like]-("Marco Belinelli")-[:like]->("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>                              |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("LaMarcus Aldridge")-[:like]->("Tony Parker")>                            |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>                                |
      | <("Tim Duncan")<-[:like]-("Boris Diaw")-[:like]->("Tony Parker")>                                   |
      | <("Tim Duncan")<-[:like]-("Danny Green")<-[:like]-("Marco Belinelli")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Danny Green")-[:like]->("Marco Belinelli")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Marco Belinelli")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Danny Green")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")>     |
      | <("Tim Duncan")<-[:like]-("Marco Belinelli")<-[:like]-("Dejounte Murray")-[:like]->("Tony Parker")> |
      | <("Tim Duncan")<-[:like]-("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Tony Parker")<-[:like]-("LaMarcus Aldridge")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")-[:like]->("Tony Parker")-[:like]->("LaMarcus Aldridge")-[:like]->("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")<-[:like]-("Tony Parker")>        |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")<-[:like]-("Tony Parker")>        |
      | <("Tim Duncan")<-[:like]-("Manu Ginobili")<-[:like]-("Tim Duncan")-[:like]->("Tony Parker")>        |
      | <("Tim Duncan")-[:like]->("Manu Ginobili")-[:like]->("Tim Duncan")-[:like]->("Tony Parker")>        |
      | <("Tim Duncan")<-[:like]-("Dejounte Murray")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>   |
      | <("Tim Duncan")<-[:like]-("Tiago Splitter")-[:like]->("Manu Ginobili")<-[:like]-("Tony Parker")>    |
