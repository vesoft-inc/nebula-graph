Feature: Value parsing

  Scenario: Parsing from text
  Given a global graph with space named "nba"
  When executing query:
    """
    MATCH (:player{name:'Tim Duncan'})-[e:serve*2..3{start_year: 2000}]-(v)
    RETURN e, v
    """
  Then the result should be, in any order:
    | e | v |
  And no side effects
  When executing query:
    """
    MATCH (:player{name:'Tim Duncan'})<-[e:like*2..3{likeness: 90}]-(v)
    RETURN e, v
    """
  Then the result should be, in any order, with relax comparision:
    | e                                                                                              | v                  |
    | [("Tim Duncan")<-[:like@0]-("Manu Ginobili"), ("Manu Ginobili")<-[:like@0]-("Tiago Splitter")] | ("Tiago Splitter") |
  And no side effects
