Feature: Start From Any Node

Background:
    Given a graph with space named "nba"

@skip
Scenario: start from middle node with totally 2 steps
    When executing query:
        """
        MATCH (n)-[]-(m:player{name:"Tim Duncan"})-[]-(l) RETURN n,m,l
        """
    Then the result should be, in any order, with relax comparision:
        | n|m|l|
