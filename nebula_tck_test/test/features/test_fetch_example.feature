Feature: string
    Scenario: string
	Given a space nba
	
	When fetch player:
	    fetch prop on player "Boris Diaw";

	Then the result should be:
            | VertexID | player.name | player.age |
            | "Boris Diaw" | "Boris Diaw"    | 36          |
