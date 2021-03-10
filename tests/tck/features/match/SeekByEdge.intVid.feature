Feature: Match seek by edge in integer vid

  Background: Prepare space
    Given a graph with space named "nba_int_vid"

  Scenario: seek by edge index in integer vid
    When executing query:
      """
      MATCH (player)-[:serve]->(team)
      RETURN id(player), team.name
      """
    Then the result should be, in any order, and the columns 0 should be hashed:
      | id(player)              | team.name       |
      | "Amar'e Stoudemire"     | "Suns"          |
      | "Amar'e Stoudemire"     | "Knicks"        |
      | "Amar'e Stoudemire"     | "Heat"          |
      | "Russell Westbrook"     | "Thunders"      |
      | "James Harden"          | "Thunders"      |
      | "James Harden"          | "Rockets"       |
      | "Kobe Bryant"           | "Lakers"        |
      | "Tracy McGrady"         | "Raptors"       |
      | "Tracy McGrady"         | "Magic"         |
      | "Tracy McGrady"         | "Rockets"       |
      | "Tracy McGrady"         | "Spurs"         |
      | "Chris Paul"            | "Hornets"       |
      | "Chris Paul"            | "Clippers"      |
      | "Chris Paul"            | "Rockets"       |
      | "Boris Diaw"            | "Hawks"         |
      | "Boris Diaw"            | "Suns"          |
      | "Boris Diaw"            | "Hornets"       |
      | "Boris Diaw"            | "Spurs"         |
      | "Boris Diaw"            | "Jazz"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Heat"          |
      | "LeBron James"          | "Cavaliers"     |
      | "LeBron James"          | "Lakers"        |
      | "Klay Thompson"         | "Warriors"      |
      | "Kristaps Porzingis"    | "Knicks"        |
      | "Kristaps Porzingis"    | "Mavericks"     |
      | "Jonathon Simmons"      | "Spurs"         |
      | "Jonathon Simmons"      | "Magic"         |
      | "Jonathon Simmons"      | "76ers"         |
      | "Marco Belinelli"       | "Warriors"      |
      | "Marco Belinelli"       | "Raptors"       |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Bulls"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Marco Belinelli"       | "Kings"         |
      | "Marco Belinelli"       | "Hornets"       |
      | "Marco Belinelli"       | "Hawks"         |
      | "Marco Belinelli"       | "76ers"         |
      | "Marco Belinelli"       | "Spurs"         |
      | "Luka Doncic"           | "Mavericks"     |
      | "David West"            | "Hornets"       |
      | "David West"            | "Pacers"        |
      | "David West"            | "Spurs"         |
      | "David West"            | "Warriors"      |
      | "Tony Parker"           | "Spurs"         |
      | "Tony Parker"           | "Hornets"       |
      | "Danny Green"           | "Cavaliers"     |
      | "Danny Green"           | "Spurs"         |
      | "Danny Green"           | "Raptors"       |
      | "Rudy Gay"              | "Grizzlies"     |
      | "Rudy Gay"              | "Raptors"       |
      | "Rudy Gay"              | "Kings"         |
      | "Rudy Gay"              | "Spurs"         |
      | "LaMarcus Aldridge"     | "Trail Blazers" |
      | "LaMarcus Aldridge"     | "Spurs"         |
      | "Tim Duncan"            | "Spurs"         |
      | "Kevin Durant"          | "Thunders"      |
      | "Kevin Durant"          | "Warriors"      |
      | "Stephen Curry"         | "Warriors"      |
      | "Ray Allen"             | "Bucks"         |
      | "Ray Allen"             | "Thunders"      |
      | "Ray Allen"             | "Celtics"       |
      | "Ray Allen"             | "Heat"          |
      | "Tiago Splitter"        | "Spurs"         |
      | "Tiago Splitter"        | "Hawks"         |
      | "Tiago Splitter"        | "76ers"         |
      | "DeAndre Jordan"        | "Clippers"      |
      | "DeAndre Jordan"        | "Mavericks"     |
      | "DeAndre Jordan"        | "Knicks"        |
      | "Paul Gasol"            | "Grizzlies"     |
      | "Paul Gasol"            | "Lakers"        |
      | "Paul Gasol"            | "Bulls"         |
      | "Paul Gasol"            | "Spurs"         |
      | "Paul Gasol"            | "Bucks"         |
      | "Aron Baynes"           | "Spurs"         |
      | "Aron Baynes"           | "Pistons"       |
      | "Aron Baynes"           | "Celtics"       |
      | "Cory Joseph"           | "Spurs"         |
      | "Cory Joseph"           | "Raptors"       |
      | "Cory Joseph"           | "Pacers"        |
      | "Vince Carter"          | "Raptors"       |
      | "Vince Carter"          | "Nets"          |
      | "Vince Carter"          | "Magic"         |
      | "Vince Carter"          | "Suns"          |
      | "Vince Carter"          | "Mavericks"     |
      | "Vince Carter"          | "Grizzlies"     |
      | "Vince Carter"          | "Kings"         |
      | "Vince Carter"          | "Hawks"         |
      | "Marc Gasol"            | "Grizzlies"     |
      | "Marc Gasol"            | "Raptors"       |
      | "Ricky Rubio"           | "Timberwolves"  |
      | "Ricky Rubio"           | "Jazz"          |
      | "Ben Simmons"           | "76ers"         |
      | "Giannis Antetokounmpo" | "Bucks"         |
      | "Rajon Rondo"           | "Celtics"       |
      | "Rajon Rondo"           | "Mavericks"     |
      | "Rajon Rondo"           | "Kings"         |
      | "Rajon Rondo"           | "Bulls"         |
      | "Rajon Rondo"           | "Pelicans"      |
      | "Rajon Rondo"           | "Lakers"        |
      | "Manu Ginobili"         | "Spurs"         |
      | "Kyrie Irving"          | "Cavaliers"     |
      | "Kyrie Irving"          | "Celtics"       |
      | "Carmelo Anthony"       | "Nuggets"       |
      | "Carmelo Anthony"       | "Knicks"        |
      | "Carmelo Anthony"       | "Thunders"      |
      | "Carmelo Anthony"       | "Rockets"       |
      | "Dwyane Wade"           | "Heat"          |
      | "Dwyane Wade"           | "Bulls"         |
      | "Dwyane Wade"           | "Cavaliers"     |
      | "Dwyane Wade"           | "Heat"          |
      | "Joel Embiid"           | "76ers"         |
      | "Damian Lillard"        | "Trail Blazers" |
      | "Yao Ming"              | "Rockets"       |
      | "Kyle Anderson"         | "Spurs"         |
      | "Kyle Anderson"         | "Grizzlies"     |
      | "Dejounte Murray"       | "Spurs"         |
      | "Blake Griffin"         | "Clippers"      |
      | "Blake Griffin"         | "Pistons"       |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Mavericks"     |
      | "Steve Nash"            | "Suns"          |
      | "Steve Nash"            | "Lakers"        |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Suns"          |
      | "Jason Kidd"            | "Nets"          |
      | "Jason Kidd"            | "Mavericks"     |
      | "Jason Kidd"            | "Knicks"        |
      | "Dirk Nowitzki"         | "Mavericks"     |
      | "Paul George"           | "Pacers"        |
      | "Paul George"           | "Thunders"      |
      | "Grant Hill"            | "Pistons"       |
      | "Grant Hill"            | "Magic"         |
      | "Grant Hill"            | "Suns"          |
      | "Grant Hill"            | "Clippers"      |
      | "Shaquile O'Neal"       | "Magic"         |
      | "Shaquile O'Neal"       | "Lakers"        |
      | "Shaquile O'Neal"       | "Heat"          |
      | "Shaquile O'Neal"       | "Suns"          |
      | "Shaquile O'Neal"       | "Cavaliers"     |
      | "Shaquile O'Neal"       | "Celtics"       |
      | "JaVale McGee"          | "Wizards"       |
      | "JaVale McGee"          | "Nuggets"       |
      | "JaVale McGee"          | "Mavericks"     |
      | "JaVale McGee"          | "Warriors"      |
      | "JaVale McGee"          | "Lakers"        |
      | "Dwight Howard"         | "Magic"         |
      | "Dwight Howard"         | "Lakers"        |
      | "Dwight Howard"         | "Rockets"       |
      | "Dwight Howard"         | "Hawks"         |
      | "Dwight Howard"         | "Hornets"       |
      | "Dwight Howard"         | "Wizards"       |
    When executing query:
      """
      MATCH (p1)-[:like]->(player)-[:serve]->(team)
      RETURN p1.name, id(player), team.name
      """
    Then the result should be, in any order, and the columns 1 should be hashed:
      | p1.name              | id(player)           | team.name       |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Suns"          |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Lakers"        |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Mavericks"     |
      | "Amar'e Stoudemire"  | "Steve Nash"         | "Suns"          |
      | "Russell Westbrook"  | "James Harden"       | "Thunders"      |
      | "Russell Westbrook"  | "James Harden"       | "Rockets"       |
      | "Russell Westbrook"  | "Paul George"        | "Thunders"      |
      | "Russell Westbrook"  | "Paul George"        | "Pacers"        |
      | "James Harden"       | "Russell Westbrook"  | "Thunders"      |
      | "Tracy McGrady"      | "Rudy Gay"           | "Raptors"       |
      | "Tracy McGrady"      | "Rudy Gay"           | "Spurs"         |
      | "Tracy McGrady"      | "Rudy Gay"           | "Kings"         |
      | "Tracy McGrady"      | "Rudy Gay"           | "Grizzlies"     |
      | "Tracy McGrady"      | "Grant Hill"         | "Clippers"      |
      | "Tracy McGrady"      | "Grant Hill"         | "Magic"         |
      | "Tracy McGrady"      | "Grant Hill"         | "Pistons"       |
      | "Tracy McGrady"      | "Grant Hill"         | "Suns"          |
      | "Tracy McGrady"      | "Kobe Bryant"        | "Lakers"        |
      | "Chris Paul"         | "Dwyane Wade"        | "Heat"          |
      | "Chris Paul"         | "Dwyane Wade"        | "Cavaliers"     |
      | "Chris Paul"         | "Dwyane Wade"        | "Bulls"         |
      | "Chris Paul"         | "Dwyane Wade"        | "Heat"          |
      | "Chris Paul"         | "Carmelo Anthony"    | "Knicks"        |
      | "Chris Paul"         | "Carmelo Anthony"    | "Rockets"       |
      | "Chris Paul"         | "Carmelo Anthony"    | "Nuggets"       |
      | "Chris Paul"         | "Carmelo Anthony"    | "Thunders"      |
      | "Chris Paul"         | "LeBron James"       | "Cavaliers"     |
      | "Chris Paul"         | "LeBron James"       | "Lakers"        |
      | "Chris Paul"         | "LeBron James"       | "Heat"          |
      | "Chris Paul"         | "LeBron James"       | "Cavaliers"     |
      | "Boris Diaw"         | "Tim Duncan"         | "Spurs"         |
      | "Boris Diaw"         | "Tony Parker"        | "Spurs"         |
      | "Boris Diaw"         | "Tony Parker"        | "Hornets"       |
      | "LeBron James"       | "Ray Allen"          | "Heat"          |
      | "LeBron James"       | "Ray Allen"          | "Celtics"       |
      | "LeBron James"       | "Ray Allen"          | "Bucks"         |
      | "LeBron James"       | "Ray Allen"          | "Thunders"      |
      | "Klay Thompson"      | "Stephen Curry"      | "Warriors"      |
      | "Kristaps Porzingis" | "Luka Doncic"        | "Mavericks"     |
      | "Marco Belinelli"    | "Danny Green"        | "Cavaliers"     |
      | "Marco Belinelli"    | "Danny Green"        | "Spurs"         |
      | "Marco Belinelli"    | "Danny Green"        | "Raptors"       |
      | "Marco Belinelli"    | "Tim Duncan"         | "Spurs"         |
      | "Marco Belinelli"    | "Tony Parker"        | "Spurs"         |
      | "Marco Belinelli"    | "Tony Parker"        | "Hornets"       |
      | "Luka Doncic"        | "James Harden"       | "Thunders"      |
      | "Luka Doncic"        | "James Harden"       | "Rockets"       |
      | "Luka Doncic"        | "Kristaps Porzingis" | "Knicks"        |
      | "Luka Doncic"        | "Kristaps Porzingis" | "Mavericks"     |
      | "Luka Doncic"        | "Dirk Nowitzki"      | "Mavericks"     |
      | "Tony Parker"        | "LaMarcus Aldridge"  | "Spurs"         |
      | "Tony Parker"        | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "Tony Parker"        | "Manu Ginobili"      | "Spurs"         |
      | "Tony Parker"        | "Tim Duncan"         | "Spurs"         |
      | "Danny Green"        | "LeBron James"       | "Cavaliers"     |
      | "Danny Green"        | "LeBron James"       | "Lakers"        |
      | "Danny Green"        | "LeBron James"       | "Heat"          |
      | "Danny Green"        | "LeBron James"       | "Cavaliers"     |
      | "Danny Green"        | "Tim Duncan"         | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "76ers"         |
      | "Danny Green"        | "Marco Belinelli"    | "Warriors"      |
      | "Danny Green"        | "Marco Belinelli"    | "Hornets"       |
      | "Danny Green"        | "Marco Belinelli"    | "Kings"         |
      | "Danny Green"        | "Marco Belinelli"    | "Raptors"       |
      | "Danny Green"        | "Marco Belinelli"    | "Bulls"         |
      | "Danny Green"        | "Marco Belinelli"    | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "Hornets"       |
      | "Danny Green"        | "Marco Belinelli"    | "Spurs"         |
      | "Danny Green"        | "Marco Belinelli"    | "Hawks"         |
      | "Rudy Gay"           | "LaMarcus Aldridge"  | "Spurs"         |
      | "Rudy Gay"           | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "LaMarcus Aldridge"  | "Tim Duncan"         | "Spurs"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        | "Spurs"         |
      | "LaMarcus Aldridge"  | "Tony Parker"        | "Hornets"       |
      | "Tim Duncan"         | "Manu Ginobili"      | "Spurs"         |
      | "Tim Duncan"         | "Tony Parker"        | "Spurs"         |
      | "Tim Duncan"         | "Tony Parker"        | "Hornets"       |
      | "Ray Allen"          | "Rajon Rondo"        | "Mavericks"     |
      | "Ray Allen"          | "Rajon Rondo"        | "Kings"         |
      | "Ray Allen"          | "Rajon Rondo"        | "Celtics"       |
      | "Ray Allen"          | "Rajon Rondo"        | "Pelicans"      |
      | "Ray Allen"          | "Rajon Rondo"        | "Lakers"        |
      | "Ray Allen"          | "Rajon Rondo"        | "Bulls"         |
      | "Tiago Splitter"     | "Tim Duncan"         | "Spurs"         |
      | "Tiago Splitter"     | "Manu Ginobili"      | "Spurs"         |
      | "Paul Gasol"         | "Marc Gasol"         | "Raptors"       |
      | "Paul Gasol"         | "Marc Gasol"         | "Grizzlies"     |
      | "Paul Gasol"         | "Kobe Bryant"        | "Lakers"        |
      | "Aron Baynes"        | "Tim Duncan"         | "Spurs"         |
      | "Vince Carter"       | "Jason Kidd"         | "Nets"          |
      | "Vince Carter"       | "Jason Kidd"         | "Suns"          |
      | "Vince Carter"       | "Jason Kidd"         | "Mavericks"     |
      | "Vince Carter"       | "Jason Kidd"         | "Knicks"        |
      | "Vince Carter"       | "Jason Kidd"         | "Mavericks"     |
      | "Vince Carter"       | "Tracy McGrady"      | "Spurs"         |
      | "Vince Carter"       | "Tracy McGrady"      | "Magic"         |
      | "Vince Carter"       | "Tracy McGrady"      | "Rockets"       |
      | "Vince Carter"       | "Tracy McGrady"      | "Raptors"       |
      | "Marc Gasol"         | "Paul Gasol"         | "Lakers"        |
      | "Marc Gasol"         | "Paul Gasol"         | "Grizzlies"     |
      | "Marc Gasol"         | "Paul Gasol"         | "Bucks"         |
      | "Marc Gasol"         | "Paul Gasol"         | "Spurs"         |
      | "Marc Gasol"         | "Paul Gasol"         | "Bulls"         |
      | "Ben Simmons"        | "Joel Embiid"        | "76ers"         |
      | "Rajon Rondo"        | "Ray Allen"          | "Heat"          |
      | "Rajon Rondo"        | "Ray Allen"          | "Celtics"       |
      | "Rajon Rondo"        | "Ray Allen"          | "Bucks"         |
      | "Rajon Rondo"        | "Ray Allen"          | "Thunders"      |
      | "Manu Ginobili"      | "Tim Duncan"         | "Spurs"         |
      | "Kyrie Irving"       | "LeBron James"       | "Cavaliers"     |
      | "Kyrie Irving"       | "LeBron James"       | "Lakers"        |
      | "Kyrie Irving"       | "LeBron James"       | "Heat"          |
      | "Kyrie Irving"       | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "LeBron James"       | "Lakers"        |
      | "Carmelo Anthony"    | "LeBron James"       | "Heat"          |
      | "Carmelo Anthony"    | "LeBron James"       | "Cavaliers"     |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Heat"          |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Cavaliers"     |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Bulls"         |
      | "Carmelo Anthony"    | "Dwyane Wade"        | "Heat"          |
      | "Carmelo Anthony"    | "Chris Paul"         | "Hornets"       |
      | "Carmelo Anthony"    | "Chris Paul"         | "Clippers"      |
      | "Carmelo Anthony"    | "Chris Paul"         | "Rockets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Knicks"        |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Rockets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Nuggets"       |
      | "Dwyane Wade"        | "Carmelo Anthony"    | "Thunders"      |
      | "Dwyane Wade"        | "Chris Paul"         | "Hornets"       |
      | "Dwyane Wade"        | "Chris Paul"         | "Clippers"      |
      | "Dwyane Wade"        | "Chris Paul"         | "Rockets"       |
      | "Dwyane Wade"        | "LeBron James"       | "Cavaliers"     |
      | "Dwyane Wade"        | "LeBron James"       | "Lakers"        |
      | "Dwyane Wade"        | "LeBron James"       | "Heat"          |
      | "Dwyane Wade"        | "LeBron James"       | "Cavaliers"     |
      | "Joel Embiid"        | "Ben Simmons"        | "76ers"         |
      | "Damian Lillard"     | "LaMarcus Aldridge"  | "Spurs"         |
      | "Damian Lillard"     | "LaMarcus Aldridge"  | "Trail Blazers" |
      | "Yao Ming"           | "Tracy McGrady"      | "Spurs"         |
      | "Yao Ming"           | "Tracy McGrady"      | "Magic"         |
      | "Yao Ming"           | "Tracy McGrady"      | "Rockets"       |
      | "Yao Ming"           | "Tracy McGrady"      | "Raptors"       |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Suns"          |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Celtics"       |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Heat"          |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Magic"         |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Cavaliers"     |
      | "Yao Ming"           | "Shaquile O'Neal"    | "Lakers"        |
      | "Dejounte Murray"    | "Chris Paul"         | "Hornets"       |
      | "Dejounte Murray"    | "Chris Paul"         | "Clippers"      |
      | "Dejounte Murray"    | "Chris Paul"         | "Rockets"       |
      | "Dejounte Murray"    | "LeBron James"       | "Cavaliers"     |
      | "Dejounte Murray"    | "LeBron James"       | "Lakers"        |
      | "Dejounte Murray"    | "LeBron James"       | "Heat"          |
      | "Dejounte Murray"    | "LeBron James"       | "Cavaliers"     |
      | "Dejounte Murray"    | "James Harden"       | "Thunders"      |
      | "Dejounte Murray"    | "James Harden"       | "Rockets"       |
      | "Dejounte Murray"    | "Russell Westbrook"  | "Thunders"      |
      | "Dejounte Murray"    | "Marco Belinelli"    | "76ers"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Warriors"      |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hornets"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Kings"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Raptors"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Bulls"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Spurs"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hornets"       |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Spurs"         |
      | "Dejounte Murray"    | "Marco Belinelli"    | "Hawks"         |
      | "Dejounte Murray"    | "Danny Green"        | "Cavaliers"     |
      | "Dejounte Murray"    | "Danny Green"        | "Spurs"         |
      | "Dejounte Murray"    | "Danny Green"        | "Raptors"       |
      | "Dejounte Murray"    | "Kyle Anderson"      | "Spurs"         |
      | "Dejounte Murray"    | "Kyle Anderson"      | "Grizzlies"     |
      | "Dejounte Murray"    | "Tim Duncan"         | "Spurs"         |
      | "Dejounte Murray"    | "Manu Ginobili"      | "Spurs"         |
      | "Dejounte Murray"    | "Tony Parker"        | "Spurs"         |
      | "Dejounte Murray"    | "Tony Parker"        | "Hornets"       |
      | "Dejounte Murray"    | "Kevin Durant"       | "Warriors"      |
      | "Dejounte Murray"    | "Kevin Durant"       | "Thunders"      |
      | "Blake Griffin"      | "Chris Paul"         | "Hornets"       |
      | "Blake Griffin"      | "Chris Paul"         | "Clippers"      |
      | "Blake Griffin"      | "Chris Paul"         | "Rockets"       |
      | "Steve Nash"         | "Stephen Curry"      | "Warriors"      |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Knicks"        |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Suns"          |
      | "Steve Nash"         | "Amar'e Stoudemire"  | "Heat"          |
      | "Steve Nash"         | "Jason Kidd"         | "Nets"          |
      | "Steve Nash"         | "Jason Kidd"         | "Suns"          |
      | "Steve Nash"         | "Jason Kidd"         | "Mavericks"     |
      | "Steve Nash"         | "Jason Kidd"         | "Knicks"        |
      | "Steve Nash"         | "Jason Kidd"         | "Mavericks"     |
      | "Steve Nash"         | "Dirk Nowitzki"      | "Mavericks"     |
      | "Jason Kidd"         | "Steve Nash"         | "Suns"          |
      | "Jason Kidd"         | "Steve Nash"         | "Lakers"        |
      | "Jason Kidd"         | "Steve Nash"         | "Mavericks"     |
      | "Jason Kidd"         | "Steve Nash"         | "Suns"          |
      | "Jason Kidd"         | "Dirk Nowitzki"      | "Mavericks"     |
      | "Jason Kidd"         | "Vince Carter"       | "Hawks"         |
      | "Jason Kidd"         | "Vince Carter"       | "Kings"         |
      | "Jason Kidd"         | "Vince Carter"       | "Magic"         |
      | "Jason Kidd"         | "Vince Carter"       | "Raptors"       |
      | "Jason Kidd"         | "Vince Carter"       | "Suns"          |
      | "Jason Kidd"         | "Vince Carter"       | "Nets"          |
      | "Jason Kidd"         | "Vince Carter"       | "Mavericks"     |
      | "Jason Kidd"         | "Vince Carter"       | "Grizzlies"     |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Nets"          |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Suns"          |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Knicks"        |
      | "Dirk Nowitzki"      | "Jason Kidd"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Heat"          |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Cavaliers"     |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Bulls"         |
      | "Dirk Nowitzki"      | "Dwyane Wade"        | "Heat"          |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Suns"          |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Lakers"        |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Mavericks"     |
      | "Dirk Nowitzki"      | "Steve Nash"         | "Suns"          |
      | "Paul George"        | "Russell Westbrook"  | "Thunders"      |
      | "Grant Hill"         | "Tracy McGrady"      | "Spurs"         |
      | "Grant Hill"         | "Tracy McGrady"      | "Magic"         |
      | "Grant Hill"         | "Tracy McGrady"      | "Rockets"       |
      | "Grant Hill"         | "Tracy McGrady"      | "Raptors"       |
      | "Shaquile O'Neal"    | "Tim Duncan"         | "Spurs"         |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Lakers"        |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Warriors"      |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Wizards"       |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Nuggets"       |
      | "Shaquile O'Neal"    | "JaVale McGee"       | "Mavericks"     |

  Scenario: Seek by edge with properties in integer vid
    When executing query:
      """
      match (player)-[:serve {start_year : 2001}]->(team) return player.name AS player, team.name AS team
      """
    Then the result should be, in any order:
      | player       | team         |
      | "Paul Gasol" | "Grizzlies"  |
      | "Jason Kidd" | "Nets"       |

  Scenario: seek by edge without index
    When executing query:
      """
      MATCH (p1)-[:like]->(p2)
      RETURN p1.name, id(p2)
      """
    Then a ExecutionError should be raised at runtime: Can't solve the start vids from the sentence: MATCH (p1)-[:like]->(p2) RETURN p1.name,id(p2)
