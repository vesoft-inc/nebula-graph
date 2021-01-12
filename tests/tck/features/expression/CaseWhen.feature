# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Case When Expression

  Background:
    Given a graph with space named "nba"

  Scenario: generic case expression
    When executing query:
      """
      YIELD CASE 2 + 3 WHEN 4 THEN 0 WHEN 5 THEN 1 ELSE 2 END
      """
    Then the result should be, in any order:
      | CASE (2+3) WHEN 4 THEN 0 WHEN 5 THEN 1 ELSE 2 END |
      | 1                                                 |
    When executing query:
      """
      YIELD CASE true WHEN false THEN 0 END
      """
    Then the result should be, in any order:
      | CASE true WHEN false THEN 0 END |
      | NULL                            |
    When executing query:
      """
      GO FROM "Jonathon Simmons" OVER serve
      YIELD $$.team.name as name, CASE serve.end_year > 2017 WHEN true THEN "ok" ELSE "no" END
      """
    Then the result should be, in any order:
      | name    | CASE (serve.end_year>2017) WHEN true THEN ok ELSE no END |
      | "76ers" | "ok"                                                     |
      | "Magic" | "ok"                                                     |
      | "Spurs" | "no"                                                     |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve
      YIELD
        $^.player.name,
        serve.start_year,
        serve.end_year,
        CASE serve.start_year > 2006 WHEN true THEN "new" ELSE "old" END,
        $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | serve.end_year | CASE (serve.start_year>2006) WHEN true THEN "new" ELSE "old" END | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "old"                                                            | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "old"                                                            | "Suns"       |
      | "Boris Diaw"   | 2008             | 2012           | "new"                                                            | "Hornets"    |
      | "Boris Diaw"   | 2012             | 2016           | "new"                                                            | "Spurs"      |
      | "Boris Diaw"   | 2016             | 2017           | "new"                                                            | "Jazz"       |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE CASE serve.start_year WHEN 2016 THEN true ELSE false END
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Rajon Rondo"  | 2016             | 2017           | "Bulls"      |
    When executing query:
      """
      YIELD CASE WHEN 4 > 5 THEN 0 WHEN 3+4==7 THEN 1 ELSE 2 END
      """
    Then the result should be, in any order:
      | CASE WHEN (4>5) THEN 0 WHEN ((3+4)==7) THEN 1 ELSE 2 END |
      | 1                                                        |
    When executing query:
      """
      YIELD CASE WHEN false THEN 0 ELSE 1 END
      """
    Then the result should be, in any order:
      | CASE WHEN false THEN 0 ELSE 1 END |
      | 1                                 |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve
      YIELD $$.team.name as name, CASE WHEN serve.start_year < 1998 THEN "old" ELSE "young" END
      """
    Then the result should be, in any order:
      | name    | CASE WHEN (serve.start_year<1998) THEN "old" ELSE "young" END |
      | 'Spurs' | 'old'                                                         |
    When executing query:
      # we are not able to deduce the return type of case expression in where_clause
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE CASE WHEN serve.start_year > 2016 THEN true ELSE false END
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Rajon Rondo"  | 2018             | 2019           | "Lakers"     |
      | "Rajon Rondo"  | 2017             | 2018           | "Pelicans"   |

  Scenario: conditional case expression
    When executing query:
      """
      YIELD 3 > 5 ? 0 : 1
      """
    Then the result should be, in any order:
      | ((3>5) ? 0 : 1) |
      | 1               |
    When executing query:
      """
      YIELD true ? "yes" : "no"
      """
    Then the result should be, in any order:
      | (true ? yes : no) |
      | "yes"             |
    When executing query:
      """
      GO FROM "Tim Duncan" OVER serve
      YIELD $$.team.name as name, serve.start_year < 1998 ? "old" : "young"
      """
    Then the result should be, in any order:
      | name    | ((serve.start_year<1998) ? old : young) |
      | 'Spurs' | 'old'                                   |
    When executing query:
      """
      GO FROM "Rajon Rondo" OVER serve
      WHERE serve.start_year > 2016 ? true : false
      YIELD $^.player.name, serve.start_year, serve.end_year, $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | serve.end_year | $$.team.name |
      | "Rajon Rondo"  | 2018             | 2019           | "Lakers"     |
      | "Rajon Rondo"  | 2017             | 2018           | "Pelicans"   |

  Scenario: generic with conditional case expression
    When executing query:
      """
      YIELD CASE 2 + 3 WHEN CASE 1 WHEN 1 THEN 5 ELSE 4 END THEN 0 WHEN 5 THEN 1 ELSE 2 END
      """
    Then the result should be, in any order:
      | CASE (2+3) WHEN CASE 1 WHEN 1 THEN 5 ELSE 4 END THEN 0 WHEN 5 THEN 1 ELSE 2 END |
      | 0                                                                               |
    When executing query:
      """
      YIELD CASE 2 + 3 WHEN 5 THEN CASE 1 WHEN 1 THEN 7 ELSE 4 END ELSE 2 END
      """
    Then the result should be, in any order:
      | CASE (2+3) WHEN 5 THEN CASE 1 WHEN 1 THEN 7 ELSE 4 END ELSE 2 END |
      | 7                                                                 |
    When executing query:
      """
      YIELD CASE 2 + 3 WHEN 3 THEN 7 ELSE CASE 9 WHEN 8 THEN 10 ELSE 11 END END
      """
    Then the result should be, in any order:
      | CASE (2+3) WHEN 3 THEN 7 ELSE CASE 9 WHEN 8 THEN 10 ELSE 11 END END |
      | 11                                                                  |
    When executing query:
      """
      YIELD CASE 3 > 2 ? 1 : 0 WHEN 1 THEN 5 ELSE 4 END
      """
    Then the result should be, in any order:
      | CASE ((3>2) ? 1 : 0) WHEN 1 THEN 5 ELSE 4 END |
      | 5                                             |
    When executing query:
      """
      YIELD CASE 1 WHEN true ? 1 : 0 THEN 5 ELSE 4 END
      """
    Then the result should be, in any order:
      | CASE 1 WHEN (true ? 1 : 0) THEN 5 ELSE 4 END |
      | 5                                            |
    When executing query:
      """
      YIELD CASE 1 WHEN 1 THEN 7 > 0 ? 6 : 9 ELSE 4 END
      """
    Then the result should be, in any order:
      | CASE 1 WHEN 1 THEN ((7>0) ? 6 : 9) ELSE 4 END |
      | 6                                             |
    When executing query:
      """
      YIELD CASE 1 WHEN 2 THEN 6 ELSE false ? 4 : 9 END
      """
    Then the result should be, in any order:
      | CASE 1 WHEN 2 THEN 6 ELSE (false ? 4 : 9) END |
      | 9                                             |
    When executing query:
      """
      YIELD CASE WHEN 2 > 7 THEN false ? 3 : 8 ELSE CASE true WHEN false THEN 9 ELSE 11 END END
      """
    Then the result should be, in any order:
      | CASE WHEN (2>7) THEN (false ? 3 : 8) ELSE CASE true WHEN false THEN 9 ELSE 11 END END |
      | 11                                                                                    |
    When executing query:
      """
      YIELD CASE 3 WHEN 4 THEN 5 ELSE 6 END > 11 ? 7 : CASE WHEN true THEN 8 ELSE 9 END
      """
    Then the result should be, in any order:
      | ((CASE 3 WHEN 4 THEN 5 ELSE 6 END>11) ? 7 : CASE WHEN true THEN 8 ELSE 9 END) |
      | 8                                                                             |
    When executing query:
      """
      YIELD 8 > 11 ? CASE WHEN true THEN 8 ELSE 9 END : CASE 14 WHEN 8+6 THEN 0 ELSE 1 END
      """
    Then the result should be, in any order:
      | ((8>11) ? CASE WHEN true THEN 8 ELSE 9 END : CASE 14 WHEN (8+6) THEN 0 ELSE 1 END) |
      | 0                                                                                  |
    When executing query:
      """
      YIELD
        CASE 3 WHEN 4 THEN 5 ELSE 6 END > (3 > 2 ? 8 : 9) ?
        CASE WHEN true THEN 8 ELSE 9 END :
        CASE 14 WHEN 8+6 THEN 0 ELSE 1 END
      """
    Then the result should be, in any order:
      | ((CASE 3 WHEN 4 THEN 5 ELSE 6 END>((3>2) ? 8 : 9)) ? CASE WHEN true THEN 8 ELSE 9 END : CASE 14 WHEN (8+6) THEN 0 ELSE 1 END) |
      | 0                                                                                                                             |
    When executing query:
      """
      GO FROM "Jonathon Simmons" OVER serve
      YIELD
        $$.team.name as name,
        CASE serve.end_year > 2017 WHEN true THEN 2017 < 2020 ? "ok" : "no" ELSE CASE WHEN false THEN "good" ELSE "bad" END END
      """
    Then the result should be, in any order:
      | name    | CASE (serve.end_year>2017) WHEN true THEN ((2017<2020) ? ok : no) ELSE CASE WHEN false THEN good ELSE bad END END |
      | "76ers" | "ok"                                                                                                              |
      | "Magic" | "ok"                                                                                                              |
      | "Spurs" | "bad"                                                                                                             |
    When executing query:
      """
      GO FROM "Boris Diaw" OVER serve
      YIELD
        $^.player.name,
        serve.start_year,
        serve.end_year,
        CASE serve.start_year > 2006 ? false : true WHEN true THEN "new" ELSE CASE WHEN serve.start_year != 2012 THEN "old" WHEN serve.start_year > 2009 THEN "bad" ELSE "good" END END,
        $$.team.name
      """
    Then the result should be, in any order:
      | $^.player.name | serve.start_year | serve.end_year | CASE ((serve.start_year>2006) ? false : true) WHEN true THEN new ELSE CASE WHEN (serve.start_year!=2012) THEN old WHEN (serve.start_year>2009) THEN bad ELSE good END END | $$.team.name |
      | "Boris Diaw"   | 2003             | 2005           | "new"                                                                                                                                                                     | "Hawks"      |
      | "Boris Diaw"   | 2005             | 2008           | "new"                                                                                                                                                                     | "Suns"       |
      | "Boris Diaw"   | 2008             | 2012           | "old"                                                                                                                                                                     | "Hornets"    |
      | "Boris Diaw"   | 2012             | 2016           | "bad"                                                                                                                                                                     | "Spurs"      |
      | "Boris Diaw"   | 2016             | 2017           | "old"                                                                                                                                                                     | "Jazz"       |
