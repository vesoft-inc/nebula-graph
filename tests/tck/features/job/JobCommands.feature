Feature: Job compact, flush, rebuild_index

  Scenario: submit job 
  Given a graph with space named "nba"
    When executing query:
      """
      SUBMIT JOB COMPACT;
      """
    Then the result should include:
      | New Job Id |
      | /\d+/      |
    When executing query:
      """
      SUBMIT JOB FLUSH;
      """
    Then the result should include:
      | New Job Id |
      | /\d+/      |
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then the result should include:
      | New Job Id |
      | /\d+/      |

  Scenario: rebuild indexes
    When executing query:
      """
      SUBMIT JOB STATS;
      """
    Then the result should include:
      | New Job Id |
      | /\d+/      |

  Scenario: show jobs
    Given a graph with space named "nba"
    When executing query:
      """
      SUBMIT JOB FLUSH; SHOW JOBS;
      """
    Then the result should include:
      | Job Id | Command | Status | Start Time                | Stop Time                 |
      | /\d+/  | "FLUSH" | /\w+/  | /\d+-\d+-\d+T\d+:\d+:\d+/ | /\d+-\d+-\d+T\d+:\d+:\d+/ |
    # The Status sould be
    # "QUEUE"|"FINISHED"|"RUNNING"
    # but the test framework can't support
    When executing query:
      """
      SUBMIT JOB COMPACT; SHOW JOBS;
      """
    Then the result should include:
      | Job Id | Command   | Status | Start Time                | Stop Time                 |
      | /\d+/  | "COMPACT" | /\w+/  | /\d+-\d+-\d+T\d+:\d+:\d+/ | /\d+-\d+-\d+T\d+:\d+:\d+/ |
    When executing query:
      """
      SUBMIT JOB STATS; SHOW JOBS;
      """
    Then the result should include:
      | Job Id | Command | Status | Start Time                | Stop Time                 |
      | /\d+/  | "STATS" | /\w+/  | /\d+-\d+-\d+T\d+:\d+:\d+/ | /\d+-\d+-\d+T\d+:\d+:\d+/ |
    When executing query:
      """
      SUBMIT JOB STATS; SHOW JOBS;
      """
    Then the result should include:
      | Job Id | Command | Status | Start Time                | Stop Time                 |
      | /\d+/  | "STATS" | /\w+/  | /\d+-\d+-\d+T\d+:\d+:\d+/ | /\d+-\d+-\d+T\d+:\d+:\d+/ |
    When executing query:
      """
      CREATE TAG INDEX single_tag_index ON tag_1(col5);
      REBUILD TAG INDEX single_tag_index;
      SHOW JOBS;
      """
    Then the result should include:
      | Job Id | Command             | Status | Start Time                | Stop Time                 |
      | /\d+/  | "REBUILD_TAG_INDEX" | /\w+/  | /\d+-\d+-\d+T\d+:\d+:\d+/ | /\d+-\d+-\d+T\d+:\d+:\d+/ |
