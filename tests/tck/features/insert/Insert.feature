# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
Feature: Insert string vid of vertex and edge

  Scenario Outline: insert vertex and edge test
    Given an empty graph
    And create a space with following options:
      | vid_type | <vid_type> |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE TAG IF NOT EXISTS personWithDefault(
        name string DEFAULT "",
        age int DEFAULT 18, isMarried bool DEFAULT false,
        BMI double DEFAULT 18.5, department string DEFAULT "engineering",
        birthday timestamp DEFAULT timestamp("2020-01-10T10:00:00")
      );
      CREATE TAG IF NOT EXISTS student(grade string, number int);
      CREATE TAG IF NOT EXISTS studentWithDefault(grade string DEFAULT "one", number int);
      CREATE TAG IF NOT EXISTS employee(name int);
      CREATE TAG IF NOT EXISTS interest(name string);
      CREATE TAG IF NOT EXISTS school(name string, create_time timestamp);
      CREATE EDGE IF NOT EXISTS schoolmate(likeness int, nickname string);
      CREATE EDGE IF NOT EXISTS schoolmateWithDefault(likeness int DEFAULT 80);
      CREATE EDGE IF NOT EXISTS study(start_time timestamp, end_time timestamp);
      """
    And wait 3 seconds
    # insert vertex wrong type value
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES <vid_Tom>:("Tom", "2");
      """
    Then a ExecutionError should be raised at runtime:
    # insert vertex wrong num of value
    When executing query:
      """
      INSERT VERTEX person(name) VALUES <vid_Tom>:("Tom", 2);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex wrong field
    When executing query:
      """
      INSERT VERTEX person(Name, age) VALUES <vid_Tom>:("Tom", 3);
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex wrong type
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        <vid_Laura>-><vid_Amber>:("87", "");
      """
    Then a ExecutionError should be raised at runtime:
    # insert edge wrong number of value
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        <vid_Laura>-><vid_Amber>:("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong num of prop
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness)
      VALUES
        <vid_Laura>-><vid_Amber>:("hello", "87", "");
      """
    Then a SemanticError should be raised at runtime:
    # insert edge wrong field name
    When executing query:
      """
      INSERT EDGE schoolmate(like, HH) VALUES <vid_Laura>-><vid_Amber>:(88);
      """
    Then a SemanticError should be raised at runtime:
    # insert edge invalid timestamp
    When executing query:
      """
      INSERT EDGE
        study(start_time, end_time)
      VALUES
        <vid_Laura>-><vid_sun_school>:(timestamp("2300-01-01T10:00:00"), now()+3600*24*365*3);
      """
    Then a ExecutionError should be raised at runtime:
    # insert vertex succeeded
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES <vid_Tom>:("Tom", 22)
      """
    Then the execution should be successful
    # insert vertex unordered order prop vertex succeeded
    When executing query:
      """
      INSERT VERTEX person(age, name) VALUES <vid_Conan>:(10, "Conan")
      """
    Then the execution should be successful
    # check vertex result with fetch
    When executing query:
      """
      FETCH PROP ON person <vid_Conan>
      """
    Then the result should be, in any order:
      | VertexID    | person.name | person.age |
      | <vid_Conan> | 'Conan'     | 10         |
    # insert vertex with string timestamp succeeded
    When executing query:
      """
      INSERT VERTEX
        school(name, create_time)
      VALUES
        <vid_sun_school>:("sun_school", timestamp("2010-01-01T10:00:00"))
      """
    Then the execution should be successful
    # insert edge succeeded
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Tom>-><vid_Lucy>:(85, "Lily")
      """
    Then the execution should be successful
    # insert edge with unordered prop edge
    When executing query:
      """
      INSERT EDGE schoolmate(nickname, likeness) VALUES <vid_Tom>-><vid_Bob>:("Superman", 87)
      """
    Then the execution should be successful
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON schoolmate <vid_Tom>-><vid_Bob>
      """
    Then the result should be, in any order:
      | schoolmate._src | schoolmate._dst | schoolmate._rank | schoolmate.likeness | schoolmate.nickname |
      | <vid_Tom>       | <vid_Bob>       | 0                | 87                  | 'Superman'          |
    # insert edge with timestamp succeed
    When executing query:
      """
      INSERT EDGE
        study(start_time, end_time)
      VALUES
        <vid_Laura>-><vid_sun_school>:(timestamp("2019-01-01T10:00:00"), now()+3600*24*365*3)
      """
    Then the execution should be successful
    # check edge result with go
    When executing query:
      """
      GO FROM <vid_Laura> OVER study
      YIELD $$.school.name, study._dst, $$.school.create_time, (string)study.start_time
      """
    Then the result should be, in any order:
      | $$.school.name | study._dst       | $$.school.create_time | (STRING)study.start_time |
      | "sun_school"   | <vid_sun_school> | 1262340000            | "1546336800"             |
    # check edge result with fetch
    When executing query:
      """
      FETCH PROP ON school <vid_sun_school>
      """
    Then the result should be, in any order:
      | VertexID         | school.name  | school.create_time |
      | <vid_sun_school> | "sun_school" | 1262340000         |
    # insert one vertex multi tags
    When executing query:
      """
      INSERT VERTEX
        person(name, age), student(grade, number)
      VALUES
        <vid_Lucy>:("Lucy", 8, "three", 20190901001)
      """
    Then the execution should be successful
    # insert one vertex multi tags with unordered order prop
    When executing query:
      """
      INSERT VERTEX
        person(age, name),student(number, grade)
      VALUES
        <vid_Bob>:(9, "Bob", 20191106001, "four")
      """
    Then the execution should be successful
    # check person tag result with fetch
    When executing query:
      """
      FETCH PROP ON person <vid_Bob>
      """
    Then the result should be, in any order:
      | VertexID  | person.name | person.age |
      | <vid_Bob> | 'Bob'       | 9          |
    # check student tag result with fetch
    When executing query:
      """
      FETCH PROP ON student <vid_Bob>
      """
    Then the result should be, in any order:
      | VertexID  | student.grade | student.number |
      | <vid_Bob> | 'four'        | 20191106001    |
    # insert multi vertex multi tags
    When executing query:
      """
      INSERT VERTEX
        person(name, age),student(grade, number)
      VALUES
        <vid_Laura>:("Laura", 8, "three", 20190901008),
        <vid_Amber>:("Amber", 9, "four", 20180901003)
      """
    Then the execution should be successful
    # insert multi vertex one tag
    When executing query:
      """
      INSERT VERTEX
        person(name, age)
      VALUES
        <vid_Kitty>:("Kitty", 8),
        <vid_Peter>:("Peter", 9)
      """
    Then the execution should be successful
    # insert multi edges
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        <vid_Tom>-><vid_Kitty>:(81, "Kitty"),
        <vid_Tom>-><vid_Peter>:(83, "Kitty")
      """
    Then the execution should be successful
    # check edge result with go
    When executing query:
      """
      GO FROM <vid_Tom> OVER schoolmate
      YIELD $^.person.name, schoolmate.likeness, $$.person.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | $$.person.name |
      | 'Tom'          | 85                  | 'Lucy'         |
      | 'Tom'          | 81                  | 'Kitty'        |
      | 'Tom'          | 83                  | 'Peter'        |
      | 'Tom'          | 87                  | 'Bob'          |
    # insert multi tag
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        <vid_Lucy>-><vid_Laura>:(90, "Laura"),
        <vid_Lucy>-><vid_Amber>:(95, "Amber")
      """
    Then the execution should be successful
    # insert with edge
    When executing query:
      """
      INSERT EDGE
        schoolmate(likeness, nickname)
      VALUES
        <vid_Laura>-><vid_Aero>:(90, "Aero")
      """
    Then the execution should be successful
    # get multi tag through go
    When executing query:
      """
      GO FROM <vid_Lucy> OVER schoolmate
      YIELD schoolmate.likeness, $$.person.name,$$.student.grade, $$.student.number
      """
    Then the result should be, in any order:
      | schoolmate.likeness | $$.person.name | $$.student.grade | $$.student.number |
      | 90                  | 'Laura'        | 'three'          | 20190901008       |
      | 95                  | 'Amber'        | 'four'           | 20180901003       |
    # test multi sentences multi tags succeeded
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES <vid_Aero>:("Aero", 8);
      INSERT VERTEX student(grade, number) VALUES <vid_Aero>:("four", 20190901003);
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Laura>-><vid_Aero>:(90, "Aero")
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM <vid_Laura> OVER schoolmate YIELD $$.student.number, $$.person.name
      """
    Then the result should be, in any order:
      | $$.student.number | $$.person.name |
      | 20190901003       | 'Aero'         |
    # test same prop name diff type
    When executing query:
      """
      INSERT VERTEX
        person(name, age), employee(name)
      VALUES
        <vid_Joy>:("Joy", 18, 123),
        <vid_Petter>:("Petter", 19, 456);
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Joy>-><vid_Petter>:(90, "Petter");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM <vid_Joy> OVER schoolmate
      YIELD $^.person.name,schoolmate.likeness, $$.person.name, $$.person.age,$$.employee.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmate.likeness | $$.person.name | $$.person.age | $$.employee.name |
      | 'Joy'          | 90                  | 'Petter'       | 19            | 456              |
    # test same prop name same type diff type
    When executing query:
      """
      INSERT VERTEX person(name, age),interest(name) VALUES <vid_Bob>:("Bob", 19, "basketball");
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Petter>-><vid_Bob>:(90, "Bob");
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM <vid_Petter> OVER schoolmate
      YIELD $^.person.name, $^.employee.name,schoolmate.likeness, $$.person.name, $$.interest.name, $$.person.age
      """
    Then the result should be, in any order:
      | $^.person.name | $^.employee.name | schoolmate.likeness | $$.person.name | $$.interest.name | $$.person.age |
      | 'Petter'       | 456              | 90                  | 'Bob'          | 'basketball'     | 19            |
    # insert vertex using name and age default value
    When executing query:
      """
      INSERT VERTEX personWithDefault() VALUES <vid_111>:();
      """
    Then the execution should be successful
    # insert vertex lack of the column value
    When executing query:
      """
      INSERT VERTEX personWithDefault(age, isMarried, BMI) VALUES <vid_Tom>:(18, false);
      """
    Then a SemanticError should be raised at runtime:
    # insert column doesn't match value count
    When executing query:
      """
      INSERT VERTEX studentWithDefault(grade, number) VALUES <vid_Tom>:("one", 111, "");
      """
    Then a SemanticError should be raised at runtime:
    # insert vertex using age default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES <vid_Tom>:("Tom")
      """
    Then the execution should be successful
    # insert vertex with BMI default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name, age) VALUES <vid_Tom>:("Tom", 20)
      """
    Then the execution should be successful
    # insert vertices multi tags with default value
    When executing query:
      """
      INSERT VERTEX
        personWithDefault(name, BMI),
        studentWithDefault(number)
      VALUES
        <vid_Laura>:("Laura", 21.5, 20190901008),
        <vid_Amber>:("Amber", 22.5, 20180901003)
      """
    Then the execution should be successful
    # multi vertices one tag with default value
    When executing query:
      """
      INSERT VERTEX personWithDefault(name) VALUES <vid_Kitty>:("Kitty"), <vid_Peter>:("Peter")
      """
    Then the execution should be successful
    # insert edge lack of the column value
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES <vid_Tom>-><vid_Lucy>:()
      """
    Then a SemanticError should be raised at runtime:
    # insert edge column count doesn't match value count
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness) VALUES <vid_Tom>-><vid_Lucy>:(60, "")
      """
    Then a SemanticError should be raised at runtime:
    # insert edge with all default value
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault() VALUES <vid_Tom>-><vid_Lucy>:()
      """
    Then the execution should be successful
    # insert edge with unknown filed name
    When executing query:
      """
      INSERT EDGE schoolmateWithDefault(likeness, redundant) VALUES <vid_Tom>-><vid_Lucy>:(90, 0)
      """
    Then a SemanticError should be raised at runtime:
    # insert multi edges with default value
    When executing query:
      """
      INSERT EDGE
        schoolmateWithDefault()
      VALUES
        <vid_Tom>-><vid_Kitty>:(),
        <vid_Tom>-><vid_Peter>:(),
        <vid_Lucy>-><vid_Laura>:(),
        <vid_Lucy>-><vid_Amber>:()
      """
    Then the execution should be successful
    # get result through go
    When executing query:
      """
      GO FROM <vid_Tom> OVER schoolmateWithDefault
      YIELD $^.person.name, schoolmateWithDefault.likeness, $$.person.name
      """
    Then the result should be, in any order:
      | $^.person.name | schoolmateWithDefault.likeness | $$.person.name |
      | 'Tom'          | 80                             | 'Kitty'        |
      | 'Tom'          | 80                             | 'Peter'        |
      | 'Tom'          | 80                             | 'Lucy'         |
    # get result through go
    When executing query:
      """
      GO FROM <vid_Lucy> OVER schoolmateWithDefault
      YIELD
        schoolmateWithDefault.likeness,
        $$.personWithDefault.name,
        $$.personWithDefault.birthday,
        $$.personWithDefault.department,
        $$.studentWithDefault.grade,
        $$.studentWithDefault.number
      """
    Then the result should be, in any order:
      | schoolmateWithDefault.likeness | $$.personWithDefault.name | $$.personWithDefault.birthday | $$.personWithDefault.department | $$.studentWithDefault.grade | $$.studentWithDefault.number |
      | 80                             | 'Laura'                   | 1578650400                    | 'engineering'                   | 'one'                       | 20190901008                  |
      | 80                             | 'Amber'                   | 1578650400                    | 'engineering'                   | 'one'                       | 20180901003                  |
    # insert multi version vertex
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES <vid_Tony>:("Tony", 18), <vid_Mack>:("Mack", 19);
      INSERT VERTEX person(name, age) VALUES <vid_Mack>:("Mack", 20);
      INSERT VERTEX person(name, age) VALUES <vid_Mack>:("Mack", 21)
      """
    Then the execution should be successful
    # insert multi version edge
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Tony>-><vid_Mack>@1:(1, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Tony>-><vid_Mack>@1:(2, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES <vid_Tony>-><vid_Mack>@1:(3, "");
      """
    Then the execution should be successful
    # get multi version result through go
    When executing query:
      """
      GO FROM <vid_Tony> OVER schoolmate YIELD $$.person.name, $$.person.age, schoolmate.likeness
      """
    Then the result should be, in any order:
      | $$.person.name | $$.person.age | schoolmate.likeness |
      | 'Mack'         | 21            | 3                   |
    Then drop the used space

    Examples: Vertical
      | vid_type       | FIXED_STRING(20) | int                |
      | vid_Tom        | "Tom"            | hash("Tom")        |
      | vid_Laura      | "Laura"          | hash("Laura")      |
      | vid_Amber      | "Amber"          | hash("Amber")      |
      | vid_Conan      | "Conan"          | hash("Conan")      |
      | vid_Lucy       | "Lucy"           | hash("Lucy")       |
      | vid_Bob        | "Bob"            | hash("Bob")        |
      | vid_Kitty      | "Kitty"          | hash("Kitty")      |
      | vid_Peter      | "Peter"          | hash("Peter")      |
      | vid_Petter     | "Petter"         | hash("Petter")     |
      | vid_sun_school | "sun_school"     | hash("sun_school") |
      | vid_Aero       | "Aero"           | hash("Aero")       |
      | vid_Joy        | "Joy"            | hash("Joy")        |
      | vid_111        | "111"            | 111                |
      | vid_Tony       | "Tony"           | hash("Tony")       |
      | vid_Mack       | "Mack"           | hash("Mack")       |

  @skip
  Scenario: insert multi version edge with uuid
    Given an empty graph
    And create a space with following options:
      | vid_type | int |
    And having executed:
      """
      CREATE TAG IF NOT EXISTS person(name string, age int);
      CREATE EDGE IF NOT EXISTS schoolmate(likeness int, nickname string);
      """
    And wait 3 seconds
    When executing query:
      """
      INSERT VERTEX person(name, age) VALUES uuid("Tony"):("Tony", 18), uuid("Mack"):("Mack", 19);
      INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 20);
      INSERT VERTEX person(name, age) VALUES uuid("Mack"):("Mack", 21);
      """
    Then the execution should be successful
    # insert multi version edge with uuid
    When executing query:
      """
      INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(1, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(2, "");
      INSERT EDGE schoolmate(likeness, nickname) VALUES uuid("Tony")->uuid("Mack")@1:(3, "");
      """
    Then the execution should be successful
    # get multi version result through go
    When executing query:
      """
      GO FROM uuid("Tony") OVER schoolmate YIELD $$.person.name, $$.person.age, schoolmate.likeness
      """
    Then the result should be, in any order:
      | $$.person.name | $$.person.age | schoolmate.likeness |
      | 'Mack'         | 21            | 3                   |
    Then drop the used space

  Scenario: insert vertex and edge test by the 2.0 new type
    Given an empty graph
    And create a space with following options:
      | partition_num  | 9                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(10) |
    And having executed:
      """
      CREATE TAG student(name string NOT NULL, age int);
      CREATE TAG course(name fixed_string(5) NOT NULL, introduce string DEFAULT NULL);
      """
    And wait 3 seconds
    # test insert out of range id size
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "12345678901":("Tom", 2)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The VID must be a 64-bit interger or a string.
    # test insert not null prop
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "Tom":(NULL, 12)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The not null field cannot be null.
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "Tom":(NULL, 12)
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The not null field cannot be null.
    # test insert with fixed_string
    When executing query:
      """
      INSERT VERTEX course(name) VALUES "Math":("Math")
      """
    Then the execution should be successful
    # out of fixed_string's size
    When executing query:
      """
      INSERT VERTEX course(name) VALUES "English":("English")
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      FETCH PROP ON course "English"
      """
    Then the result should be, in any order:
      | VertexID  | course.name | course.introduce |
      | 'English' | 'Engli'     | NULL             |
    # test insert with empty str vid
    When executing query:
      """
      INSERT VERTEX student(name, age) VALUES "":("Tom", 12)
      """
    Then the execution should be successful
    # check result
    When executing query:
      """
      FETCH PROP ON student ""
      """
    Then the result should be, in any order:
      | VertexID | student.name | student.age |
      | ''       | 'Tom'        | 12          |
    Then drop the used space
