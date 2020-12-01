Feature: Value parsing

  Scenario: Parsing from text
  Given A set of string:
    | format                                               | type         |
    | EMPTY                                                | EMPTY        |
    | NULL                                                 | NULL         |
    | NaN                                                  | NaN          |
    | BAD_DATA                                             | BAD_DATA     |
    | BAD_TYPE                                             | BAD_TYPE     |
    | OVERFLOW                                             | ERR_OVERFLOW |
    | UNKNOWN_PROP                                         | UNKNOWN_PROP |
    | DIV_BY_ZERO                                          | DIV_BY_ZERO  |
    | OUT_OF_RANGE                                         | OUT_OF_RANGE |
    | 123                                                  | iVal         |
    | -123                                                 | iVal         |
    | 3.14                                                 | fVal         |
    | -3.14                                                | fVal         |
    | true                                                 | bVal         |
    | false                                                | bVal         |
    | 'string'                                             | sVal         |
    | "string"                                             | sVal         |
    | "string'substr'"                                     | sVal         |
    | 'string"substr"'                                     | sVal         |
    | []                                                   | lVal         |
    | [1,2,3]                                              | lVal         |
    | [<-[:e2{}]-,-[:e3{}]->]                              | lVal         |
    | {1,2,3}                                              | uVal         |
    | {}                                                   | mVal         |
    | {k1: 1, 'k2':true}                                   | mVal         |
    | ()                                                   | vVal         |
    | ('vid')                                              | vVal         |
    | (:t)                                                 | vVal         |
    | (:t{}:t)                                             | vVal         |
    | ('vid':t)                                            | vVal         |
    | ('vid':t:t)                                          | vVal         |
    | ('vid':t{p1:0,p2:' '})                               | vVal         |
    | ('vid':t{p1:0,p2:' '}:t{})                           | vVal         |
    | -->                                                  | eVal         |
    | <--                                                  | eVal         |
    | -[]->                                                | eVal         |
    | -[:e]->                                              | eVal         |
    | -[@-1]->                                             | eVal         |
    | -['1'->'2']->                                        | eVal         |
    | -[{p:0}]->                                           | eVal         |
    | <-[:e{}]-                                            | eVal         |
    | -[:e{p1:0,p2:true}]->                                | eVal         |
    | <-[:e@0{p1:0,p2:true}]-                              | eVal         |
    | -[:e{p1:0,p2:true}]->                                | eVal         |
    | <-[:e@-1{p1:0,p2:true}]-                             | eVal         |
    | <()>                                                 | pVal         |
    | <()-->()<--()>                                       | pVal         |
    | <('v1':t{})>                                         | pVal         |
    | <('v1':t{})-[:e1{}]->('v2':t{})<-[:e2{}]-('v3':t{})> | pVal         |
  When They are parsed as Nebula Value
  Then It must succeed
  And The type of the parsed value should be as expected

  Scenario: Convert string table to nebula DataSet
  Given A set of string:
    | _path                                                | vertex                                                               |
    | <('v1':t{})-[:e1{}]->('v2':t{})<-[:e2{}]-('v3':t{})> | ("vid":player{name:'Tim Duncan'}:bachelor{speciality: "psychology"}) |
  When They are parsed as Nebula DataSet
  Then It must succeed

  Scenario: Test executing query
  Given A set of string:
    | _path                                                | vertex                                                               |
    | <('v1':t{})-[:e1{}]->('v2':t{})<-[:e2{}]-('v3':t{})> | ("vid":player{name:'Tim Duncan'}:bachelor{speciality: "psychology"}) |
  When executing query:
    """
    SHOW SPACES;
    SHOW HOSTS;
    """
  Then the result should be, in any order:
    | Name  |
    | 'nba' |
