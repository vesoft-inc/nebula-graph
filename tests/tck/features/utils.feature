Feature: Value parsing

    Scenario: Parsing from text
    Given A set of string
        | string                                                | typename  |
        | 123                                                   | int       |
        | -123                                                  | int       |
        | 3.14                                                  | float     |
        | -3.14                                                 | float     |
        | true                                                  | bool      |
        | false                                                 | bool      |
        | 'string'                                              | str       |
        | "string"                                              | str       |
        | "string'substr'"                                      | str       |
        | 'string"substr"'                                      | str       |
        | []                                                    | list      |
        | [1,2,3]                                               | list      |
        | [-[:e1{}]-,<-[:e2{}]-,-[:e3{}]->]                     | list      |
        | {1,2,3}                                               | set       |
        | {}                                                    | dict      |
        | {k1: 1, k2:true}                                      | dict      |
        | ('vid':t{p1:0,p2:' '})                                | Node      |
        | -[:e{p1:0,p2:true}]-                                  | Edge      |
        | <-[:e@0{p1:0,p2:true}]-                               | Edge      |
        | -[:e{p1:0,p2:true}]->                                 | Edge      |
        | <-[:e@-1{p1:0,p2:true}]->                             | Edge      |
        | <('v1':t{})>                                          | Path      |
        | <('v1':t{})-[:e1{}]-('v2':t{})<-[:e2{}]->('v3':t{})>  | Path      |
        | len([1, 2, 3])                                        | int      |
    When They are parsed as Nebula Value
    Then It must succeed
    And The type of the parsed value should be as expected
