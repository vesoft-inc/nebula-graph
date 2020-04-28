# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


nebula_add_test(
    NAME id_generator_test
    SOURCES IdGeneratorTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:id_generator_obj>
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_concurrent_obj>
    LIBRARIES
        gtest gtest_main
)
