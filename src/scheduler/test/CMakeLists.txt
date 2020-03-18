# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

nebula_add_test(
    NAME
        scheduler_test
    SOURCES
        SchedulerTest.cpp
    OBJECTS
        # common_base_obj
        # $<TARGET_OBJECTS:common_parser_obj>
        $<TARGET_OBJECTS:common_filter_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        # $<TARGET_OBJECTS:common_gflags_man_obj>
        # $<TARGET_OBJECTS:common_schema_obj>
        # $<TARGET_OBJECTS:common_meta_client>
        # $<TARGET_OBJECTS:common_stats_obj>
        $<TARGET_OBJECTS:common_time_obj>
        # $<TARGET_OBJECTS:common_meta_thrift_obj>
        # $<TARGET_OBJECTS:common_storage_client>
        # $<TARGET_OBJECTS:common_storage_thrift_obj>
        # $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_base_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        wangle
        gtest
        gtest_main
  )
