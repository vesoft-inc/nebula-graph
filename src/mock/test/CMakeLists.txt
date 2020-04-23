# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#


nebula_add_test(
    NAME
        mock_server_test
    SOURCES
        TestMock.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_fs_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_conf_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_graph_thrift_obj>
        $<TARGET_OBJECTS:common_storage_thrift_obj>
        $<TARGET_OBJECTS:common_network_obj>
        $<TARGET_OBJECTS:common_meta_obj>
        $<TARGET_OBJECTS:common_meta_client_obj>
        $<TARGET_OBJECTS:common_graph_client_obj>
        $<TARGET_OBJECTS:common_graph_storage_client_obj>
        $<TARGET_OBJECTS:common_storage_client_base_obj>
        $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
        $<TARGET_OBJECTS:mock_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
        gtest_main
)
