# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

if(NOT ${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    find_program(PYTHON_EXECUTABLE python3
        /usr/bin /usr/local/bin
        NO_CMAKE_ENVIRONMENT_PATH
        NO_SYSTEM_ENVIRONMENT_PATH
    )
    find_program(PYTHON_EXECUTABLE python)

    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/tck_test.out-of-source
        ${CMAKE_CURRENT_BINARY_DIR}/nebula_tck_test.py
        @ONLY
    )
endif()

execute_process(
   COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/nebula.json ${CMAKE_CURRENT_BINARY_DIR}
   COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/pytest.ini ${CMAKE_CURRENT_BINARY_DIR}
   COMMAND cp -r ${CMAKE_CURRENT_SOURCE_DIR}/test ${CMAKE_CURRENT_BINARY_DIR}
   COMMAND cp -r ${CMAKE_CURRENT_SOURCE_DIR}/data ${CMAKE_CURRENT_BINARY_DIR}
   COMMAND chmod +x nebula_tck_test.py
   COMMAND ${CMAKE_COMMAND} -E create_symlink ./nebula_tck_test.py tck_test
   COMMAND ${CMAKE_COMMAND} -E create_symlink ./nebula_tck_test.py nebula_tck_test
   WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
 )
