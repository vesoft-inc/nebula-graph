# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

macro(add_dependent_project)
    cmake_parse_arguments(
        dep_proj_args               # prefix
        ""                          # <options>
        "BASE;NAME;REPO;TAG"        # <one_value_args>
        ""                          # <multi_value_args>
        ${ARGN}
    )

    set(CLONE_DIR ${dep_proj_args_BASE}/${dep_proj_args_NAME})
    # Clone or update the repo
    if(EXISTS ${CLONE_DIR}/.git)
        message(STATUS "Updating from the repo \"" ${dep_proj_args_REPO} "\"")
        execute_process(
            COMMAND ${GIT_EXECUTABLE} pull --depth=1
            WORKING_DIRECTORY ${CLONE_DIR}
            RESULT_VARIABLE clone_result
        )
    else()
        message(STATUS "Cloning from the repo \"" ${dep_proj_args_REPO} "\"")
        execute_process(
            COMMAND ${GIT_EXECUTABLE} clone
                --depth 1
                --progress
                --single-branch
                --branch ${dep_proj_args_TAG}
                ${dep_proj_args_REPO}
                ${CLONE_DIR}
            RESULT_VARIABLE clone_result
        )
    endif()

    if(NOT ${clone_result} EQUAL 0)
        message(FATAL_ERROR "Cannot clone the repo from \"${dep_proj_args_REPO}\" (branch \"${dep_proj_args_TAG}\"): \"${clone_result}\"")
    else()
        message(STATUS "Updated the repo from \"${dep_proj_args_REPO}\" (branch \"${dep_proj_args_TAG}\")")
    endif()

    # Configure the repo
    execute_process(
        COMMAND
            ${CMAKE_COMMAND}
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DNEBULA_THIRDPARTY_ROOT=${NEBULA_THIRDPARTY_ROOT}
            -DNEBULA_OTHER_ROOT=${NEBULA_OTHER_ROOT}
            -DENABLE_JEMALLOC=${ENABLE_JEMALLOC}
            -DENABLE_NATIVE=${ENABLE_NATIVE}
            -DENABLE_TESTING=false
            -DENABLE_CCACHE=${ENABLE_CCACHE}
            -DENABLE_ASAN=${ENABLE_ASAN}
            -DENABLE_UBSAN=${ENABLE_UBSAN}
            ${dep_proj_args_ARGN}
            .
        WORKING_DIRECTORY ${CLONE_DIR}
        RESULT_VARIABLE cmake_result
    )
    if(NOT ${cmake_result} EQUAL 0)
        message(FATAL_ERROR "Failed to configure the dependent project \"" ${dep_proj_args_NAME} "\"")
    endif()

    # Add a custom target to build the project
    add_custom_target(
        ${dep_proj_args_NAME}_project ALL
        COMMAND make -j
        WORKING_DIRECTORY ${CLONE_DIR}
    )

endmacro()
