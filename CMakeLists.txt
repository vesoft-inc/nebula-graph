# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#
# CMakeLists.txt file contains a set of directives and instructions describing
# the project's source files and targets (executable, library, or both).
<<<<<<< HEAD
=======
#
# The building can be controlled by defining the following variables on the
# <cmake> command line.
>>>>>>> Support package
#
# The building can be controlled by defining the following variables on the
# <cmake> command line.
#
#   CMAKE_C_COMPILER                -- Specify the compiler for C language
#   CMAKE_CXX_COMPILER              -- Specify the compiler for C++ language
#
<<<<<<< HEAD
#   NEBULA_THIRDPARTY_ROOT          -- Specify the root directory for third-party
#   NEBULA_OTHER_ROOT               -- Specify the root directory for user build
#                                   -- Split with ":", exp: DIR:DIR
=======
#   NEBULA_THIRDPARTY_ROOT         -- Specify the root directory for third-party
#   NEBULA_OTHER_ROOT              -- Specify the root directory for user building
#                                  -- Split with ":", exp: DIR:DIR
>>>>>>> Support package
#
#   NEBULA_COMMON_REPO_URL          -- Git URL for the nebula-common repo
#   NEBULA_COMMON_REPO_TAG          -- Tag/branch of the nebula-common repo
#
#   NEBULA_STORAGE_REPO_URL         -- Git URL for the nebula-storage repo
#   NEBULA_STORAGE_REPO_TAG         -- Tag/branch of the nebula-storage repo
#   ENABLE_BUILD_STORAGE            -- Build storage repo
#
#   ENABLE_JEMALLOC                -- Link jemalloc into all executables
#   ENABLE_NATIVE                  -- Build native client
#   ENABLE_TESTING                 -- Build unit test
#   ENABLE_UPDATE_EXTERNAL_PROJ    -- Whether to update external project
#
# CMake version check
cmake_minimum_required(VERSION 3.5.0)

# Set the project name
project("Nebula Graph" C CXX)

<<<<<<< HEAD
option(ENABLE_PACK_ONE "Whether to package into one" ON)
option(ENABLE_BUILD_STORAGE "Whether to build storage" ON)
option(ENABLE_MODULE_UPDATE "Automatically update module" OFF)
=======
# Set the project home dir
set(NEBULA_HOME ${CMAKE_CURRENT_SOURCE_DIR})
add_definitions(-DNEBULA_HOME=${NEBULA_HOME})

set(PACKAGE_NAME "nebula")

# To include customized FindXXX.cmake modules
set(CMAKE_MODULE_PATH
    "${NEBULA_HOME}/cmake"
    "${NEBULA_HOME}/modules/common/"
    "${NEBULA_HOME}/modules/common/cmake"
    ${CMAKE_MODULE_PATH})

<<<<<<< HEAD
>>>>>>> Support package
=======
>>>>>>> Support package

add_definitions(-DNEBULA_HOME=${CMAKE_SOURCE_DIR})

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# Submodules
if("${NEBULA_COMMON_REPO_URL}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_URL "git@github.com:vesoft-inc-private/nebula-common.git")
endif()

if("${NEBULA_COMMON_REPO_TAG}" STREQUAL "")
    SET(NEBULA_COMMON_REPO_TAG "master")
endif()

<<<<<<< HEAD
if("${NEBULA_STORAGE_REPO_URL}" STREQUAL "")
    SET(NEBULA_STORAGE_REPO_URL "git@github.com:vesoft-inc-private/nebula-storage.git")
endif()

if("${NEBULA_STORAGE_REPO_TAG}" STREQUAL "")
    SET(NEBULA_STORAGE_REPO_TAG "master")
endif()

include(FetchModule)
if(NOT NEBULA_COMMON_SOURCE_DIR)
    nebula_fetch_module(
        NAME
            common
        URL
            ${NEBULA_COMMON_REPO_URL}
        TAG
            ${NEBULA_COMMON_REPO_TAG}
        UPDATE
            ${ENABLE_MODULE_UPDATE}
    )
    set(nebula_common_source_dir ${CMAKE_SOURCE_DIR}/modules/common)
    set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
else()
    set(nebula_common_source_dir ${NEBULA_COMMON_SOURCE_DIR})
    if(NOT NEBULA_COMMON_BUILD_DIR)
        set(nebula_common_build_dir ${CMAKE_BINARY_DIR}/modules/common)
    else()
        set(nebula_common_build_dir ${NEBULA_COMMON_BUILD_DIR})
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake)
list(APPEND CMAKE_MODULE_PATH ${nebula_common_source_dir}/cmake/nebula)

include(GeneralCMakeOptions)
include(GeneralCMakeConfig)
include(GeneralCompilerConfig)
include(LinkerConfig)
include(CcacheConfig)
include(ThirdPartyConfig)
include(SanitizerConfig)
include(GitHooksConfig)
include(GitInfoConfig)
include(NebulaCMakeMacros)
include(NebulaCustomTargets)
include(ConfigNebulaCommon)
config_nebula_common(
    SOURCE_DIR ${nebula_common_source_dir}
    BUILD_DIR ${nebula_common_build_dir}
)
if(ENABLE_BUILD_STORAGE)
    include(ConfigNebulaStorage)
    nebula_fetch_module(
        NAME
            storage
        URL
            ${NEBULA_STORAGE_REPO_URL}
        TAG
            ${NEBULA_STORAGE_REPO_TAG}
        UPDATE
            ${ENABLE_MODULE_UPDATE}
    )
    config_nebula_storage(
        SOURCE_DIR ${CMAKE_SOURCE_DIR}/modules/storage
        BUILD_DIR ${CMAKE_BINARY_DIR}/modules/storage
        COMMON_SOURCE_DIR ${CMAKE_SOURCE_DIR}/modules/common
        COMMON_BUILD_DIR ${CMAKE_BINARY_DIR}/modules/common
    )
endif()

add_custom_target(
    clean-modules
    DEPENDS clean-common
)
if(TARGET clean-storage)
    add_dependencies(clean-modules clean-storage)
endif()


if (ENABLE_NATIVE)
    message(STATUS "ENABLE_NATIVE is ${ENABLE_NATIVE}")
    add_compile_options(-fPIC)
endif()

include_directories(AFTER ${CMAKE_SOURCE_DIR}/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)

# For simplicity, we make all ordinary libraries depend on the compile-time generated files,
# including the precompiled header, a.k.a Base.h.gch, and thrift headers.
macro(nebula_add_library name type)
    add_library(${name} ${type} ${ARGN})
    if (PCHSupport_FOUND)
        add_dependencies(
            ${name}
            base_obj_gch
        )
    endif()
    add_dependencies(
        ${name}
        common_project
        parser_target
    )
endmacro()

nebula_add_subdirectory(src)
nebula_add_subdirectory(conf)
nebula_add_subdirectory(resources)
nebula_add_subdirectory(tests)

# package nebula to a deb/rpm file
set(CPACK_PACKAGE_DESCRIPTION "nebula-graph")
set(CPACK_PACKAGE_CONTACT "nebula-graph")
set(CPACK_PACKAGE_VERSION ${NEBULA_BUILD_VERSION})
set(CPACK_RPM_PACKAGE_LICENSE "Apache 2.0 + Common Clause 1.0")
set(CPACK_PACKAGE_NAME nebula-graph)
set(CPACK_SET_DESTDIR TRUE)
set(CPACK_PACKAGE_RELOCATABLE FALSE)
set(CPACK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)

set(CPACK_RPM_SPEC_MORE_DEFINE "%define debug_package %{nil}
        %define __os_install_post %{nil}")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
set(CPACK_RPM_PACKAGE_URL "https://github.com/vesoft-inc/nebula/releases")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_CURRENT_SOURCE_DIR}/package/postinst)
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION /usr/local)
=======
SET(NEBULA_COMMON_PACKAGE "nebula-common")
SET(nebula-common_DIR ${CMAKE_CURRENT_SOURCE_DIR}/modules/common)

# Configure the dependent projects
include(AddDependentProject)
message(STATUS "")
message(STATUS ">>>>> Configuring the nebula-common repo <<<<<")
add_dependent_project(
    BASE ${NEBULA_HOME}/modules
    NAME common
    REPO ${NEBULA_COMMON_REPO_URL}
    TAG  ${NEBULA_COMMON_REPO_TAG}
    JOBS ${MODULE_BUILDING_JOBS}
)
find_package(${NEBULA_COMMON_PACKAGE} REQUIRED)
message(STATUS ">>>>> The nebula-common repo has been configured successfully <<<<<")
message(STATUS "")

if(ENABLE_BUILD_STORAGE)
    if("${NEBULA_STORAGE_REPO_URL}" STREQUAL "")
        SET(NEBULA_STORAGE_REPO_URL "git@github.com:vesoft-inc-private/nebula-storage.git")
    endif()

    if("${NEBULA_STORAGE_REPO_TAG}" STREQUAL "")
        SET(NEBULA_STORAGE_REPO_TAG "master")
    endif()

    SET(NEBULA_STORAGE_PACKAGE "nebula-storage")
    SET(nebula-storage_DIR ${CMAKE_CURRENT_SOURCE_DIR}/modules/storage)

    # Configure the dependent projects
    message(STATUS "")
    message(STATUS ">>>>> Configuring the nebula-storage repo <<<<<")
    add_dependent_project(
        BASE ${NEBULA_HOME}/modules
        NAME storage
        REPO ${NEBULA_STORAGE_REPO_URL}
        TAG  ${NEBULA_STORAGE_REPO_TAG}
             -DNEBULA_COMMON_PATH=${nebula-common_DIR} " "
             -DNEBULA_COMMON_REPO_URL=${NEBULA_COMMON_REPO_URL} " "
             -DNEBULA_COMMON_REPO_TAG=${NEBULA_COMMON_REPO_TAG}
        JOBS ${MODULE_BUILDING_JOBS}
    )
    message(STATUS ">>>>> The nebula-storage repo has been configured successfully <<<<<")
    message(STATUS "")
endif()

add_subdirectory(tests)

include(GetGitInfo)
include(LinkerConfig)
include(BuildOption)
<<<<<<< HEAD
<<<<<<< HEAD
option(ENABLE_UPDATE_EXTERNAL_PROJ "Whether to update external project" OFF)
=======

>>>>>>> Support package
=======
option(ENABLE_UPDATE_EXTERNAL_PROJ "Whether to update external project" OFF)
>>>>>>> rebase upstream

find_package(Bzip2 REQUIRED)
find_package(DoubleConversion REQUIRED)
find_package(Fbthrift REQUIRED)
find_package(Folly REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(Googletest REQUIRED)
if(ENABLE_JEMALLOC)
    find_package(Jemalloc REQUIRED)
endif()
find_package(Libevent REQUIRED)
find_package(Mstch REQUIRED)
find_package(Proxygen REQUIRED)
find_package(Snappy REQUIRED)
find_package(Wangle REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Zstd REQUIRED)
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Krb5 REQUIRED gssapi)
find_package(GPERF 2.8 REQUIRED)
find_package(Libunwind REQUIRED)
find_package(BISON 3.0.5 REQUIRED)
include(MakeBisonRelocatable)
find_package(FLEX REQUIRED)
find_package(Readline REQUIRED)
find_package(NCURSES REQUIRED)
find_package(LibLZMA MODULE)

include(BuildMacro)

include_directories(AFTER ${NEBULA_HOME}/src)
include_directories(AFTER ${NEBULA_HOME}/modules/common/src)
include_directories(AFTER ${CMAKE_CURRENT_BINARY_DIR}/src)

add_subdirectory(src)
add_subdirectory(conf)
add_subdirectory(scripts)
add_subdirectory(resources)
<<<<<<< HEAD
>>>>>>> Support package
=======
>>>>>>> Support package

include(CleanBuild)
