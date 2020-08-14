#!/usr/bin/env bash
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex -o pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)/.."
BUILD_DIR=$PROJ_DIR/build
TOOLSET_DIR=/opt/vesoft/toolset/clang/9.0.0

mkdir -p $BUILD_DIR

function prepare() {
    pip3 install -U setuptools -i https://mirrors.aliyun.com/pypi/simple/
    pip3 install -r $PROJ_DIR/tests/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
}

function lint() {
    cd $PROJ_DIR
    ln -snf $PROJ_DIR/.linters/cpp/hooks/pre-commit.sh $PROJ_DIR/.linters/cpp/pre-commit.sh
    $PROJ_DIR/.linters/cpp/pre-commit.sh $(git --no-pager diff --diff-filter=d --name-only HEAD^ HEAD)
}

function build_common() {
    cmake --build $BUILD_DIR/modules/common -j$(nproc)
}

function build_storage() {
    cmake --build $BUILD_DIR/modules/storage --target nebula-storaged -j$(nproc)
    cmake --build $BUILD_DIR/modules/storage --target nebula-metad -j$(nproc)
}

function gcc_compile() {
    cd $PROJ_DIR
    cmake \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=on \
        -DCMAKE_CXX_COMPILER=$TOOLSET_DIR/bin/g++ \
        -DCMAKE_C_COMPILER=$TOOLSET_DIR/bin/gcc \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_TESTING=on \
        -DENABLE_BUILD_STORAGE=on \
        -DNEBULA_STORAGE_REPO_URL=$NEBULA_STORAGE_REPO_URL \
        -DNEBULA_COMMON_REPO_URL=$NEBULA_COMMON_REPO_URL \
        -B $BUILD_DIR
    build_common
    build_storage
    cmake --build $BUILD_DIR -j$(nproc)
}

function clang_compile() {
    cd $PROJ_DIR
    cmake \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=on \
        -DCMAKE_CXX_COMPILER=$TOOLSET_DIR/bin/clang++ \
        -DCMAKE_C_COMPILER=$TOOLSET_DIR/bin/clang \
        -DCMAKE_BUILD_TYPE=Debug \
        -DENABLE_ASAN=on \
        -DENABLE_TESTING=on \
        -DENABLE_BUILD_STORAGE=on \
        -DNEBULA_STORAGE_REPO_URL=$NEBULA_STORAGE_REPO_URL \
        -DNEBULA_COMMON_REPO_URL=$NEBULA_COMMON_REPO_URL \
        -B $BUILD_DIR
    build_common
    build_storage
    cmake --build $BUILD_DIR -j$(nproc)
}

function run_ctest() {
    cd $BUILD_DIR
    ctest -j$(nproc) \
          --timeout 400 \
          --output-on-failure
}

function run_test() {
    cd $BUILD_DIR/tests
    ./ntr \
        -n=8 \
        --dist=loadfile \
        --debug_log=false \
        $PROJ_DIR/tests/admin/* \
        $PROJ_DIR/tests/maintain/* \
        $PROJ_DIR/tests/mutate/* \
        $PROJ_DIR/tests/query/stateless/test_new_go.py \
        $PROJ_DIR/tests/query/stateless/test_new_groupby.py \
        $PROJ_DIR/tests/query/v1/* \
        $PROJ_DIR/tests/query/v2/* \
        $PROJ_DIR/tests/query/stateless/test_schema.py \
        $PROJ_DIR/tests/query/stateless/test_if_exists.py \
        $PROJ_DIR/tests/query/stateless/test_range.py \
        $PROJ_DIR/tests/query/stateless/test_go.py \
        $PROJ_DIR/tests/query/stateless/test_simple_query.py
}

case "$1" in
    prepare)
        prepare
        ;;
    lint)
        lint
        ;;
    clang)
        clang_compile
        ;;
    gcc)
        gcc_compile
        ;;
    ctest)
        run_ctest
        ;;
    test)
        run_test
        ;;
    *)
        prepare
        gcc_compile
        run_ctest
        run_test
        ;;
esac
