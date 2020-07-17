#!/usr/bin/env bash
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex -o pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)/.."
BUILD_DIR=$PROJ_DIR/_build
TOOLSET_DIR=/opt/vesoft/toolset/clang/9.0.0

mkdir -p $BUILD_DIR

function prepare_pip3() {
    pip3 install -U setuptools -i https://mirrors.aliyun.com/pypi/simple/
    pip3 install -r $PROJ_DIR/tests/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
}

function prepare() {
    $PROJ_DIR/ci/deploy.sh
    prepare_pip3
}

function lint() {
    cd $PROJ_DIR
    ln -snf $PROJ_DIR/.linters/cpp/hooks/pre-commit.sh $PROJ_DIR/.linters/cpp/pre-commit.sh
    $PROJ_DIR/.linters/cpp/pre-commit.sh $(git --no-pager diff --diff-filter=d --name-only HEAD^ HEAD)
}

function build_common() {
    cmake --build $PROJ_DIR/modules/common -j$(nproc)
}

function build_storage() {
    cmake --build $PROJ_DIR/modules/storage --target nebula-storaged -j$(nproc)
    cmake --build $PROJ_DIR/modules/storage --target nebula-metad -j$(nproc)
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
    # CI
    cd $BUILD_DIR/tests
    ./ntr -h
    #./ntr $PROJ_DIR/tests/admin/* $PROJ_DIR/tests/maintain/* $PROJ_DIR/tests/query/stateless/test_schema.py
}

case "$1" in
    prepare)
        prepare
        ;;
    pip)
        prepare_pip3
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
