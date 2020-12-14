#!/usr/bin/env bash
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -e -o pipefail

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)/.."
BUILD_DIR=$PROJ_DIR/build
TOOLSET_DIR=/opt/vesoft/toolset/clang/9.0.0

mkdir -p $BUILD_DIR

function get_py_client() {
    git clone https://github.com/vesoft-inc/nebula-python.git
    pushd nebula-python
    python3 setup.py install --user
    popd
    rm -rf nebula-python
}

function prepare() {
    pip3 install --user -U setuptools -i https://mirrors.aliyun.com/pypi/simple/
    pip3 install --user -r $PROJ_DIR/tests/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
    get_py_client
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

function configure_clang() {
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
}

function clang_compile() {
    configure_clang
    cd $PROJ_DIR
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

function test_in_cluster() {
    cd $BUILD_DIR/tests
    export PYTHONPATH=$PROJ_DIR:$PYTHONPATH
    testpath=$(cat $PROJ_DIR/ci/tests.txt | sed "s|\(.*\)|$PROJ_DIR/tests/\1|g" | tr '\n' ' ')
    ./ntr \
        -n=8 \
        --dist=loadfile \
        --address="nebulaclusters-graphd:3699" \
        $testpath

    ./ntr --address="nebulaclusters-graphd:3699" $PROJ_DIR/tests/job/*
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
    k8s)
        prepare
        configure_clang
        test_in_cluster
        ;;
    *)
        prepare
        gcc_compile
        run_ctest
        run_test
        ;;
esac
