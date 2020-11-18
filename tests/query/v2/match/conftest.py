# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import pytest


@pytest.fixture
def like(request):
    def _like(src: str, dst: str):
        EDGES = request.cls.EDGES
        return EDGES[src+dst+'like'+str(0)]
    return _like


@pytest.fixture
def like_row_2hop(like):
    def _like_row_2hop(src: str, dst1: str, dst2: str):
        return [like(src, dst1), like(dst1, dst2)]
    return _like_row_2hop


@pytest.fixture
def like_row_2hop_start_with(like):
    def _like_row_2hop_start_with(start: str):
        def _like_row_2hop(dst1: str, dst2: str):
            return [like(start, dst1), like(dst1, dst2)]
        return _like_row_2hop
    return _like_row_2hop_start_with


@pytest.fixture
def like_row_3hop(like, like_row_2hop):
    def _like_row_3hop(src: str, dst1: str, dst2: str, dst3: str):
        return like_row_2hop(src, dst1, dst2) + [like(dst2, dst3)]
    return _like_row_3hop


@pytest.fixture
def like_row_3hop_start_with(like, like_row_2hop_start_with):
    def _like_row_3hop_start_with(start: str):
        like_row_2hop = like_row_2hop_start_with(start)

        def _like_row_3hop(dst1: str, dst2: str, dst3: str):
            return like_row_2hop(dst1, dst2) + [like(dst2, dst3)]
        return _like_row_3hop
    return _like_row_3hop_start_with


@pytest.fixture
def serve(request):
    def _serve(src: str, dst: str):
        EDGES = request.cls.EDGES
        return EDGES[src+dst+'serve'+str(0)]
    return _serve
