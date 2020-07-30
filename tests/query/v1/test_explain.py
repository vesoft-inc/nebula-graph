# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

import pytest
from nebula2.graph import ttypes

from nebula_test_common.nebula_test_suite import NebulaTestSuite


class TestYield(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        # cls.load_data()
        resp = cls.execute(
            'CREATE SPACE IF NOT EXISTS mySpace(partition_num=1, vid_size=20)')
        cls.check_resp_succeeded(resp)

        # 2.0 use space get from cache
        time.sleep(cls.delay)

        resp = cls.execute('USE mySpace')
        cls.check_resp_succeeded(resp)

    @classmethod
    def cleanup(cls):
        pass

    def test_explain(self):
        query = 'EXPLAIN YIELD 1 AS id;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="row" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="dot" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="unknown" YIELD 1;'
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

    def test_explain_stmts_block(self):
        query = 'EXPLAIN {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="row" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = 'EXPLAIN FORMAT="dot" {$var = YIELD 1 AS a; YIELD $var.*;};'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)

        query = '''EXPLAIN FORMAT="unknown" \
            {$var = YIELD 1 AS a; YIELD $var.*;};'''
        resp = self.execute_query(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)
