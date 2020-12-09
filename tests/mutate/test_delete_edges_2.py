# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from tests.common.nebula_test_suite import NebulaTestSuite


class TestDeleteEdges2(NebulaTestSuite):
    @classmethod
    def prepare(self):
        self.load_data()

    def test_delete_with_pipe_wrong_vid_type(self):
        resp = self.execute('GO FROM "Boris Diaw" OVER like YIELD like._type as id | DELETE EDGE like $-.id->$-.id')
        self.check_resp_failed(resp)

    def test_delete_with_pipe(self):
        resp = self.execute('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Tony Parker"], ["Tim Duncan"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('GO FROM "Boris Diaw" OVER like YIELD '
                                  'like._src as src, like._dst as dst, like._rank as rank'
                                  ' | DELETE EDGE like $-.src->$-.dst @ $-.rank')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.execute('GO FROM "Boris Diaw" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)

    def test_delete_with_var(self):
        resp = self.execute('GO FROM "Russell Westbrook" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = [["Paul George"], ["James Harden"]]
        self.check_out_of_order_result(resp, expect_result)

        resp = self.execute('$var = GO FROM "Russell Westbrook" OVER like YIELD '
                                  'like._src as src, like._dst as dst, like._rank as rank;'
                                  ' DELETE EDGE like $var.src -> $var.dst @ $var.rank')
        self.check_resp_succeeded(resp)

        # check result
        resp = self.execute('GO FROM "Russell Westbrook" OVER like')
        self.check_resp_succeeded(resp)
        expect_result = []
        self.check_out_of_order_result(resp, expect_result)
