# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from tests.common.nebula_test_suite import NebulaTestSuite

class TestSession(NebulaTestSuite):
    def __init__(self):
        super(TestSession, self).__init__(5)

    @classmethod
    def prepare(self):
        query = 'CREATE USER session_user WITH PASSWORD "123456"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(self):
        query = 'DROP USER session_user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_create_session(self):
        # add session with right username
        self.get_new_session()

        # add session with not exist username

