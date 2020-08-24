# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite

from nebula2.graph import ttypes


class TestPermission(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.client.execute('UPDATE CONFIGS graph:enable_authorize=true')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    @classmethod
    def cleanup(self):
        self.switch_user('root', 'nebula')
        resp = self.client.execute('UPDATE CONFIGS graph:enable_authorize=false')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.switch_user(pytest.cmdline.user, pytest.cmdline.password)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE my_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP SPACE space4'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_simple(self):
        # incorrect user/password
        result =  self.switch_user('root', 'pwd')
        self.check_resp_failed(result)

        result = self.switch_user('user', 'nebula')
        self.check_resp_failed(result)

        result = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(result)

        # test root user password and use space.
        query = 'CREATE SPACE my_space(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'USE my_space; CREATE TAG person(name string)'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'USE my_space'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG person(name string)"
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # change root password, incorrect password.
        query = 'CHANGE PASSWORD root FROM "aa" TO "bb"'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # change root password, correct password.
        query = 'CHANGE PASSWORD root FROM "nebula" TO "bb"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # verify password changed
        result = self.switch_user("root", "nebula")
        self.check_resp_failed(result)

        result = self.switch_user('root', 'bb')
        self.check_resp_succeeded(result)

        query = 'CHANGE PASSWORD root FROM "bb" TO "nebula"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

    def test_user_write(self):
        query = 'CREATE SPACE space1(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'CREATE USER admin WITH PASSWORD "admin"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW ROLES IN space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        # TODO(shylock) check result
        time.sleep(self.delay)

        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)
        query = 'ALTER USER root WITH PASSWORD "root"'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE ADMIN ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GRANT ROLE GOD ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # Reject the admin user grant or revoke to himself self
        query = 'GRANT ROLE GUEST ON space1 TO admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'DROP USER admin'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = 'DROP USER admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_schema_and_data(self):
        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)

        query = 'CREATE SPACE space2(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'CREATE USER admin WITH PASSWORD "admin"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON space2 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER dba WITH PASSWORD "dba"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE DBA ON space2 TO dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER user WITH PASSWORD "user"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE USER ON space2 TO user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE USER guest WITH PASSWORD "guest"'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space2 TO guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # god write schema test
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)


        query = 'CREATE EDGE e1(e_c int)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = 'CREATE TAG INDEX tid1 ON t1(t_c)';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'CREATE EDGE INDEX eid1 ON e1(e_c)';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = 'DESCRIBE TAG t1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DESCRIBE EDGE e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = 'DESCRIBE TAG INDEX tid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DESCRIBE EDGE INDEX eid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DROP TAG INDEX tid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = 'DROP EDGE INDEX eid1';
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = 'ALTER TAG t1 DROP (t_c)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'ALTER EDGE e1 DROP (e_c)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP TAG t1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'DROP EDGE e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # admin write schema test
        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)
     
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG t1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE e1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # dba write schema test
        resp = self.switch_user('dba', 'dba')
        self.check_resp_succeeded(resp)
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported index
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = "DESCRIBE TAG t1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DESCRIBE EDGE e1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # TODO(shylock) not supported index
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_succeeded(resp)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP TAG t1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "DROP EDGE e1";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # user write schema test
        resp = self.switch_user('user', 'user')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # TODO(shylock) not supported index
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DROP TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP TAG t1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE e1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)
        time.sleep(self.delay)

        # guest write schema test
        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE TAG t1(t_c int)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "CREATE EDGE e1(e_c int)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # TODO(shylock) not supported sentence
        # query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DESCRIBE TAG t1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        query = "DESCRIBE EDGE e1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # TODO(shylock) not supported sentence
        # query = "DESCRIBE TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DESCRIBE EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

        # query = "DROP TAG INDEX tid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # query = "DROP EDGE INDEX eid1";
        # resp = self.execute(query)
        # self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER TAG t1 DROP (t_c)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "ALTER EDGE e1 DROP (e_c)";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP TAG t1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = "DROP EDGE e1";
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        # god write data test
        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'CREATE TAG t1(t_c int)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = "CREATE EDGE e1(e_c int)"
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # admin write data test
        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # dba write data test
        resp = self.switch_user('dba', 'dba')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # user write data test
        resp = self.switch_user('user', 'user')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GO FROM "1" OVER e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # guest write data test
        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)

        query = 'USE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'INSERT VERTEX t1(t_c) VALUES "1":(1)'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'INSERT EDGE e1(e_c) VALUES "1" -> "2":(95)';
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'GO FROM "1" OVER e1';
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        # use space test
        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = "CREATE SPACE space3(partition_num=1, replica_factor=1)";
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'USE space3'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.switch_user('dba', 'dba')
        self.check_resp_succeeded(resp)
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.switch_user('user', 'user')
        self.check_resp_succeeded(resp)
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

    def test_show_test(self):
        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = 'CREATE SPACE space4(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'SHOW SPACES'
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_column_names = ['Name']
        expected_result = [['my_space'], ['nba'], ['space1'], ['space2'], ['space3'], ['space4']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('dba', 'dba')
        self.check_resp_succeeded(resp)
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('user', 'user')
        self.check_resp_succeeded(resp)
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        expected_result = [['space2']]
        self.check_column_names(resp, expected_column_names)
        self.check_out_of_order_result(resp, expected_result)

        query = 'SHOW ROLES IN space1'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        query = 'SHOW ROLES IN space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = 'SHOW ROLES IN space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        query = 'SHOW CREATE SPACE space1'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_BAD_PERMISSION)

        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = 'SHOW CREATE SPACE space1'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        query = 'SHOW CREATE SPACE space2'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    def test_show_roles(self):
        resp = self.switch_user('root', 'nebula')
        self.check_resp_succeeded(resp)
        query = 'CREATE SPACE space5(partition_num=1, replica_factor=1)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'GRANT ROLE ADMIN ON space5 TO admin'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE DBA ON space5 TO dba'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE USER ON space5 TO user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE GUEST ON space5 TO guest'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        query = 'SHOW ROLES IN space5'
        expected_result = [['guest', 'GUEST'],
                           ['user', 'USER'],
                           ['dba', 'DBA'],
                           ['admin', 'ADMIN']]
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('admin', 'admin')
        self.check_resp_succeeded(resp)
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('dba', 'dba')
        self.check_resp_succeeded(resp)
        expected_result = [['dba', 'DBA']]
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('user', 'user')
        self.check_resp_succeeded(resp)
        expected_result = [['user', 'USER']]
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)

        resp = self.switch_user('guest', 'guest')
        self.check_resp_succeeded(resp)
        expected_result = [['guest', 'GUEST']]
        resp = self.execute_query(query)
        self.check_resp_succeeded(resp)
        self.check_out_of_order_result(resp, expected_result)
