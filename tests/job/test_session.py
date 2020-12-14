# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import time
import pytest
import concurrent

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from nebula2.graph import GraphService
from nebula2.graph import ttypes
from nebula2.data.ResultSet import ResultSet
from tests.common.nebula_test_suite import NebulaTestSuite

class TestSession(NebulaTestSuite):
    @classmethod
    def prepare(self):
        resp = self.execute('UPDATE CONFIGS graph:session_idle_timeout_secs = 5')
        self.check_resp_succeeded(resp)

        resp = self.execute('UPDATE CONFIGS graph:session_reclaim_interval_secs = 1')
        self.check_resp_succeeded(resp)

        resp = self.execute('CREATE USER IF NOT EXISTS session_user WITH PASSWORD "123456"')
        self.check_resp_succeeded(resp)

        query = 'GRANT ROLE ADMIN ON nba TO session_user'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

        time.sleep(3)

    @classmethod
    def cleanup(self):
        resp = self.execute('UPDATE CONFIGS graph:session_idle_timeout_secs = 0')
        self.check_resp_succeeded(resp)
        resp = self.execute('DROP USER session_user')
        self.check_resp_succeeded(resp)

    def test_sessions(self):
        # 1: test add session with right username
        try:
            client_ok = self.client_pool.get_session('session_user', '123456')
            assert client_ok is not None
            assert True
        except Exception as e:
            assert False, e

        # 2: test add session with not exist username
        try:
            self.client_pool.get_session('session_not_exist', '123456')
            assert False
        except Exception as e:
            assert True

        # 3: test show sessions
        resp = self.execute('SHOW SESSIONS')
        self.check_resp_succeeded(resp)
        expect_col_names = ['SessionId',
                            'UserName',
                            'SpaceName',
                            'CreateTime',
                            'UpdateTime',
                            'GraphAddr',
                            'Timezone',
                            'ClientIp']
        expect_result = [[re.compile(r'\d+'),
                          re.compile(r'session_user'),
                          re.compile(r''),
                          re.compile(r'\d+'),
                          re.compile(r'\d+'),
                          re.compile(r'\d+.\d+.\d+.\d+:.*'),
                          re.compile(r'\d+'),
                          re.compile(r'\d+.\d+.\d+.\d+')]]
        self.check_column_names(resp, expect_col_names)
        self.search_result(resp, expect_result, is_regex=True)

        session_id = 0
        for row in resp.rows():
            if bytes.decode(row.values[1].get_sVal()) == 'session_user':
                session_id = row.values[0].get_iVal()
                break

        assert session_id != 0

        # 4: test get session info
        resp = client_ok.execute('USE nba')
        self.check_resp_succeeded(resp)

        resp = self.execute('SHOW SESSION {}'.format(session_id))
        self.check_resp_succeeded(resp)
        expect_col_names = ['VariableName', 'Value']
        expect_result = [[re.compile(r'SessionID'), re.compile(r'\d+')],
                         [re.compile(r'UserName'), re.compile(r'session_user')],
                         [re.compile(r'SpaceName'), re.compile(r'nba')],
                         [re.compile(r'CreateTime'), re.compile(r'\d+')],
                         [re.compile(r'UpdateTime'), re.compile(r'\d+')],
                         [re.compile(r'GraphAddr'), re.compile(r'\d+.\d+.\d+.\d+:.*')],
                         [re.compile(r'Timezone'), re.compile(r'\d+')],
                         [re.compile(r'ClientIp'), re.compile(r'\d+.\d+.\d+.\d+')]]
        self.check_column_names(resp, expect_col_names)
        self.search_result(resp, expect_result, is_regex=True)

        # 5: test expired session
        time.sleep(3)
        resp = self.execute('SHOW SPACES;')
        self.check_resp_succeeded(resp)
        time.sleep(3)
        resp = self.execute('SHOW SESSION {}'.format(session_id))
        time.sleep(3)
        resp = self.execute('SHOW SESSION {}'.format(session_id))
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

    def test_the_same_id_to_different_graphd(self):
        def get_connection(ip, port):
            try:
                socket = TSocket.TSocket(ip, port)
                transport = TTransport.TBufferedTransport(socket)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                transport.open()
                connection = GraphService.Client(protocol)
            except Exception as ex:
                assert False, 'Create connection to {}:{} failed'.format(ip, port)
            return connection

        resp = self.execute('SHOW HOSTS GRAPH')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        assert resp.row_size() == 2
        addr_host1 = resp.row_values(0)[0].as_string()
        addr_port1 = resp.row_values(0)[1].as_int()
        addr_host2 = resp.row_values(1)[0].as_string()
        addr_port2 = resp.row_values(1)[1].as_int()

        conn1 = get_connection(addr_host1, addr_port1)
        conn2 = get_connection(addr_host2, addr_port2)

        resp = conn1.authenticate('root', 'nebula')
        assert resp.error_code == ttypes.ErrorCode.SUCCEEDED
        session_id = resp.session_id

        resp = conn1.execute(session_id, 'CREATE SPACE aSpace(partition_num=1);USE aSpace;')
        self.check_resp_succeeded(ResultSet(resp))
        time.sleep(3)
        resp = conn1.execute(session_id, 'CREATE TAG a();')
        self.check_resp_succeeded(ResultSet(resp))
        resp = conn2.execute(session_id, 'CREATE TAG b();')
        self.check_resp_succeeded(ResultSet(resp))

        def do_test(connection, sid, num):
            result = connection.execute(sid, 'USE aSpace;')
            assert result.error_code == ttypes.ErrorCode.SUCCEEDED
            result = connection.execute(sid, 'CREATE TAG aa{}()'.format(num))
            assert result.error_code == ttypes.ErrorCode.SUCCEEDED, result.error_msg

        # test multi connection with the same session_id
        test_jobs = []
        with concurrent.futures.ThreadPoolExecutor(3) as executor:
            for i in range(0, 3):
                future = executor.submit(do_test,
                                         get_connection(addr_host2, addr_port2),
                                         session_id,
                                         i)
                test_jobs.append(future)

            for future in concurrent.futures.as_completed(test_jobs):
                if future.exception() is not None:
                    assert False, future.exception()
                else:
                    assert True
        resp = conn2.execute(session_id, 'SHOW TAGS')
        self.check_resp_succeeded(ResultSet(resp))
        expect_result = [['a'], ['b'], ['aa0'], ['aa1'], ['aa2']]
        self.check_out_of_order_result(ResultSet(resp), expect_result)


