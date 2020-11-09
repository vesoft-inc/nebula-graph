# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
import time
import pytest

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from nebula2.graph import GraphService
from nebula2.graph import ttypes
from nebula2.Client import GraphClient
from nebula2.ConnectionPool import ConnectionPool
from tests.common.nebula_test_suite import NebulaTestSuite

class TestSession(NebulaTestSuite):
    @classmethod
    def get_session(self):
        pool = ConnectionPool(host=self.host,
                              port=self.port,
                              socket_num=1,
                              network_timeout=0)
        return GraphClient(pool)

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
        resp = self.execute_query('UPDATE CONFIGS graph:session_idle_timeout_secs = 0')
        self.check_resp_succeeded(resp)
        resp = self.execute('DROP USER session_user')
        self.check_resp_succeeded(resp)

    def test_sessions(self):
        # 1: test add session with right username
        client_ok = self.get_session()
        resp = client_ok.authenticate('session_user', '123456')
        self.check_resp_succeeded(resp)

        # 2: test add session with not exist username
        client = self.get_session()
        resp = client.authenticate('session_not_exist', '123456')
        self.check_resp_failed(resp)

        # 3: test show sessions
        resp = self.execute_query('SHOW SESSIONS')
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
        for row in resp.data.rows:
            if bytes.decode(row.values[1].get_sVal()) == 'session_user':
                session_id = row.values[0].get_iVal()
                break

        assert session_id != 0

        # 4: test get session info
        resp = client_ok.execute_query('USE nba')
        self.check_resp_succeeded(resp)

        resp = self.execute_query('GET SESSION {}'.format(session_id))
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
        resp = self.execute_query('SHOW SPACES;')
        self.check_resp_succeeded(resp)
        time.sleep(3)
        resp = self.execute_query('GET SESSION {}'.format(session_id))
        time.sleep(3)
        resp = self.execute_query('GET SESSION {}'.format(session_id))
        self.check_resp_failed(resp, ttypes.ErrorCode.E_EXECUTION_ERROR)

    def test_the_same_id_to_different_graphd(self):
        addresses = pytest.cmdline.address.split(',')
        assert len(addresses) >= 2
        address1 = addresses[0].split(':')
        address2 = addresses[1].split(':')

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

        conn1 = get_connection(address1[0], address1[1])
        conn2 = get_connection(address2[0], address2[1])

        resp = conn1.authenticate('root', 'nebula')
        self.check_resp_succeeded(resp)
        session_id = resp.session_id

        resp = conn1.execute(session_id, 'CREATE SPACE aSpace(partition_num=1);USE aSpace;')
        self.check_resp_succeeded(resp)
        time.sleep(3)
        resp = conn1.execute(session_id, 'CREATE TAG a();')
        self.check_resp_succeeded(resp)
        resp = conn2.execute(session_id, 'CREATE TAG b();')
        self.check_resp_succeeded(resp)

