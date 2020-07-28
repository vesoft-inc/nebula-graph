# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time
import re

from nebula_test_common.nebula_test_suite import NebulaTestSuite
from nebula2.common import ttypes

class TestConfigs(NebulaTestSuite):

    @classmethod
    def prepare(self):
        pass

    @classmethod
    def cleanup(self):
        pass

    def test_configs(self):
        # set/get without declaration
        resp = self.client.execute('UPDATE CONFIGS storage:notRegistered=123;')
        self.check_resp_failed(resp)

        # update immutable config will fail, read-only
        resp = self.client.execute_query('UPDATE CONFIGS storage:num_io_threads=10')
        self.check_resp_failed(resp)

        # set and get config after declaration
        v = 3
        resp = self.client.execute('UPDATE CONFIGS meta:v={}'.format(3))
        self.check_resp_failed(resp)
        resp = self.client.execute('UPDATE CONFIGS graph:v={}'.format(3))
        self.check_resp_succeeded(resp)
        resp = self.client.execute('UPDATE CONFIGS storage:v={}'.format(3))
        self.check_resp_succeeded(resp)

        # get
        resp = self.client.execute('GET CONFIGS meta:v')
        self.check_resp_failed(resp)

        resp = self.client.execute_query('GET CONFIGS graph:v')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'v', 'INT', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        resp = self.client.execute_query('GET CONFIGS storage:v')
        self.check_resp_succeeded(resp)
        expected_result = [['STORAGE', 'v', 'INT', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        # show configs
        resp = self.client.execute_query('SHOW CONFIGS meta')
        self.check_resp_succeeded(resp)

        resp = self.client.execute_query('SHOW CONFIGS graph')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'v', 'INT', 'MUTABLE', v],
                           ['GRAPH', 'minloglevel', 'INT', 'MUTABLE', 0],
                           ['GRAPH', 'slow_op_threshhold_ms', 'INT', 'MUTABLE', 50],
                           ['GRAPH', 'heartbeat_interval_secs', 'INT', 'MUTABLE', 3],
                           ['GRAPH', 'meta_client_retry_times', 'INT', 'MUTABLE', 3]]
        self.check_out_of_order_result(resp, expected_result)

        resp = self.client.execute_query('SHOW CONFIGS storage')
        self.check_resp_succeeded(resp)
        expected_result = [['STORAGE', 'v', 'INT', 'MUTABLE', 3],
                           ['STORAGE', 'wal_ttl', 'INT', 'MUTABLE', 14400],
                           ['STORAGE', 'minloglevel', 'INT', 'MUTABLE', 0],
                           ['STORAGE', 'enable_reservoir_sampling', 'BOOL', 'MUTABLE', False],
                           ['STORAGE', 'custom_filter_interval_secs', 'INT', 'MUTABLE', 86400],
                           ['STORAGE', 'slow_op_threshhold_ms', 'INT', 'MUTABLE', 50],
                           ['STORAGE', 'heartbeat_interval_secs', 'INT', 'MUTABLE', 3],
                           ['STORAGE', 'meta_client_retry_times', 'INT', 'MUTABLE', 3],
                           ['STORAGE', 'rocksdb_db_options', 'MAP', 'MUTABLE', {}],
                           ['STORAGE', 'max_edge_returned_per_vertex', 'INT', 'MUTABLE', 2147483647],
                           ['STORAGE', 'rocksdb_column_family_options', 'MAP', 'MUTABLE',
                            {"write_buffer_size":"67108864","max_bytes_for_level_base":"268435456","max_write_buffer_number":"4"}],
                           ['STORAGE', 'rocksdb_block_based_table_options', 'MAP', 'MUTABLE', {"block_size":"8192"}]]
        self.check_out_of_order_result(resp, expected_result)

        # set and get a config of all module
        resp = self.client.execute('UPDATE CONFIGS minloglevel={}'.format(2))
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute_query('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'INT', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'INT', 'MUTABLE', 2]]
        self.check_out_of_order_result(resp, expected_result)

        # update storage
        resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(3))
        self.check_resp_succeeded(resp)

        # get result
        resp = self.client.execute_query('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'INT', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'INT', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        # update rocksdb
        resp = self.client.execute('''
                                   UPDATE CONFIGS storage:rocksdb_column_family_options={
                                   max_bytes_for_level_base=1024,
                                   write_buffer_size=1024,
                                   max_write_buffer_number=4}
                                   ''')
        self.check_resp_succeeded(resp)

        # get result
        resp = self.client.execute_query('GET CONFIGS storage:rocksdb_column_family_options')
        self.check_resp_succeeded(resp)
        value = {"max_bytes_for_level_base": 1024, "write_buffer_size": 1024, "max_write_buffer_number": 4}
        expected_result = [['STORAGE', 'rocksdb_column_family_options', 'MAP', 'MUTABLE', value]]
        self.check_result(resp, expected_result)
