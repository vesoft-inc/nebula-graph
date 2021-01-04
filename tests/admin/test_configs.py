# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import re
from tests.common.nebula_test_suite import NebulaTestSuite

storage_name_pattern = re.compile(r'STORAGE')
graph_name_pattern = re.compile(r'GRAPH')

class TestConfigs(NebulaTestSuite):

    @classmethod
    def prepare(self):
        resp = self.client.execute('GET CONFIGS storage:v')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        self.storage_v = resp.row_values(0)[4].as_int()

        resp = self.client.execute('GET CONFIGS graph:v')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        self.graph_v = resp.row_values(0)[4].as_int()

        resp = self.client.execute('GET CONFIGS storage:minloglevel')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        self.storage_minlog = resp.row_values(0)[4].as_int()

        resp = self.client.execute('GET CONFIGS graph:minloglevel')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        self.graph_minlog = resp.row_values(0)[4].as_int()

        resp = self.client.execute('GET CONFIGS storage:rocksdb_column_family_options')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        self.storage_rocksdb = resp.row_values(0)[4].as_map()

    @classmethod
    def cleanup(self):
        # restore
        if self.graph_v is not None:
            resp = self.client.execute('UPDATE CONFIGS graph:v={}'.format(self.graph_v))
            self.check_resp_succeeded(resp)
        if self.storage_v is not None:
            resp = self.client.execute('UPDATE CONFIGS storage:v={}'.format(self.storage_v))
            self.check_resp_succeeded(resp)
        if self.graph_minlog is not None:
            resp = self.client.execute('UPDATE CONFIGS graph:minloglevel={}'.format(self.graph_minlog))
            self.check_resp_succeeded(resp)
        if self.storage_minlog is not None:
            resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(self.storage_minlog))
            self.check_resp_succeeded(resp)
        if self.storage_rocksdb is not None:
            key_lists = []
            for key in self.storage_rocksdb.keys():
                key_lists.append('{}={}'.format(key, str(self.storage_rocksdb[key])))
            cmd = 'UPDATE CONFIGS storage:rocksdb_column_family_options={%s}' % (','.join(key_lists))
            resp = self.client.execute(cmd)
            self.check_resp_succeeded(resp)

    def test_configs(self):
        # set/get without declaration
        resp = self.client.execute('UPDATE CONFIGS storage:notRegistered=123;')
        self.check_resp_failed(resp)

        # update immutable config will fail, read-only
        resp = self.client.execute('UPDATE CONFIGS storage:num_io_threads=10')
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

        resp = self.client.execute('GET CONFIGS graph:v')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'v', 'int', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)

        resp = self.client.execute('GET CONFIGS storage:v')
        self.check_resp_succeeded(resp)
        expected_result = [['STORAGE', 'v', 'int', 'MUTABLE', 3]]
        self.check_result(resp, expected_result)
        # show configs
        resp = self.client.execute('SHOW CONFIGS meta')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('SHOW CONFIGS graph')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'v', 'int', 'MUTABLE'],
                           ['GRAPH', 'minloglevel', 'int', 'MUTABLE'],
                           ['GRAPH', 'slow_op_threshhold_ms', 'int', 'MUTABLE'],
                           ['GRAPH', 'heartbeat_interval_secs', 'int', 'MUTABLE'],
                           ['GRAPH', 'meta_client_retry_times', 'int', 'MUTABLE']]
        self.check_out_of_order_result(resp, expected_result, [4])

        resp = self.client.execute('SHOW CONFIGS storage')
        self.check_resp_succeeded(resp)
        expected_result = [['STORAGE', 'v', 'int', 'MUTABLE'],
                           ['STORAGE', 'wal_ttl', 'int', 'MUTABLE'],
                           ['STORAGE', 'minloglevel', 'int', 'MUTABLE'],
                           ['STORAGE', 'custom_filter_interval_secs', 'int', 'MUTABLE'],
                           ['STORAGE', 'slow_op_threshhold_ms', 'int', 'MUTABLE'],
                           ['STORAGE', 'heartbeat_interval_secs', 'int', 'MUTABLE'],
                           ['STORAGE', 'meta_client_retry_times', 'int', 'MUTABLE'],
                           ['STORAGE', 'rocksdb_db_options', 'map', 'MUTABLE'],
                           ['STORAGE', 'enable_multi_versions', 'bool', 'MUTABLE'],
                           ['STORAGE', 'rocksdb_column_family_options', 'map', 'MUTABLE'],
                           ['STORAGE', 'rocksdb_block_based_table_options', 'map', 'MUTABLE'],
                           ["STORAGE", "max_edge_returned_per_vertex", "int", "MUTABLE"]]
        self.check_out_of_order_result(resp, expected_result, [4])

        for record in resp:
            if record.get_value_by_key('name').as_string().startswith('rocksdb'):
                assert record.get_value_by_key('value').is_map()

        # set and get a config of all module
        resp = self.client.execute('UPDATE CONFIGS minloglevel={}'.format(2))
        self.check_resp_succeeded(resp)

        # check result
        resp = self.client.execute('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'int', 'MUTABLE', 2]]
        self.check_out_of_order_result(resp, expected_result)

        # update storage
        resp = self.client.execute('UPDATE CONFIGS storage:minloglevel={}'.format(3))
        self.check_resp_succeeded(resp)

        # get result
        resp = self.client.execute('GET CONFIGS minloglevel')
        self.check_resp_succeeded(resp)
        expected_result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', 2],
                           ['STORAGE', 'minloglevel', 'int', 'MUTABLE', 3]]
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
        resp = self.client.execute('GET CONFIGS storage:rocksdb_column_family_options')
        self.check_resp_succeeded(resp)
        value = {"max_bytes_for_level_base": 1024, "write_buffer_size": 1024, "max_write_buffer_number": 4}
        expected_result = [['STORAGE', 'rocksdb_column_family_options', 'map', 'MUTABLE', value]]
        self.check_result(resp, expected_result)

