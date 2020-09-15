# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import time

from tests.common.nebula_test_suite import NebulaTestSuite
from tests.common.nebula_test_suite import T_EMPTY, T_NULL

class TestZone(NebulaTestSuite):
    @classmethod
    def prepare(self):
        pass

    def test_zone(self):
        # Add Zone
        resp = self.client.execute('ADD ZONE zone_0 127.0.0.1:8980,127.0.0.1:8981,127.0.0.1:8982')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('ADD ZONE zone_1 127.0.0.1:8983,127.0.0.1:8984,127.0.0.1:8985')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('ADD ZONE zone_2 127.0.0.1:8986,127.0.0.1:8987,127.0.0.1:8988')
        self.check_resp_succeeded(resp)

        # Host have overlap
        resp = self.client.execute('ADD ZONE zone_3 127.0.0.1:8988,127.0.0.1:8989,127.0.0.1:8990')
        self.check_resp_failed(resp)

        # Add Zone with duplicate node
        resp = self.client.execute('ADD ZONE zone_3 127.0.0.1:8988,127.0.0.1:8988,127.0.0.1:8990')
        self.check_resp_failed(resp)

        # Add Zone already existed
        resp = self.client.execute('ADD ZONE zone_0 127.0.0.1:8980,127.0.0.1:8981,127.0.0.1:8982')
        self.check_resp_failed(resp)

        # Get Zone
        resp = self.client.execute('DESC ZONE zone_0')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESCRIBE ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Get Zone which is not exist
        resp = self.client.execute('DESC ZONE zone_not_exist')
        self.check_resp_failed(resp)

        resp = self.client.execute('DESCRIBE ZONE zone_not_exist')
        self.check_resp_failed(resp)

        # List Zones
        resp = self.client.execute('LIST ZONES')
        self.check_resp_succeeded(resp)

        # Add host into zone
        resp = self.client.execute('ADD HOST 9:9 INTO ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Add host into zone which zone is not exist
        resp = self.client.execute('ADD HOST 4:4 INTO ZONE zone_not_exist')
        self.check_resp_failed(resp)

        # Add host into zone which the node have existed
        resp = self.client.execute('ADD HOST 3:3 INTO ZONE zone_0')
        self.check_resp_failed(resp)

        # Drop host from zone
        resp = self.client.execute('DROP HOST 3:3 FROM ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Drop host from zone which zone is not exist
        resp = self.client.execute('DROP HOST 3:3 FROM ZONE zone_not_exist')
        self.check_resp_failed(resp)

        # Drop host from zone which the node not exist
        resp = self.client.execute('DROP HOST 3:3 FROM ZONE zone_0')
        self.check_resp_failed(resp)

        # Add Group
        resp = self.client.execute('ADD GROUP group_0 zone_0,zone_1,zone_2')
        self.check_resp_succeeded(resp)

        # Group already existed
        resp = self.client.execute('ADD GROUP group_1 zone_0,zone_1,zone_2')
        self.check_resp_failed(resp)

        # Group already existed although the order is different
        resp = self.client.execute('ADD GROUP group_1 zone_2,zone_1,zone_0')
        self.check_resp_failed(resp)

        # Add Group with duplicate zone name
        resp = self.client.execute('ADD GROUP group_1 zone_0,zone_0,zone_2')
        self.check_resp_failed(resp)

        # Add Group name already existed
        resp = self.client.execute('ADD GROUP group_0 zone_0,zone_1')
        self.check_resp_failed(resp)

        resp = self.client.execute('ADD GROUP group_1 zone_0,zone_1')
        self.check_resp_succeeded(resp)

        # Get Group
        resp = self.client.execute('DESC GROUP group_0')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESCRIBE GROUP group_0')
        self.check_resp_succeeded(resp)

        # Get Group which is not exist
        resp = self.client.execute('DESC GROUP group_not_exist')
        self.check_resp_failed(resp)

        resp = self.client.execute('DESCRIBE GROUP group_not_exist')
        self.check_resp_failed(resp)

        # List Groups
        resp = self.client.execute('LIST GROUPS')
        self.check_resp_succeeded(resp)

        # Add zone into group
        resp = self.client.execute('ADD ZONE zone_3 INTO GROUP group_0')
        self.check_resp_succeeded(resp)

        # Add zone into group which group not exist
        resp = self.client.execute('ADD ZONE zone_0 INTO GROUP group_not_exist')
        self.check_resp_failed(resp)

        # Add zone into group which zone already exist
        resp = self.client.execute('ADD ZONE zone_0 INTO GROUP group_0')
        self.check_resp_failed(resp)

        # Drop zone from group
        resp = self.client.execute('DROP ZONE zone_3 FROM GROUP group_0')
        self.check_resp_succeeded(resp)

        # Drop zone from group which group not exist
        resp = self.client.execute('DROP ZONE zone_0 FROM GROUP group_not_exist')
        self.check_resp_failed(resp)

        # Drop zone from group which zone not exist
        resp = self.client.execute('DROP ZONE zone_not_exist FROM GROUP group_0')
        self.check_resp_failed(resp)

        # Drop Group
        resp = self.client.execute('DROP GROUP group_0')
        self.check_resp_succeeded(resp)

        # Drop Group which is not exist
        resp = self.client.execute('DROP GROUP group_0')
        self.check_resp_failed(resp)

        # Drop Zone
        resp = self.client.execute('DROP ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Drop Zone which is not exist
        resp = self.client.execute('DROP ZONE zone_0')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(self):
        pass
