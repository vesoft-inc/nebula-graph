/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Status.h"
#include "network/NetworkUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class DeleteTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
    };

    void TearDown() override {
        TestBase::TearDown();
    };

    static void SetUpTestCase() {
        client_ = gEnv->getGraphClient();
        ASSERT_NE(nullptr, client_);
        ASSERT_TRUE(prepareSchema());
    }

    static void TearDownTestCase() {
        client_.reset();
    }

    static ::testing::AssertionResult prepareSchema();

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> DeleteTest::client_{nullptr};

::testing::AssertionResult DeleteTest::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=10, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code " << static_cast<int32_t>(code);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code " << static_cast<int32_t>(code);
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE like(likeness double)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code " << static_cast<int32_t>(code);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE like(likeness) VALUES \"A\" -> \"B\":(10.0)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code " << static_cast<int32_t>(code);
        }
    }

    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE like(likeness) VALUES \"A\" -> \"C\":(20.0)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd
                               << " failed, error code " << static_cast<int32_t>(code);
        }
    }
    return TestOK();
}

TEST_F(DeleteTest, TestEdges) {
    // Wrong type
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DELETE EDGE liken \"A\" -> \"C\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DELETE EDGE like \"A\" -> \"C\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
