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
class SchemaTest : public TestBase {
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
    }

    static void TearDownTestCase() {
        client_.reset();
    }

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> SchemaTest::client_{nullptr};

TEST_F(SchemaTest, TestSpace) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_for_default(partition_num=9, replica_factor=1);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestTag) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG student(name STRING, age INT8, grade FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestEdge) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE schoolmate(start int, end int);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestInsert) {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX student(name, age, grade) "
                            "VALUES \"Tom\":(\"Tom\", 18, \"three\");";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX student(name, age, grade) "
                            "VALUES \"Lily\":(\"Lily\", 18, \"three\");";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE schoolmate(start, end) "
                            "VALUES \"Tom\"->\"Lily\":(2009, 2011)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
