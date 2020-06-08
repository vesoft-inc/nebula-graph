/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class FetchEdgesTest : public TestBase {
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

        mockSchema();
        mockData();
    }

    static void TearDownTestCase() {
        // dropSpace();  // TODO(shylock) not supported now
        client_.reset();
    }

private:
    // some mock
    static void mockSchema() {
        {
            cpp2::ExecutionResponse resp;
            std::string query = "CREATE SPACE test;";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        sleep(FLAGS_heartbeat_interval_secs + 1);
        {
            cpp2::ExecutionResponse resp;
            std::string query = "USE test;";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "CREATE TAG tag1(prop1 int);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "CREATE TAG tag2(prop1 int, prop2 string);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "CREATE TAG tag2(prop1 int, prop2 string);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "CREATE EDGE edge1(prop1 int);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        sleep(FLAGS_heartbeat_interval_secs + 1);
    }

    static void mockData() {
        {
            cpp2::ExecutionResponse resp;
            std::string query = "INSERT VERTEX tag1(prop1) values \"1\":(1);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "INSERT VERTEX tag1(prop1), tag2(prop1, prop2) "
                                "values \"2\":(2, 2, \"2\");";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string query = "INSERT EDGE edge1(prop1) values \"1\"->\"2\":(3);";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
    }

    static void dropSpace() {
        {
            cpp2::ExecutionResponse resp;
            std::string query = "DROP SPACE test;";
            client_->execute(query, resp);
            ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        }
    }

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> FetchEdgesTest::client_{nullptr};

TEST_F(FetchEdgesTest, FetchEdgesProp) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON edge1 \"1\"->\"2\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"edge1._src", "edge1._dst", "edge1._rank", "edge1.prop1"});
        expected.emplace_back(Row({
            "1", "2", 0, 3
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    // With YIELD TODO(shylock) delay on Project
    // {
        // cpp2::ExecutionResponse resp;
        // std::string query = "FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge1.prop1 AS p";
        // client_->execute(query, resp);
        // ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        // DataSet expected({"p"});
        // expected.emplace_back(Row({
            // 3
        // }));
        // ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    // }

    {
        // not exists edge id
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON edge1 \"1\"->\"not_exists_key\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"edge1._src", "edge1._dst", "edge1._rank", "edge1.prop1"});
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }

    {
        // mix exists and not exists edge id
        // not exists edge id
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON edge1 \"1\"->\"not_exists_key\", \"1\"->\"2\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"edge1._src", "edge1._dst", "edge1._rank", "edge1.prop1"});
        expected.emplace_back(Row({
            "1", "2", 0, 3
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
}

TEST_F(FetchEdgesTest, FetchEdgesPropFailed) {
    // mismatched edge type
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON edge1 \"1\" YIELD edge2.prop2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }

    // notexist edge type
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON not_exist_edge \"1\" YIELD not_exist_edge.prop2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }

    // notexist edge property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON edge1 \"1\" YIELD edge1.not_exist_prop";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
}

}   // namespace graph
}   // namespace nebula
