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

class FetchVerticesTest : public TestBase {
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
        // dropSpace();  TODO(shylock) not supported now
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

std::unique_ptr<GraphClient> FetchVerticesTest::client_{nullptr};

TEST_F(FetchVerticesTest, FetchVerticesProp) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON tag1 \"1\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    // With YIELD
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON tag1 \"1\" YIELD tag1.prop1";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    // ON *
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON * \"2\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
}

TEST_F(FetchVerticesTest, FetchVerticesPropFailed) {
    // mismatched tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON tag1 \"1\" YIELD tag2.prop2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }

    // not exist tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON not_exist_tag \"1\" YIELD not_exist_tag.prop2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }

    // not exist property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "FETCH PROP ON tag1 \"1\" YIELD tag1.not_exist_property";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
}

}   // namespace graph
}   // namespace nebula
