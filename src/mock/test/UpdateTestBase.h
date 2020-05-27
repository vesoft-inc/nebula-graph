/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_UPDATETESTBASE_H
#define GRAPH_TEST_UPDATETESTBASE_H

#include "common/base/Base.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class UpdateTestBase : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
    }

    void TearDown() override {
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getGraphClient();

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());

        ASSERT_TRUE(prepareData());
    }

    static void TearDownTestCase() {
        client_.reset();
    }

    static ::testing::AssertionResult prepareSchema();

    static ::testing::AssertionResult prepareData();

protected:
    static std::unique_ptr<GraphClient>         client_;
};


std::unique_ptr<GraphClient> UpdateTestBase::client_;

// static
::testing::AssertionResult UpdateTestBase::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE mySpace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG course(name string, credits int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG building(name string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG student(name string, age int, gender string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE like(likeness double)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE select(grade int, year int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    return TestOK();
}


::testing::AssertionResult UpdateTestBase::prepareData() {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE mySpace";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "USE mySpace failed"
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert vertices 'student'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT VERTEX student(name, age, gender) VALUES "
                "\"Monica\":(\"Monica\", 16, \"female\"), "
                "\"Mike\":(\"Mike\", 18, \"male\"), "
                "\"Jane\":(\"Jane\", 17, \"female\")";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'student' failed: "
                               << static_cast<int32_t>(code);
        }
        // Insert vertices 'course' and 'building'
        query.clear();
        query = "INSERT VERTEX course(name, credits),building(name) VALUES "
                "\"Math\":(\"Math\", 3, \"No5\"), "
                "\"English\":(\"English\", 6, \"No11\")";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' and 'building' failed: "
                               << static_cast<int32_t>(code);
        }
        // Insert vertices only 'course'(for update insertable testing)
        query.clear();
        query = "INSERT VERTEX course(name, credits) VALUES \"CS\":(\"CS\", 5)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'course' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {   // Insert edges 'select' and 'like'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query = "INSERT EDGE select(grade, year) VALUES "
                "\"Monica\" -> \"Math\"@0:(5, 2018), "
                "\"Monica\" -> \"English\"@0:(3, 2018), "
                "\"Mike\" -> \"English\"@0:(3, 2019), "
                "\"Jane\" -> \"English\"@0:(3, 2019)";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'select' failed: "
                               << static_cast<int32_t>(code);
        }
        query.clear();
        query = "INSERT EDGE like(likeness) VALUES "
                "\"Monica\" -> \"Mike\"@0:(92.5), "
                "\"Mike\" -> \"Monica\"@0:(85.6), "
                "\"Mike\" -> \"Jane\"@0:(93.2)";
        code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert 'like' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    return TestOK();
}

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_TEST_UPDATETESTBASE_H
