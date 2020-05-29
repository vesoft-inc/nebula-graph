/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"

#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"

namespace nebula {
namespace graph {
class SubgraphTest : public TestBase {
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

std::unique_ptr<GraphClient> SubgraphTest::client_{nullptr};

TEST_F(SubgraphTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GET SUBGRAPH 3 STEPS FROM 1";
        auto code = client_->execute(query, resp);
        UNUSED(code);
        // ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        // TODO: check result.
    }
}

}  // namespace graph
}  // namespace nebula
