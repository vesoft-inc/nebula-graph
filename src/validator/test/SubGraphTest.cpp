/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"
#include "validator/test/ValidatorTest.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, Subgraph) {
    {
        auto status = validate("GET SUBGRAPH 3 STEPS FROM \"1\"");
        ASSERT_TRUE(status.ok());
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        using PK = nebula::graph::PlanNode::Kind;
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}


}  // namespace graph
}  // namespace nebula
