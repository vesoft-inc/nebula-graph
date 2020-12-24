/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/ObjectPool.h"
#include "planner/Logic.h"
#include "planner/Query.h"
#include "validator/LookupValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class LookupValidatorTest : public ValidatorTestBase {};

TEST_F(LookupValidatorTest, InputOutput) {
    // pipe
    {
        const std::string query = "LOOKUP ON person where person.age == 35 | "
                                  "FETCH PROP ON person $-.VertexID";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kIndexScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // pipe with yield
    {
        const std::string query =
            "LOOKUP ON person where person.age == 35 YIELD person.name AS name | "
            "FETCH PROP ON person $-.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kIndexScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // variable
    {
        const std::string query = "$a = LOOKUP ON person where person.age == 35; "
                                  "FETCH PROP ON person $a.VertexID";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kIndexScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
    // var with yield
    {
        const std::string query =
            "$a = LOOKUP ON person where person.age == 35 YIELD person.name AS name;"
            "FETCH PROP ON person $a.name";
        EXPECT_TRUE(checkResult(query,
                                {
                                    PlanNode::Kind::kGetVertices,
                                    PlanNode::Kind::kProject,
                                    PlanNode::Kind::kIndexScan,
                                    PlanNode::Kind::kStart,
                                }));
    }
}

TEST_F(LookupValidatorTest, InvalidYieldExpression) {
    // TODO(shylock)
    {
        const std::string query =
            "LOOKUP ON person where person.age == 35 YIELD person.age + 1 AS age;";
        EXPECT_FALSE(checkResult(query,
                                 {
                                     PlanNode::Kind::kProject,
                                     PlanNode::Kind::kIndexScan,
                                     PlanNode::Kind::kStart,
                                 }));
    }
}

}   // namespace graph
}   // namespace nebula
