/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;
class MutateValidatorTest : public ValidatorTestBase {
};

TEST_F(MutateValidatorTest, InsertVertexTest) {
    // get schema from validate context
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertVertices, PK::kCreateTag, PK::kStart
        };
        auto cmd = "CREATE TAG TEST(name string); "
                   "INSERT VERTEX TEST(name) VALUES \"A\":(\"A\");";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // wrong schema
    {
        auto cmd = "CREATE TAG TEST(name string, age int); "
                   "INSERT VERTEX TEST(name, age2) VALUES \"A\":(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // get schema from cache
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertVertices, PK::kStart
        };
        auto cmd = "INSERT VERTEX person(name, age) VALUES \"A\":(\"a\", 19);";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // wrong schema
    {
        auto cmd = "INSERT VERTEX person(name, age2) VALUES \"A\":(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // create new space and create schema
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertVertices, PK::kCreateTag, PK::kSwitchSpace, PK::kCreateSpace, PK::kStart
        };
        auto cmd = "CREATE SPACE test_2; USE test_2;"
                   "CREATE TAG TEST(name string); "
                   "INSERT VERTEX TEST(name) VALUES \"A\":(\"A\");";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
}

TEST_F(MutateValidatorTest, InsertEdgeTest) {
    session_->setSpace("test_space", 1);
    // get schema from validate context
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertEdges, PK::kCreateEdge, PK::kStart
        };
        auto cmd = "CREATE EDGE TEST(name string); "
                   "INSERT EDGE TEST(name) VALUES \"A\"->\"B\":(\"\");";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // wrong schema
    {
        auto cmd = "CREATE EDGE TEST(name string); "
                   "INSERT EDGE TEST(name1) VALUES \"A\"->\"B\":(\"\");";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // get schema from cache
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertEdges, PK::kStart
        };
        auto cmd = "INSERT EDGE like(start, end) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // wrong schema
    {
        auto cmd = "INSERT EDGE like(start, end2) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // create new space and create schema
    {
        std::vector<PlanNode::Kind> expected = {
            PK::kInsertEdges, PK::kCreateEdge, PK::kSwitchSpace, PK::kCreateSpace, PK::kStart
        };
        auto cmd = "CREATE SPACE test_2; USE test_2;"
                   "CREATE EDGE TEST(name string); "
                   "INSERT EDGE TEST(name) VALUES \"A\"->\"B\":(\"\");";
        ASSERT_TRUE(checkResult(cmd, expected));
    }
}
}  // namespace graph
}  // namespace nebula
