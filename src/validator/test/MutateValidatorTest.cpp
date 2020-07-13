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
    // wrong schema
    {
        auto cmd = "INSERT VERTEX person(name, age2) VALUES \"A\":(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}

TEST_F(MutateValidatorTest, InsertEdgeTest) {
    // wrong schema
    {
        auto cmd = "INSERT EDGE like(start, end2) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}

TEST_F(MutateValidatorTest, DeleteVertexTest) {
    // succeed
    {
        auto cmd = "DELETE VERTEX \"A\"";
        ASSERT_TRUE(checkResult(cmd, {}));
    }
    // pipe
    {
        auto cmd = "GO FROM \"C\" OVER like YIELD like._dst as dst | DELETE VERTEX $-.dst";
        ASSERT_TRUE(checkResult(cmd, {}));
    }
    // pipe wrong input
    {
        auto cmd = "GO FROM \"C\" OVER E YIELD E._dst as dst | DELETE VERTEX $-.a";
        ASSERT_FALSE(checkResult(cmd));
    }
}

TEST_F(MutateValidatorTest, DeleteEdgeTest) {
    // succeed
    {
        auto cmd = "DELETE EDGE like \"A\"->\"B\"";
        ASSERT_TRUE(checkResult(cmd, {}));
    }
    // not existed edge name
    {
        auto cmd = "DELETE EDGE study \"A\"->\"B\"";
        ASSERT_FALSE(checkResult(cmd));
    }
    // pipe
    {
        auto cmd = "GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "| DELETE EDGE like $-.src -> $-.dst @ $-.rank";
        ASSERT_TRUE(checkResult(cmd, {}));
    }
    // pipe wrong input
    {
        auto cmd = "GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "| DELETE EDGE like $-.dd -> $-.dst @ $-.rank";
        ASSERT_FALSE(checkResult(cmd));
    }
}
}  // namespace graph
}  // namespace nebula
