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
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteVertices,
            PK::kDeleteEdges,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // pipe
    {
        auto cmd = "GO FROM \"C\" OVER like YIELD like._dst as dst | DELETE VERTEX $-.dst";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteVertices,
            PK::kDeleteEdges,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
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
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
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
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // var
    {
        auto cmd = "$var = GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "; DELETE EDGE like $var.src -> $var.dst @ $var.rank";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // pipe wrong input
    {
        auto cmd = "GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "| DELETE EDGE like $-.dd -> $-.dst @ $-.rank";
        ASSERT_FALSE(checkResult(cmd));
    }
}

TEST_F(MutateValidatorTest, UpdateVertexTest) {
    // not exist tag
    {
        auto cmd = "UPDATE VERTEX ON student \"Tom\" SET count = 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // succeed
    {
        auto cmd = "UPDATE VERTEX ON person \"Tom\""
                   "SET age = person.age + 1 "
                   "WHEN person.age == 18 "
                   "YIELD person.name AS name, person.age AS age";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateVertex, PK::kStart}));
    }
}

TEST_F(MutateValidatorTest, UpdateEdgeTest) {
    // not exist edge
    {
        auto cmd = "UPDATE EDGE ON study \"Tom\"->\"Lily\" SET count = 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // succeed
    {
        auto cmd = "UPDATE EDGE ON like \"Tom\"->\"Lily\""
                   "SET end = like.end + 1 "
                   "WHEN like.start >= 2010 "
                   "YIELD like.start AS start, like.end AS end";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateEdge, PK::kStart}));
    }
}
}  // namespace graph
}  // namespace nebula
