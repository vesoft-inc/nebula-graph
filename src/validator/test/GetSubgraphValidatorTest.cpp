/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

class GetSubgraphValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GetSubgraphValidatorTest, Base) {
    {
        std::string query = "GET SUBGRAPH FROM \"1\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string = "GET SUBGRAPH FROM \"1\" BOTH like";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kDedup,
            PK::kAggregate,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GetSubgraphValidatorTest, Input) {
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD like._src AS src | GET SUBGRAPH FROM $-.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kProject,
            PK::kAggregate,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $a.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kProject,
            PK::kAggregate,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GetSubgraphValidatorTest, RefNotExist) {
    {
        EXPECT_FALSE(checkResult("$a = GO FROM \"1\" OVER like YIELD like._src AS src;"
                                "GET SUBGRAPH FROM $b.src"));
    }
    {
        EXPECT_FALSE(checkResult("GET SUBGRAPH FROM $-.src"));
    }
}

}  // namespace graph
}  // namespace nebula
