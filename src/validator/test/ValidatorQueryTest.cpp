/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {
class ValidatorQueryTest : public ValidatorTestBase {
};

TEST_F(ValidatorQueryTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        auto plan = context->plan();
        ASSERT_NE(plan, nullptr);
        using PK = nebula::graph::PlanNode::Kind;
        std::vector<PlanNode::Kind> expected = {
            PK::kLoop, PK::kStart, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}

TEST_F(ValidatorQueryTest, Limit) {
    std::string query = "GO FROM \"Ann\" OVER study YIELD study._dst AS school | LIMIT 1, 3";
    auto result = GQLParser().parse(query);
    ASSERT_TRUE(result.ok()) << result.status();
    auto sentences = std::move(result).value();
    auto context = buildContext();
    ASTValidator validator(sentences.get(), context.get());
    auto validateResult = validator.validate();
    ASSERT_TRUE(validateResult.ok()) << validateResult;
    auto plan = context->plan();
    ASSERT_NE(plan, nullptr);
    using PK = nebula::graph::PlanNode::Kind;
    std::vector<PlanNode::Kind> expected = {
        PK::kProject, PK::kLimit, PK::kProject, PK::kGetNeighbors, PK::kStart
    };
    ASSERT_TRUE(verifyPlan(plan->root(), expected));
}

TEST_F(ValidatorQueryTest, OrderBy) {
    {
        std::string query = "GO FROM \"Ann\" OVER schoolmate YIELD $$.student.age AS age"
                            " | ORDER BY $-.age";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        auto plan = context->plan();
        ASSERT_NE(plan, nullptr);
        using PK = nebula::graph::PlanNode::Kind;
        std::vector<PlanNode::Kind> expected = {
                PK::kProject, PK::kSort, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    // not exist factor
    {
        std::string query = "GO FROM \"Ann\" OVER schoolmate YIELD $$.student.age AS age"
                            " | ORDER BY $-.name";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        auto context = buildContext();
        ASTValidator validator(sentences.get(), context.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok()) << validateResult;
    }
}
}  // namespace graph
}  // namespace nebula
