/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchEdgesValidator.h"
#include "validator/test/ValidatorTest.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchEdgesProp) {
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\""));
        // check plan
        // GetEdgess
        auto *plan = qCtx_->plan();
        ASSERT_EQ(plan->root()->kind(), PlanNode::Kind::kGetEdges);
        const auto *getEdgesNode = static_cast<const GetEdges *>(plan->root());
        auto *input = getEdgesNode->input();
        ASSERT_EQ(input, nullptr);
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        ASSERT_EQ(getEdgesNode->edges(), edges);
        ASSERT_EQ(getEdgesNode->props().size(), 1);
        auto expr = Expression::decode(getEdgesNode->props().front().get_prop());
        const auto symbolExpr = std::unique_ptr<SymbolPropertyExpression>(
            static_cast<SymbolPropertyExpression*>(expr.release()));
        ASSERT_EQ(*symbolExpr->sym(), "like");
        ASSERT_EQ(*symbolExpr->prop(), "*");
    }
    // With YIELD
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, like.end"));
        // check plan
        // Project
        auto *plan = qCtx_->plan();
        ASSERT_EQ(plan->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan->root());
        auto *cols = projectNode->columns();
        ASSERT_NE(cols, nullptr);
        std::vector<std::string> propNames{"start", "end"};
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &col = cols->columns()[i];
            const auto *expr = col->expr();
            ASSERT_EQ(expr->kind(), Expression::Kind::kEdgeProperty);
            const auto *aliasPropertyExpr = static_cast<const SymbolPropertyExpression *>(expr);
            ASSERT_EQ(*aliasPropertyExpr->sym(), "like");
            ASSERT_EQ(*aliasPropertyExpr->prop(), propNames[i]);
        }
        // GetEdges
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetEdges);
        const auto *getEdgesNode = static_cast<const GetEdges *>(input);
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({"1", edgeType, 0, "2"})};
        ASSERT_EQ(getEdgesNode->edges(), edges);
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &prop = getEdgesNode->props()[i];
            auto expr = Expression::decode(prop.get_prop());
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->kind(), Expression::Kind::kEdgeProperty);
            auto aliasPropertyExpr = reinterpret_cast<SymbolPropertyExpression *>(expr.get());
            ASSERT_EQ(*aliasPropertyExpr->sym(), "like");
            ASSERT_EQ(*aliasPropertyExpr->prop(), propNames[i]);
        }
    }
}

TEST_F(ValidatorTest, FetchEdgesPropFailed) {
    // mismatched tag
    {
        auto result = GQLParser().parse("FETCH PROP ON edge1 \"1\" YIELD edge2.prop2");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge
    {
        auto result =
            GQLParser().parse("FETCH PROP ON not_exist_edge \"1\" YIELD not_exist_edge.prop1");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge property
    {
        auto result = GQLParser().parse("FETCH PROP ON like \"1\" YIELD like.not_exist_prop");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
