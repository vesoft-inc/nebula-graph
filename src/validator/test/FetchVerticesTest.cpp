/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchVerticesValidator.h"
#include "validator/test/ValidatorTest.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchVerticesProp) {
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\""));
        // check plan
        // GetVertices
        auto *plan = qCtx_->plan();
        ASSERT_EQ(plan->root()->kind(), PlanNode::Kind::kGetVertices);
        const auto *getVerticesNode = static_cast<const GetVertices *>(plan->root());
        auto *input = getVerticesNode->input();
        ASSERT_EQ(input, nullptr);

        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);

        ASSERT_TRUE(getVerticesNode->exprs().empty());

        ASSERT_EQ(getVerticesNode->props().size(), 1);
        auto tagId = getVerticesNode->props().front().get_tag();
        auto expectedTagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(expectedTagIdResult.ok());
        ASSERT_EQ(expectedTagIdResult.value(), tagId);
        const auto &props = getVerticesNode->props().front().get_props();
        ASSERT_TRUE(props.empty());
    }
    // With YIELD
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD person.name, person.age"));
        // check plan
        // Project
        auto *plan = qCtx_->plan();
        ASSERT_EQ(plan->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan->root());
        auto *cols = projectNode->columns();
        ASSERT_NE(cols, nullptr);
        std::vector<std::string> props{"name", "age"};
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &col = cols->columns()[i];
            const auto *expr = col->expr();
            ASSERT_EQ(expr->kind(), Expression::Kind::kEdgeProperty);
            const auto *aliasPropertyExpr = static_cast<const SymbolPropertyExpression *>(expr);
            ASSERT_EQ(*aliasPropertyExpr->sym(), "person");
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
        // GetVertices
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetVertices);
        const auto *getVerticesNode = static_cast<const GetVertices *>(input);
        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &exprAlias = getVerticesNode->exprs()[i];
            auto expr = Expression::decode(exprAlias.get_expr());
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->kind(), Expression::Kind::kEdgeProperty);
            auto aliasPropertyExpr = static_cast<SymbolPropertyExpression *>(expr.get());
            ASSERT_EQ(*aliasPropertyExpr->sym(), "person");
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
    }
    // ON *
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON * \"1\""));
        // check plan
        // GetVertices
        auto *plan = qCtx_->plan();
        ASSERT_EQ(plan->root()->kind(), PlanNode::Kind::kGetVertices);
        const auto *getVerticesNode = static_cast<const GetVertices *>(plan->root());
        auto *input = getVerticesNode->input();
        ASSERT_EQ(input, nullptr);
        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        ASSERT_TRUE(getVerticesNode->props().empty());
    }
}

TEST_F(ValidatorTest, FetchVerticesPropFailed) {
    // mismatched tag
    {
        auto result = GQLParser().parse("FETCH PROP ON tag1 \"1\" YIELD tag2.prop2");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // not exist tag
    {
        auto result =
            GQLParser().parse("FETCH PROP ON not_exist_tag \"1\" YIELD not_exist_tag.prop1");
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // not exist property
    {
        auto result = GQLParser().parse(
            "FETCH PROP ON person \"1\" YIELD person.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
