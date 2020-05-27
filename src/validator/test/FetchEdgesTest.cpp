/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchEdgesValidator.h"
#include "validator/test/ValidatorTest.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchEdgesProp) {
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON edge1 \"1\"->\"2\""));
        // check plan
        // GetEdgess
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kGetEdges);
        const auto *getEdgesNode = static_cast<const GetEdges *>(plan_->root());
        auto *input = getEdgesNode->input();
        ASSERT_EQ(input, nullptr);
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            0 /*TODO(shylock)type*/,
            0,
            "2",
        })};
        ASSERT_EQ(getEdgesNode->edges(), edges);
        ASSERT_TRUE(getEdgesNode->props().empty());
    }
    // With YIELD
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON edge1 \"1\"->\"2\" YIELD edge1.prop1, edge1.prop2"));
        // check plan
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        auto *cols = projectNode->columns();
        ASSERT_NE(cols, nullptr);
        std::vector<std::string> propNames{"prop1", "prop2"};
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &col = cols->columns()[i];
            const auto *expr = col->expr();
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            const auto *aliasPropertyExpr = reinterpret_cast<const AliasPropertyExpression *>(expr);
            ASSERT_EQ(*aliasPropertyExpr->alias(), "edge1");
            ASSERT_EQ(*aliasPropertyExpr->prop(), propNames[i]);
        }
        // GetEdges
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetEdges);
        auto *getEdgesNode = static_cast<GetEdges *>(input);
        std::vector<nebula::Row> edges{nebula::Row({"1", 0 /*TODO(shylock)type*/, 0, "2"})};
        ASSERT_EQ(getEdgesNode->edges(), edges);
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &prop = getEdgesNode->props()[i];
            auto expr = Expression::decode(prop.get_prop());
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            auto aliasPropertyExpr = reinterpret_cast<AliasPropertyExpression *>(expr.get());
            ASSERT_EQ(*aliasPropertyExpr->alias(), "edge1");
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
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge
    {
        auto result =
            GQLParser().parse("FETCH PROP ON not_exist_edge \"1\" YIELD not_exist_edge.prop");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }

    // notexist edge property
    {
        auto result = GQLParser().parse("FETCH PROP ON edge1 \"1\" YIELD edge1.not_exist_prop");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
