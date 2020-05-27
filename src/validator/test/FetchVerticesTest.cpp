/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FetchVerticesValidator.h"
#include "validator/test/ValidatorTest.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchVerticesProp) {
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON tag1 \"1\""));
        // check plan
        // GetVertices
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kGetVertices);
        const auto *getVerticesNode = static_cast<const GetVertices *>(plan_->root());
        auto *input = getVerticesNode->input();
        ASSERT_EQ(input, nullptr);
        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        ASSERT_TRUE(getVerticesNode->props().empty());
    }
    // With YIELD
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON tag1 \"1\" YIELD tag1.prop1, tag1.prop2"));
        // check plan
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        auto *cols = projectNode->columns();
        ASSERT_NE(cols, nullptr);
        std::vector<std::string> props{"prop1", "prop2"};
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &col = cols->columns()[i];
            const auto *expr = col->expr();
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            const auto *aliasPropertyExpr = reinterpret_cast<const AliasPropertyExpression *>(expr);
            ASSERT_EQ(*aliasPropertyExpr->alias(), "tag1");
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
        // GetVertices
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetVertices);
        auto *getVerticesNode = static_cast<GetVertices *>(input);
        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &prop = getVerticesNode->props()[i];
            auto expr = Expression::decode(prop.get_prop());
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            auto aliasPropertyExpr = reinterpret_cast<AliasPropertyExpression *>(expr.get());
            ASSERT_EQ(*aliasPropertyExpr->alias(), "tag1");
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
    }
    // ON *
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON * \"1\""));
        // check plan
        // GetVertices
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kGetVertices);
        const auto *getVerticesNode = static_cast<const GetVertices *>(plan_->root());
        auto *input = getVerticesNode->input();
        ASSERT_EQ(input, nullptr);
        std::vector<nebula::Row> vertices{nebula::Row({"1"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        ASSERT_TRUE(getVerticesNode->props().empty());
    }
    // With * and YIELD
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON * \"1\", \"2\" YIELD tag1.prop1, tag2.prop2"));
        // check plan
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        auto *cols = projectNode->columns();
        ASSERT_NE(cols, nullptr);
        std::vector<std::string> alias{"tag1", "tag2"};
        std::vector<std::string> props{"prop1", "prop2"};
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &col = cols->columns()[i];
            const auto *expr = col->expr();
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            const auto *aliasPropertyExpr = reinterpret_cast<const AliasPropertyExpression *>(expr);
            ASSERT_EQ(*aliasPropertyExpr->alias(), alias[i]);
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
        // GetVertices
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetVertices);
        auto *getVerticesNode = static_cast<GetVertices *>(input);
        std::vector<nebula::Row> vertices{nebula::Row({"1", "2"})};
        ASSERT_EQ(getVerticesNode->vertices(), vertices);
        for (std::size_t i = 0; i < 2; ++i) {
            const auto &prop = getVerticesNode->props()[i];
            auto expr = Expression::decode(prop.get_prop());
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->kind(), Expression::Kind::kAliasProperty);
            auto aliasPropertyExpr = reinterpret_cast<AliasPropertyExpression *>(expr.get());
            ASSERT_EQ(*aliasPropertyExpr->alias(), alias[i]);
            ASSERT_EQ(*aliasPropertyExpr->prop(), props[i]);
        }
    }
}

TEST_F(ValidatorTest, FetchVerticesPropFailed) {
    // mismatched tag
    {
        auto result = GQLParser().parse("FETCH PROP ON tag1 \"1\" YIELD tag2.prop2");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }

    // not exist tag
    {
        auto result =
            GQLParser().parse("FETCH PROP ON not_exist_tag \"1\" YIELD not_exist_tag.prop");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }

    // not exist property
    {
        auto result = GQLParser().parse("FETCH PROP ON tag1 \"1\" YIELD tag1.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
