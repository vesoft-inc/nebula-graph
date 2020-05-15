/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

#include <gtest/gtest.h>

#include "parser/GQLParser.h"
#include "planner/ExecutionPlan.h"
#include "planner/Query.h"
#include "service/ExecutionContext.h"
#include "validator/ASTValidator.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/FetchVerticesValidator.h"

namespace nebula {
namespace graph {

class ValidatorTest : public ::testing::Test {
public:
    void SetUp() override {
        session_ = new ClientSession(0);
        session_->setSpace("test", 0);
        ectx_ = std::make_unique<ExecutionContext>();
        plan_ = std::make_unique<ExecutionPlan>(ectx_.get());
        charsetInfo_ = CharsetInfo::instance();
        // TODO: Need AdHocSchemaManager here.
    }

    void TearDown() override {
        delete session_;
    }

protected:
    // some utils
    inline ::testing::AssertionResult toPlan(const std::string &query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) {
            return ::testing::AssertionFailure() << result.status().toString();
        }
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        if (!validateResult.ok()) {
            return ::testing::AssertionFailure() << validateResult.toString();
        }
        if (nullptr == plan_) {
            return ::testing::AssertionFailure() << "Null plan";
        }
        return ::testing::AssertionSuccess();
    }

protected:
    ClientSession *session_;
    meta::SchemaManager *schemaMng_;
    std::unique_ptr<ExecutionContext> ectx_;
    std::unique_ptr<ExecutionPlan> plan_;
    CharsetInfo *charsetInfo_;
};

TEST_F(ValidatorTest, Subgraph) {
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM 1";
        auto result = GQLParser().parse(query);
        ASSERT_TRUE(result.ok()) << result.status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), session_, schemaMng_, charsetInfo_);
        auto validateResult = validator.validate(plan_.get());
        ASSERT_TRUE(validateResult.ok()) << validateResult;
        // TODO: Check the plan.
    }
}

TEST_F(ValidatorTest, FetchVerticesProp) {
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON tag1 \"1\""));
        // check plan
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        ASSERT_EQ(projectNode->columns(), nullptr);
        // GetVertices
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetVertices);
        auto *getVerticesNode = static_cast<GetVertices *>(input);
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
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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
            auto expr = Expression::decode(prop);
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        ASSERT_EQ(projectNode->columns(), nullptr);
        // GetVertices
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetVertices);
        auto *getVerticesNode = static_cast<GetVertices *>(input);
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
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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
            auto expr = Expression::decode(prop);
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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

TEST_F(ValidatorTest, FetchEdgesProp) {
    {
        // TODO(shylock) mock schema
        ASSERT_TRUE(toPlan("FETCH PROP ON edge1 \"1\"->\"2\""));
        // check plan
        // Project
        ASSERT_EQ(plan_->root()->kind(), PlanNode::Kind::kProject);
        const auto *projectNode = static_cast<const Project *>(plan_->root());
        ASSERT_EQ(projectNode->columns(), nullptr);
        // GetEdgess
        auto *input = projectNode->input();
        ASSERT_NE(input, nullptr);
        ASSERT_EQ(input->kind(), PlanNode::Kind::kGetEdges);
        auto *getEdgesNode = static_cast<GetEdges *>(input);
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
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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
            auto expr = Expression::decode(prop);
            ASSERT_NE(expr, nullptr);
            ASSERT_EQ(expr->type(), Expression::Type::EXP_ALIAS_PROPERTY);
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
