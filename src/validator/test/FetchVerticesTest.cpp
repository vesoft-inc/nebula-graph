/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/test/ValidatorTest.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchVerticesProp) {
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\""));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        auto *gv = GetVertices::make(expectedQueryCtx_->plan(),
                                     start,
                                     1,
                                     std::vector<Row>{Row({"1"})},
                                     nullptr,
                                     std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                                     {});
        auto result = Eq(plan->root(), gv);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD person.name, person.age"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv =
            GetVertices::make(expectedQueryCtx_->plan(),
                              start,
                              1,
                              std::vector<Row>{Row({"1"})},
                              nullptr,
                              std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                              std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2)});
        auto result = Eq(plan->root(), gv);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD const expression
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD person.name, 1 > 1, person.age"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();

        // get vertices
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("person"), new std::string("name")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("person"), new std::string("age")).encode());
        auto *gv =
            GetVertices::make(expectedQueryCtx_->plan(),
                              start,
                              1,
                              std::vector<Row>{Row({"1"})},
                              nullptr,
                              std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                              std::vector<storage::cpp2::Expr>{std::move(expr1), std::move(expr2)});

        // project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("person"), new std::string("name"))));
        yieldColumns->addColumn(new YieldColumn(new RelationalExpression(
            Expression::Kind::kRelGT, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("person"), new std::string("age"))));
        auto *project = Project::make(expectedQueryCtx_->plan(), gv, std::move(yieldColumns));

        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD person.name + person.age"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto tagIdResult = schemaMng_->toTagID(1, "person");
        ASSERT_TRUE(tagIdResult.ok());
        auto tagId = tagIdResult.value();
        storage::cpp2::VertexProp prop;
        prop.set_tag(tagId);
        prop.set_props(std::vector<std::string>{"name", "age"});
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            ArithmeticExpression(
                Expression::Kind::kAdd,
                new EdgePropertyExpression(new std::string("person"), new std::string("name")),
                new EdgePropertyExpression(new std::string("person"), new std::string("age")))
                .encode());

        auto *gv = GetVertices::make(expectedQueryCtx_->plan(),
                                     start,
                                     1,
                                     std::vector<Row>{Row({"1"})},
                                     nullptr,
                                     std::vector<storage::cpp2::VertexProp>{std::move(prop)},
                                     std::vector<storage::cpp2::Expr>{std::move(expr1)});
        auto result = Eq(plan->root(), gv);
        ASSERT_TRUE(result.ok()) << result;
    }
    // ON *
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON * \"1\""));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();

        auto *gv = GetVertices::make(
            expectedQueryCtx_->plan(), start, 1, std::vector<Row>{Row({"1"})}, nullptr, {}, {});
        auto result = Eq(plan->root(), gv);
        ASSERT_TRUE(result.ok()) << result;
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
        auto result =
            GQLParser().parse("FETCH PROP ON person \"1\" YIELD person.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
