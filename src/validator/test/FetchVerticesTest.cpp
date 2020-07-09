/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"
#include "validator/FetchVerticesValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class FetchVerticesValidatorTest : public ValidatorTestBase {};

TEST_F(FetchVerticesValidatorTest, FetchVerticesProp) {
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
        auto *project = Project::make(expectedQueryCtx_->plan(), gv, yieldColumns.get());
        project->setColNames({"person.name", "(1>1)", "person.age"});

        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD person.name + person.age"));
        auto *plan = qCtx_->plan();

        auto *start = StartNode::make(expectedQueryCtx_->plan());

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

        // project, TODO(shylock) could push down to storage is it supported
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new EdgePropertyExpression(new std::string("person"), new std::string("name")),
                new EdgePropertyExpression(new std::string("person"), new std::string("age")))));
        auto *project = Project::make(expectedQueryCtx_->plan(), gv, yieldColumns.get());
        project->setColNames({"(person.name+person.age)"});

        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD distinct
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON person \"1\" YIELD distinct person.name, person.age"));

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

        std::vector<std::string> colNames{"person.name", "person.age"};
        // dedup
        auto *dedup = Dedup::make(expectedQueryCtx_->plan(), gv);
        dedup->setColNames(colNames);

        // data collect
        auto *dataCollect = DataCollect::make(expectedQueryCtx_->plan(), dedup,
            DataCollect::CollectKind::kRowBasedMove, {dedup->varName()});
        dataCollect->setColNames(colNames);

        auto result = Eq(plan->root(), dataCollect);
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

TEST_F(FetchVerticesValidatorTest, FetchVerticesPropFailed) {
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

TEST_F(FetchVerticesValidatorTest, FetchVerticesInputFailed) {
    // mismatched varirable
    {
        auto result = GQLParser().parse("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                                        "FETCH PROP ON person $b.name");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // mismatched varirable property
    {
        auto result = GQLParser().parse("$a = FETCH PROP ON person \"1\" YIELD person.name AS name;"
                                        "FETCH PROP ON person $a.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }

    // mismatched input property
    {
        auto result = GQLParser().parse("FETCH PROP ON person \"1\" YIELD person.name AS name | "
                                        "FETCH PROP ON person $-.not_exist_property");
        ASSERT_TRUE(result.ok());
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qCtx_.get());
        auto validateResult = validator.validate();
        ASSERT_FALSE(validateResult.ok());
    }
}

}   // namespace graph
}   // namespace nebula
