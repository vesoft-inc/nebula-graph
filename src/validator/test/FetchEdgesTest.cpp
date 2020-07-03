/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Query.h"
#include "validator/FetchEdgesValidator.h"
#include "validator/test/ValidatorTest.h"

namespace nebula {
namespace graph {

TEST_F(ValidatorTest, FetchEdgesProp) {
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\""));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  {});
        expectedQueryCtx_->plan()->setRoot(ge);
        auto result = Eq(plan->root(), ge);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        std::vector<std::string> propsName{"start", "end"};
        prop.set_props(std::move(propsName));
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("start")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("end")).encode());
        exprs.emplace_back(std::move(expr1));
        exprs.emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));
        expectedQueryCtx_->plan()->setRoot(ge);
        auto result = Eq(plan->root(), ge);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD const expression
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start, 1 + 1, like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();

        // GetEdges
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        std::vector<std::string> propsName{"start", "end"};
        prop.set_props(std::move(propsName));
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("start")).encode());
        storage::cpp2::Expr expr2;
        expr2.set_expr(
            EdgePropertyExpression(new std::string("like"), new std::string("end")).encode());
        exprs.emplace_back(std::move(expr1));
        exprs.emplace_back(std::move(expr2));
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));

        // Project
        auto yieldColumns = std::make_unique<YieldColumns>();
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("start"))));
        yieldColumns->addColumn(new YieldColumn(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(1))));
        yieldColumns->addColumn(new YieldColumn(
            new EdgePropertyExpression(new std::string("like"), new std::string("end"))));
        auto *project = Project::make(expectedQueryCtx_->plan(), ge, std::move(yieldColumns));
        // TODO(shylock) waiting expression toString
        // project->setColNames({"like.start", "1 + 1", "like.end"});
        expectedQueryCtx_->plan()->setRoot(project);
        auto result = Eq(plan->root(), project);
        ASSERT_TRUE(result.ok()) << result;
    }
    // With YIELD combine properties
    {
        ASSERT_TRUE(toPlan("FETCH PROP ON like \"1\"->\"2\" YIELD like.start > like.end"));

        auto *start = StartNode::make(expectedQueryCtx_->plan());

        auto *plan = qCtx_->plan();
        auto edgeTypeResult = schemaMng_->toEdgeType(1, "like");
        ASSERT_TRUE(edgeTypeResult.ok());
        auto edgeType = edgeTypeResult.value();
        std::vector<nebula::Row> edges{nebula::Row({
            "1",
            edgeType,
            0,
            "2",
        })};
        storage::cpp2::EdgeProp prop;
        prop.set_type(edgeType);
        std::vector<std::string> propsName{"start", "end"};
        prop.set_props(std::move(propsName));
        std::vector<storage::cpp2::EdgeProp> props;
        props.emplace_back(std::move(prop));
        std::vector<storage::cpp2::Expr> exprs;
        storage::cpp2::Expr expr1;
        expr1.set_expr(
            RelationalExpression(
                Expression::Kind::kRelGT,
                new EdgePropertyExpression(new std::string("like"), new std::string("start")),
                new EdgePropertyExpression(new std::string("like"), new std::string("end")))
                .encode());
        exprs.emplace_back(std::move(expr1));
        auto *ge = GetEdges::make(expectedQueryCtx_->plan(),
                                  start,
                                  1,
                                  std::move(edges),
                                  nullptr,
                                  edgeType,
                                  nullptr,
                                  nullptr,
                                  std::move(props),
                                  std::move(exprs));
        expectedQueryCtx_->plan()->setRoot(ge);
        auto result = Eq(plan->root(), ge);
        ASSERT_TRUE(result.ok()) << result;
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
