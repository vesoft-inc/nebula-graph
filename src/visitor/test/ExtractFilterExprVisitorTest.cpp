/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"
#include "common/expression/Expression.h"
#include "common/expression/PropertyExpression.h"
#include "util/ExpressionUtils.h"
#include "visitor/ExtractFilterExprVisitor.h"
#include "visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {

class ExtractFilterExprVisitorTest : public VisitorTestBase {
protected:
    ObjectPool pool_;
};

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushNoAnd) {
    // true
    ExtractFilterExprVisitor visitor;
    auto expr = pool_.add(constantExpr(true));
    expr->accept(&visitor);
    ASSERT_TRUE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushAnd) {
    // true AND false
    ExtractFilterExprVisitor visitor;
    auto expr = pool_.add(andExpr(constantExpr(true), constantExpr(false)));
    expr->accept(&visitor);
    ASSERT_TRUE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushOr) {
    // true OR false
    ExtractFilterExprVisitor visitor;
    auto expr = pool_.add(orExpr(constantExpr(true), constantExpr(false)));
    expr->accept(&visitor);
    ASSERT_TRUE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushNoAnd) {
    // $$.player.name
    ExtractFilterExprVisitor visitor;
    auto expr =
        pool_.add(new DestPropertyExpression(new std::string("player"), new std::string("name")));
    expr->accept(&visitor);
    ASSERT_FALSE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushAnd) {
    // $$.player.name AND $var.name
    ExtractFilterExprVisitor visitor;
    auto dstExpr = new DestPropertyExpression(new std::string("player"), new std::string("name"));
    auto varExpr = new VariablePropertyExpression(new std::string("var"), new std::string("name"));
    auto expr = pool_.add(andExpr(dstExpr, varExpr));
    expr->accept(&visitor);
    ASSERT_FALSE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanNotBePushOr) {
    // $$.player.name OR $var.name
    ExtractFilterExprVisitor visitor;
    auto dstExpr = new DestPropertyExpression(new std::string("player"), new std::string("name"));
    auto varExpr = new VariablePropertyExpression(new std::string("var"), new std::string("name"));
    auto expr = pool_.add(orExpr(dstExpr, varExpr));
    expr->accept(&visitor);
    ASSERT_FALSE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeAnd) {
    // $$.player.name AND true
    ExtractFilterExprVisitor visitor;
    auto dstExpr = new DestPropertyExpression(new std::string("player"), new std::string("name"));
    auto expr = pool_.add(andExpr(dstExpr, constantExpr(true)));
    expr->accept(&visitor);
    ASSERT_TRUE(visitor.ok());
    auto rmExpr = std::move(visitor).remainedExpr();
    ASSERT_EQ(rmExpr->kind(), Expression::Kind::kDstProperty);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeOr) {
    // $$.player.name OR $$.player.name
    ExtractFilterExprVisitor visitor;
    auto dstExpr = new DestPropertyExpression(new std::string("player"), new std::string("name"));
    auto expr = pool_.add(orExpr(dstExpr, constantExpr(true)));
    expr->accept(&visitor);
    ASSERT_FALSE(visitor.ok());
    ASSERT(std::move(visitor).remainedExpr() == nullptr);
}

TEST_F(ExtractFilterExprVisitorTest, TestCanBePushSomeAndOr) {
    // $$.player.name AND (true OR $^.player.name)
    ExtractFilterExprVisitor visitor;
    auto dstExpr = new DestPropertyExpression(new std::string("player"), new std::string("name"));
    auto srcExpr = new SourcePropertyExpression(new std::string("player"), new std::string("name"));
    auto rExpr = orExpr(srcExpr, constantExpr(true));
    auto expr = pool_.add(andExpr(dstExpr, rExpr));
    expr->accept(&visitor);
    ASSERT_TRUE(visitor.ok());
    auto rmexpr = std::move(visitor).remainedExpr();
    ASSERT_EQ(rmexpr->kind(), Expression::Kind::kDstProperty);
    ASSERT_EQ(expr->kind(), Expression::Kind::kLogicalAnd);
    // auto newExpr = ExpressionUtils::foldConstantExpr(expr, &pool_);
    // ASSERT_EQ(newExpr->kind(), Expression::Kind::kLogicalOr);
}

}   // namespace graph
}   // namespace nebula
