
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/RewriteUnaryNotExprVisitor.h"
#include "visitor/test/VisitorTestBase.h"

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"

namespace nebula {
namespace graph {
class RewriteUnaryNotExprVisitorTest : public ValidatorTestBase {
public:
    void TearDown() override {
        pool.clear();
    }

protected:
    ObjectPool pool;
};

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExprEven) {
    // !!(5 == 10)  =>  (5 == 10)
    auto expr = pool.add(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))));
    RewriteUnaryNotExprVisitor visitor;
    expr->accept(&visitor);
    auto res = visitor.getExpr();
    auto expected = pool.add(eqExpr(constantExpr(5), constantExpr(10)));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExpr_Even_Nested) {
    // !!!!(5 == 10)  =>  (5 == 10)
    auto expr =
        pool.add(notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))))));
    RewriteUnaryNotExprVisitor visitor;
    expr->accept(&visitor);
    auto res = visitor.getExpr();
    auto expected = pool.add(eqExpr(constantExpr(5), constantExpr(10)));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExprOdd) {
    // !!!(5 == 10)  =>  !(5 == 10)
    auto expr = pool.add(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))));
    RewriteUnaryNotExprVisitor visitor;
    expr->accept(&visitor);
    auto res = visitor.getExpr();
    auto expected = pool.add(notExpr(eqExpr(constantExpr(5), constantExpr(10))));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExprOddNested) {
    // !!!!!(5 == 10)  =>  !(5 == 10)
    auto expr = pool.add(
        notExpr(notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))))));
    RewriteUnaryNotExprVisitor visitor;
    expr->accept(&visitor);
    auto res = visitor.getExpr();
    auto expected = pool.add(notExpr(eqExpr(constantExpr(5), constantExpr(10))));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotExprLogicalRelExpr) {
    // !!(5 == 10) AND !!(30 > 20)  =>  (5 == 10) AND (30 > 20)
    auto expr = pool.add(andExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                                 notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20))))));
    RewriteUnaryNotExprVisitor visitor;
    expr->accept(&visitor);
    auto res = visitor.getExpr();
    auto expected = pool.add(andExpr(eqExpr(constantExpr(5), constantExpr(10)),
                                     gtExpr(constantExpr(30), constantExpr(20))));
    ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
}

}   // namespace graph
}   // namespace nebula
