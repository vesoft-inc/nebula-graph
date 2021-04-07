
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/test/VisitorTestBase.h"
#include "util/ExpressionUtils.h"

#include <gtest/gtest.h>

#include "common/base/ObjectPool.h"

namespace nebula {
namespace graph {
class RewriteRelExprVisitorTest : public ValidatorTestBase {
public:
    void TearDown() override {
        pool.clear();
    }

protected:
    ObjectPool pool;
};

TEST_F(RewriteRelExprVisitorTest, ArithmeticalExpr) {
    // (label + 1 < 40)  =>  (label < 40 - 1)
    {
        auto expr =
            pool.add(ltExpr(addExpr(laExpr("v", "age"), constantExpr(1)), constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            pool.add(ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(1))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (1 + label < 40)  =>  (label < 40 - 1)
    {
        auto expr =
            pool.add(ltExpr(addExpr(constantExpr(1), laExpr("v", "age")), constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            pool.add(ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(1))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // (-1 + label < 40)  =>  (label < 40 - (-1))
    {
        auto expr =
            pool.add(ltExpr(addExpr(constantExpr(-1), laExpr("v", "age")), constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected =
            pool.add(ltExpr(laExpr("v", "age"), minusExpr(constantExpr(40), constantExpr(-1))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

TEST_F(RewriteRelExprVisitorTest, NestedArithmeticalExpr) {
    // (label + 1 + 2 < 40)  =>  (label < 40 - 2 - 1)
    {
        auto expr =
            pool.add(ltExpr(addExpr(addExpr(laExpr("v", "age"), constantExpr(1)),
            constantExpr(2)),
                            constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = pool.add(
            ltExpr(laExpr("v", "age"),
                   minusExpr(minusExpr(constantExpr(40), constantExpr(2)), constantExpr(1))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // (label + 1 - 2 < 40)  =>  (label < 40 + 2 - 1)
    {
        auto expr =
            pool.add(ltExpr(minusExpr(addExpr(laExpr("v", "age"), constantExpr(1)),
            constantExpr(2)),
                            constantExpr(40)));
        auto res = ExpressionUtils::rewriteRelExpr(expr);
        auto expected = pool.add(
            ltExpr(laExpr("v", "age"),
                   minusExpr(addExpr(constantExpr(40), constantExpr(2)), constantExpr(1))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

}   // namespace graph
}   // namespace nebula
