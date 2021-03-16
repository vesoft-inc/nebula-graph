
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

TEST_F(RewriteUnaryNotExprVisitorTest, TestNestedMultipleUnaryNotExpr) {
    // !!(5 == 10)  =>  (5 == 10)
    {
        auto expr = pool.add(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(eqExpr(constantExpr(5), constantExpr(10)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!!(5 == 10)  =>  (5 == 10)
    {
        auto expr =
            pool.add(notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))))));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(eqExpr(constantExpr(5), constantExpr(10)));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!(5 == 10)  =>  !(5 == 10)
    {
        auto expr = pool.add(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(notExpr(eqExpr(constantExpr(5), constantExpr(10))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // !!!!!(5 == 10)  =>  !(5 == 10)
    {
        auto expr = pool.add(
            notExpr(notExpr(notExpr(notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))))));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(notExpr(eqExpr(constantExpr(5), constantExpr(10))));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
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

TEST_F(RewriteUnaryNotExprVisitorTest, TestMultipleUnaryNotContainerExpr) {
    // List
    {
        // [!!(5 == 10), !!!(30 > 20)]  =>  [(5 == 10), !(30 > 20)]
        auto expr = pool.add(
            listExpr({notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                      notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))}));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(listExpr({eqExpr(constantExpr(5), constantExpr(10)),
                                           notExpr(gtExpr(constantExpr(30), constantExpr(20)))}));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
    // Set
    {
        // {!!(5 == 10), !!!(30 > 20)}  =>  {(5 == 10), !(30 > 20)}
        auto expr = pool.add(
            setExpr({notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10)))),
                     notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))}));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected = pool.add(setExpr({eqExpr(constantExpr(5), constantExpr(10)),
                                          notExpr(gtExpr(constantExpr(30), constantExpr(20)))}));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }

    // Map
    {
        // {"k1":!!(5 == 10), "k2":!!!(30 > 20)}} => {"k1":(5 == 10), "k2":!(30 > 20)}
        auto expr = pool.add(mapExpr(
            {{"k1", notExpr(notExpr(eqExpr(constantExpr(5), constantExpr(10))))},
             {"k2", notExpr(notExpr(notExpr(gtExpr(constantExpr(30), constantExpr(20)))))}}));
        RewriteUnaryNotExprVisitor visitor;
        expr->accept(&visitor);
        auto res = visitor.getExpr();
        auto expected =
            pool.add(mapExpr({{"k1", eqExpr(constantExpr(5), constantExpr(10))},
                              {"k2", notExpr(gtExpr(constantExpr(30), constantExpr(20)))}}));
        ASSERT_EQ(*res, *expected) << res->toString() << " vs. " << expected->toString();
    }
}

}   // namespace graph
}   // namespace nebula
