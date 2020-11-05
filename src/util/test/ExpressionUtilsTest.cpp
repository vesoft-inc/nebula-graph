/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

class ExpressionUtilsTest : public ::testing::Test {};

TEST_F(ExpressionUtilsTest, CheckComponent) {
    {
        // single node
        const auto root = std::make_unique<ConstantExpression>();

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kConstant}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kConstant}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kConstant, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(),
                                            {Expression::Kind::kConstant, Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

        // find
        const Expression *found =
            ExpressionUtils::findAny(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(
            root.get(),
            {Expression::Kind::kConstant, Expression::Kind::kAdd, Expression::Kind::kEdgeProperty});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kEdgeDst});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(
            root.get(), {Expression::Kind::kEdgeRank, Expression::Kind::kInputProperty});
        ASSERT_EQ(found, nullptr);

        // find all
        const auto willFoundAll = std::vector<const Expression *>{root.get()};
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds, willFoundAll);

        founds = ExpressionUtils::collectAll(
            root.get(),
            {Expression::Kind::kAdd, Expression::Kind::kConstant, Expression::Kind::kEdgeDst});
        ASSERT_EQ(founds, willFoundAll);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kSrcProperty});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(root.get(),
                                             {Expression::Kind::kUnaryNegate,
                                              Expression::Kind::kEdgeDst,
                                              Expression::Kind::kEdgeDst});
        ASSERT_TRUE(founds.empty());
    }

    {
        // list like
        const auto root = std::make_unique<TypeCastingExpression>(
            Value::Type::BOOL,
            new TypeCastingExpression(
                Value::Type::BOOL,
                new TypeCastingExpression(Value::Type::BOOL, new ConstantExpression())));

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kTypeCasting}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kConstant}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kAdd}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kAdd}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kAdd}));

        // found
        const Expression *found =
            ExpressionUtils::findAny(root.get(), {Expression::Kind::kTypeCasting});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kFunctionCall,
                                          Expression::Kind::kTypeCasting,
                                          Expression::Kind::kLogicalAnd});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kDivision});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kLogicalXor,
                                          Expression::Kind::kRelGE,
                                          Expression::Kind::kEdgeProperty});
        ASSERT_EQ(found, nullptr);

        // found all
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds.size(), 1);

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kFunctionCall, Expression::Kind::kTypeCasting});
        ASSERT_EQ(founds.size(), 3);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kAdd});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kRelLE, Expression::Kind::kDstProperty});
        ASSERT_TRUE(founds.empty());
    }

    {
        // tree like
        const auto root = std::make_unique<ArithmeticExpression>(
            Expression::Kind::kAdd,
            new ArithmeticExpression(Expression::Kind::kDivision,
                                     new ConstantExpression(3),
                                     new ArithmeticExpression(Expression::Kind::kMinus,
                                                              new ConstantExpression(4),
                                                              new ConstantExpression(2))),
            new ArithmeticExpression(Expression::Kind::kMod,
                                     new ArithmeticExpression(Expression::Kind::kMultiply,
                                                              new ConstantExpression(3),
                                                              new ConstantExpression(10)),
                                     new ConstantExpression(2)));

        ASSERT_TRUE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kMinus}));

        ASSERT_TRUE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kTypeCasting, Expression::Kind::kAdd}));
        ASSERT_TRUE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kLabelAttribute, Expression::Kind::kDivision}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(root.get(), {Expression::Kind::kConstant}));
        ASSERT_FALSE(ExpressionUtils::hasAny(root.get(), {Expression::Kind::kFunctionCall}));

        ASSERT_FALSE(ExpressionUtils::isKindOf(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kEdgeProperty}));
        ASSERT_FALSE(ExpressionUtils::hasAny(
            root.get(), {Expression::Kind::kDstProperty, Expression::Kind::kLogicalAnd}));

        // found
        const Expression *found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kAdd});
        ASSERT_EQ(found, root.get());

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kFunctionCall,
                                          Expression::Kind::kRelLE,
                                          Expression::Kind::kMultiply});
        ASSERT_NE(found, nullptr);

        found = ExpressionUtils::findAny(root.get(), {Expression::Kind::kInputProperty});
        ASSERT_EQ(found, nullptr);

        found = ExpressionUtils::findAny(root.get(),
                                         {Expression::Kind::kLogicalXor,
                                          Expression::Kind::kEdgeRank,
                                          Expression::Kind::kUnaryNot});
        ASSERT_EQ(found, nullptr);

        // found all
        std::vector<const Expression *> founds =
            ExpressionUtils::collectAll(root.get(), {Expression::Kind::kConstant});
        ASSERT_EQ(founds.size(), 6);

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kDivision, Expression::Kind::kMinus});
        ASSERT_EQ(founds.size(), 2);

        founds = ExpressionUtils::collectAll(root.get(), {Expression::Kind::kEdgeDst});
        ASSERT_TRUE(founds.empty());

        founds = ExpressionUtils::collectAll(
            root.get(), {Expression::Kind::kLogicalAnd, Expression::Kind::kUnaryNegate});
        ASSERT_TRUE(founds.empty());
    }
}

TEST_F(ExpressionUtilsTest, PullAnds) {
    using Kind = Expression::Kind;
    // true AND false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalAnd, first, second);
        auto ands = ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(2UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
    }
    // true AND false AND true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd, first, second), third);
        auto ands = ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(3UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
        ASSERT_EQ(third, ands[2]);
    }
    // true AND (false AND true)
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                first,
                new LogicalExpression(Kind::kLogicalAnd, second, third));
        auto ands = ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(3UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
        ASSERT_EQ(third, ands[2]);
    }
    // (true OR false) AND (true OR false)
    {
        auto *first = new LogicalExpression(Kind::kLogicalOr,
                new ConstantExpression(true),
                new ConstantExpression(false));
        auto *second = new LogicalExpression(Kind::kLogicalOr,
                new ConstantExpression(true),
                new ConstantExpression(false));
        LogicalExpression expr(Kind::kLogicalAnd, first, second);
        auto ands = ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(2UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
    }
    // true AND ((false AND true) OR false) AND true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new LogicalExpression(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalAnd,
                    new ConstantExpression(false),
                    new ConstantExpression(true)),
                new ConstantExpression(false));
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalAnd, first, second), third);
        auto ands = ExpressionUtils::pullAnds(&expr);
        ASSERT_EQ(3UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
        ASSERT_EQ(third, ands[2]);
    }
}

TEST_F(ExpressionUtilsTest, PullOrs) {
    using Kind = Expression::Kind;
    // true OR false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalOr, first, second);
        auto ors = ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(2UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
    }
    // true OR false OR true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr, first, second), third);
        auto ors = ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(3UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
        ASSERT_EQ(third, ors[2]);
    }
    // true OR (false OR true)
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                first,
                new LogicalExpression(Kind::kLogicalOr, second, third));
        auto ors = ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(3UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
        ASSERT_EQ(third, ors[2]);
    }
    // (true AND false) OR (true AND false)
    {
        auto *first = new LogicalExpression(Kind::kLogicalAnd,
                new ConstantExpression(true),
                new ConstantExpression(false));
        auto *second = new LogicalExpression(Kind::kLogicalAnd,
                new ConstantExpression(true),
                new ConstantExpression(false));
        LogicalExpression expr(Kind::kLogicalOr, first, second);
        auto ors = ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(2UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
    }
    // true OR ((false OR true) AND false) OR true
    {
        auto *first = new ConstantExpression(true);
        auto *second = new LogicalExpression(Kind::kLogicalAnd,
                new LogicalExpression(Kind::kLogicalOr,
                    new ConstantExpression(false),
                    new ConstantExpression(true)),
                new ConstantExpression(false));
        auto *third = new ConstantExpression(true);
        LogicalExpression expr(Kind::kLogicalOr,
                new LogicalExpression(Kind::kLogicalOr, first, second), third);
        auto ors = ExpressionUtils::pullOrs(&expr);
        ASSERT_EQ(3UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
        ASSERT_EQ(third, ors[2]);
    }
}

}   // namespace graph
}   // namespace nebula
