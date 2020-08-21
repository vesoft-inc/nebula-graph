/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "optimizer/OptimizerUtils.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LabelAttributeExpression.h"

namespace nebula {
namespace graph {

using Kind = Expression::Kind;

TEST(LogicalExprPullTest, AND) {
    // true AND false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalAnd, first, second);
        auto ands = OptimizerUtils::pullAnds(&expr);
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
        auto ands = OptimizerUtils::pullAnds(&expr);
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
        auto ands = OptimizerUtils::pullAnds(&expr);
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
        auto ands = OptimizerUtils::pullAnds(&expr);
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
        auto ands = OptimizerUtils::pullAnds(&expr);
        ASSERT_EQ(3UL, ands.size());
        ASSERT_EQ(first, ands[0]);
        ASSERT_EQ(second, ands[1]);
        ASSERT_EQ(third, ands[2]);
    }
}

TEST(LogicalExprPullTest, OR) {
    // true OR false
    {
        auto *first = new ConstantExpression(true);
        auto *second = new ConstantExpression(false);
        LogicalExpression expr(Kind::kLogicalOr, first, second);
        auto ors = OptimizerUtils::pullOrs(&expr);
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
        auto ors = OptimizerUtils::pullOrs(&expr);
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
        auto ors = OptimizerUtils::pullOrs(&expr);
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
        auto ors = OptimizerUtils::pullOrs(&expr);
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
        auto ors = OptimizerUtils::pullOrs(&expr);
        ASSERT_EQ(3UL, ors.size());
        ASSERT_EQ(first, ors[0]);
        ASSERT_EQ(second, ors[1]);
        ASSERT_EQ(third, ors[2]);
    }
}

}   // namespace graph
}   // namespace nebula

