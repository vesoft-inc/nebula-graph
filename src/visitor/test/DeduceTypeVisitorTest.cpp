/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeduceTypeVisitor.h"

#include <gtest/gtest.h>

using Type = nebula::Value::Type;

namespace nebula {
namespace graph {
class DeduceTypeVisitorTest : public ::testing::Test {
protected:
    static ColsDef emptyInput_;
};

ColsDef DeduceTypeVisitorTest::emptyInput_;

TEST_F(DeduceTypeVisitorTest, SubscriptExpr) {
    {
        auto* items = new ExpressionList();
        items->add(new ConstantExpression(1));
        auto expr = SubscriptExpression(new ListExpression(items), new ConstantExpression(1));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(new ConstantExpression(Value::kNullValue),
                                        new ConstantExpression(1));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(new ConstantExpression(Value::kEmpty),
                                        new ConstantExpression(1));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(new ConstantExpression(Value::kNullValue),
                                        new ConstantExpression(Value::kNullValue));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }

    // exceptions
    {
        auto expr = SubscriptExpression(new ConstantExpression("test"),
                                        new ConstantExpression(1));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
    {
        auto* items = new ExpressionList();
        items->add(new ConstantExpression(1));
        auto expr = SubscriptExpression(new ListExpression(items), new ConstantExpression("test"));

        DeduceTypeVisitor visitor(nullptr, nullptr, emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
}
}  // namespace graph
}  // namespace nebula
