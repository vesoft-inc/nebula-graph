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
        items->add(ConstantExpression::make(pool1));
        auto expr = SubscriptExpression(new ListExpression(items), ConstantExpression::make(pool1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(ConstantExpression::make(poolValue::kNullValue),
                                        ConstantExpression::make(pool1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok()) << std::move(visitor).status();
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(ConstantExpression::make(poolValue::kEmpty),
                                        ConstantExpression::make(pool1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(ConstantExpression::make(poolValue::kNullValue),
                                        ConstantExpression::make(poolValue::kNullValue));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(
            ConstantExpression::make(poolValue(Map({{"k", "v"}, {"k1", "v1"}}))),
            ConstantExpression::make(pool, "test"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(ConstantExpression::make(poolValue::kEmpty),
                                        ConstantExpression::make(pool, "test"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = SubscriptExpression(ConstantExpression::make(poolValue::kNullValue),
                                        ConstantExpression::make(pool, "test"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok()) << std::move(visitor).status();
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }

    // exceptions
    {
        auto expr = SubscriptExpression(ConstantExpression::make(pool, "test"),
                                        ConstantExpression::make(pool, 1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
    {
        auto* items = new ExpressionList();
        items->add(ConstantExpression::make(pool1));
        auto expr =
            SubscriptExpression(new ListExpression(items), ConstantExpression::make(pool, "test"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
    {
        auto expr = SubscriptExpression(
            ConstantExpression::make(poolValue(Map({{"k", "v"}, {"k1", "v1"}}))),
            ConstantExpression::make(pool, 1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
}

TEST_F(DeduceTypeVisitorTest, Attribute) {
    {
        auto expr = AttributeExpression(
            ConstantExpression::make(poolValue(Map({{"k", "v"}, {"k1", "v1"}}))),
            ConstantExpression::make(pool, "a"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = AttributeExpression(ConstantExpression::make(poolValue(Vertex("vid", {}))),
                                        ConstantExpression::make(pool, "a"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = AttributeExpression(
            ConstantExpression::make(poolValue(Edge("v1", "v2", 1, "edge", 0, {}))),
            ConstantExpression::make(pool, "a"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = AttributeExpression(ConstantExpression::make(poolValue::kNullValue),
                                        ConstantExpression::make(pool, "a"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }
    {
        auto expr = AttributeExpression(ConstantExpression::make(poolValue::kNullValue),
                                        ConstantExpression::make(poolValue::kNullValue));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_TRUE(visitor.ok());
        EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
    }

    // exceptions
    {
        auto expr = AttributeExpression(ConstantExpression::make(pool, "test"),
                                        ConstantExpression::make(pool, "a"));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
    {
        auto expr = AttributeExpression(
            ConstantExpression::make(poolValue(Map({{"k", "v"}, {"k1", "v1"}}))),
            ConstantExpression::make(pool1));

        DeduceTypeVisitor visitor(emptyInput_, -1);
        expr.accept(&visitor);
        EXPECT_FALSE(visitor.ok());
    }
}
}   // namespace graph
}   // namespace nebula
