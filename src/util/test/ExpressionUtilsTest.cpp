/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "util/ExpressionUtils.h"
#include "common/expression/ConstantExpression.h"

namespace nebula {
namespace graph {

TEST(ExpressionUtilsTest, Simple) {
    {
        auto expr = std::make_unique<ConstantExpression>(3);
        EvaluableVisitor visitor;
        traverse(expr.get(), visitor);
        EXPECT_TRUE(visitor.evaluable());
    }
}

}  // namespace graph
}  // namespace nebula
