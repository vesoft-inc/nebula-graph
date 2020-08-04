/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "optimizer/OptimizerUtils.h"
namespace nebula {
namespace graph {

class OptimizerUtilsTest : public ::testing::Test {};

TEST_F(OptimizerUtilsTest, SimpleTest) {
//    double num = std::numeric_limits<double>::max();
    double num = 3.1415926;
    OptimizerUtils::DoubleFormat df(num);
    auto expect = folly::stringPrintf("%E", num);
    auto actual = folly::stringPrintf("%E", df.toDouble());
    ASSERT_EQ(expect, actual);
}
}   // namespace graph
}   // namespace nebula
