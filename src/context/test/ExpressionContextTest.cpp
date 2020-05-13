/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/QueryContext.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
TEST(ExpressionContextTest, GetVar) {
    QueryContext qCtxt;
    qCtxt.setValue("v1", 10);
    qCtxt.setValue("v2", "Hello world");

    graph::ExpressionContextImpl eCtxt(&qCtxt);
    EXPECT_EQ(Value(10), eCtxt.getVar("v1"));
    EXPECT_EQ(Value("Hello world"), eCtxt.getVar("v2"));

    qCtxt.setValue("v1", "Hello world");
    qCtxt.setValue("v1", 3.14);
    qCtxt.setValue("v1", true);
    EXPECT_EQ(Value(true), eCtxt.getVersionedVar("v1", 0));
    EXPECT_EQ(Value(3.14), eCtxt.getVersionedVar("v1", -1));
    EXPECT_EQ(Value(10), eCtxt.getVersionedVar("v1", 1));
}
}  // namespace nebula
