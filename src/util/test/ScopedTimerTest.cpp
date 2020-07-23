/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "util/ScopedTimer.h"

#include <gtest/gtest.h>

namespace nebula {
namespace graph {

TEST(ScopedTimerTest, TestScopedTimer) {
    int64_t var = 0;
    {
        SCOPED_TIMER(&var);
        ::usleep(100);
    }
    EXPECT_GE(var, 99);

    {
        ScopedTimer st([&](uint64_t et) { var = et; });
        ::usleep(10);
    }
    EXPECT_GE(var, 9);

    int64_t var2 = 0;
    {
      SCOPED_TIMER(&var);
      SCOPED_TIMER(&var2);
      ::usleep(1);
    }
    EXPECT_GE(var, var2);
}

}   // namespace graph
}   // namespace nebula
