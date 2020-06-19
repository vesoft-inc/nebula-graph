/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/DataCollectExecutor.h"

namespace nebula {
namespace graph {
class DataCollectTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        qctx_ = std::make_unique<QueryContext>();
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> ProjectTest::qctx_;

}  // namespace graph
}  // namespace nebula
