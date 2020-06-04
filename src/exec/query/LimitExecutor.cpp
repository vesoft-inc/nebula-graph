/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LimitExecutor.h"

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
    dumpLog();
    // TODO(yee): Get all neighbors by storage client
    return start();
}

}   // namespace graph
}   // namespace nebula
