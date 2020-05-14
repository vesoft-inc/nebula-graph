/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/IntersectExecutor.h"

#include "planner/PlanNode.h"

namespace nebula {
namespace graph {

<<<<<<< HEAD
=======
IntersectExecutor::IntersectExecutor(const PlanNode *node,
                                     QueryContext *qctx,
                                     Executor *left,
                                     Executor *right)
    : MultiInputsExecutor("IntersectExecutor", node, qctx, {left, right}) {
    DCHECK_NOTNULL(left);
    DCHECK_NOTNULL(right);
}

>>>>>>> Replace ExecutionContext with QueryContext.
folly::Future<Status> IntersectExecutor::execute() {
    dumpLog();
    // TODO(yee):
    return start();
}

}   // namespace graph
}   // namespace nebula
