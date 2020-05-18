/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/logic/LoopExecutor.h"

#include <folly/String.h>

#include "gen-cpp2/common_types.h"
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

LoopExecutor::LoopExecutor(const PlanNode *node, ExecutionContext *ectx, Executor *body)
    : Executor("LoopExecutor", node, ectx), body_(DCHECK_NOTNULL(body)) {}

folly::Future<Status> LoopExecutor::execute() {
    dumpLog();
    auto *loopNode = asNode<Loop>(node());
    const Expression *expr = loopNode->condition();
    // TODO(yee): eval expression result
    UNUSED(expr);

    if (iterCount_ >= 1) {
        // FIXME: Just for test
        return Status::Error("Just for test");
    }

    // Update iterate variable value in execution context before loop body running
    nebula::Value value;
    value.setBool(++iterCount_ >= 1);
    ectx()->addValue(loopNode->varName(), std::move(value));
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
