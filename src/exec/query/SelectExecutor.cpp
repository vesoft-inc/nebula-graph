/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/SelectExecutor.h"

#include "planner/Query.h"

namespace nebula {
namespace graph {

SelectExecutor::SelectExecutor(const PlanNode* node,
                               ExecutionContext* ectx,
                               Executor* input,
                               Executor* then,
                               Executor* els)
    : SingleInputExecutor("SelectExecutor", node, ectx, input), then_(then), else_(els) {
    DCHECK_NOTNULL(then_);
    DCHECK_NOTNULL(else_);
}

Status SelectExecutor::prepare() {
    auto status = then_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = else_->prepare();
    if (!status.ok()) {
        return status;
    }
    return input_->prepare();
}

folly::Future<Status> SelectExecutor::execute() {
    dumpLog();

    auto* select = asNode<Selector>(node());
    auto* expr = select->condition();
    UNUSED(expr);

    // FIXME: store expression value to execution context
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
