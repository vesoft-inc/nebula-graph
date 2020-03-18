/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/SelectExecutor.h"

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

folly::Future<Status> SelectExecutor::exec() {
    auto* select = static_cast<const Selector*>(node());
    auto* expr = select->condition();
    UNUSED(expr);
    auto result = true;
    if (result) {
        return then_->execute();
    } else {
        return else_->execute();
    }
}

}   // namespace graph
}   // namespace nebula
