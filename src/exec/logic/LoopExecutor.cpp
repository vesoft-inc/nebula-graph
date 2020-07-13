/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/logic/LoopExecutor.h"

#include <folly/String.h>

#include "common/interface/gen-cpp2/common_types.h"
#include "context/ExpressionContextImpl.h"
#include "planner/Query.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

LoopExecutor::LoopExecutor(const PlanNode *node, QueryContext *qctx, Executor *body)
    : Executor("LoopExecutor", node, qctx), body_(DCHECK_NOTNULL(body)) {}

folly::Future<Status> LoopExecutor::execute() {
    dumpLog();
    auto *loopNode = asNode<Loop>(node());
    Expression *expr = loopNode->condition();
    ExpressionContextImpl ctx(ectx_, nullptr);
    auto value = expr->eval(ctx);
    DCHECK(value.isBool());
    finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).finish());
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
