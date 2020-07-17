/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/logic/LoopExecutor.h"

#include <folly/String.h>

#include "common/interface/gen-cpp2/common_types.h"
#include "context/QueryExpressionContext.h"
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
    QueryExpressionContext ctx(ectx_, nullptr);
    auto value = expr->eval(ctx);
    DCHECK(value.isBool());
    return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).finish());
}

}   // namespace graph
}   // namespace nebula
