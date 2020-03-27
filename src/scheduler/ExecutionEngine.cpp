/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/ExecutionEngine.h"

#include "base/Logging.h"
#include "exec/ExecutionContext.h"
#include "exec/Executor.h"
#include "planner/PlanNode.h"
#include "util/ObjectPool.h"

namespace nebula {
namespace graph {

ExecutionEngine::ExecutionEngine(const PlanNode* planRoot)
    : planRoot_(planRoot), objPool_(new ObjectPool), ectx_(new ExecutionContext) {
    DCHECK(planRoot_);
}

ExecutionEngine::~ExecutionEngine() {
    if (objPool_) delete objPool_;
    if (ectx_) delete ectx_;
}

Executor* ExecutionEngine::makeExecutor() {
    return Executor::makeExecutor(planRoot_, ectx_, objPool_);
}

}   // namespace graph
}   // namespace nebula
