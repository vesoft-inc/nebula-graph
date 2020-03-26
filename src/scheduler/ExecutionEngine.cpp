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

ExecutionEngine::ExecutionEngine(std::shared_ptr<PlanNode> planRoot)
    : planRoot_(planRoot),
      objPool_(std::make_shared<ObjectPool>()),
      ectx_(std::make_shared<ExecutionContext>()) {
    DCHECK(planRoot_);
}

Executor *ExecutionEngine::makeExecutor() const {
    return Executor::makeExecutor(planRoot_.get(), ectx_.get(), objPool_.get());
}

}   // namespace graph
}   // namespace nebula
