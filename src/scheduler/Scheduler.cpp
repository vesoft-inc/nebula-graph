/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "exec/Executor.h"
#include "planner/PlanNode.h"
#include "scheduler/ExecutionEngine.h"

namespace nebula {
namespace graph {

void Scheduler::schedule(std::shared_ptr<PlanNode> planRoot) {
    auto ee = std::make_unique<ExecutionEngine>(planRoot);
    auto *executor = ee->makeExecutor();
    auto future = executor->execute();
    future.then([=](Status s) {
        if (!s.ok()) {
            // Response error
        } else {
            // Response successfully
        }
    });
}

}   // namespace graph
}   // namespace nebula
