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

void Scheduler::schedule(const PlanNode* planRoot) {
    auto ee = new ExecutionEngine(planRoot);
    ee->makeExecutor()
        ->execute()
        .then([=](Status s) {
            if (!s.ok()) {
                // Response error
            } else {
                // Response successfully
            }
        })
        // .onError([]() {
        //     // Response error
        // })
        .ensure([=]() { delete ee; });
}

}   // namespace graph
}   // namespace nebula
