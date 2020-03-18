/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>

#include "exec/ExecutionContext.h"
#include "exec/Executor.h"
#include "scheduler/PlanFragment.h"

namespace nebula {
namespace graph {

Scheduler::Scheduler(uint32_t numThreads) {
    auto taskQueue = std::make_unique<
            folly::UnboundedBlockingQueue<folly::CPUThreadPoolExecutor::CPUTask>>();
    threadPool_ = std::make_unique<folly::CPUThreadPoolExecutor>(
            numThreads,
            std::move(taskQueue),
            std::make_shared<folly::NamedThreadFactory>("graph-scheduler"));
}

void Scheduler::schedulePlanFragment(PlanFragment* root, ExecutionContext* ectx) {
    // Handle plan fragment dependencies firstly
    for (auto& dep : root->depends()) {
        schedulePlanFragment(dep, ectx);
    }

    auto exec = root->convertPlan(ectx);
    addTask([=]() {
        auto status = exec->execute();
        if (!status.ok()) {
            LOG(ERROR) << status.toString();
        }
    });
}

void Scheduler::addTask(std::function<void()> task) {
    threadPool_->add(task);
}

}   // namespace graph
}   // namespace nebula
