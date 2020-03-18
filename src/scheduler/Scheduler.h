/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_SCHEDULER_H_
#define SCHEDULER_SCHEDULER_H_

#include <cstdint>
#include <functional>
#include <memory>

#include <folly/futures/Future.h>

#include "base/Status.h"

namespace folly {

class CPUThreadPoolExecutor;

}   // namespace folly

namespace nebula {
namespace graph {

class PlanFragment;
class Executor;
class ExecutionContext;

class Scheduler {
public:
    // TODO(yee): Use same folly executors as graph daemon process
    explicit Scheduler(uint32_t numThreads);

    // Make PlanFragment executable conversion and schedule them according to their dependent
    // relationship
    void schedulePlanFragment(PlanFragment* root, ExecutionContext* ectx);

    // Add task to thread pool. If the number of tasks is more than worker threads, they will be
    // queued in the pool
    void addTask(std::function<void()> task);

private:
    // Invoke execute interface of each executor
    static folly::Future<Status> exec(Executor* node, ExecutionContext* ectx);

    std::unique_ptr<folly::CPUThreadPoolExecutor> threadPool_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_SCHEDULER_H_
