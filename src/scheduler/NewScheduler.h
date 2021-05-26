/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEWSCHEDULER_SCHEDULER_H_
#define NEWSCHEDULER_SCHEDULER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "executor/Executor.h"
#include "planner/plan/PlanNode.h"
#include "executor/logic/LoopExecutor.h"

namespace nebula {
namespace graph {
class NewScheduler final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    explicit NewScheduler(QueryContext* qctx);

    folly::Future<Status> schedule();

private:
    folly::Future<Status> doSchedule(Executor* root) const;

    /**
     *  futures: current executor will be triggered when all the futures are notified.
     *  exe: current executor
     *  runner: a thread-pool
     *  promises: the promises will be set a value which triggers the other executors
     *            if current executor is done working.
     */
    void scheduleExecutor(std::vector<folly::Future<Status>>&& futures,
                          Executor* exe,
                          folly::Executor* runner,
                          std::vector<folly::Promise<Status>>&& promises) const;

    void runExecutor(std::vector<folly::Future<Status>>&& futures,
                     Executor* exe,
                     folly::Executor* runner,
                     std::vector<folly::Promise<Status>>&& promises) const;

    void runLeafExecutor(Executor* exe,
                         folly::Executor* runner,
                         std::vector<folly::Promise<Status>>&& promises) const;

    void runLoop(std::vector<folly::Future<Status>>&& futures,
                 LoopExecutor* loop,
                 folly::Executor* runner,
                 std::vector<folly::Promise<Status>>&& promises) const;

    Status checkStatus(std::vector<Status>&& status) const;

    void notifyOK(std::vector<folly::Promise<Status>>& promises) const;

    void notifyError(std::vector<folly::Promise<Status>>& promises, Status status) const;

    folly::Future<Status> execute(Executor *executor) const;

    QueryContext *qctx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // NEWSCHEDULER_SCHEDULER_H_
