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

    folly::Future<Status> doSchedule(Executor* root) const;

    folly::Future<Status> scheduleExecutor(std::vector<folly::Future<Status>>&& futures,
                                           Executor* exe,
                                           folly::Executor* runner,
                                           std::vector<folly::Promise<Status>>&& promises) const;

    folly::Future<Status> runExecutor(std::vector<folly::Future<Status>>&& futures,
                                      Executor* exe,
                                      folly::Executor* runner,
                                      std::vector<folly::Promise<Status>>&& promises) const;

    folly::Future<Status> runLeafExecutor(Executor* exe,
                                          folly::Executor* runner,
                                          std::vector<folly::Promise<Status>>&& promises) const;

    folly::Future<Status> runLoop(std::vector<folly::Future<Status>>&& futures,
                                  LoopExecutor* loop,
                                  folly::Executor* runner,
                                  std::vector<folly::Promise<Status>>&& promises) const;

    Status checkStatus(std::vector<Status>&& status) const;

    void notifyOK(std::vector<folly::Promise<Status>>& promises) const;

    void notifyError(std::vector<folly::Promise<Status>>& promises, Status status) const;

    folly::Future<Status> execute(Executor *executor) const;

private:
    QueryContext *qctx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // NEWSCHEDULER_SCHEDULER_H_
