/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_
#define SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_

#include "scheduler/Scheduler.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/SelectExecutor.h"

namespace nebula {
namespace graph {
/**
 * This is an scheluder implementation based on asynchronous message notification
 * and bread first search.
 * Each node in execution plan would be triggered to run when the node itself receives all
 * the messages that send by its dependencies. And once the node is done running, it will
 * send a message to the nodes who is depend on it.
 * A bread first search would be applied to traverse the whole execution plan, and build the
 * message notifiers according to the previously described mechanism.
 */
class AsyncMsgNotifyBasedScheduler final : public Scheduler {
public:
    explicit AsyncMsgNotifyBasedScheduler(QueryContext* qctx);

    folly::Future<Status> schedule() override;

private:
    using Notifier = folly::Promise<Status>;
    using Receiver = folly::Future<Status>;
    folly::Future<Status> doSchedule(Executor* root) const;

    /**
     *  receivers: current executor will be triggered when all the futures are notified.
     *  exe: current executor
     *  runner: a thread-pool
     *  notifiers: the promises will be set a value which triggers the other executors
     *            if current executor is done working.
     */
    void scheduleExecutor(std::vector<Receiver>&& receivers,
                          Executor* exe,
                          folly::Executor* runner,
                          std::vector<Notifier>&& notifiers) const;

    void runSelect(std::vector<Receiver>&& receivers,
                   SelectExecutor* select,
                   folly::Executor* runner,
                   std::vector<Notifier>&& notifiers) const;

    void runExecutor(std::vector<Receiver>&& receivers,
                     Executor* exe,
                     folly::Executor* runner,
                     std::vector<Notifier>&& notifiers) const;

    void runLeafExecutor(Executor* exe,
                         folly::Executor* runner,
                         std::vector<Notifier>&& notifiers) const;

    void runLoop(std::vector<Receiver>&& receivers,
                 LoopExecutor* loop,
                 folly::Executor* runner,
                 std::vector<Notifier>&& notifiers) const;

    Status checkStatus(std::vector<Status>&& status) const;

    void notifyOK(std::vector<Notifier>& notifiers) const;

    void notifyError(std::vector<Notifier>& notifiers, Status status) const;

    folly::Future<Status> execute(Executor *executor) const;

    QueryContext *qctx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_
