/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_SCHEDULER_H_
#define SCHEDULER_SCHEDULER_H_

#include <memory>
#include <set>
#include <unordered_map>

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"

namespace nebula {
namespace graph {

class Executor;
class QueryContext;
class LoopExecutor;

class Scheduler final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // check whether a task is a Scheduler::Task by std::is_base_of<>::value in thread pool
    struct Task {
        int64_t planId;
        explicit Task(const Executor *e);
    };

    explicit Scheduler(QueryContext *qctx);
    ~Scheduler();

    folly::Future<Status> schedule();

private:
    // Enable thread pool check the query plan id of each callback registered in future. The functor
    // is only the proxy of the invocable function `fn'.
    template <typename F>
    struct ExecTask;

    template <typename Fn>
    ExecTask<Fn> task(Executor *e, Fn &&f) const;

    void analyze(Executor *executor);
    folly::Future<Status> doSchedule(Executor *executor);
    folly::Future<Status> doScheduleParallel(const std::set<Executor *> &dependents);
    folly::Future<Status> iterate(LoopExecutor *loop);
    folly::Future<Status> execute(Executor *executor);

    struct PassThroughData;

    QueryContext *qctx_{nullptr};
    std::unordered_map<int64_t, std::unique_ptr<PassThroughData>> passThroughPromiseMap_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULER_SCHEDULER_H_
