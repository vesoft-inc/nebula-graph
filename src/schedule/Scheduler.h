/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULE_SCHEDULER_H_
#define SCHEDULE_SCHEDULER_H_

#include <folly/futures/Future.h>

#include "base/Status.h"

namespace nebula {
namespace graph {

class Executor;
class ExecutionContext;
class LoopExecutor;

class Scheduler final {
public:
    // For check whether a task is a Scheduler::Task by std::is_base_of<>::value in thread pool
    struct Task {
        int64_t planId;

        explicit Task(const Executor *e);
    };

    explicit Scheduler(ExecutionContext *ectx);
    ~Scheduler() = default;

    folly::Future<Status> schedule(Executor *executor);

private:
    // Enable thread pool check the query plan id of each callback registered in future. The functor
    // is only the proxy of the invocable function fn.
    template <typename F>
    struct ExecTask : Task {
        using Extract = folly::futures::detail::Extract<F>;
        using Return = typename Extract::Return;
        using FirstArg = typename Extract::FirstArg;

        F fn;

        ExecTask(const Executor *e, F f) : Task(e), fn(std::move(f)) {}

        Return operator()(FirstArg &&arg) {
            return fn(std::forward<FirstArg>(arg));
        }
    };

    template <typename Fn>
    ExecTask<Fn> task(Executor *e, Fn &&f) const {
        return ExecTask<Fn>(e, std::forward<Fn>(f));
    }

    // Throw execution exception for early failure
    folly::Future<Status> error(Status status) const;

    folly::Future<Status> iterate(LoopExecutor *loop);

    ExecutionContext *ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULE_SCHEDULER_H_
