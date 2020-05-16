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
    explicit Scheduler(ExecutionContext *ectx);
    ~Scheduler() = default;

    folly::Future<Status> schedule(Executor *executor);

private:
    // Throw execution exception for early failure
    folly::Future<Status> error(Status status) const;

    folly::Future<Status> iterate(LoopExecutor *loop);

    ExecutionContext *ectx_;
};

}   // namespace graph
}   // namespace nebula

#endif   // SCHEDULE_SCHEDULER_H_
