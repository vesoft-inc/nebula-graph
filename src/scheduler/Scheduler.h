/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SCHEDULER_SCHEDULER_H_
#define SCHEDULER_SCHEDULER_H_

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"

namespace nebula {
namespace graph {

class PlanNode;
class QueryContext;

class Scheduler : private cpp::NonCopyable, private cpp::NonMovable {
public:
    explicit Scheduler(QueryContext *qctx);
    virtual ~Scheduler() = default;

    virtual folly::Future<Status> schedule() = 0;

protected:
    QueryContext *qctx_{nullptr};
};

}   // namespace graph
}   // namespace nebula
#endif   // SCHEDULER_SCHEDULER_H_
