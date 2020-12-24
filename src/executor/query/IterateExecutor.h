/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_ITERATEEXECUTOR_H_
#define EXECUTOR_QUERY_ITERATEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class IterateExecutor final : public Executor {
public:
    IterateExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("IterateExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    size_t idx_{0};
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_ITERATEEXECUTOR_H_
