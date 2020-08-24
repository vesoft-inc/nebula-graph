/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_
#define EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_

#include "executor/Executor.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

class StopBalanceExecutor final : public Executor {
public:
    StopBalanceExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("StopBalanceExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;

private:
    folly::Future<GraphStatus> stopBalance();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_STOPBALANCEEXECUTOR_H_
