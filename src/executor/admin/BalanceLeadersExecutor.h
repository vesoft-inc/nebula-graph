/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_
#define EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_

#include "executor/Executor.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

class BalanceLeadersExecutor final : public Executor {
public:
    BalanceLeadersExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("BaanceLeadersExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;

private:
    folly::Future<GraphStatus> balanceLeaders();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_BALANCELEADERSEXECUTOR_H_
