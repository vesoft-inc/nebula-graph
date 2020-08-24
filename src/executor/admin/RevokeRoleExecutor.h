/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_
#define EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class RevokeRoleExecutor final : public Executor {
public:
    RevokeRoleExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("RevokeRoleExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;

private:
    folly::Future<GraphStatus> revokeRole();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_REVOKEROLEEXECUTOR_H_
