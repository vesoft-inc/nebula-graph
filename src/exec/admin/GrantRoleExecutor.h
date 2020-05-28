/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class GrantRoleExecutor final : public Executor {
public:
    GrantRoleExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("GrantRoleExecutor", node, ectx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> grantRole();
};

}   // namespace graph
}   // namespace nebula
