/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_
#define EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class ListRolesExecutor final : public Executor {
public:
    ListRolesExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ListRolesExecutor", node, ectx) {}

    folly::Future<GraphStatus> execute() override;

private:
    folly::Future<GraphStatus> listRoles();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_LISTROLESEXECUTOR_H_
