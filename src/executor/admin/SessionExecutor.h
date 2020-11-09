/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
#define EXECUTOR_ADMIN_SESSIONEXECUTOR_H_

#include "executor/Executor.h"
#include "service/RequestContext.h"

namespace nebula {
namespace graph {

class ShowSessionsExecutor final : public Executor {
public:
    ShowSessionsExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("ShowSessionsExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class GetSessionExecutor final : public Executor {
public:
    GetSessionExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("GetSessionExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

class UpdateSessionExecutor final : public Executor {
public:
    UpdateSessionExecutor(const PlanNode *node, QueryContext *ectx)
        : Executor("UpdateSessionExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
