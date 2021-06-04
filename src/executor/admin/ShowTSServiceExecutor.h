/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SHOWTSSERVICEEXECUTOR_H_
#define EXECUTOR_ADMIN_SHOWTSSERVICEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class ShowTSServiceExecutor final : public Executor {
public:
    ShowTSServiceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("ShowTSServiceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SHOWTSSERVICEEXECUTOR_H_
