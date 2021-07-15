/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ADMIN_SIGNOUTSTREAMINGSERVICEEXECUTOR_H_
#define EXECUTOR_ADMIN_SIGNOUTSTREAMINGSERVICEEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {

class SignOutStreamingServiceExecutor final : public Executor {
public:
    SignOutStreamingServiceExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("SignOutStreamingServiceExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> signOutStreamingService();
};

}   // namespace graph
}   // namespace nebula

#endif  // EXECUTOR_ADMIN_SIGNOUTSTREAMINGSERVICEEXECUTOR_H_
