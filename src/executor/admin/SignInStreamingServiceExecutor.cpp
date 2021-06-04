/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SignInStreamingServiceExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignInStreamingServiceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return signInStreamingService();
}

folly::Future<Status> SignInStreamingServiceExecutor::signInStreamingService() {
    auto *siNode = asNode<SignInTSService>(node());
    return qctx()->getMetaClient()->signInService(siNode->type(), siNode->clients())
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Sign in streaming failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula

