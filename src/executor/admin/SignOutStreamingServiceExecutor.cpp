/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SignOutStreamingServiceExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignOutStreamingServiceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return signOutStreamingService();
}

folly::Future<Status> SignOutStreamingServiceExecutor::signOutStreamingService() {
    return qctx()->getMetaClient()->signOutService()
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Sign out streaming failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula

