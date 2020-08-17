/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ChangePasswordExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ChangePasswordExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return changePassword().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> ChangePasswordExecutor::changePassword() {
    auto *cpNode = asNode<ChangePassword>(node());
    return qctx()->getMetaClient()->changePassword(
            *cpNode->username(), *cpNode->newPassword(), *cpNode->password())
        .via(runner())
        .then([this](StatusOr<bool> &&resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Change password failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
