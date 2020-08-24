/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/ChangePasswordExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> ChangePasswordExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return changePassword();
}

folly::Future<GraphStatus> ChangePasswordExecutor::changePassword() {
    auto *cpNode = asNode<ChangePassword>(node());
    return qctx()->getMetaClient()->changePassword(
            *cpNode->username(), *cpNode->newPassword(), *cpNode->password())
        .via(runner())
        .then([this](auto&& resp) {
            SCOPED_TIMER(&execTime_);
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            return GraphStatus::OK();
        });
}

}   // namespace graph
}   // namespace nebula
