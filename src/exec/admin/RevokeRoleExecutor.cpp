/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/RevokeRoleExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> RevokeRoleExecutor::execute() {
    return revokeRole().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> RevokeRoleExecutor::revokeRole() {
    dumpLog();

    auto *rrNode = asNode<RevokeRole>(node());
    return ectx()->getMetaClient()->revokeFromUser(rrNode->item())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            if (resp.value()) {
                return Status::OK();
            } else {
                return Status::Error("Revoke role failed");
            }
        });
}

}   // namespace graph
}   // namespace nebula
