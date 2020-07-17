/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/RevokeRoleExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> RevokeRoleExecutor::execute() {
    return revokeRole().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> RevokeRoleExecutor::revokeRole() {
    dumpLog();

    auto *rrNode = asNode<RevokeRole>(node());
    const auto *spaceName = rrNode->spaceName();
    auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
    if (!spaceIdResult.ok()) {
        return std::move(spaceIdResult).status();
    }
    auto spaceId = spaceIdResult.value();
    meta::cpp2::RoleItem item;
    item.set_space_id(spaceId);
    item.set_user_id(*rrNode->username());
    item.set_role_type(rrNode->role());
    return qctx()->getMetaClient()->revokeFromUser(std::move(item))
        .via(runner())
        .then([](StatusOr<bool> resp) {
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Revoke role failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
