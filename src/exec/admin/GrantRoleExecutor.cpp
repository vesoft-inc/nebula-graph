/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/GrantRoleExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> GrantRoleExecutor::execute() {
    return grantRole().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> GrantRoleExecutor::grantRole() {
    auto *grNode = asNode<GrantRole>(node());
    const auto *spaceName = grNode->spaceName();
    auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
    if (!spaceIdResult.ok()) {
        return std::move(spaceIdResult).status();
    }
    auto spaceId = spaceIdResult.value();
    meta::cpp2::RoleItem item;
    item.set_space_id(spaceId);  // TODO(shylock) pass space name directly
    item.set_user_id(*grNode->username());
    item.set_role_type(grNode->role());
    return qctx()->getMetaClient()->grantToUser(std::move(item))
        .via(runner())
        .then([](StatusOr<bool> resp) {
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Grant role failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
