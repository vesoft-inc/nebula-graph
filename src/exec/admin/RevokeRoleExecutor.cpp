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
    const auto &spaceName = rrNode->spaceName();
    auto spaceIdResult = ectx()->getMetaClient()->getSpaceIdByNameFromCache(spaceName);
    if (!spaceIdResult.ok()) {
        return std::move(spaceIdResult).status();
    }
    auto spaceId = spaceIdResult.value();
    meta::cpp2::RoleItem item;
    item.set_space_id(spaceId);
    item.set_user_id(rrNode->username());
    item.set_role_type(rrNode->role());
    return ectx()->getMetaClient()->revokeFromUser(std::move(item))
        .via(runner())
        .then([](StatusOr<bool> resp) {
            HANDLE_EXEC_RESPONSE(resp);
        });
}

}   // namespace graph
}   // namespace nebula
