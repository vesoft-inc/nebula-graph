/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/RevokeRoleExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> RevokeRoleExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return revokeRole();
}

folly::Future<GraphStatus> RevokeRoleExecutor::revokeRole() {
    auto *rrNode = asNode<RevokeRole>(node());
    const auto *spaceName = rrNode->spaceName();
    auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
    if (!spaceIdResult.ok()) {
        return GraphStatus::setSpaceNotFound(*spaceName);
    }
    auto spaceId = spaceIdResult.value();
    meta::cpp2::RoleItem item;
    item.set_space_id(spaceId);
    item.set_user_id(*rrNode->username());
    item.set_role_type(rrNode->role());
    return qctx()->getMetaClient()->revokeFromUser(std::move(item))
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
