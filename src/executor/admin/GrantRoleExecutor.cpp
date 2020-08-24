/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/GrantRoleExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> GrantRoleExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return grantRole();
}

folly::Future<GraphStatus> GrantRoleExecutor::grantRole() {
    SCOPED_TIMER(&execTime_);
    auto *grNode = asNode<GrantRole>(node());
    const auto *spaceName = grNode->spaceName();
    auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(*spaceName);
    if (!spaceIdResult.ok()) {
        return GraphStatus::setSpaceNotFound(*spaceName);
    }
    auto spaceId = spaceIdResult.value();
    meta::cpp2::RoleItem item;
    item.set_space_id(spaceId);  // TODO(shylock) pass space name directly
    item.set_user_id(*grNode->username());
    item.set_role_type(grNode->role());
    return qctx()->getMetaClient()->grantToUser(std::move(item))
        .via(runner())
        .then([this](StatusOr<meta::cpp2::ExecResp> resp) {
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
