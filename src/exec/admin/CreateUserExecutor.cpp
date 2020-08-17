/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/CreateUserExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateUserExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return createUser().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> CreateUserExecutor::createUser() {
    auto *cuNode = asNode<CreateUser>(node());
    return qctx()->getMetaClient()->createUser(
            *cuNode->username(), *cuNode->password(), cuNode->ifNotExist())
        .via(runner())
        .then([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.value()) {
                return Status::Error("Create User failed!");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
