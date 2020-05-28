/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ChangePasswordExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ChangePasswordExecutor::execute() {
    return changePassword().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> ChangePasswordExecutor::changePassword() {
    dumpLog();

    auto *cpNode = asNode<ChangePassword>(node());
    return ectx()->getMetaClient()->changePassword(
            cpNode->username(), cpNode->newPassword(), cpNode->password())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            HANDLE_EXEC_RESPONSE(resp);
        });
}

}   // namespace graph
}   // namespace nebula
