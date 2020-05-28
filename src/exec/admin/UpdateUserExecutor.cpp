/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/UpdateUserExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> UpdateUserExecutor::execute() {
    return updateUser().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateUserExecutor::updateUser() {
    dumpLog();

    auto *uuNode = asNode<UpdateUser>(node());
    return ectx()->getMetaClient()->alterUser(uuNode->username(), uuNode->password())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            if (resp.value()) {
                return Status::OK();
            } else {
                return Status::Error("Update user failed.");
            }
        });
}

}   // namespace graph
}   // namespace nebula
