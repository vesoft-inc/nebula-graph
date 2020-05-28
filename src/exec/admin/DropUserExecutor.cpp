/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/DropUserExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DropUserExecutor::execute() {
    return dropUser().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> DropUserExecutor::dropUser() {
    dumpLog();

    auto *duNode = asNode<DropUser>(node());
    return ectx()->getMetaClient()->dropUser(duNode->username(), duNode->ifExist())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            HANDLE_EXEC_RESPONSE(resp);
        });
}

}   // namespace graph
}   // namespace nebula
