/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/CreateUserExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateUserExecutor::execute() {
    return createUser().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> CreateUserExecutor::createUser() {
    dumpLog();

    auto *cuNode = asNode<CreateUser>(node());
    return ectx()->getMetaClient()->createUser(
            cuNode->username(), cuNode->password(), cuNode->ifNotExist())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            HANDLE_EXEC_RESPONSE(resp);
        });
}

}   // namespace graph
}   // namespace nebula
