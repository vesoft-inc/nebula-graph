/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/DropUserExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> DropUserExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return dropUser();
}

folly::Future<GraphStatus> DropUserExecutor::dropUser() {
    auto *duNode = asNode<DropUser>(node());
    return qctx()->getMetaClient()->dropUser(*duNode->username(), duNode->ifExist())
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
