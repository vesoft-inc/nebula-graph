/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/BalanceLeaderExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> BalanceLeaderExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return balanceLeaders();
}

folly::Future<Status> BalanceLeaderExecutor::balanceLeaders() {
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->balanceLeader(spaceId)
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            if (!resp.value()) {
                return Status::Error("Balance leaders failed");
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
