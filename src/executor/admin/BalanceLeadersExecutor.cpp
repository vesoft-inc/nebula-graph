/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/BalanceLeadersExecutor.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> BalanceLeadersExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return balanceLeaders();
}

folly::Future<GraphStatus> BalanceLeadersExecutor::balanceLeaders() {
    return qctx()->getMetaClient()->balanceLeader()
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
