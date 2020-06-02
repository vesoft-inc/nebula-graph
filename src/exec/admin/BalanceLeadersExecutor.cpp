/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/BalanceLeadersExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> BalanceLeadersExecutor::execute() {
    return balanceLeaders().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> BalanceLeadersExecutor::balanceLeaders() {
    dumpLog();

    return ectx()->getMetaClient()->balanceLeader()
        .via(runner())
        .then([](StatusOr<bool> resp) {
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
