/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/StopBalanceExecutor.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> StopBalanceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return stopBalance();
}

folly::Future<GraphStatus> StopBalanceExecutor::stopBalance() {
    return qctx()->getMetaClient()->balance({}, true)
        .via(runner())
        .then([this](auto&& resp) {
            SCOPED_TIMER(&execTime_);
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            DataSet v({"ID"});
            v.emplace_back(Row({resp.value().get_id()}));
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
