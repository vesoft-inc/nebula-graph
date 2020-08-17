/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/StopBalanceExecutor.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> StopBalanceExecutor::execute() {
    return stopBalance();
}

folly::Future<Status> StopBalanceExecutor::stopBalance() {
    return qctx()->getMetaClient()->balance({}, true)
        .via(runner())
        .then([this](StatusOr<int64_t> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            DataSet v({"ID"});
            v.emplace_back(Row({resp.value()}));
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
