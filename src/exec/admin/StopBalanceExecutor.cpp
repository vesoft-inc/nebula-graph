/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/StopBalanceExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> StopBalanceExecutor::execute() {
    return stopBalance().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> StopBalanceExecutor::stopBalance() {
    dumpLog();

    return ectx()->getMetaClient()->balance({}, true)
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
