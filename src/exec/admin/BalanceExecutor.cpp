/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/BalanceExecutor.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> BalanceExecutor::execute() {
    return balance().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> BalanceExecutor::balance() {
    auto *bNode = asNode<Balance>(node());
    return qctx()->getMetaClient()->balance(bNode->deleteHosts(), false)
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
