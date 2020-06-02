/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/ShowBalanceExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowBalanceExecutor::execute() {
    return showBalance().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> ShowBalanceExecutor::showBalance() {
    dumpLog();

    auto *sbNode = asNode<ShowBalance>(node());
    return ectx()->getMetaClient()->showBalance(sbNode->id())
        .via(runner())
        .then([this](StatusOr<std::vector<meta::cpp2::BalanceTask>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return std::move(resp).status();
            }
            auto tasks = std::move(resp).value();
            // TODO(shylock) typed items instead binary
            // E.G. "balanceId", "spaceId", "partId", "from", "to"
            DataSet v({"balanceId, spaceId:partId, src->dst", "status"});
            for (auto &task : tasks) {
                v.emplace_back(Row({
                    std::move(task).get_id(),
                    meta::cpp2::_TaskResult_VALUES_TO_NAMES.at(task.get_result())
                }));
            }
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
