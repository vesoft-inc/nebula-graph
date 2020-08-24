/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/ShowBalanceExecutor.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> ShowBalanceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return showBalance();
}

folly::Future<GraphStatus> ShowBalanceExecutor::showBalance() {
    auto *sbNode = asNode<ShowBalance>(node());
    return qctx()->getMetaClient()->showBalance(sbNode->jobId())
        .via(runner())
        .then([this](StatusOr<meta::cpp2::BalanceResp> resp) {
            SCOPED_TIMER(&execTime_);
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto tasks = resp.value().get_tasks();
            // TODO(shylock) typed items instead binary
            // E.G. "balanceId", "spaceId", "partId", "from", "to"
            uint32_t total = tasks.size(), succeeded = 0, failed = 0, inProgress = 0, invalid = 0;
            DataSet v({"balanceId, spaceId:partId, src->dst", "status"});
            for (auto &task : tasks) {
                switch (task.get_result()) {
                    case meta::cpp2::TaskResult::FAILED:
                        ++failed;
                        break;
                    case meta::cpp2::TaskResult::IN_PROGRESS:
                        ++inProgress;
                        break;
                    case meta::cpp2::TaskResult::INVALID:
                        ++invalid;
                        break;
                    case meta::cpp2::TaskResult::SUCCEEDED:
                        ++succeeded;
                        break;
                }
                v.emplace_back(Row({
                    std::move(task).get_id(),
                    meta::cpp2::_TaskResult_VALUES_TO_NAMES.at(task.get_result())
                }));
            }
            double percentage = total == 0 ? 0 : static_cast<double>(succeeded) / total * 100;
            v.emplace_back(Row({
                folly::sformat("Total:{}, Succeeded:{}, Failed:{}, In Progress:{}, Invalid:{}",
                    total, succeeded, failed, inProgress, invalid),
                percentage
            }));
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
