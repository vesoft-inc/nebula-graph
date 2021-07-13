/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Admin.h"
#include "context/QueryContext.h"
#include "executor/admin/ShowDataBalanceExecutor.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowDataBalanceExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return showDataBalance();
}

folly::Future<Status> ShowDataBalanceExecutor::showDataBalance() {
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->getBalancePlan(spaceId)
        .via(runner())
        .thenValue([this](StatusOr<meta::cpp2::BalancePlanItem> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(resp);
            if (!resp.ok()) {
                LOG(ERROR) << "Get BalancePlan failed!";
                return resp.status();
            }

            auto planItem = std::move(resp).value();
            DataSet dataSet({"JobID", "TaskID", "GraphSpaceID", "PartitionID", "Source", "Destination"});

            for (const auto &task : planItem.get_tasks()) {
                Row row;
                row.values.emplace_back(task.get_job_id());
                row.values.emplace_back(task.get_task_id());
                row.values.emplace_back(task.get_space_id());
                row.values.emplace_back(task.get_part_id());
                row.values.emplace_back(task.get_src().toString());
                row.values.emplace_back(task.get_dst().toString());
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

}   // namespace graph
}   // namespace nebula
