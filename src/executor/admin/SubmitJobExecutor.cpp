/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SubmitJobExecutor.h"

#include "planner/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> SubmitJobExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sjNode = asNode<SubmitJob>(node());
    auto jobOp = sjNode->jobOp();
    meta::cpp2::AdminCmd cmd = meta::cpp2::AdminCmd::COMPACT;
    if (jobOp == meta::cpp2::AdminJobOp::ADD) {
        std::vector<std::string> params;
        folly::split(" ", sjNode->params().front(), params, true);
        if (params.front() == "compact") {
            cmd = meta::cpp2::AdminCmd::COMPACT;
        } else if (params.front() == "flush") {
            cmd = meta::cpp2::AdminCmd::FLUSH;
        } else {
            DLOG(FATAL) << "Unknown job command " << params.front();
            return GraphStatus::setInternalError(
                    folly::stringPrintf("Unknown job command %s",
                                         params.front().c_str()));
        }
    }
    return qctx()->getMetaClient()->submitJob(jobOp, cmd, sjNode->params())
        .via(runner())
        .then([jobOp, this](auto &&resp) {
            SCOPED_TIMER(&execTime_);

            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto result = resp.value().get_result();

            switch (jobOp) {
                case meta::cpp2::AdminJobOp::ADD: {
                    nebula::DataSet v({"New Job Id"});
                    DCHECK(result.__isset.job_id);
                    if (!result.__isset.job_id) {
                        return GraphStatus::setInternalError("Response unexpected.");
                    }
                    v.emplace_back(nebula::Row({*DCHECK_NOTNULL(result.get_job_id())}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::RECOVER: {
                    nebula::DataSet v({"Recovered job num"});
                    DCHECK(result.__isset.recovered_job_num);
                    if (!result.__isset.recovered_job_num) {
                        return GraphStatus::setInternalError("Response unexpected.");
                    }
                    v.emplace_back(
                        nebula::Row({*DCHECK_NOTNULL(result.get_recovered_job_num())}));
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW: {
                    nebula::DataSet v(
                        {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"});
                    DCHECK(result.__isset.job_desc);
                    if (!result.__isset.job_desc) {
                        return GraphStatus::setInternalError("Response unexpected.");
                    }
                    DCHECK(result.__isset.task_desc);
                    if (!result.__isset.task_desc) {
                        return GraphStatus::setInternalError("Response unexpected");
                    }
                    auto &jobDesc = *result.get_job_desc();
                    // job desc
                    v.emplace_back(
                        nebula::Row(
                            {jobDesc.front().get_id(),
                            meta::cpp2::_AdminCmd_VALUES_TO_NAMES.at(jobDesc.front().get_cmd()),
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc.front().get_status()),
                            jobDesc.front().get_start_time(),
                            jobDesc.front().get_stop_time(),
                            }));
                    // tasks desc
                    auto &tasksDesc = *result.get_task_desc();
                    for (const auto & taskDesc : tasksDesc) {
                        v.emplace_back(nebula::Row({
                            taskDesc.get_task_id(),
                            taskDesc.get_host().host,
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(taskDesc.get_status()),
                            taskDesc.get_start_time(),
                            taskDesc.get_stop_time(),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::SHOW_All: {
                    nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
                    DCHECK(result.__isset.job_desc);
                    if (!result.__isset.job_desc) {
                        return GraphStatus::setInternalError("Response unexpected");
                    }
                    const auto &jobsDesc = *result.get_job_desc();
                    for (const auto &jobDesc : jobsDesc) {
                        v.emplace_back(nebula::Row({
                            jobDesc.get_id(),
                            meta::cpp2::_AdminCmd_VALUES_TO_NAMES.at(jobDesc.get_cmd()),
                            meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc.get_status()),
                            jobDesc.get_start_time(),
                            jobDesc.get_stop_time(),
                        }));
                    }
                    return finish(std::move(v));
                }
                case meta::cpp2::AdminJobOp::STOP: {
                    nebula::DataSet v({"Result"});
                    v.emplace_back(nebula::Row({
                        "Job stopped"
                    }));
                    return finish(std::move(v));
                }
            // no default so the compiler will warning when lack
            }
            DLOG(FATAL) << "Unknown job operation " << static_cast<int>(jobOp);
            return GraphStatus::setInternalError(
                    folly::stringPrintf("Unknown job job operation %d.",
                                         static_cast<int>(jobOp)));
        });
}

}   // namespace graph
}   // namespace nebula
