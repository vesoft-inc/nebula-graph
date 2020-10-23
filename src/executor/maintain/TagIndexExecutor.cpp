/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/TagIndexExecutor.h"
#include "planner/Maintain.h"
#include "util/IndexUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ctiNode = asNode<CreateTagIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->createTagIndex(spaceId,
                         ctiNode->getIndexName(),
                         ctiNode->getSchemaName(),
                         ctiNode->getFields(),
                         ctiNode->getIfNotExists())
        .via(runner())
        .then([ctiNode, spaceId](StatusOr<IndexID> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Create index `"
                           << ctiNode->getIndexName() << "' at tag: `" << ctiNode->getSchemaName()
                           << "' failed: " << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> DropTagIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtiNode = asNode<DropTagIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->dropTagIndex(spaceId, dtiNode->getIndexName(), dtiNode->getIfExists())
        .via(runner())
        .then([dtiNode, spaceId](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Drop tag index `"
                           << dtiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> DescTagIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtiNode = asNode<DescTagIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getTagIndex(spaceId, dtiNode->getIndexName())
        .via(runner())
        .then([this, dtiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Desc tag index `"
                           << dtiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }

            auto ret = IndexUtil::toDescIndex(resp.value());
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
            }
            return finish(ResultBuilder()
                              .value(std::move(ret).value())
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowCreateTagIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sctiNode = asNode<ShowCreateTagIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getTagIndex(spaceId, sctiNode->getIndexName())
        .via(runner())
        .then([this, sctiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show create tag index `"
                           << sctiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }
            auto ret = IndexUtil::toShowCreateIndex(true, sctiNode->getIndexName(), resp.value());
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
            }
            return finish(ResultBuilder()
                              .value(std::move(ret).value())
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowTagIndexesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listTagIndexes(spaceId).via(runner()).then(
        [this, spaceId](StatusOr<std::vector<meta::cpp2::IndexItem>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show tag indexes failed"
                           << resp.status();
                return resp.status();
            }

            auto tagIndexItems = std::move(resp).value();

            DataSet dataSet;
            dataSet.colNames = {"Names"};
            std::set<std::string> orderTagIndexNames;
            for (auto &tagIndex : tagIndexItems) {
                orderTagIndexNames.emplace(tagIndex.get_index_name());
            }
            for (auto &name : orderTagIndexNames) {
                Row row;
                row.values.emplace_back(name);
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowTagIndexStatusExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()
        ->getMetaClient()
        ->submitJob(meta::cpp2::AdminJobOp::SHOW_All,
                    meta::cpp2::AdminCmd::COMPACT,
                    std::vector<std::string>{})
        .via(runner())
        .then([this](StatusOr<meta::cpp2::AdminJobResult> &&resp) {
            SCOPED_TIMER(&execTime_);

            if (!resp.ok()) {
                LOG(ERROR) << resp.status().toString();
                return std::move(resp).status();
            }

            nebula::DataSet v({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
            DCHECK(resp.value().__isset.job_desc);
            if (!resp.value().__isset.job_desc) {
                return Status::Error("Response unexpected");
            }
            const auto &jobsDesc = *resp.value().get_job_desc();
            for (const auto &jobDesc : jobsDesc) {
                if (jobDesc.get_cmd() == meta::cpp2::AdminCmd::REBUILD_TAG_INDEX) {
                    v.emplace_back(nebula::Row({
                        jobDesc.get_id(),
                        meta::cpp2::_AdminCmd_VALUES_TO_NAMES.at(jobDesc.get_cmd()),
                        meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc.get_status()),
                        jobDesc.get_start_time(),
                        jobDesc.get_stop_time(),
                    }));
                }
            }
            return finish(std::move(v));
        });
}
}   // namespace graph
}   // namespace nebula
