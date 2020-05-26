/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/SnapshotExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateSnapshotExecutor::execute() {
    dumpLog();

    return ectx()->getMetaClient()->createSnapshot()
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropSnapshotExecutor::execute() {
    dumpLog();

    auto *dsNode = asNode<DropSnapshot>(node());
    return ectx()->getMetaClient()->dropSnapshot(dsNode->getShapshotName())
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ShowSnapshotsExecutor::execute() {
    dumpLog();

    return ectx()->getMetaClient()->listSnapshots()
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::Snapshot>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }

                auto snapshots = std::move(resp).value();
                DataSet dataSet;
                std::vector<Row> rows;
                dataSet.colNames = {"Name", "Status", "Hosts"};

                auto getStatus = [](meta::cpp2::SnapshotStatus status) -> std::string {
                    std::string str;
                    switch (status) {
                        case meta::cpp2::SnapshotStatus::INVALID :
                            str = "INVALID";
                            break;
                        case meta::cpp2::SnapshotStatus::VALID :
                            str = "VALID";
                            break;
                    }
                    return str;
                };

                for (auto &snapshot : snapshots) {
                    Row row;
                    row.columns.emplace_back(snapshot.name);
                    row.columns.emplace_back(getStatus(snapshot.status));
                    row.columns.emplace_back(snapshot.hosts);
                    rows.emplace_back(row);
                }
                dataSet.rows = std::move(rows);
                finish(Value(std::move(dataSet)));
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
