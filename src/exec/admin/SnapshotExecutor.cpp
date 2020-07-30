/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/SnapshotExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateSnapshotExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()->getMetaClient()->createSnapshot()
            .via(runner())
            .then([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropSnapshotExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DropSnapshot>(node());
    return qctx()->getMetaClient()->dropSnapshot(dsNode->getShapshotName())
            .via(runner())
            .then([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ShowSnapshotsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()->getMetaClient()->listSnapshots()
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::Snapshot>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }

                auto snapshots = std::move(resp).value();
                DataSet dataSet({"Name", "Status", "Hosts"});

                for (auto &snapshot : snapshots) {
                    Row row;
                    row.values.emplace_back(snapshot.name);
                    row.values.emplace_back(
                            meta::cpp2::_SnapshotStatus_VALUES_TO_NAMES.at(snapshot.status));
                    row.values.emplace_back(snapshot.hosts);
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
