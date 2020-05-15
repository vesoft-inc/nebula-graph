/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "exec/maintain/ShowSchemaExecutor.h"
#include "planner/Maintain.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowTagsExecutor::execute() {
    dumpLog();

    auto *stNode = asNode<ShowTags>(node());
    return ectx()->getMetaClient()->listTagSchemas(stNode->getSpaceId())
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::TagItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto tagItems = std::move(resp).value();

                DataSet dataSet;
                dataSet.colNames = {"Name"};
                std::vector<Row> rows;
                std::set<std::string> orderTagNames;
                for (auto &tag : tagItems) {
                    orderTagNames.emplace(tag.get_tag_name());
                }
                for (auto &name : orderTagNames) {
                    std::vector<Value> columns;
                    columns.emplace_back(name);
                    Row row;
                    row.columns = std::move(columns);
                    rows.emplace_back(row);
                }
                dataSet.rows = std::move(rows);
                finish(dataSet);
                return Status::OK();
            });
}

folly::Future<Status> ShowEdgesExecutor::execute() {
    dumpLog();

    auto *seNode = asNode<ShowEdges>(node());
    return ectx()->getMetaClient()->listEdgeSchemas(seNode->getSpaceId())
            .via(runner())
            .then([this](StatusOr<std::vector<meta::cpp2::EdgeItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto edgeItems = std::move(resp).value();

                DataSet dataSet;
                dataSet.colNames = {"Name"};
                std::vector<Row> rows;
                std::set<std::string> orderEdgeNames;
                for (auto &edge : edgeItems) {
                    orderEdgeNames.emplace(edge.get_edge_name());
                }
                for (auto &name : orderEdgeNames) {
                    std::vector<Value> columns;
                    columns.emplace_back(name);
                    Row row;
                    row.columns = std::move(columns);
                    rows.emplace_back(row);
                }
                dataSet.rows = std::move(rows);
                finish(dataSet);
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
