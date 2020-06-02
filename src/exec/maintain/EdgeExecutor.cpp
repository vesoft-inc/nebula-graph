/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/maintain/EdgeExecutor.h"
#include "planner/Maintain.h"
#include "service/ExecutionContext.h"
#include "util/SchemaUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateEdgeExecutor::execute() {
    dumpLog();

    auto *ceNode = asNode<CreateEdge>(node());
    return ectx()->getMetaClient()->createEdgeSchema(ceNode->space(),
            ceNode->getName(), ceNode->getSchema(), ceNode->getIfNotExists())
            .via(runner())
            .then([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}


folly::Future<Status> DescEdgeExecutor::execute() {
    dumpLog();

    auto *deNode = asNode<DescEdge>(node());
    return ectx()->getMetaClient()->getEdgeSchema(deNode->getSpaceId(), deNode->getName())
            .via(runner())
            .then([this](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toDescSchema(resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                finish(Value(std::move(ret).value()));
                return Status::OK();
            });
}


folly::Future<Status> DropEdgeExecutor::execute() {
    dumpLog();

    auto *deNode = asNode<DropEdge>(node());
    return ectx()->getMetaClient()->dropEdgeSchema(deNode->getSpaceId(),
                                                   deNode->getName(),
                                                   deNode->getIfExists())
            .via(runner())
            .then([this](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
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
                std::set<std::string> orderEdgeNames;
                for (auto &edge : edgeItems) {
                    orderEdgeNames.emplace(edge.get_edge_name());
                }
                for (auto &name : orderEdgeNames) {
                    Row row;
                    row.columns.emplace_back(name);
                    dataSet.rows.emplace_back(std::move(row));
                }
                finish(dataSet);
                return Status::OK();
            });
}

folly::Future<Status> ShowCreateEdgeExecutor::execute() {
    dumpLog();

    auto *sceNode = asNode<ShowCreateEdge>(node());
    return ectx()->getMetaClient()->getEdgeSchema(sceNode->getSpaceId(), sceNode->getName())
            .via(runner())
            .then([this, sceNode](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toShowCreateSchema(false, sceNode->getName(), resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << ret.status();
                    return ret.status();
                }
                finish(Value(std::move(ret).value()));
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
