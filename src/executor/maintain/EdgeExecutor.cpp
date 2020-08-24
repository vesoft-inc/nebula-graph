/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/EdgeExecutor.h"
#include "context/QueryContext.h"
#include "planner/Maintain.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<GraphStatus> CreateEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ceNode = asNode<CreateEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->createEdgeSchema(spaceId,
            ceNode->getName(), ceNode->getSchema(), ceNode->getIfNotExists())
            .via(runner())
            .then([this, ceNode](auto resp) {
                return checkMetaResp(resp, ceNode->getName());
            });
}


folly::Future<GraphStatus> DescEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DescEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->getEdgeSchema(spaceId, deNode->getName())
            .via(runner())
            .then([this, deNode, spaceId](auto&& resp) {
                auto gStatus = checkMetaResp(resp, deNode->getName());
                if (!gStatus.ok()) {
                    return gStatus;
                }
                auto ds = SchemaUtil::toDescSchema(resp.value().get_schema());
                return finish(ResultBuilder()
                                  .value(Value(std::move(ds)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}


folly::Future<GraphStatus> DropEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DropEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->dropEdgeSchema(spaceId,
                                                   deNode->getName(),
                                                   deNode->getIfExists())
            .via(runner())
            .then([this, deNode](auto&& resp) {
                return checkMetaResp(resp, deNode->getName());
            });
}

folly::Future<GraphStatus> ShowEdgesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()->getMetaClient()->listEdgeSchemas(spaceId).via(runner()).then(
        [this, spaceId](auto &&resp) {
            auto gStatus = checkMetaResp(resp);
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto edgeItems = resp.value().get_edges();

            DataSet dataSet;
            dataSet.colNames = {"Name"};
            std::set<std::string> orderEdgeNames;
            for (auto &edge : edgeItems) {
                orderEdgeNames.emplace(edge.get_edge_name());
            }
            for (auto &name : orderEdgeNames) {
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

folly::Future<GraphStatus> ShowCreateEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sceNode = asNode<ShowCreateEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space();
    return qctx()
        ->getMetaClient()
        ->getEdgeSchema(spaceId, sceNode->getName())
        .via(runner())
        .then([this, sceNode, spaceId](auto &&resp) {
            auto gStatus = checkMetaResp(resp, sceNode->getName());
            if (!gStatus.ok()) {
                return gStatus;
            }
            auto ds = SchemaUtil::toShowCreateSchema(false,
                                                     sceNode->getName(),
                                                     resp.value().get_schema());
            return finish(ResultBuilder()
                              .value(std::move(ds))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<GraphStatus> AlterEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *aeNode = asNode<AlterEdge>(node());
    return qctx()->getMetaClient()->alterEdgeSchema(aeNode->space(),
                                                    aeNode->getName(),
                                                    aeNode->getSchemaItems(),
                                                    aeNode->getSchemaProp())
            .via(runner())
            .then([this, aeNode](auto &&resp) {
                auto gStatus = checkMetaResp(resp, aeNode->getName());
                if (!gStatus.ok()) {
                    return gStatus;
                }
                return finish(
                    ResultBuilder().value(Value()).iter(Iterator::Kind::kDefault).finish());
            });
}
}   // namespace graph
}   // namespace nebula
