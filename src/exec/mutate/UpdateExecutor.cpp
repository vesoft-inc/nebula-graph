/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "UpdateExecutor.h"
#include "planner/Mutate.h"
#include "service/ExecutionContext.h"
#include "clients/storage/GraphStorageClient.h"
#include "util/SchemaUtil.h"


namespace nebula {
namespace graph {

StatusOr<DataSet> UpdateBaseExecutor::handleResult(const DataSet &data) {
    if (yieldPros_.empty() && (data.colNames.empty() || data.colNames.size() == 1)) {
        return Status::OK();
    }

    if (data.colNames.empty() || data.colNames.size() == 1) {
        return Status::Error("Empty return props");
    }

    if (yieldPros_.size() != data.colNames.size() - 1) {
        LOG(ERROR) << "Expect colName size is " << yieldPros_.size()
                   << ", return colName size is " << data.colNames.size() - 1;
        return Status::Error("Wrong return prop size");
    }
    DataSet result;
    std::vector<Row> rows;
    result.colNames = std::move(yieldPros_);
    for (auto &row : result.rows) {
        std::vector<Value> columns;
        for (auto i = 1u; i < row.columns.size(); i++) {
            columns.emplace_back(std::move(row.columns[i]));
        }
        rows.emplace_back(std::move(columns));
    }
    result.rows = std::move(rows);
    return result;
}

folly::Future<Status> UpdateVertexExecutor::execute() {
    return updateVertex().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateVertexExecutor::updateVertex() {
    dumpLog();
    auto *uvNode = asNode<UpdateVertex>(node());
    auto vIdRet = SchemaUtil::toVertexID(uvNode->getVertex());
    if (!vIdRet.ok()) {
        return vIdRet.status();
    }
    auto vertexId = std::move(vIdRet).value();
    yieldPros_ = uvNode->getYieldProps();
    return ectx()->getStorageClient()->updateVertex(uvNode->space(),
                                                    vertexId,
                                                    uvNode->getUpdatedProps(),
                                                    uvNode->getInsertable(),
                                                    uvNode->getReturnProps(),
                                                    uvNode->getCondition())
        .via(runner())
        .then([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
            if (!resp.ok()) {
                return resp.status();
            }
            auto value = std::move(resp).value();
            if (value.__isset.props) {
                auto status = handleResult(*value.get_props());
                if (!status.ok()) {
                    return status.status();
                }
                finish(std::move(status).value());
            }
            return Status::OK();
        });
}

folly::Future<Status> UpdateEdgeExecutor::execute() {
    return updateEdge().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> UpdateEdgeExecutor::updateEdge() {
    dumpLog();
    auto *ueNode = asNode<UpdateEdge>(node());
    auto srcIdRet = SchemaUtil::toVertexID(ueNode->getSrcId());
    if (!srcIdRet.ok()) {
        return srcIdRet.status();
    }
    auto dstIdRet = SchemaUtil::toVertexID(ueNode->getDstId());
    if (!dstIdRet.ok()) {
        return dstIdRet.status();
    }
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(std::move(srcIdRet).value());
    edgeKey.set_ranking(ueNode->getRank());
    edgeKey.set_edge_type(ueNode->getEdgeType());
    edgeKey.set_dst(std::move(dstIdRet).value());
    yieldPros_ = ueNode->getYieldProps();
    return ectx()->getStorageClient()->updateEdge(ueNode->space(),
                                                  edgeKey,
                                                  ueNode->getUpdatedProps(),
                                                  ueNode->getInsertable(),
                                                  ueNode->getReturnProps(),
                                                  ueNode->getCondition())
            .via(runner())
            .then([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
                if (!resp.ok()) {
                    return resp.status();
                }
                auto value = std::move(resp).value();
                if (value.__isset.props) {
                    auto status = handleResult(*value.get_props());
                    if (!status.ok()) {
                        return status.status();
                    }
                    finish(std::move(status).value());
                }
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
